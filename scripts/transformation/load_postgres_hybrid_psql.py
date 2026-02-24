"""
HYBRID POSTGRES LOADER - PSQL WITH SMART CHANGE DETECTION
Uses psql copy for speed + smart logic for change detection
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026

FIXES vs previous version
--------------------------
Root cause of all 13 failures: TABLES dict had hardcoded column schemas that
drifted from the actual CSV files. Three distinct errors resulted:

  TYPE 1 - "unterminated CSV quoted field"
    disease_ml_features, ml_dataset_disease_validation
    Cause : disease_name_enriched contains embedded double-quotes that confuse
            the psql CSV parser.
    Fix   : first attempt psql copy; on ANY quoting error fall back to
            psycopg2 copy_expert which streams bytes directly and handles
            embedded quotes correctly.

  TYPE 2 - "extra data after last expected column"
    ml_dataset_drug_response_*, ml_dataset_expression_*
    Cause : CSV had MORE columns than the hardcoded TABLES schema.
    Fix   : CREATE TABLE schema is now derived from the actual CSV header row
            at runtime - guaranteed to match.

  TYPE 3 - "missing data for column X"
    ml_dataset_pharmacogene_*, pharmacogene_ml_features, variant_impact_ml_features
    Cause : CSV had FEWER columns than the hardcoded TABLES schema.
    Fix   : same - dynamic schema from CSV header.

  All three error types are eliminated by removing the static TABLES dict and
  reading the column list from each CSV before creating its table.
"""

import os
import subprocess
import csv
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

CHUNK_SIZE = 100_000

load_dotenv()

PSQL_PATH         = os.getenv("PSQL_PATH")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT")
POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

PROJECT_ROOT        = Path(__file__).parent.parent.parent
PROCESSED_DIR       = PROJECT_ROOT / "data" / "processed"
POSTGRES_CHECKPOINT = PROCESSED_DIR / ".postgres_checkpoint.json"

# Tables that must never be loaded (temp/intermediate)
SKIP_TABLES = {
    "temp_df_impact",
}


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint():
    if POSTGRES_CHECKPOINT.exists():
        with open(POSTGRES_CHECKPOINT, 'r') as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoint):
    with open(POSTGRES_CHECKPOINT, 'w') as f:
        json.dump(checkpoint, f, indent=2)


# ---------------------------------------------------------------------------
# CSV introspection
# ---------------------------------------------------------------------------

def get_csv_info(csv_file):
    """Return (headers_list, data_row_count, size_mb).
    Reads the file once to get an exact row count."""
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        row_count = sum(1 for _ in reader)
    size_mb = csv_file.stat().st_size / (1024 * 1024)
    return headers, row_count, size_mb


def get_csv_hash(csv_file, sample_size=10_000):
    """Sample-based MD5 fingerprint for change detection."""
    h = hashlib.md5()
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            h.update(','.join(row).encode('utf-8'))
    return h.hexdigest()


def build_column_ddl(headers):
    """Build column list for CREATE TABLE.
    All columns TEXT - type conversion is handled by fix_postgres_types_fast.py.
    Each name is double-quoted to handle reserved words safely."""
    return ', '.join(f'"{col}" TEXT' for col in headers)


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def run_psql_command(sql):
    env = os.environ.copy()
    env['PGPASSWORD'] = POSTGRES_PASSWORD
    cmd = [
        PSQL_PATH,
        '-h', POSTGRES_HOST, '-p', POSTGRES_PORT,
        '-U', POSTGRES_USER, '-d', POSTGRES_DB,
        '-c', sql, '-t',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise Exception(f"psql error: {result.stderr}")
    return result.stdout


def run_psql_file(sql_file):
    env = os.environ.copy()
    env['PGPASSWORD'] = POSTGRES_PASSWORD
    cmd = [
        PSQL_PATH,
        '-h', POSTGRES_HOST, '-p', POSTGRES_PORT,
        '-U', POSTGRES_USER, '-d', POSTGRES_DB,
        '-f', str(sql_file),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise Exception(f"psql error: {result.stderr} | stdout: {result.stdout}")
    if result.stderr and "ERROR" in result.stderr.upper():
        raise Exception(f"psql ERROR in copy: {result.stderr}")


def table_exists(table_name):
    try:
        run_psql_command(f'SELECT COUNT(*) FROM gold."{table_name}"')
        return True
    except Exception:
        return False


def get_table_count(table_name):
    try:
        result = run_psql_command(f'SELECT COUNT(*) FROM gold."{table_name}"')
        return int(result.strip())
    except Exception:
        return 0


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


# ---------------------------------------------------------------------------
# Three-tier load strategy
# ---------------------------------------------------------------------------

def load_via_psql_copy(csv_file, table_name):
    """Tier 1 (fastest): psql \\copy.  Works for ~90% of tables."""
    temp_sql = PROCESSED_DIR / "temp_load.sql"
    csv_path = str(csv_file).replace("\\", "/")   # psql on Windows accepts /
    with open(temp_sql, "w") as f:
        f.write(
            f'\\copy gold."{table_name}" '
            f"FROM '{csv_path}' "
            f"WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"
        )
    try:
        run_psql_file(temp_sql)
    finally:
        if temp_sql.exists():
            temp_sql.unlink()


def load_via_psycopg2_copy(csv_file, table_name):
    """Tier 2: psycopg2 copy_expert (COPY FROM STDIN).

    Handles TYPE 1 embedded-quote tables that trip up psql:
    - Streams raw bytes directly to the Postgres server
    - Server's COPY parser correctly handles RFC-4180 nested quotes like
      'See ""Cases""' inside a quoted field
    Slower than psql but still bulk-load fast (no row-by-row Python overhead).
    """
    print(f"  Falling back to psycopg2 copy_expert (handles embedded quotes)...")
    conn = get_pg_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    copy_sql = (
        f'COPY gold."{table_name}" '
        f"FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"
    )
    t0 = time.time()
    with open(csv_file, 'r', encoding='utf-8') as f:
        cursor.copy_expert(copy_sql, f)
    print(f"  psycopg2 copy completed in {time.time() - t0:.1f}s")
    cursor.close()
    conn.close()


def load_via_psycopg2_chunked(csv_file, table_name, headers):
    """Tier 3 (last resort): executemany in CHUNK_SIZE batches.
    Only used if both COPY approaches fail (e.g. psql 1GB buffer overflow)."""
    print(f"  Last resort: chunked executemany ({CHUNK_SIZE:,} rows/chunk)...")
    conn = get_pg_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    cols         = ', '.join(f'"{h}"' for h in headers)
    placeholders = ', '.join(['%s'] * len(headers))
    insert_sql   = f'INSERT INTO gold."{table_name}" ({cols}) VALUES ({placeholders})'

    total   = 0
    chunk_n = 0
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        batch = []
        t0    = time.time()
        for row in reader:
            batch.append(row)
            if len(batch) >= CHUNK_SIZE:
                chunk_n += 1
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                print(f"    Chunk {chunk_n}: {total:,} rows ({time.time()-t0:.1f}s)")
                batch = []
                t0    = time.time()
        if batch:
            chunk_n += 1
            cursor.executemany(insert_sql, batch)
            conn.commit()
            total += len(batch)

    cursor.close()
    conn.close()
    return total


# ---------------------------------------------------------------------------
# Main per-table loader
# ---------------------------------------------------------------------------

def load_table(table_name, checkpoint):
    csv_file = PROCESSED_DIR / f"{table_name}.csv"

    print(f"\n{table_name}:")
    t_start = datetime.now()
    print(f"  Start: {t_start.strftime('%Y-%m-%d %H:%M:%S')}")

    if not csv_file.exists():
        print(f"  SKIP: CSV not found (not yet downloaded)")
        return None

    # --- Read actual schema from CSV (TYPE 2 + TYPE 3 fix) ---
    csv_headers, csv_rows, csv_size_mb = get_csv_info(csv_file)
    csv_hash = get_csv_hash(csv_file)

    print(f"  CSV: {csv_rows:,} rows, {len(csv_headers)} cols, {csv_size_mb:.1f} MB")

    # --- Change detection ---
    prev = checkpoint.get(table_name, {})
    if table_exists(table_name):
        table_rows = get_table_count(table_name)
        print(f"  Existing: {table_rows:,} rows")

        if table_rows == csv_rows and csv_hash == prev.get('hash'):
            t_end = datetime.now()
            print(f"  End: {t_end.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Duration: {(t_end - t_start).total_seconds():.1f}s")
            print(f"  Status: UNCHANGED")
            return True

        if table_rows < csv_rows:
            print(f"  Incomplete: {table_rows / csv_rows * 100:.1f}% loaded")

        print(f"  Dropping and rebuilding...")
        run_psql_command(f'DROP TABLE IF EXISTS gold."{table_name}"')

    # --- Create table with CSV-derived schema ---
    col_ddl = build_column_ddl(csv_headers)
    print(f"  Creating table ({len(csv_headers)} cols from CSV header)...")
    run_psql_command(f'CREATE TABLE gold."{table_name}" ({col_ddl})')

    # --- Three-tier load ---
    load_start = time.time()
    loaded_ok  = False

    # Tier 1: psql copy (fast, fails on embedded-quote tables)
    print(f"  Loading {csv_size_mb:.1f} MB via psql copy...")
    try:
        load_via_psql_copy(csv_file, table_name)
        print(f"  Loaded in {time.time() - load_start:.1f}s")
        loaded_ok = True

    except Exception as e:
        err = str(e)
        print(f"  psql copy failed: {err[:200]}")
        is_buffer_error = "string buffer exceeds" in err or "1073741823" in err

        # Tier 2: psycopg2 copy_expert (handles embedded quotes)
        if not loaded_ok:
            try:
                run_psql_command(f'DROP TABLE IF EXISTS gold."{table_name}"')
                run_psql_command(f'CREATE TABLE gold."{table_name}" ({col_ddl})')
                load_via_psycopg2_copy(csv_file, table_name)
                print(f"  Total load time: {time.time() - load_start:.1f}s")
                loaded_ok = True
            except Exception as e2:
                print(f"  psycopg2 copy_expert failed: {str(e2)[:200]}")

        # Tier 3: chunked executemany (last resort)
        if not loaded_ok:
            try:
                run_psql_command(f'DROP TABLE IF EXISTS gold."{table_name}"')
                run_psql_command(f'CREATE TABLE gold."{table_name}" ({col_ddl})')
                load_via_psycopg2_chunked(csv_file, table_name, csv_headers)
                print(f"  Total load time: {time.time() - load_start:.1f}s")
                loaded_ok = True
            except Exception as e3:
                print(f"  Chunked fallback failed: {str(e3)[:200]}")

    if not loaded_ok:
        print(f"  Status: FAILED (all load methods exhausted)")
        return False

    # --- Verify ---
    final_count = get_table_count(table_name)
    t_end       = datetime.now()
    duration    = (t_end - t_start).total_seconds()

    print(f"  Loaded: {final_count:,} rows")
    print(f"  End: {t_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Duration: {duration:.1f}s ({duration / 60:.1f} min)")

    if final_count != csv_rows:
        print(f"  WARNING: Row count mismatch! CSV={csv_rows:,}  PG={final_count:,}")
        return False

    checkpoint[table_name] = {
        "rows":    csv_rows,
        "columns": len(csv_headers),
        "hash":    csv_hash,
    }
    save_checkpoint(checkpoint)
    print(f"  Status: OK")
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("=" * 80)
    print("HYBRID POSTGRES LOADER - PSQL WITH SMART CHANGE DETECTION")
    print("=" * 80)
    print(f"Database : {POSTGRES_DB}.gold")
    print(f"Method   : schema auto-detected from CSV header")
    print(f"Load path: psql copy  ->  psycopg2 copy_expert  ->  chunked insert")
    print("=" * 80)

    if not POSTGRES_PASSWORD:
        print("\nERROR: PostgreSQL password not found in .env")
        return

    run_psql_command("CREATE SCHEMA IF NOT EXISTS gold")
    print("\nGold schema: OK")

    checkpoint = load_checkpoint()
    print(f"Previously loaded: {len(checkpoint)} tables")

    # Auto-discover all CSVs - no static TABLES dict needed
    all_csv    = sorted(PROCESSED_DIR.glob("*.csv"))
    table_names = [
        f.stem for f in all_csv
        if f.stem not in SKIP_TABLES
        and not f.stem.startswith('.')
    ]

    print(f"CSV files found   : {len(table_names)}")
    print(f"Tables to skip    : {SKIP_TABLES}")
    print("=" * 80)

    overall_start = datetime.now()
    results       = {}

    for table_name in table_names:
        success = load_table(table_name, checkpoint)
        if success is not None:
            results[table_name] = success

    total_duration = (datetime.now() - overall_start).total_seconds()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total time : {total_duration:.1f}s ({total_duration / 60:.1f} min)")

    successful = [t for t, s in results.items() if     s]
    failed     = [t for t, s in results.items() if not s]

    print(f"\nSuccessful : {len(successful)}/{len(results)}")
    for t in successful:
        print(f"  - {t}")

    if failed:
        print(f"\nFailed : {len(failed)}")
        for t in failed:
            print(f"  - {t}")

    print("\n" + "=" * 80)
    if failed:
        print("NEXT: Fix errors above and re-run")
    else:
        print("NEXT: Run fix_postgres_types_fast.py")
    print("=" * 80)


if __name__ == "__main__":
    main()
