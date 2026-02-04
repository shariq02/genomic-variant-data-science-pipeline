# ====================================================================
# Load Gold Layer to PostgreSQL — RESUMABLE + INCREMENTAL
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 2026
# ====================================================================
# Features:
#   1. Chunked loading with checkpoint files — if the script crashes
#      mid-table it resumes from the last committed chunk on next run.
#   2. Post-load verification — row count in PostgreSQL is compared
#      to the CSV; mismatch leaves checkpoint as "failed" so next run
#      retries that table.
#   3. Incremental append — if a table already exists and its
#      checkpoint is "complete", the script compares CSV vs PG row
#      counts.  If the CSV is larger (new rows added upstream), only
#      rows whose primary key does NOT already exist in PG are inserted.
#      Nothing is re-loaded unnecessarily.
#
# Primary keys used for deduplication:
#   clinical_ml_features            -> variant_id
#   disease_ml_features             -> variant_id
#   pharmacogene_ml_features        -> variant_id
#   structural_variant_ml_features  -> sv_id
#   variant_impact_ml_features      -> variant_id
# ====================================================================

import pandas as pd
import json
import time
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent.parent
EXPORTS_DIR  = PROJECT_ROOT / "data" / "processed"

print(f"Project Root: {PROJECT_ROOT}")
print(f"Looking for CSVs in: {EXPORTS_DIR}")

DB_CONFIG = {
    'host':     os.getenv('POSTGRES_HOST', 'localhost'),
    'port':     int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'genome_db'),
    'user':     os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# ====================================================================
# TABLE DEFINITIONS
# ====================================================================
# (csv_name, pg_schema, pg_table, primary_key_column)
TABLES = [
    ("clinical_ml_features",            "gold", "clinical_ml_features",            "variant_id"),
    ("disease_ml_features",             "gold", "disease_ml_features",             "variant_id"),
    ("pharmacogene_ml_features",        "gold", "pharmacogene_ml_features",        "variant_id"),
    ("structural_variant_ml_features",  "gold", "structural_variant_ml_features",  "sv_id"),
    ("variant_impact_ml_features",      "gold", "variant_impact_ml_features",      "variant_id"),
]

CHUNK_SIZE = 50000  # rows per INSERT batch

# ====================================================================
# BOOLEAN COLUMNS — every BOOLEAN in gold_table_schema.csv
# ====================================================================
BOOLEAN_COLUMNS = [
    # --- clinical_ml_features (35) ---
    'gene_is_validated', 'gene_has_omim', 'gene_has_ensembl',
    'gene_is_well_characterized', 'target_is_pathogenic', 'target_is_benign',
    'target_is_vus', 'clinical_sig_is_uncertain', 'has_strong_evidence',
    'is_coding_variant', 'is_regulatory_variant', 'is_missense_variant',
    'is_frameshift_variant', 'is_nonsense_variant', 'is_splice_variant',
    'is_highly_conserved', 'is_constrained', 'is_likely_deleterious',
    'is_high_impact', 'is_very_high_impact', 'is_domain_affecting',
    'is_loss_of_function', 'is_deleterious_by_cadd', 'has_functional_domain',
    'has_conservation_data', 'has_complete_annotation', 'is_mitochondrial_variant',
    'is_y_linked_variant', 'is_x_linked_variant', 'is_autosomal_variant',
    'gene_is_pathogenic_enriched', 'gene_is_benign_enriched',
    'gene_is_vus_enriched', 'gene_has_high_lof_burden', 'gene_has_quality_annotations',

    # --- disease_ml_features (37) ---
    'is_pathogenic', 'is_benign', 'is_vus',
    'has_omim_disease', 'has_mondo_disease', 'has_orphanet_disease',
    'disease_is_well_annotated', 'disease_name_is_generic',
    'is_disease_associated', 'is_multi_disease_gene', 'is_omim_gene',
    'is_polygenic_disease', 'disease_has_high_pathogenic_burden',
    'is_clinically_actionable', 'is_research_candidate',
    'is_pharmacogene', 'is_pharmacogene_priority', 'has_excellent_annotation',
    'is_cancer_gene_variant', 'is_neurological_gene_variant',
    'is_cardiovascular_gene_variant', 'is_metabolic_gene_variant',
    'is_rare_disease_gene_variant', 'has_drug_development_potential',
    'is_highly_actionable', 'has_cancer_keyword', 'has_syndrome_keyword',
    'has_neurological_keyword', 'has_cardiovascular_keyword',
    'has_metabolic_keyword', 'has_immune_keyword', 'has_rare_disease_keyword',
    'is_disease_related_gene', 'has_drug_target_in_desc',
    'has_biomarker_in_desc', 'has_essential_in_desc', 'is_well_characterized',

    # --- pharmacogene_ml_features (29) ---
    'gene_description_mentions_drug', 'has_complete_pharmacogene_annotation',
    'is_drug_target', 'is_metabolizing_enzyme', 'is_cyp_gene',
    'is_phase2_metabolism', 'is_drug_transporter', 'is_hla_gene',
    'is_hla_variant', 'is_kinase', 'is_phosphatase', 'is_receptor',
    'is_enzyme', 'is_gpcr', 'is_transporter',
    'has_drug_interaction_potential', 'is_metabolizer_variant',
    'is_transporter_variant', 'is_kinase_inhibitor_target',
    'has_pharmgkb_annotation', 'gene_has_multiple_drug_variants',
    'is_clinical_pharmacogene',

    # --- structural_variant_ml_features (6) ---
    'has_gene_overlap', 'is_multi_gene_sv', 'affects_pharmacogenes',
    'affects_omim_genes', 'is_high_risk_sv', 'is_autosomal',

    # --- variant_impact_ml_features (35) ---
    'is_snv', 'alters_protein_length', 'has_multiple_domain_types',
    'has_zinc_finger', 'has_kinase_domain', 'has_receptor_domain',
    'has_sh2_domain', 'has_sh3_domain', 'has_ph_domain',
    'affects_functional_domain', 'is_missense_in_conserved_domain',
    'is_conservation_constrained', 'has_protein_annotation',
    'has_conservation_scores', 'is_critical_splice_variant',
    'splice_site_is_well_defined', 'gene_has_high_impact_burden',
    'gene_is_well_annotated',
]


# ====================================================================
# ENGINE
# ====================================================================
def get_engine():
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


# ====================================================================
# CHECKPOINT HELPERS
# ====================================================================
def checkpoint_path(table_name):
    """Path to the JSON checkpoint file for a given table."""
    return EXPORTS_DIR / f".checkpoint_{table_name}.json"


def load_checkpoint(table_name):
    """Load checkpoint for a table. Returns dict or None."""
    cp = checkpoint_path(table_name)
    if cp.exists():
        try:
            return json.loads(cp.read_text())
        except Exception:
            return None
    return None


def save_checkpoint(table_name, data):
    """Write checkpoint dict to disk."""
    checkpoint_path(table_name).write_text(json.dumps(data, indent=2))


# ====================================================================
# BOOLEAN CONVERSION
# ====================================================================
def convert_booleans(df):
    bool_map = {
        'true': True,  'True': True,  'TRUE': True,  True: True,
        'false': False, 'False': False, 'FALSE': False, False: False,
        1: True, 0: False, '1': True, '0': False
    }
    converted = 0
    for col in BOOLEAN_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(bool_map)
            converted += 1
    return df, converted


# ====================================================================
# POSTGRES HELPERS
# ====================================================================
def table_exists(engine, schema, table_name):
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables "
            "  WHERE table_schema = :s AND table_name = :t"
            ")"), {"s": schema, "t": table_name})
        return result.scalar()


def get_pg_row_count(engine, schema, table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
        return result.scalar()


def create_table_from_df(engine, schema, table_name, df):
    """Create an empty table matching the DataFrame schema."""
    df.iloc[:0].to_sql(
        name=table_name, schema=schema, con=engine,
        if_exists='replace', index=False
    )


# ====================================================================
# CORE LOADING LOGIC
# ====================================================================
def load_full(engine, schema, table_name, pk_col, df, checkpoint):
    """Full load with chunked resume. Uses checkpoint to skip already-committed chunks."""
    start_offset = checkpoint.get("rows_committed", 0) if checkpoint else 0
    total_rows   = len(df)

    if start_offset > 0:
        logger.info(f"  Resuming from row {start_offset:,} (previous run committed this far)")
    else:
        # Fresh load — create the table first so chunks can append
        logger.info(f"  Creating table {schema}.{table_name}")
        create_table_from_df(engine, schema, table_name, df)

    rows_committed = start_offset
    start_time = time.time()

    for i in range(start_offset, total_rows, CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]
        chunk.to_sql(
            name=table_name, schema=schema, con=engine,
            if_exists='append', index=False, method='multi'
        )
        rows_committed = min(i + CHUNK_SIZE, total_rows)

        # Save checkpoint after every chunk
        save_checkpoint(table_name, {
            "status": "in_progress",
            "rows_committed": rows_committed,
            "total_rows": total_rows,
            "pk_col": pk_col
        })

        # Progress log every 200k rows
        if rows_committed % 200000 < CHUNK_SIZE:
            elapsed = time.time() - start_time
            rate = (rows_committed - start_offset) / elapsed if elapsed > 0 else 0
            logger.info(f"    {rows_committed:,}/{total_rows:,} rows ({rate:,.0f} rows/s)")

    return rows_committed


def load_incremental(engine, schema, table_name, pk_col, df):
    """Append only rows whose PK does not already exist in PG.
    Uses a temporary table + anti-join INSERT for efficiency."""
    total_rows = len(df)
    logger.info(f"  Incremental mode: CSV has {total_rows:,} rows")

    pg_count = get_pg_row_count(engine, schema, table_name)
    logger.info(f"  PostgreSQL currently has {pg_count:,} rows")

    if total_rows <= pg_count:
        logger.info(f"  CSV row count <= PG row count — nothing new to insert")
        return 0

    # Write full CSV into a temp table
    temp_table = f"_tmp_{table_name}"
    logger.info(f"  Writing CSV to temp table {schema}.{temp_table} for anti-join...")

    # Drop temp if it exists from a previous failed run
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE"))
        conn.commit()

    # Chunked insert into temp table
    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]
        chunk.to_sql(
            name=temp_table, schema=schema, con=engine,
            if_exists='replace' if i == 0 else 'append',
            index=False, method='multi'
        )
        if (i + CHUNK_SIZE) % 200000 < CHUNK_SIZE:
            logger.info(f"    Temp table: {min(i + CHUNK_SIZE, total_rows):,}/{total_rows:,}")

    # Anti-join INSERT: insert rows from temp that don't exist in main table
    logger.info(f"  Running anti-join INSERT (new PKs only)...")
    cols = ", ".join(df.columns)
    insert_sql = f"""
        INSERT INTO {schema}.{table_name} ({cols})
        SELECT {cols} FROM {schema}.{temp_table}
        WHERE {pk_col} NOT IN (
            SELECT {pk_col} FROM {schema}.{table_name}
        )
    """
    with engine.connect() as conn:
        result = conn.execute(text(insert_sql))
        conn.commit()
        new_rows = result.rowcount

    # Drop temp table
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE"))
        conn.commit()

    logger.info(f"  Incremental insert complete: {new_rows:,} new rows added")
    return new_rows


# ====================================================================
# MAIN
# ====================================================================
def load_gold_tables():
    print("\n" + "="*70)
    print("LOADING GOLD LAYER TO POSTGRESQL — RESUMABLE + INCREMENTAL")
    print("="*70)
    print(f"\n  Chunk size:  {CHUNK_SIZE:,} rows")
    print(f"  CSV dir:     {EXPORTS_DIR}")
    print("="*70)

    if not EXPORTS_DIR.exists():
        logger.error(f"Directory does not exist: {EXPORTS_DIR}")
        return

    engine = get_engine()
    results = {}  # table -> {status, rows, message}

    for csv_name, schema, table_name, pk_col in TABLES:
        csv_path = EXPORTS_DIR / f"{csv_name}.csv"
        results[csv_name] = {"status": "skipped", "rows": 0, "message": ""}

        if not csv_path.exists():
            results[csv_name]["message"] = "CSV not found"
            logger.warning(f"  {csv_name}: CSV not found, skipping")
            continue

        print(f"\n{'─'*70}")
        print(f"  {csv_name}")
        print(f"{'─'*70}")

        # --- Read CSV ---
        logger.info(f"  Reading {csv_path.name} ({csv_path.stat().st_size/(1024*1024):.1f} MB)...")
        df = pd.read_csv(csv_path)
        csv_row_count = len(df)
        logger.info(f"  CSV: {csv_row_count:,} rows, {len(df.columns)} columns")

        # Convert booleans
        df, n_bool = convert_booleans(df)
        if n_bool:
            logger.info(f"  Converted {n_bool} boolean columns")

        # --- Decide: full load, resume, or incremental ---
        cp = load_checkpoint(csv_name)
        pg_exists = table_exists(engine, schema, table_name)

        try:
            if cp and cp.get("status") == "complete" and pg_exists:
                # Table was fully loaded before — check for new rows
                pg_count = get_pg_row_count(engine, schema, table_name)
                if csv_row_count > pg_count:
                    logger.info(f"  Checkpoint=complete, CSV ({csv_row_count:,}) > PG ({pg_count:,}) — incremental append")
                    new_rows = load_incremental(engine, schema, table_name, pk_col, df)
                    results[csv_name] = {"status": "incremental", "rows": new_rows,
                                         "message": f"+{new_rows:,} new rows appended"}
                else:
                    logger.info(f"  Checkpoint=complete, CSV ({csv_row_count:,}) <= PG ({pg_count:,}) — nothing to do")
                    results[csv_name] = {"status": "up_to_date", "rows": 0,
                                         "message": f"Already loaded ({pg_count:,} rows)"}
                    continue

            elif cp and cp.get("status") == "in_progress" and pg_exists:
                # Previous run crashed mid-load — resume from checkpoint
                logger.info(f"  Checkpoint=in_progress at {cp['rows_committed']:,} — resuming")
                rows_done = load_full(engine, schema, table_name, pk_col, df, cp)
                results[csv_name] = {"status": "resumed", "rows": rows_done, "message": "Resumed and completed"}

            else:
                # No valid checkpoint or table doesn't exist — full load
                if pg_exists:
                    logger.info(f"  Dropping existing table for fresh load")
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE"))
                        conn.commit()

                logger.info(f"  Full load: {csv_row_count:,} rows")
                rows_done = load_full(engine, schema, table_name, pk_col, df, None)
                results[csv_name] = {"status": "full_load", "rows": rows_done, "message": "Full load completed"}

            # --- Post-load verification ---
            logger.info(f"  Verifying row count...")
            pg_count_after = get_pg_row_count(engine, schema, table_name)
            logger.info(f"  PG row count: {pg_count_after:,}")

            if results[csv_name]["status"] in ("full_load", "resumed"):
                # For full/resumed loads, PG count must match CSV count exactly
                if pg_count_after == csv_row_count:
                    logger.info(f"  VERIFIED: {pg_count_after:,} rows match CSV")
                    save_checkpoint(csv_name, {
                        "status": "complete",
                        "rows_committed": pg_count_after,
                        "total_rows": csv_row_count,
                        "pk_col": pk_col
                    })
                    results[csv_name]["status"] = results[csv_name]["status"] + " + verified"
                else:
                    logger.error(f"  MISMATCH: PG={pg_count_after:,} vs CSV={csv_row_count:,}")
                    save_checkpoint(csv_name, {
                        "status": "failed_verification",
                        "rows_committed": pg_count_after,
                        "total_rows": csv_row_count,
                        "pk_col": pk_col
                    })
                    results[csv_name]["status"] = "FAILED"
                    results[csv_name]["message"] = f"Verification failed: PG={pg_count_after:,} CSV={csv_row_count:,}"

            elif results[csv_name]["status"] == "incremental":
                # For incremental, just confirm PG grew
                logger.info(f"  Post-incremental PG count: {pg_count_after:,}")
                save_checkpoint(csv_name, {
                    "status": "complete",
                    "rows_committed": pg_count_after,
                    "total_rows": pg_count_after,
                    "pk_col": pk_col
                })

        except Exception as e:
            logger.error(f"  ERROR: {e}")
            results[csv_name] = {"status": "FAILED", "rows": 0, "message": str(e)}
            # Checkpoint stays at in_progress so next run resumes
            continue

    # ================================================================
    # SUMMARY TABLE
    # ================================================================
    print("\n" + "="*70)
    print("LOAD SUMMARY")
    print("="*70)
    print(f"\n  {'Table':<42} {'Status':<28} Details")
    print("  " + "-"*90)
    for csv_name, r in results.items():
        print(f"  {csv_name:<42} {r['status']:<28} {r['message']}")

    print("\n" + "="*70)
    print("Verify in PostgreSQL:")
    print("  SELECT table_name, row_count FROM (")
    for i, (csv_name, schema, table_name, _) in enumerate(TABLES):
        union = "UNION ALL" if i < len(TABLES) - 1 else ""
        print(f"    SELECT '{table_name}' as table_name, COUNT(*) as row_count FROM {schema}.{table_name} {union}")
    print("  ) t ORDER BY table_name;")
    print("="*70)


if __name__ == "__main__":
    load_gold_tables()
