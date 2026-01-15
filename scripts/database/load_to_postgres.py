# ====================================================================
# SMART INCREMENTAL LOAD WITH PROGRESS TRACKING
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 15, 2026 (Updated)
# ====================================================================

"""
Smart Incremental Load to PostgreSQL
- Small chunks (10K rows)
- Real-time progress tracking
- Only loads missing data
- Fast and efficient
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from dotenv import load_dotenv
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Data files
GENES_CSV = PROJECT_ROOT / "data" / "raw" / "genes" / "gene_metadata_all.csv"
VARIANTS_CSV = PROJECT_ROOT / "data" / "raw" / "variants" / "clinvar_all_variants.csv"
ALLELE_MAPPING_CSV = PROJECT_ROOT / "data" / "raw" / "variants" / "allele_id_mapping.csv"

# Chunk size for loading (smaller = more progress updates)
CHUNK_SIZE = 10000

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DATABASE", "genome_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def get_database_engine():
    """
    Create a database engine for connecting to PostgreSQL.

    Returns:
        engine (sqlalchemy.engine.Engine): A database engine object.
    """
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(conn_str)
    return engine


def table_exists(engine, schema, table_name):
    """Check if a table exists."""
    query = text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema 
            AND table_name = :table_name
        );
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema, "table_name": table_name})
        return result.scalar()


def get_table_count(engine, schema, table_name):
    """Get row count from a table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            return result.scalar()
    except:
        return 0


def get_existing_ids(engine, schema, table_name, id_column):
    """Get existing IDs from a table."""
    try:
        query = f"SELECT {id_column} FROM {schema}.{table_name}"
        df = pd.read_sql(query, engine)
        return set(df[id_column].astype(str))
    except:
        return set()


def print_progress(current, total, new_count, skipped_count, elapsed_time):
    """Print progress bar."""
    percent = (current / total * 100) if total > 0 else 0
    bar_length = 40
    filled = int(bar_length * current / total) if total > 0 else 0
    bar = '=' * filled + '-' * (bar_length - filled)
    
    rate = current / elapsed_time if elapsed_time > 0 else 0
    eta = (total - current) / rate if rate > 0 else 0
    
    print(f"\r  [{bar}] {percent:5.1f}% | "
          f"{current:,}/{total:,} rows | "
          f"New: {new_count:,} | "
          f"Skip: {skipped_count:,} | "
          f"ETA: {eta:.0f}s", 
          end='', flush=True)


def load_allele_mapping_incremental(engine):
    """Load allele_id_mapping incrementally."""
    print("\n" + "="*70)
    print("LOADING ALLELE ID MAPPING")
    print("="*70)
    
    if not ALLELE_MAPPING_CSV.exists():
        print(f"File not found: {ALLELE_MAPPING_CSV}")
        print("Run: python scripts/extraction/03_download_allele_mapping.py")
        return False
    
    # Check existing data
    exists = table_exists(engine, "bronze", "allele_id_mapping")
    
    if exists:
        existing_count = get_table_count(engine, "bronze", "allele_id_mapping")
        print(f"Table exists: {existing_count:,} rows")
        print("Loading only NEW mappings...")
        existing_ids = get_existing_ids(engine, "bronze", "allele_id_mapping", "variation_id")
    else:
        print("Table empty - loading ALL data")
        existing_ids = set()
        existing_count = 0
    
    # Count total rows in CSV
    print("Counting rows in CSV...")
    total_rows = sum(1 for _ in open(ALLELE_MAPPING_CSV)) - 1
    print(f"CSV has {total_rows:,} rows")
    
    # Load in chunks
    print(f"\nProcessing in chunks of {CHUNK_SIZE:,} rows...")
    chunks_processed = 0
    rows_processed = 0
    new_rows = 0
    skipped_rows = 0
    start_time = time.time()
    
    for chunk in pd.read_csv(ALLELE_MAPPING_CSV, chunksize=CHUNK_SIZE):
        chunks_processed += 1
        
        df_mapped = pd.DataFrame()
        df_mapped['variation_id'] = chunk['variation_id'].astype(str)
        df_mapped['allele_id'] = chunk['allele_id'].astype(str)
        
        # Filter existing
        if existing_ids:
            before = len(df_mapped)
            df_mapped = df_mapped[~df_mapped['variation_id'].isin(existing_ids)]
            skipped_rows += (before - len(df_mapped))
        
        # Load new rows
        if len(df_mapped) > 0:
            df_mapped.to_sql(
                name="allele_id_mapping",
                schema="bronze",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000
            )
            new_rows += len(df_mapped)
        
        rows_processed += len(chunk)
        elapsed = time.time() - start_time
        
        # Show progress every chunk
        print_progress(rows_processed, total_rows, new_rows, skipped_rows, elapsed)
    
    print()  # New line after progress
    
    final_count = get_table_count(engine, "bronze", "allele_id_mapping")
    elapsed_total = time.time() - start_time
    
    print(f"\nSummary:")
    print(f"  Time: {elapsed_total:.1f}s")
    print(f"  Existing: {existing_count:,}")
    print(f"  New: {new_rows:,}")
    print(f"  Skipped: {skipped_rows:,}")
    print(f"  Final total: {final_count:,}")
    
    return True


def load_genes_incremental(engine):
    """Load genes incrementally."""
    print("\n" + "="*70)
    print("LOADING GENES")
    print("="*70)
    
    if not GENES_CSV.exists():
        print(f"File not found: {GENES_CSV}")
        return
    
    # Check existing
    exists = table_exists(engine, "bronze", "genes_raw")
    
    if exists:
        existing_count = get_table_count(engine, "bronze", "genes_raw")
        print(f"Table exists: {existing_count:,} rows")
        print("Loading only NEW genes...")
        existing_ids = get_existing_ids(engine, "bronze", "genes_raw", "gene_id")
    else:
        print("Table empty - loading ALL data")
        existing_ids = set()
        existing_count = 0
    
    # Count total
    print("Counting rows in CSV...")
    total_rows = sum(1 for _ in open(GENES_CSV)) - 1
    print(f"CSV has {total_rows:,} rows")
    
    # Load in chunks
    print(f"\nProcessing in chunks of {CHUNK_SIZE:,} rows...")
    chunks_processed = 0
    rows_processed = 0
    new_rows = 0
    skipped_rows = 0
    start_time = time.time()
    
    for chunk in pd.read_csv(GENES_CSV, chunksize=CHUNK_SIZE):
        chunks_processed += 1
        
        df_mapped = pd.DataFrame()
        df_mapped['gene_id'] = chunk['gene_id'].astype(str)
        df_mapped['gene_name'] = chunk['gene_name']
        df_mapped['official_symbol'] = chunk['official_symbol']
        df_mapped['description'] = chunk['description']
        df_mapped['chromosome'] = chunk['chromosome']
        df_mapped['map_location'] = chunk['map_location']
        df_mapped['gene_type'] = chunk['gene_type']
        df_mapped['summary'] = chunk.get('full_name', chunk['description'])
        df_mapped['start_position'] = chunk['start_position']
        df_mapped['end_position'] = chunk['end_position']
        df_mapped['strand'] = chunk['strand']
        df_mapped['gene_length'] = chunk['gene_length']
        df_mapped['other_aliases'] = chunk['other_aliases']
        df_mapped['other_designations'] = chunk['other_designations']
        
        # Filter existing
        if existing_ids:
            before = len(df_mapped)
            df_mapped = df_mapped[~df_mapped['gene_id'].isin(existing_ids)]
            skipped_rows += (before - len(df_mapped))
        
        # Load new rows
        if len(df_mapped) > 0:
            df_mapped.to_sql(
                name="genes_raw",
                schema="bronze",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000
            )
            new_rows += len(df_mapped)
        
        rows_processed += len(chunk)
        elapsed = time.time() - start_time
        
        print_progress(rows_processed, total_rows, new_rows, skipped_rows, elapsed)
    
    print()
    
    final_count = get_table_count(engine, "bronze", "genes_raw")
    elapsed_total = time.time() - start_time
    
    print(f"\nSummary:")
    print(f"  Time: {elapsed_total:.1f}s")
    print(f"  Existing: {existing_count:,}")
    print(f"  New: {new_rows:,}")
    print(f"  Skipped: {skipped_rows:,}")
    print(f"  Final total: {final_count:,}")


def load_variants_incremental(engine):
    """Load variants incrementally."""
    print("\n" + "="*70)
    print("LOADING VARIANTS")
    print("="*70)
    
    if not VARIANTS_CSV.exists():
        print(f"File not found: {VARIANTS_CSV}")
        return
    
    # Check existing
    exists = table_exists(engine, "bronze", "variants_raw")
    
    if exists:
        existing_count = get_table_count(engine, "bronze", "variants_raw")
        print(f"Table exists: {existing_count:,} rows")
        print("Loading only NEW variants...")
        print("Reading existing IDs (this may take 30-60 seconds)...")
        existing_ids = get_existing_ids(engine, "bronze", "variants_raw", "variant_id")
        print(f"Found {len(existing_ids):,} existing IDs")
    else:
        print("Table empty - loading ALL data")
        existing_ids = set()
        existing_count = 0
    
    # Count total
    print("Counting rows in CSV (this may take 1-2 minutes)...")
    total_rows = sum(1 for _ in open(VARIANTS_CSV)) - 1
    print(f"CSV has {total_rows:,} rows")
    
    # Load in chunks
    print(f"\nProcessing in chunks of {CHUNK_SIZE:,} rows...")
    print("This will take 10-15 minutes for full load...")
    chunks_processed = 0
    rows_processed = 0
    new_rows = 0
    skipped_rows = 0
    start_time = time.time()
    
    for chunk in pd.read_csv(VARIANTS_CSV, chunksize=CHUNK_SIZE):
        chunks_processed += 1
        
        df_mapped = pd.DataFrame()
        df_mapped['variant_id'] = chunk['variant_id'].astype(str)
        df_mapped['accession'] = chunk['accession']
        df_mapped['gene_name'] = chunk['gene_name']
        df_mapped['clinical_significance'] = chunk['clinical_significance']
        df_mapped['disease'] = chunk['disease']
        df_mapped['chromosome'] = chunk['chromosome']
        df_mapped['position'] = chunk['position']
        df_mapped['stop_position'] = chunk['stop_position']
        df_mapped['variant_type'] = chunk['variant_type']
        df_mapped['molecular_consequence'] = None
        df_mapped['protein_change'] = None
        df_mapped['allele_id'] = chunk.get('allele_id', 'Unknown').astype(str)
        df_mapped['review_status'] = chunk['review_status']
        df_mapped['assembly'] = chunk['assembly']
        df_mapped['cytogenetic'] = chunk['cytogenetic']
        
        # Filter existing
        if existing_ids:
            before = len(df_mapped)
            df_mapped = df_mapped[~df_mapped['variant_id'].isin(existing_ids)]
            skipped_rows += (before - len(df_mapped))
        
        # Load new rows
        if len(df_mapped) > 0:
            df_mapped.to_sql(
                name="variants_raw",
                schema="bronze",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000
            )
            new_rows += len(df_mapped)
        
        rows_processed += len(chunk)
        elapsed = time.time() - start_time
        
        print_progress(rows_processed, total_rows, new_rows, skipped_rows, elapsed)
    
    print()
    
    final_count = get_table_count(engine, "bronze", "variants_raw")
    elapsed_total = time.time() - start_time
    
    print(f"\nSummary:")
    print(f"  Time: {elapsed_total/60:.1f} minutes")
    print(f"  Existing: {existing_count:,}")
    print(f"  New: {new_rows:,}")
    print(f"  Skipped: {skipped_rows:,}")
    print(f"  Final total: {final_count:,}")


def copy_bronze_to_silver_incremental(engine):
    """Copy new data to silver."""
    print("\n" + "="*70)
    print("UPDATING SILVER LAYER")
    print("="*70)

    with engine.begin() as conn:
        
        print("Copying NEW genes...")
        start = time.time()
        result = conn.execute(text("""
            INSERT INTO silver.genes_clean (
                gene_id, gene_name, official_symbol, description,
                chromosome, map_location, gene_type, summary,
                start_position, end_position, strand, gene_length
            )
            SELECT
                gene_id,
                UPPER(TRIM(gene_name)),
                UPPER(TRIM(official_symbol)),
                description,
                TRIM(chromosome),
                map_location,
                gene_type,
                summary,
                CASE 
                    WHEN start_position IS NOT NULL AND end_position IS NOT NULL
                    THEN LEAST(start_position, end_position)
                    ELSE start_position
                END,
                CASE 
                    WHEN start_position IS NOT NULL AND end_position IS NOT NULL
                    THEN GREATEST(start_position, end_position)
                    ELSE end_position
                END,
                strand,
                gene_length
            FROM bronze.genes_raw
            WHERE chromosome IN (
                '1','2','3','4','5','6','7','8','9','10',
                '11','12','13','14','15','16','17','18','19','20',
                '21','22','X','Y','MT'
            )
            AND gene_id IS NOT NULL
            AND gene_name IS NOT NULL
            ON CONFLICT (gene_id) DO NOTHING;
        """))
        genes_added = result.rowcount
        print(f"  Added {genes_added:,} genes ({time.time()-start:.1f}s)")

        print("Copying NEW variants...")
        start = time.time()
        result = conn.execute(text("""
            INSERT INTO silver.variants_clean (
                variant_id, accession, gene_name, clinical_significance,
                disease, chromosome, position, stop_position,
                variant_type, molecular_consequence, protein_change,
                review_status, assembly
            )
            SELECT
                COALESCE(variant_id, accession),
                COALESCE(UPPER(TRIM(accession)), variant_id),
                UPPER(TRIM(gene_name)),
                clinical_significance,
                disease,
                CASE
                    WHEN TRIM(chromosome) IN (
                        '1','2','3','4','5','6','7','8','9','10',
                        '11','12','13','14','15','16','17','18','19','20',
                        '21','22','X','Y','MT'
                    )
                    THEN TRIM(chromosome)
                    ELSE NULL
                END,
                CASE
                    WHEN position IS NOT NULL AND stop_position IS NOT NULL
                    THEN LEAST(position, stop_position)
                    ELSE position
                END,
                CASE
                    WHEN position IS NOT NULL AND stop_position IS NOT NULL
                    THEN GREATEST(position, stop_position)
                    ELSE stop_position
                END,
                variant_type,
                molecular_consequence,
                protein_change,
                review_status,
                COALESCE(assembly, 'GRCh38')
            FROM bronze.variants_raw
            WHERE gene_name IS NOT NULL
            AND accession IS NOT NULL
            ON CONFLICT (accession) DO NOTHING;
        """))
        variants_added = result.rowcount
        print(f"  Added {variants_added:,} variants ({time.time()-start:.1f}s)")

    return genes_added, variants_added


def verify_data_load(engine):
    """Verify final state."""
    print("\n" + "="*70)
    print("FINAL VERIFICATION")
    print("="*70)

    bronze_genes = get_table_count(engine, "bronze", "genes_raw")
    silver_genes = get_table_count(engine, "silver", "genes_clean")
    bronze_variants = get_table_count(engine, "bronze", "variants_raw")
    silver_variants = get_table_count(engine, "silver", "variants_clean")
    allele_mappings = get_table_count(engine, "bronze", "allele_id_mapping")

    print("\nBRONZE LAYER:")
    print(f"  genes_raw           : {bronze_genes:,}")
    print(f"  variants_raw        : {bronze_variants:,}")
    print(f"  allele_id_mapping   : {allele_mappings:,}")
    
    print("\nSILVER LAYER:")
    print(f"  genes_clean         : {silver_genes:,}")
    print(f"  variants_clean      : {silver_variants:,}")
    
    if silver_genes > 0 and silver_variants > 0:
        print("\nSTATUS: SUCCESS")
        if allele_mappings > 0:
            print("\nAllele ID mapping available for SQL joins")
    else:
        print("\nSTATUS: INCOMPLETE")
    
    return silver_genes > 0 and silver_variants > 0


def main():
    """
    Main function to perform smart incremental load of data from bronze to silver layer.
    
    Features:
        - Real-time progress bar
        - Only loads missing data
        - ETA calculation
        - Fast re-runs
    
    Chunk size: {CHUNK_SIZE:,} rows
    """
    print("\n" + "="*70)
    print("SMART INCREMENTAL LOAD - WITH PROGRESS TRACKING")
    print("="*70)
    print(f"Chunk size: {CHUNK_SIZE:,} rows")
    print("Features:")
    print("  - Real-time progress bar")
    print("  - Only loads missing data")
    print("  - ETA calculation")
    print("  - Fast re-runs")
    print("="*70)

    start_total = time.time()
    engine = get_database_engine()

    # Load each table
    load_allele_mapping_incremental(engine)
    load_genes_incremental(engine)
    load_variants_incremental(engine)
    
    # Update silver
    copy_bronze_to_silver_incremental(engine)
    
    # Verify
    verify_data_load(engine)

    total_time = time.time() - start_total
    print("\n" + "="*70)
    print(f"COMPLETE - Total time: {total_time/60:.1f} minutes")
    print("="*70)


if __name__ == "__main__":
    main()