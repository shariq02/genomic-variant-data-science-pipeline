# ====================================================================
# DATABASE LOAD TO POSTGRESQL
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 31 December 2025
# ====================================================================
# FILE 4: scripts/database/load_to_postgres.py
# Purpose: Load CSV data to PostgreSQL
# ====================================================================

"""
Load Data to PostgreSQL
Bronze to Silver pipeline (genes + variants)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# UPDATED: Load ALL data (bigger files)
GENES_CSV = PROJECT_ROOT / "data" / "raw" / "genes" / "gene_metadata_all.csv"
VARIANTS_CSV = PROJECT_ROOT / "data" / "raw" / "variants" / "clinvar_all_variants.csv"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DATABASE", "genome_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

def get_database_engine():
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(conn_str)
    logger.info("Database engine created successfully")
    return engine

def load_genes_to_bronze(engine):
    logger.info("Loading ALL gene data to bronze.genes_raw...")
    logger.info(f"Reading from: {GENES_CSV}")
    
    if not GENES_CSV.exists():
        logger.error(f"File not found: {GENES_CSV}")
        return
    
    logger.info("Reading genes CSV in chunks...")
    chunk_size = 10000
    chunks_processed = 0
    total_rows = 0
    
    for chunk in pd.read_csv(GENES_CSV, chunksize=chunk_size):
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
        
        df_mapped.to_sql(
            name="genes_raw",
            schema="bronze",
            con=engine,
            if_exists="append" if chunks_processed > 1 else "replace",
            index=False,
            method="multi",
            chunksize=1000
        )
        
        total_rows += len(df_mapped)
        logger.info(f"  Chunk {chunks_processed}: Loaded {len(df_mapped)} genes (Total: {total_rows})")
    
    logger.info(f"Loaded {total_rows} total genes to bronze")

def load_variants_to_bronze(engine):
    logger.info("Loading ALL variant data to bronze.variants_raw...")
    logger.info(f"Reading from: {VARIANTS_CSV}")
    
    if not VARIANTS_CSV.exists():
        logger.error(f"File not found: {VARIANTS_CSV}")
        return
    
    logger.info("Reading variants CSV in chunks (this will take 10-15 minutes)...")
    chunk_size = 50000
    chunks_processed = 0
    total_rows = 0
    
    for chunk in pd.read_csv(VARIANTS_CSV, chunksize=chunk_size):
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
        df_mapped['allele_id'] = chunk['allele_id'].astype(str)
        df_mapped['review_status'] = chunk['review_status']
        df_mapped['assembly'] = chunk['assembly']
        df_mapped['cytogenetic'] = chunk['cytogenetic']
        
        df_mapped.to_sql(
            name="variants_raw",
            schema="bronze",
            con=engine,
            if_exists="append" if chunks_processed > 1 else "replace",
            index=False,
            method="multi",
            chunksize=1000
        )
        
        total_rows += len(df_mapped)
        logger.info(f"  Chunk {chunks_processed}: Loaded {len(df_mapped)} variants (Total: {total_rows})")
    
    logger.info(f"Loaded {total_rows} total variants to bronze")

def copy_bronze_to_silver(engine):
    logger.info("Copying data from bronze to silver layer...")
    logger.info("This may take 5-10 minutes for large datasets...")

    with engine.begin() as conn:

        logger.info("Copying genes to silver.genes_clean...")
        conn.execute(text("""
            INSERT INTO silver.genes_clean (
                gene_id,
                gene_name,
                official_symbol,
                description,
                chromosome,
                map_location,
                gene_type,
                summary,
                start_position,
                end_position,
                strand,
                gene_length
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

        logger.info("Copying variants to silver.variants_clean...")
        conn.execute(text("""
            INSERT INTO silver.variants_clean (
                variant_id,
                accession,
                gene_name,
                clinical_significance,
                disease,
                chromosome,
                position,
                stop_position,
                variant_type,
                molecular_consequence,
                protein_change,
                review_status,
                assembly
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

    logger.info("Data copied to silver layer successfully")

def verify_data_load(engine):
    logger.info("Verifying data load...")

    with engine.connect() as conn:
        bronze_genes = pd.read_sql("SELECT COUNT(*) FROM bronze.genes_raw", conn).iloc[0, 0]
        silver_genes = pd.read_sql("SELECT COUNT(*) FROM silver.genes_clean", conn).iloc[0, 0]
        bronze_variants = pd.read_sql("SELECT COUNT(*) FROM bronze.variants_raw", conn).iloc[0, 0]
        silver_variants = pd.read_sql("SELECT COUNT(*) FROM silver.variants_clean", conn).iloc[0, 0]

    print("\n" + "="*70)
    print("DATA LOAD VERIFICATION")
    print("="*70)
    print(f"Bronze genes     : {bronze_genes:,}")
    print(f"Silver genes     : {silver_genes:,}")
    print(f"Bronze variants  : {bronze_variants:,}")
    print(f"Silver variants  : {silver_variants:,}")
    print("="*70)
    
    if silver_genes > 0 and silver_variants > 0:
        print("\nSUCCESS: ALL data loaded to bronze and silver layers")
        print(f"  Genes: {silver_genes:,} (includes ALL gene types)")
        print(f"  Variants: {silver_variants:,} (includes ALL clinical significance)")
        return True
    else:
        print("\nWARNING: Some data may be missing")
        return False

def main():
    print("\n" + "="*70)
    print("LOAD ALL DATA TO POSTGRESQL - BRONZE AND SILVER LAYERS")
    print("="*70)
    print("Loading COMPLETE datasets:")
    print(f"  - ALL genes (190K rows - all gene types)")
    print(f"  - ALL variants (1M+ rows - all clinical significance)")
    print("This will take 15-20 minutes...")
    print("="*70 + "\n")

    engine = get_database_engine()

    load_genes_to_bronze(engine)
    load_variants_to_bronze(engine)
    copy_bronze_to_silver(engine)
    verify_data_load(engine)

    print("\n" + "="*70)
    print("BRONZE AND SILVER LOADING COMPLETE")
    print("="*70)
    print("Next: Upload to Databricks and run PySpark processing")

if __name__ == "__main__":
    main()
