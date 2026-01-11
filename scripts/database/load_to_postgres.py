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

GENES_CSV = PROJECT_ROOT / "data" / "raw" / "genes" / "gene_metadata.csv"
VARIANTS_CSV = PROJECT_ROOT / "data" / "raw" / "variants" / "clinvar_pathogenic.csv"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DATABASE", "genome_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

def get_database_engine():
    """Create database engine"""
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(conn_str)
    logger.info("Database engine created successfully")
    return engine

def load_genes_to_bronze(engine):
    """Load gene data to bronze.genes_raw"""
    logger.info("Loading gene data to bronze.genes_raw...")
    
    if not GENES_CSV.exists():
        logger.error(f"File not found: {GENES_CSV}")
        return
    
    df = pd.read_csv(GENES_CSV)
    df.to_sql(
        name="genes_raw",
        schema="bronze",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )
    logger.info(f"Loaded {len(df)} genes to bronze")

def load_variants_to_bronze(engine):
    """Load variant data to bronze.variants_raw"""
    logger.info("Loading variant data to bronze.variants_raw...")
    
    if not VARIANTS_CSV.exists():
        logger.error(f"File not found: {VARIANTS_CSV}")
        return
    
    df = pd.read_csv(VARIANTS_CSV)
    df.to_sql(
        name="variants_raw",
        schema="bronze",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )
    logger.info(f"Loaded {len(df)} variants to bronze")

def copy_bronze_to_silver(engine):
    """Copy data from bronze to silver layer with transformations"""
    logger.info("Copying data from bronze to silver layer...")

    with engine.begin() as conn:

        # Copy genes
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

        # Copy variants
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
    """Verify data load"""
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
        print("\nSUCCESS: Data loaded to bronze and silver layers")
        return True
    else:
        print("\nWARNING: Some data may be missing")
        return False

def main():
    """Main entry point"""
    print("\n" + "="*70)
    print("LOAD DATA TO POSTGRESQL - BRONZE AND SILVER LAYERS")
    print("="*70)

    engine = get_database_engine()

    load_genes_to_bronze(engine)
    load_variants_to_bronze(engine)
    copy_bronze_to_silver(engine)
    verify_data_load(engine)

    print("\n" + "="*70)
    print("BRONZE AND SILVER LOADING COMPLETE")
    print("="*70)
    print("\nNext step:")
    print("  python scripts/transformation/load_gold_to_postgres.py")

if __name__ == "__main__":
    main()