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
Bronze → Silver pipeline (genes + variants)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# Database Engine
# -------------------------------------------------------------------
def get_database_engine():
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(conn_str)
    logger.info("✅ Database engine created successfully")
    return engine

# -------------------------------------------------------------------
# Bronze Loaders
# -------------------------------------------------------------------
def load_genes_to_bronze(engine):
    logger.info("Loading gene data to bronze.genes_raw...")
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

    logger.info(f"✅ Loaded {len(df)} genes")

def load_variants_to_bronze(engine):
    logger.info("Loading variant data to bronze.variants_raw...")
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

    logger.info(f"✅ Loaded {len(df)} variants")

# -------------------------------------------------------------------
# Bronze → Silver
# -------------------------------------------------------------------
def copy_bronze_to_silver(engine):
    logger.info("Copying data from bronze to silver layer...")

    with engine.begin() as conn:

        # -------------------- GENES --------------------
        logger.info("Copying genes...")

        conn.execute(text("""
            INSERT INTO silver.genes (
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
                gene_name,
                official_symbol,
                description,
                chromosome,
                map_location,
                gene_type,
                summary,
                LEAST(start_position, end_position),
                GREATEST(start_position, end_position),
                strand,
                gene_length
            FROM bronze.genes_raw
            WHERE chromosome IN (
                '1','2','3','4','5','6','7','8','9','10',
                '11','12','13','14','15','16','17','18','19','20',
                '21','22','X','Y','MT'
            )
            ON CONFLICT (gene_id) DO NOTHING;
        """))

        # -------------------- VARIANTS --------------------
        logger.info("Copying variants...")

        conn.execute(text("""
            INSERT INTO silver.variants (
                variant_id,
                accession,
                gene_name,
                gene_id,
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
                md5(
                    concat_ws(
                        '|',
                        v.accession,
                        v.gene_name,
                        v.chromosome,
                        v.position,
                        v.stop_position
                    )
                ) AS variant_id,

                v.accession,
                v.gene_name,
                g.gene_id,

                LEFT(v.clinical_significance, 100),
                LEFT(v.disease, 200),

                CASE
                    WHEN TRIM(v.chromosome) IN (
                        '1','2','3','4','5','6','7','8','9','10',
                        '11','12','13','14','15','16','17','18','19','20',
                        '21','22','X','Y','MT'
                    )
                    THEN TRIM(v.chromosome)
                    ELSE NULL
                END AS chromosome,

                CASE
                    WHEN v.position IS NOT NULL AND v.stop_position IS NOT NULL
                    THEN LEAST(v.position, v.stop_position)
                    ELSE v.position
                END AS position,

                CASE
                    WHEN v.position IS NOT NULL AND v.stop_position IS NOT NULL
                    THEN GREATEST(v.position, v.stop_position)
                    ELSE v.stop_position
                END AS stop_position,

                LEFT(v.variant_type, 100),
                LEFT(v.molecular_consequence, 200),
                LEFT(v.protein_change, 200),
                LEFT(v.review_status, 100),
                COALESCE(v.assembly, 'GRCh38')

            FROM bronze.variants_raw v
            LEFT JOIN silver.genes g
                ON v.gene_name = g.gene_name

            ON CONFLICT (variant_id) DO NOTHING;
        """))

    logger.info("✅ Data copied to silver layer successfully")

# -------------------------------------------------------------------
# Verification
# -------------------------------------------------------------------
def verify_data_load(engine):
    logger.info("Verifying data load...")

    with engine.connect() as conn:
        bronze_genes = pd.read_sql("SELECT COUNT(*) FROM bronze.genes_raw", conn).iloc[0, 0]
        silver_genes = pd.read_sql("SELECT COUNT(*) FROM silver.genes", conn).iloc[0, 0]
        bronze_variants = pd.read_sql("SELECT COUNT(*) FROM bronze.variants_raw", conn).iloc[0, 0]
        silver_variants = pd.read_sql("SELECT COUNT(*) FROM silver.variants", conn).iloc[0, 0]

    print("\n==============================")
    print("DATA LOAD VERIFICATION")
    print("==============================")
    print(f"Bronze genes     : {bronze_genes:,}")
    print(f"Silver genes     : {silver_genes:,}")
    print(f"Bronze variants  : {bronze_variants:,}")
    print(f"Silver variants  : {silver_variants:,}")
    print("==============================")

    return bronze_variants == silver_variants

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    print("\n==============================")
    print("LOAD DATA TO POSTGRESQL")
    print("==============================")

    engine = get_database_engine()

    load_genes_to_bronze(engine)
    load_variants_to_bronze(engine)
    copy_bronze_to_silver(engine)

    if verify_data_load(engine):
        print("\n✅ Data loading completed successfully!")
    else:
        print("\n❌ Data loss detected!")

if __name__ == "__main__":
    main()
