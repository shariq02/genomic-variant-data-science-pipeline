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
Loads gene and variant CSV files to database.
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Data paths
GENES_CSV = PROJECT_ROOT / "data" / "raw" / "genes" / "gene_metadata.csv"
VARIANTS_CSV = PROJECT_ROOT / "data" / "raw" / "variants" / "clinvar_pathogenic.csv"

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'genome_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD')
}


def get_database_engine():
    """Create SQLAlchemy database engine."""
    try:
        connection_string = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(connection_string)
        logger.info("✅ Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"❌ Error creating database engine: {e}")
        raise


def load_genes_to_bronze(engine):
    """Load gene data to bronze.genes_raw table."""
    try:
        logger.info("Loading gene data to bronze.genes_raw...")
        
        # Read CSV
        df_genes = pd.read_csv(GENES_CSV)
        logger.info(f"Read {len(df_genes)} genes from CSV")
        
        # Load to database
        df_genes.to_sql(
            name='genes_raw',
            schema='bronze',
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"✅ Loaded {len(df_genes)} genes to bronze.genes_raw")
        return len(df_genes)
        
    except Exception as e:
        logger.error(f"❌ Error loading genes: {e}")
        raise


def load_variants_to_bronze(engine):
    """Load variant data to bronze.variants_raw table."""
    try:
        logger.info("Loading variant data to bronze.variants_raw...")
        
        # Read CSV
        df_variants = pd.read_csv(VARIANTS_CSV)
        logger.info(f"Read {len(df_variants)} variants from CSV")
        
        # Load to database
        df_variants.to_sql(
            name='variants_raw',
            schema='bronze',
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"✅ Loaded {len(df_variants)} variants to bronze.variants_raw")
        return len(df_variants)
        
    except Exception as e:
        logger.error(f"❌ Error loading variants: {e}")
        raise


def copy_bronze_to_silver(engine):
    """Copy and clean data from bronze to silver layer."""
    try:
        logger.info("Copying data from bronze to silver layer...")
        
        with engine.connect() as conn:
            # Copy genes
            logger.info("Copying genes...")
            conn.execute("""
                INSERT INTO silver.genes (
                    gene_id, gene_name, official_symbol, description,
                    chromosome, map_location, gene_type, summary,
                    start_position, end_position, strand, gene_length
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
                    start_position,
                    end_position,
                    strand,
                    gene_length
                FROM bronze.genes_raw
                WHERE chromosome IN ('1','2','3','4','5','6','7','8','9','10',
                                    '11','12','13','14','15','16','17','18','19','20',
                                    '21','22','X','Y','MT')
                ON CONFLICT (gene_id) DO NOTHING;
            """)
            
            # Copy variants
            logger.info("Copying variants...")
            conn.execute("""
                INSERT INTO silver.variants (
                    variant_id, accession, gene_name, gene_id,
                    clinical_significance, disease, chromosome,
                    position, stop_position, variant_type,
                    molecular_consequence, protein_change, review_status, assembly
                )
                SELECT 
                    v.variant_id,
                    v.accession,
                    v.gene_name,
                    g.gene_id,
                    v.clinical_significance,
                    v.disease,
                    v.chromosome,
                    v.position,
                    v.stop_position,
                    v.variant_type,
                    v.molecular_consequence,
                    v.protein_change,
                    v.review_status,
                    v.assembly
                FROM bronze.variants_raw v
                LEFT JOIN silver.genes g ON v.gene_name = g.gene_name
                WHERE v.chromosome IN ('1','2','3','4','5','6','7','8','9','10',
                                      '11','12','13','14','15','16','17','18','19','20',
                                      '21','22','X','Y','MT')
                ON CONFLICT (variant_id) DO NOTHING;
            """)
            
            conn.commit()
            
        logger.info("✅ Data copied to silver layer successfully")
        
    except Exception as e:
        logger.error(f"❌ Error copying to silver: {e}")
        raise


def verify_data_load(engine):
    """Verify data was loaded correctly."""
    try:
        logger.info("\nVerifying data load...")
        
        with engine.connect() as conn:
            # Count bronze records
            bronze_genes = pd.read_sql("SELECT COUNT(*) as count FROM bronze.genes_raw", conn).iloc[0]['count']
            bronze_variants = pd.read_sql("SELECT COUNT(*) as count FROM bronze.variants_raw", conn).iloc[0]['count']
            
            # Count silver records
            silver_genes = pd.read_sql("SELECT COUNT(*) as count FROM silver.genes", conn).iloc[0]['count']
            silver_variants = pd.read_sql("SELECT COUNT(*) as count FROM silver.variants", conn).iloc[0]['count']
            
            # Display results
            print("\n" + "="*70)
            print("DATA LOAD VERIFICATION")
            print("="*70)
            print(f"Bronze Layer:")
            print(f"  Genes:    {bronze_genes:,}")
            print(f"  Variants: {bronze_variants:,}")
            print(f"\nSilver Layer:")
            print(f"  Genes:    {silver_genes:,}")
            print(f"  Variants: {silver_variants:,}")
            print("="*70)
            
            if silver_genes > 0 and silver_variants > 0:
                logger.info("✅ Data load verification passed!")
                return True
            else:
                logger.error("❌ Data load verification failed!")
                return False
                
    except Exception as e:
        logger.error(f"❌ Error verifying data: {e}")
        return False


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("LOAD DATA TO POSTGRESQL")
    print("="*70)
    print(f"Genes CSV: {GENES_CSV}")
    print(f"Variants CSV: {VARIANTS_CSV}")
    print(f"Database: {DB_CONFIG['database']}")
    print("="*70 + "\n")
    
    try:
        # Create database engine
        engine = get_database_engine()
        
        # Load data
        genes_count = load_genes_to_bronze(engine)
        variants_count = load_variants_to_bronze(engine)
        
        # Copy to silver
        copy_bronze_to_silver(engine)
        
        # Verify
        if verify_data_load(engine):
            print("\n✅ Data loading completed successfully!")
        else:
            print("\n❌ Data loading completed with errors!")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        print("\n❌ Data loading failed!")


if __name__ == "__main__":
    main()