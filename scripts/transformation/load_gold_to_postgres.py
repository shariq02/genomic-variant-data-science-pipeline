# ====================================================================
# Load Gold Layer to PostgreSQL
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 13 January 2026 (Updated)
# ====================================================================

"""
Load Gold Layer to PostgreSQL
Loads Databricks Gold layer exports to PostgreSQL database.
Updated to handle disease names with commas/quotes.
"""

import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent.parent
EXPORTS_DIR = PROJECT_ROOT / "data" / "processed"

print(f"Project Root: {PROJECT_ROOT}")
print(f"Looking for CSVs in: {EXPORTS_DIR}")

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'genome_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD')
}


def get_engine():
    """Create database engine."""
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


def load_gold_tables():
    """Load all Gold tables to PostgreSQL."""
    
    print("\n" + "="*70)
    print("LOADING GOLD LAYER TO POSTGRESQL")
    print("="*70)
    
    if not EXPORTS_DIR.exists():
        logger.error(f"Directory does not exist: {EXPORTS_DIR}")
        logger.error("Please create the directory and place CSV files there")
        return
    
    files_in_dir = list(EXPORTS_DIR.glob("*.csv"))
    if files_in_dir:
        print(f"\nFound {len(files_in_dir)} CSV files in {EXPORTS_DIR}:")
        for f in files_in_dir:
            file_size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  - {f.name} ({file_size_mb:.2f} MB)")
    else:
        print(f"\nNo CSV files found in {EXPORTS_DIR}")
        return
    
    engine = get_engine()
    
    tables = [
        ("gene_features", "gold", "gene_features"),
        ("chromosome_features", "gold", "chromosome_features"),
        ("gene_disease_association", "gold", "gene_disease_association"),
        ("ml_features", "gold", "ml_features")
    ]
    
    for csv_name, schema, table_name in tables:
        csv_path = EXPORTS_DIR / f"{csv_name}.csv"
        
        if not csv_path.exists():
            logger.warning(f"WARNING: File not found: {csv_path}")
            logger.warning(f"  Please download {csv_name}.csv from Databricks")
            logger.warning(f"  and place it in: {EXPORTS_DIR}")
            continue
        
        logger.info(f"Loading {csv_name} to {schema}.{table_name}")
        
        try:
            # Special handling for gene_disease_association (has disease names with commas)
            if csv_name == "gene_disease_association":
                logger.info(f"  Using robust CSV parser for disease names...")
                df = pd.read_csv(
                    csv_path,
                    escapechar='\\',
                    quotechar='"',
                    on_bad_lines='warn',  # Warn but continue
                    engine='python',  # More robust parser
                    encoding='utf-8'
                )
            else:
                df = pd.read_csv(csv_path)
            
            logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")
            
            # Load in chunks for large tables
            chunk_size = 10000
            if len(df) > chunk_size:
                logger.info(f"  Loading in chunks of {chunk_size:,}...")
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    if i == 0:
                        chunk.to_sql(
                            name=table_name,
                            schema=schema,
                            con=engine,
                            if_exists='replace',
                            index=False,
                            method='multi'
                        )
                    else:
                        chunk.to_sql(
                            name=table_name,
                            schema=schema,
                            con=engine,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                    if (i + chunk_size) % 50000 == 0:
                        logger.info(f"    Progress: {i+chunk_size:,} rows loaded...")
            else:
                df.to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
            
            logger.info(f"  SUCCESS: Loaded {len(df):,} rows to {schema}.{table_name}")
            
        except Exception as e:
            logger.error(f"  ERROR loading {csv_name}: {e}")
            logger.error(f"  File path: {csv_path}")
            logger.error(f"  Try checking the CSV file for malformed data")
    
    print("\n" + "="*70)
    print("LOADING COMPLETE")
    print("="*70)
    print("\nVerify in PostgreSQL:")
    print("  SELECT COUNT(*) FROM gold.gene_features;")
    print("  SELECT COUNT(*) FROM gold.chromosome_features;")
    print("  SELECT COUNT(*) FROM gold.gene_disease_association;")
    print("  SELECT COUNT(*) FROM gold.ml_features;")
    print("\n" + "="*70)
    print("Check column counts:")
    print("  SELECT COUNT(*) FROM information_schema.columns")
    print("  WHERE table_schema = 'gold';")
    print("="*70)


if __name__ == "__main__":
    load_gold_tables()
