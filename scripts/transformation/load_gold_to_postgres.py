# ====================================================================
# Load Gold Layer to PostgreSQL - UPDATED FOR NEW SCHEMA
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 17 January 2026 (Updated for 65-column schema)
# ====================================================================

"""
Load Gold Layer to PostgreSQL
Loads Databricks Gold layer exports to PostgreSQL database.
UPDATED: Handles new 65-column schema with functional flags and derived columns.
"""

import pandas as pd
from sqlalchemy import create_engine, text
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
    print("LOADING GOLD LAYER TO POSTGRESQL (UPDATED SCHEMA)")
    print("="*70)
    print("\nNEW SCHEMA FEATURES:")
    print("  - 65 columns in gene_features (was 51)")
    print("  - 10 functional protein flags")
    print("  - 4 derived classification columns")
    print("  - All boolean columns properly handled")
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
                    on_bad_lines='warn',
                    engine='python',
                    encoding='utf-8'
                )
            else:
                df = pd.read_csv(csv_path)
            
            logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")
            
            # NEW: Convert boolean columns for functional flags
            boolean_columns = [
                # Functional protein flags (NEW in this release)
                'is_kinase', 'is_phosphatase', 'is_receptor', 'is_enzyme', 'is_transporter',
                'has_glycoprotein', 'has_receptor_keyword', 'has_enzyme_keyword', 
                'has_kinase_keyword', 'has_binding_keyword',
                
                # Legacy columns (keep for backwards compatibility)
                'is_gpcr', 'is_transcription_factor', 'is_channel', 
                'is_membrane_protein', 'is_growth_factor', 'is_structural', 
                'is_regulatory', 'is_metabolic', 'is_dna_binding', 'is_rna_binding', 
                'is_ubiquitin_related', 'is_protease',
                
                # Disease-related flags
                'cancer_related', 'immune_related', 'neurological_related', 
                'cardiovascular_related', 'metabolic_related', 'developmental_related',
                'alzheimer_related', 'diabetes_related', 'breast_cancer_related',
                
                # Location flags
                'nuclear', 'mitochondrial', 'cytoplasmic', 'membrane',
                'extracellular', 'endoplasmic_reticulum', 'golgi',
                'lysosomal', 'peroxisomal',
                
                # Quality flags
                'is_telomeric', 'is_centromeric', 'is_well_characterized'
            ]
            
            converted_count = 0
            for col in boolean_columns:
                if col in df.columns:
                    # Handle various boolean representations
                    df[col] = df[col].map({
                        'true': True, 'True': True, 'TRUE': True, True: True,
                        'false': False, 'False': False, 'FALSE': False, False: False,
                        1: True, 0: False, '1': True, '0': False
                    })
                    converted_count += 1
            
            if converted_count > 0:
                logger.info(f"  Converted {converted_count} boolean columns")
            
            # NEW: Verify new columns are present in gene_features
            if csv_name == "gene_features":
                expected_new_cols = [
                    'is_kinase', 'is_phosphatase', 'is_receptor', 'is_enzyme', 'is_transporter',
                    'has_glycoprotein', 'has_receptor_keyword', 'has_enzyme_keyword',
                    'has_kinase_keyword', 'has_binding_keyword',
                    'primary_function', 'biological_process', 'cellular_location', 'druggability_score'
                ]
                
                present_cols = [col for col in expected_new_cols if col in df.columns]
                missing_cols = [col for col in expected_new_cols if col not in df.columns]
                
                logger.info(f"  NEW COLUMNS CHECK:")
                logger.info(f"    Present: {len(present_cols)}/14")
                if present_cols:
                    logger.info(f"    Found: {', '.join(present_cols[:5])}{'...' if len(present_cols) > 5 else ''}")
                
                if missing_cols:
                    logger.warning(f"    MISSING: {len(missing_cols)}/14")
                    logger.warning(f"    Missing: {', '.join(missing_cols)}")
                    logger.warning(f"    WARNING: Gene features may not have new schema!")
                    logger.warning(f"    Please run: 05_feature_engineering_COMPLETE.py")
                else:
                    logger.info(f"    SUCCESS: All new columns present!")
            
            logger.info(f"  Processed {len(df):,} rows, {len(df.columns)} columns")
            
            # Drop table with CASCADE to remove dependent views
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE"))
                    conn.commit()
                    logger.info(f"  Dropped existing table {schema}.{table_name}")
            except Exception as e:
                logger.warning(f"  Could not drop table with CASCADE: {e}")
            
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
    print("Check NEW columns in gene_features:")
    print("  SELECT column_name FROM information_schema.columns")
    print("  WHERE table_schema = 'gold' AND table_name = 'gene_features'")
    print("  AND column_name LIKE 'is_%' OR column_name LIKE 'has_%'")
    print("  OR column_name IN ('primary_function', 'biological_process',")
    print("                     'cellular_location', 'druggability_score')")
    print("  ORDER BY column_name;")
    print("\n" + "="*70)
    print("Expected column count:")
    print("  gene_features: 65 columns (was 51)")
    print("  - 10 functional flags (is_kinase, is_receptor, etc.)")
    print("  - 4 derived columns (primary_function, cellular_location, etc.)")
    print("="*70)


if __name__ == "__main__":
    load_gold_tables()
