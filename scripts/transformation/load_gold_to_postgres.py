# ====================================================================
# LOAD GOLD TABLES TO POSTGRESQL WITH UPSERT
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 2026
# ====================================================================
# Features:
# - Loads 11 gold tables from CSV to PostgreSQL
# - Upsert logic: Skip existing records (idempotent)
# - Batch processing for large files (100K rows per batch)
# - Progress tracking and verification
# ====================================================================

import os
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
import time

load_dotenv()

# ====================================================================
# CONFIGURATION
# ====================================================================

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'genomics_ml')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

BATCH_SIZE = 100000

TABLES_CONFIG = {
    # 5 Gold Feature Tables
    "clinical_ml_features": {"primary_key": "variant_id"},
    "disease_ml_features": {"primary_key": "variant_id"},
    "pharmacogene_ml_features": {"primary_key": "variant_id"},
    "structural_variant_ml_features": {"primary_key": "sv_id"},
    "variant_impact_ml_features": {"primary_key": "variant_id"},
    
    # 6 ML Dataset Tables
    "ml_dataset_variants_train": {"primary_key": "variant_id"},
    "ml_dataset_variants_validation": {"primary_key": "variant_id"},
    "ml_dataset_variants_test": {"primary_key": "variant_id"},
    "ml_dataset_structural_variants_train": {"primary_key": "sv_id"},
    "ml_dataset_structural_variants_validation": {"primary_key": "sv_id"},
    "ml_dataset_structural_variants_test": {"primary_key": "sv_id"}
}

# ====================================================================
# UPSERT FUNCTIONS
# ====================================================================

def load_table_with_upsert(df, table_name, engine, primary_key):
    """Load data with upsert - skip existing records"""
    
    if len(df) == 0:
        print(f"    Empty dataframe - skipping")
        return 0
    
    with engine.connect() as conn:
        table_exists = conn.execute(text(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
        )).scalar()
        
        if not table_exists:
            print(f"    Creating table (first load)...")
            df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=10000)
            print(f"    Created table with {len(df):,} rows")
            return len(df)
        
        existing_ids = pd.read_sql(f"SELECT {primary_key} FROM {table_name}", engine)
        existing_set = set(existing_ids[primary_key])
        
        new_df = df[~df[primary_key].isin(existing_set)]
        
        if len(new_df) == 0:
            print(f"    All {len(df):,} records already exist - skipped")
            return 0
        
        new_df.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=10000)
        print(f"    Added {len(new_df):,} new rows (skipped {len(df) - len(new_df):,} existing)")
        return len(new_df)

def load_in_batches(csv_file, table_name, engine, primary_key, batch_size):
    """Load large CSV in batches with upsert"""
    
    print(f"  Loading in batches of {batch_size:,} rows...")
    
    total_rows = 0
    total_added = 0
    batch_num = 0
    
    for chunk in pd.read_csv(csv_file, chunksize=batch_size, low_memory=False):
        batch_num += 1
        print(f"  Batch {batch_num}: {len(chunk):,} rows")
        
        added = load_table_with_upsert(chunk, table_name, engine, primary_key)
        total_rows += len(chunk)
        total_added += added
    
    return total_rows, total_added

# ====================================================================
# MAIN LOAD FUNCTION
# ====================================================================

def load_gold_to_postgres():
    """Load all 11 gold tables to PostgreSQL"""
    
    print("="*80)
    print("LOADING 11 GOLD TABLES TO POSTGRESQL")
    print("="*80)
    print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Data Directory: {PROCESSED_DIR}")
    print(f"Batch Size: {BATCH_SIZE:,} rows")
    print("="*80)
    
    if not POSTGRES_PASSWORD:
        print("\nERROR: PostgreSQL password not found!")
        print("Please add to your .env file:")
        print("  POSTGRES_PASSWORD=your-password")
        return
    
    connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print(f"PostgreSQL connection: OK\n")
    except Exception as e:
        print(f"\nERROR: Cannot connect to PostgreSQL")
        print(f"Error: {e}")
        return
    
    load_summary = {}
    total_start_time = time.time()
    
    for table_name, config in TABLES_CONFIG.items():
        primary_key = config["primary_key"]
        csv_file = PROCESSED_DIR / f"{table_name}.csv"
        
        load_summary[table_name] = {
            "status": "NOT_FOUND",
            "rows_processed": 0,
            "rows_added": 0,
            "time_seconds": 0
        }
        
        print(f"\n{'='*80}")
        print(f"Table {list(TABLES_CONFIG.keys()).index(table_name) + 1}/{len(TABLES_CONFIG)}: {table_name}")
        print(f"{'='*80}")
        
        if not csv_file.exists():
            print(f"  CSV file not found: {csv_file}")
            load_summary[table_name]["status"] = "NOT_FOUND"
            continue
        
        file_size_mb = csv_file.stat().st_size / (1024 * 1024)
        print(f"  CSV file: {csv_file.name}")
        print(f"  File size: {file_size_mb:.2f} MB")
        print(f"  Primary key: {primary_key}")
        
        try:
            start_time = time.time()
            
            total_rows, total_added = load_in_batches(
                csv_file, 
                table_name, 
                engine, 
                primary_key, 
                BATCH_SIZE
            )
            
            elapsed = time.time() - start_time
            
            load_summary[table_name]["status"] = "LOADED"
            load_summary[table_name]["rows_processed"] = total_rows
            load_summary[table_name]["rows_added"] = total_added
            load_summary[table_name]["time_seconds"] = elapsed
            
            print(f"  Completed in {elapsed:.1f}s ({total_rows/elapsed:.0f} rows/sec)")
            
            with engine.connect() as conn:
                db_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                print(f"  PostgreSQL table now has: {db_count:,} rows")
        
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            load_summary[table_name]["status"] = "FAILED"
            continue
    
    total_elapsed = time.time() - total_start_time
    
    print("\n" + "="*80)
    print("LOAD SUMMARY")
    print("="*80)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
    
    print(f"\n{'Table':<50} {'Status':<10} {'Processed':<12} {'Added':<12} {'Time (s)':<10}")
    print("-"*110)
    
    total_processed = 0
    total_added = 0
    success_count = 0
    
    for table_name, summary in load_summary.items():
        status = summary["status"]
        processed = summary["rows_processed"]
        added = summary["rows_added"]
        seconds = summary["time_seconds"]
        
        print(f"  {table_name:<48} {status:<10} {processed:>10,}  {added:>10,}  {seconds:>8.1f}")
        
        if status == "LOADED":
            total_processed += processed
            total_added += added
            success_count += 1
    
    print("-"*110)
    print(f"  {'TOTAL':<48} {success_count}/{len(TABLES_CONFIG):<10} {total_processed:>10,}  {total_added:>10,}  {total_elapsed:>8.1f}")
    
    if success_count == len(TABLES_CONFIG):
        print("\n" + "="*80)
        print("SUCCESS! All 11 tables loaded to PostgreSQL")
        print("="*80)
        print(f"Database: {POSTGRES_DB}")
        print(f"Total rows: {total_added:,} new rows added")
        print("\nNEXT STEP: Start Phase 6 ML Training")
        print("Create Jupyter notebooks in local/notebooks/")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("INCOMPLETE LOAD")
        print("="*80)
        print("Re-run this script - it will skip already loaded data")
        print("="*80)

if __name__ == "__main__":
    load_gold_to_postgres()
