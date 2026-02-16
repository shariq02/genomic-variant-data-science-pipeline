"""
SMART POSTGRES LOADER - CHANGED TABLES ONLY
Loads only tables that have been downloaded and changed
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import psycopg2
import csv
import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'genome_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
LOCAL_CHECKPOINT = PROCESSED_DIR / ".download_checkpoint.json"
POSTGRES_CHECKPOINT = PROCESSED_DIR / ".postgres_checkpoint.json"

def load_checkpoint(checkpoint_file):
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint_file, data):
    with open(checkpoint_file, 'w') as f:
        json.dump(data, f, indent=2)

def get_csv_info(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        row_count = sum(1 for _ in f)
    return headers, row_count

def table_exists(cursor, schema, table_name):
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        )
    """, (schema, table_name))
    return cursor.fetchone()[0]

def load_table_to_postgres(csv_file, table_name, cursor, conn):
    headers, row_count = get_csv_info(csv_file)
    
    print(f"  Rows in CSV: {row_count:,}")
    print(f"  Columns: {len(headers)}")
    
    columns_sql = ", ".join([f'"{h}" TEXT' for h in headers])
    
    if table_exists(cursor, 'gold', table_name):
        print(f"  Dropping existing table...")
        cursor.execute(f'DROP TABLE IF EXISTS gold.{table_name}')
    
    print(f"  Creating table...")
    cursor.execute(f'CREATE TABLE gold.{table_name} ({columns_sql})')
    
    print(f"  Loading data...")
    with open(csv_file, 'r', encoding='utf-8') as f:
        cursor.copy_expert(
            f'COPY gold.{table_name} FROM STDIN WITH CSV HEADER',
            f
        )
    
    conn.commit()
    
    cursor.execute(f'SELECT COUNT(*) FROM gold.{table_name}')
    loaded_count = cursor.fetchone()[0]
    
    print(f"  Loaded: {loaded_count:,} rows")
    
    if loaded_count != row_count:
        print(f"  WARNING: Row count mismatch!")
        return False
    
    return True

def main():
    print("="*80)
    print("SMART POSTGRES LOADER - CHANGED TABLES ONLY")
    print("="*80)
    
    print("\nLoading download checkpoint...")
    download_checkpoint = load_checkpoint(LOCAL_CHECKPOINT)
    
    if not download_checkpoint:
        print("ERROR: No download checkpoint found")
        print("Run download_changed_tables_only.py first")
        return
    
    print(f"Downloaded tables: {len(download_checkpoint)}")
    
    print("\nLoading PostgreSQL checkpoint...")
    postgres_checkpoint = load_checkpoint(POSTGRES_CHECKPOINT)
    print(f"Previously loaded: {len(postgres_checkpoint)}")
    
    print("\nIdentifying tables to load...")
    print("="*80)
    
    tables_to_load = []
    
    for table_name, download_info in download_checkpoint.items():
        csv_file = PROCESSED_DIR / f"{table_name}.csv"
        
        if not csv_file.exists():
            print(f"{table_name}: SKIP (CSV not found)")
            continue
        
        if table_name not in postgres_checkpoint:
            print(f"{table_name}: LOAD (not in PostgreSQL yet)")
            tables_to_load.append(table_name)
            continue
        
        postgres_info = postgres_checkpoint[table_name]
        
        if download_info.get("rows") != postgres_info.get("rows"):
            print(f"{table_name}: LOAD (rows changed: {postgres_info.get('rows'):,} -> {download_info.get('rows'):,})")
            tables_to_load.append(table_name)
            continue
        
        if download_info.get("columns") != postgres_info.get("columns"):
            print(f"{table_name}: LOAD (columns changed)")
            tables_to_load.append(table_name)
            continue
        
        print(f"{table_name}: SKIP (up to date)")
    
    if len(tables_to_load) == 0:
        print("\nNo tables need loading - all up to date")
        return
    
    print(f"\nTables to load: {len(tables_to_load)}")
    print("="*80)
    
    print("\nConnecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        print("Connected successfully")
    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        return
    
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.commit()
    
    load_results = {}
    
    for table_name in tables_to_load:
        print(f"\n{table_name}:")
        
        csv_file = PROCESSED_DIR / f"{table_name}.csv"
        
        try:
            success = load_table_to_postgres(csv_file, table_name, cursor, conn)
            
            if success:
                load_results[table_name] = {"success": True}
                postgres_checkpoint[table_name] = download_checkpoint[table_name].copy()
                save_checkpoint(POSTGRES_CHECKPOINT, postgres_checkpoint)
                print(f"  Status: OK")
            else:
                load_results[table_name] = {"success": False}
                print(f"  Status: FAILED (row mismatch)")
                
        except Exception as e:
            load_results[table_name] = {"success": False, "error": str(e)}
            print(f"  ERROR: {str(e)[:100]}")
            conn.rollback()
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*80)
    print("LOAD SUMMARY")
    print("="*80)
    
    successful = [t for t in tables_to_load if load_results[t]["success"]]
    failed = [t for t in tables_to_load if not load_results[t]["success"]]
    
    print(f"\nSuccessful: {len(successful)}/{len(tables_to_load)}")
    for table in successful:
        print(f"  - {table}")
    
    if failed:
        print(f"\nFailed: {len(failed)}")
        for table in failed:
            error = load_results[table].get("error", "Unknown error")
            print(f"  - {table}: {error}")
    
    print("\n" + "="*80)
    print("NEXT STEP: Run fix_postgres_types_fast.py (only if new tables loaded)")
    print("="*80)

if __name__ == "__main__":
    main()
