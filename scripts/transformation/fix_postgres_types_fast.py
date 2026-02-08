#!/usr/bin/env python3

import psycopg2
import csv
from pathlib import Path
from dotenv import load_dotenv
import os
import sys
import time

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'genome_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

PROJECT_ROOT = Path(__file__).parent.parent.parent
SCHEMA_FILE = PROJECT_ROOT / 'documents' / 'schemas' / 'gold_table_schema.csv'

print("="*80)
print("POSTGRESQL TYPE CONVERSION SCRIPT - FAST VERSION")
print("="*80)
print(f"Database: {POSTGRES_DB}")
print(f"Host: {POSTGRES_HOST}")
print(f"Schema file: {SCHEMA_FILE}")
print("="*80)

if not SCHEMA_FILE.exists():
    print(f"\nERROR: Schema file not found: {SCHEMA_FILE}")
    sys.exit(1)

print("\nParsing schema file...")
schema = {}

with open(SCHEMA_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        table = row['table_name']
        col = row['column_name']
        dtype = row['data_type']
        
        if table not in schema:
            schema[table] = {
                'BOOLEAN': [],
                'INT': [],
                'DOUBLE': [],
                'LONG': [],
                'STRING': []
            }
        
        schema[table][dtype].append(col)

print(f"Parsed schema for {len(schema)} tables")

PG_TYPE_MAP = {
    'BOOLEAN': 'BOOLEAN',
    'INT': 'INTEGER',
    'DOUBLE': 'DOUBLE PRECISION',
    'LONG': 'BIGINT',
    'STRING': 'TEXT'
}

print("\nConnecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()
    print("Connected successfully")
except Exception as e:
    print(f"ERROR: Failed to connect: {e}")
    sys.exit(1)

stats = {
    'tables_processed': 0,
    'columns_converted': 0,
    'columns_failed': 0,
    'columns_skipped': 0
}

for table_name in sorted(schema.keys()):
    print(f"\n{'='*80}")
    print(f"Processing: gold.{table_name}")
    print(f"{'='*80}")
    
    cur.execute(f"SELECT COUNT(*) FROM gold.{table_name}")
    row_count = cur.fetchone()[0]
    print(f"Table has {row_count:,} rows")
    
    table_stats = {'converted': 0, 'failed': 0, 'skipped': 0}
    
    for dtype, columns in schema[table_name].items():
        if dtype == 'STRING':
            continue
        
        pg_type = PG_TYPE_MAP[dtype]
        
        for col in columns:
            try:
                cur.execute(f"""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'gold' 
                    AND table_name = '{table_name}' 
                    AND column_name = '{col}'
                """)
                
                result = cur.fetchone()
                if not result:
                    print(f"  SKIP {col}: Column not found")
                    table_stats['skipped'] += 1
                    continue
                
                current_type = result[0]
                
                if current_type.upper() in [pg_type.upper(), pg_type.replace(' ', '').upper()]:
                    print(f"  SKIP {col}: Already {current_type}")
                    table_stats['skipped'] += 1
                    continue
                
                print(f"  CONVERTING {col}: {current_type} to {pg_type}...", end='', flush=True)
                start_time = time.time()
                
                if dtype == 'BOOLEAN':
                    cur.execute(f"""
                        ALTER TABLE gold.{table_name}
                        ALTER COLUMN {col} TYPE BOOLEAN 
                        USING CASE 
                            WHEN LOWER(TRIM({col}::TEXT)) IN ('true', 't', '1', 'yes') THEN TRUE
                            WHEN LOWER(TRIM({col}::TEXT)) IN ('false', 'f', '0', 'no', '') THEN FALSE
                            ELSE NULL
                        END
                    """)
                
                elif dtype == 'INT':
                    cur.execute(f"""
                        ALTER TABLE gold.{table_name}
                        ALTER COLUMN {col} TYPE INTEGER 
                        USING CASE 
                            WHEN {col}::TEXT ~ '^-?[0-9]+$' THEN {col}::INTEGER
                            ELSE NULL
                        END
                    """)
                
                elif dtype == 'LONG':
                    cur.execute(f"""
                        ALTER TABLE gold.{table_name}
                        ALTER COLUMN {col} TYPE BIGINT 
                        USING CASE 
                            WHEN {col}::TEXT ~ '^-?[0-9]+$' THEN {col}::BIGINT
                            ELSE NULL
                        END
                    """)
                
                elif dtype == 'DOUBLE':
                    cur.execute(f"""
                        ALTER TABLE gold.{table_name}
                        ALTER COLUMN {col} TYPE DOUBLE PRECISION 
                        USING CASE 
                            WHEN {col}::TEXT ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN {col}::DOUBLE PRECISION
                            ELSE NULL
                        END
                    """)
                
                conn.commit()
                elapsed = time.time() - start_time
                print(f" OK ({elapsed:.1f}s)")
                table_stats['converted'] += 1
                
            except Exception as e:
                conn.rollback()
                elapsed = time.time() - start_time
                error_msg = str(e)[:150]
                print(f" FAIL ({elapsed:.1f}s): {error_msg}")
                table_stats['failed'] += 1
                continue
    
    stats['tables_processed'] += 1
    stats['columns_converted'] += table_stats['converted']
    stats['columns_failed'] += table_stats['failed']
    stats['columns_skipped'] += table_stats['skipped']
    
    print(f"\nTable summary:")
    print(f"  Converted: {table_stats['converted']}")
    print(f"  Failed: {table_stats['failed']}")
    print(f"  Skipped: {table_stats['skipped']}")

cur.close()
conn.close()

print(f"\n{'='*80}")
print("FINAL SUMMARY")
print(f"{'='*80}")
print(f"Tables processed: {stats['tables_processed']}")
print(f"Columns converted: {stats['columns_converted']}")
print(f"Columns failed: {stats['columns_failed']}")
print(f"Columns skipped: {stats['columns_skipped']}")
print(f"{'='*80}")

if stats['columns_failed'] > 0:
    print("\nWARNING: Some columns failed to convert")
    sys.exit(1)
else:
    print("\nSUCCESS! All tables fixed permanently")
    sys.exit(0)
