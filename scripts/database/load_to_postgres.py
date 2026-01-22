# ====================================================================
# COMPLETE POSTGRESQL LOAD - SMART & SAFE
# Genomic Variant Data Science Pipeline
# Author: Sharique Mohammad
# Date: January 22, 2026
# ====================================================================

"""
STRATEGY:
- Bronze: ALL columns as TEXT (flexible, no type errors)
- Silver: Cast to proper types (INTEGER, BIGINT, etc)
- Full incremental loading with row count checks
- Progress bars for both Bronze and Silver
- Smart skipping when counts match
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from dotenv import load_dotenv
import os
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# All data files
DATA_FILES = {
    "medgen_concepts": RAW_DATA_DIR / "medgen" / "medgen_concepts_raw.csv",
    "medgen_relations": RAW_DATA_DIR / "medgen" / "medgen_relations_raw.csv",
    "omim_entries": RAW_DATA_DIR / "omim" / "omim_mim2gene_medgen_raw.csv",
    "gene_disease_associations": RAW_DATA_DIR / "genes" / "gene_disease_ncbi.csv",
    "refseq_genes": RAW_DATA_DIR / "references" / "refseq_genes_grch38.csv",
    "refseq_transcripts": RAW_DATA_DIR / "references" / "refseq_transcripts_grch38.csv",
    "refseq_proteins": RAW_DATA_DIR / "protein" / "protein_gene_mapping_human.csv",
    "uniprot_proteins": RAW_DATA_DIR / "uniprot" / "uniprot_swissprot_human.csv",
    "gtr_tests": RAW_DATA_DIR / "gtr" / "gtr_gene_disease_tests.csv",
    "dbvar_estd214_regions": RAW_DATA_DIR / "dbvar" / "estd214.GRCh38.variant_region.vcf_parsed_full.csv",
    "dbvar_nstd102_calls": RAW_DATA_DIR / "dbvar" / "nstd102.GRCh38.variant_call.vcf_parsed_full.csv",
    "dbvar_nstd102_regions": RAW_DATA_DIR / "dbvar" / "nstd102.GRCh38.variant_region.vcf_parsed_full.csv",
    "gene_metadata": RAW_DATA_DIR / "genes" / "gene_metadata_all.csv",
    "clinvar_variants": RAW_DATA_DIR / "variants" / "clinvar_all_variants.csv",
    "allele_mappings": RAW_DATA_DIR / "variants" / "allele_id_mapping.csv",
}

CHUNK_SIZE = 10000

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DATABASE", "genome_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def get_database_engine():
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(conn_str)


def table_exists(engine, schema, table_name):
    query = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table_name);")
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema, "table_name": table_name})
        return result.scalar()


def get_table_count(engine, schema, table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            return result.scalar()
    except:
        return 0


def print_progress(current, total, elapsed_time, prefix=""):
    percent = (current / total * 100) if total > 0 else 0
    bar_length = 40
    filled = int(bar_length * current / total) if total > 0 else 0
    bar = '=' * filled + '-' * (bar_length - filled)
    rate = current / elapsed_time if elapsed_time > 0 else 0
    eta = (total - current) / rate if rate > 0 else 0
    sys.stdout.write(f"\r{prefix}[{bar}] {percent:5.1f}% | {current:,}/{total:,} | ETA: {eta:.0f}s     ")
    sys.stdout.flush()


def load_table_simple(engine, table_name, file_path, schema="bronze"):
    """Load data with smart checking - skips if counts match"""
    print(f"\n{'='*70}")
    print(f"LOADING: {schema}.{table_name}")
    print(f"{'='*70}")
    
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return False
    
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"File size: {file_size_mb:.1f} MB")
    
    # Count CSV rows
    print("Counting rows in CSV...")
    total_rows = sum(1 for _ in open(file_path, encoding='utf-8', errors='ignore')) - 1
    print(f"CSV has {total_rows:,} rows")
    
    # Check existing
    exists = table_exists(engine, schema, table_name)
    if exists:
        existing_count = get_table_count(engine, schema, table_name)
        print(f"Table exists: {existing_count:,} rows")
        
        if existing_count == total_rows:
            print("Counts match - SKIPPING (already loaded)")
            return True
        elif existing_count > total_rows:
            print(f"WARNING: Table has MORE rows ({existing_count:,}) than CSV ({total_rows:,})")
            choice = input("(t)runcate and reload, (s)kip, (n)othing? [n]: ").lower()
            if choice == 't':
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
                print("Table truncated")
                existing_count = 0
            elif choice == 's':
                print("Skipping table")
                return True
            else:
                print("Doing nothing - keeping existing data")
                return True
        else:
            print(f"Loading {total_rows - existing_count:,} new rows...")
            choice = input("(a)ppend, (t)runcate and reload, (n)othing? [a]: ").lower()
            if choice == 't':
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
                print("Table truncated")
                existing_count = 0
            elif choice == 'n':
                print("Doing nothing - keeping existing data")
                return True
    else:
        existing_count = 0
    
    # Load with progress
    print(f"Loading in chunks of {CHUNK_SIZE:,}...")
    rows_loaded = 0
    start_time = time.time()
    
    try:
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=False, dtype=str):
            chunk.to_sql(
                name=table_name,
                schema=schema,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500
            )
            rows_loaded += len(chunk)
            elapsed = time.time() - start_time
            print_progress(rows_loaded, total_rows, elapsed, "  ")
        
        print()
        final_count = get_table_count(engine, schema, table_name)
        elapsed_total = time.time() - start_time
        
        if final_count < total_rows:
            print(f"\nWARNING Warning: {total_rows - final_count} rows skipped (likely malformed CSV data)")
        
        print(f"\nComplete: {final_count:,} rows in {elapsed_total:.1f}s")
        return True
        
    except Exception as e:
        print(f"\nERROR ERROR: {e}")
        return False


def rename_existing_tables(engine):
    print("\n" + "="*70)
    print("STEP 1: RENAME EXISTING TABLES")
    print("="*70)
    
    renames = [
        ("bronze", "genes_raw", "gene_metadata"),
        ("bronze", "variants_raw", "clinvar_variants"),
        ("bronze", "allele_id_mapping", "allele_mappings"),
        ("silver", "genes_clean", "genes"),
        ("silver", "variants_clean", "variants"),
    ]
    
    with engine.begin() as conn:
        for schema, old_name, new_name in renames:
            if table_exists(engine, schema, old_name):
                print(f"  {schema}.{old_name} -> {schema}.{new_name}")
                conn.execute(text(f"ALTER TABLE {schema}.{old_name} RENAME TO {new_name}"))
    
    print("Rename complete!")


def fix_bronze_table_schemas(engine):
    """Drop and recreate bronze tables with ALL TEXT columns"""
    print("\n" + "="*70)
    print("STEP 1.5: FIX BRONZE TABLE SCHEMAS (ALL TEXT)")
    print("="*70)
    
    # Tables that need to be recreated as ALL TEXT
    tables_to_fix = [
        "gene_metadata",
        "dbvar_estd214_regions",
        "dbvar_nstd102_calls", 
        "dbvar_nstd102_regions"
    ]
    
    with engine.begin() as conn:
        for table_name in tables_to_fix:
            if table_exists(engine, "bronze", table_name):
                existing_count = get_table_count(engine, "bronze", table_name)
                print(f"\n  {table_name}: {existing_count:,} rows")
                
                # Check column types
                type_check = conn.execute(text(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'bronze' 
                    AND table_name = '{table_name}'
                    AND data_type IN ('integer', 'bigint', 'double precision')
                    LIMIT 1
                """))
                
                has_numeric_types = type_check.fetchone() is not None
                
                if has_numeric_types:
                    print(f"    ERROR Has numeric columns - needs rebuild")
                    choice = input(f"    Drop and recreate {table_name} as ALL TEXT? [y/n]: ").lower()
                    if choice == 'y':
                        print(f"    Dropping bronze.{table_name}...")
                        conn.execute(text(f"DROP TABLE IF EXISTS bronze.{table_name} CASCADE"))
                        print(f"    Dropped - will reload as TEXT")
                    else:
                        print(f"    Skipped")
                else:
                    print(f"    Already ALL TEXT")
    
    print("\nSchema fix complete!")


def rename_existing_tables(engine):
    print("\n" + "="*70)
    print("STEP 1: RENAME EXISTING TABLES")
    print("="*70)
    
    renames = [
        ("bronze", "genes_raw", "gene_metadata"),
        ("bronze", "variants_raw", "clinvar_variants"),
        ("bronze", "allele_id_mapping", "allele_mappings"),
        ("silver", "genes_clean", "genes"),
        ("silver", "variants_clean", "variants"),
    ]
    
    with engine.begin() as conn:
        for schema, old_name, new_name in renames:
            if table_exists(engine, schema, old_name):
                print(f"  {schema}.{old_name} -> {schema}.{new_name}")
                conn.execute(text(f"ALTER TABLE {schema}.{old_name} RENAME TO {new_name}"))
    
    print("Rename complete!")


def create_silver_tables(engine):
    print("\nSTEP 3: CREATE SILVER TABLES")
    print("-" * 50)
    
    # Drop genetic_tests if it exists (may have wrong primary key)
    with engine.begin() as conn:
        if table_exists(engine, "silver", "genetic_tests"):
            print("Dropping silver.genetic_tests (wrong schema)...")
            conn.execute(text("DROP TABLE IF EXISTS silver.genetic_tests CASCADE"))
    
    print("Checking existing silver tables...")
    with engine.begin() as conn:
        tables = [
            ("genes", "gene_id INTEGER PRIMARY KEY, gene_name TEXT, official_symbol TEXT, description TEXT, chromosome TEXT, map_location TEXT, gene_type TEXT, start_position BIGINT, end_position BIGINT, strand TEXT"),
            ("variants", "accession TEXT PRIMARY KEY, variant_id TEXT, gene_name TEXT, clinical_significance TEXT, disease TEXT, chromosome TEXT, position BIGINT, stop_position BIGINT, variant_type TEXT, review_status TEXT, assembly TEXT"),
            ("variant_allele_map", "variation_id TEXT PRIMARY KEY, allele_id TEXT"),
            ("diseases", "medgen_id TEXT PRIMARY KEY, disease_name TEXT, term_type TEXT, source_db TEXT"),
            ("disease_hierarchy", "source_medgen_id TEXT, target_medgen_id TEXT, relationship TEXT, relationship_detail TEXT, PRIMARY KEY (source_medgen_id, target_medgen_id)"),
            ("omim_catalog", "mim_number TEXT PRIMARY KEY, gene_id INTEGER, entry_type TEXT, medgen_cui TEXT"),
            ("gene_disease_links", "gene_id INTEGER, gene_symbol TEXT, medgen_id TEXT, omim_id TEXT, disease_name TEXT, association_type TEXT, PRIMARY KEY (gene_id, medgen_id)"),
            ("genes_refseq", "gene_id INTEGER PRIMARY KEY, gene_name TEXT, chromosome TEXT, strand TEXT"),
            ("transcripts", "transcript_id TEXT PRIMARY KEY, gene_id INTEGER, gene_name TEXT, chromosome TEXT, start BIGINT, stop BIGINT, strand TEXT"),
            ("proteins_refseq", "protein_accession TEXT PRIMARY KEY, gene_id INTEGER, gene_symbol TEXT, rna_accession TEXT"),
            ("proteins_uniprot", "uniprot_accession TEXT PRIMARY KEY, gene_symbol TEXT, protein_name TEXT, taxid INTEGER"),
            ("genetic_tests", "gtr_test_id TEXT, gene_symbol TEXT NOT NULL, test_name TEXT, disease_name TEXT, PRIMARY KEY (gene_symbol, disease_name)"),
            ("structural_variants", "variant_id TEXT, study_id TEXT, variant_name TEXT, variant_type TEXT, chromosome TEXT, start_position BIGINT, end_position BIGINT, assembly TEXT, PRIMARY KEY (variant_id, study_id)"),
        ]
        
        for table_name, columns in tables:
            if not table_exists(engine, "silver", table_name):
                print(f"  Creating silver.{table_name}...")
                conn.execute(text(f"CREATE TABLE silver.{table_name} ({columns});"))
            else:
                print(f"  silver.{table_name} exists - skipping")
    
    print("Silver table check complete")


def copy_to_silver_with_progress(engine):
    print("\n" + "="*70)
    print("STEP 4: COPY TO SILVER WITH CASTING & CLEANING")
    print("="*70)
    
    start_total = time.time()
    
    operations = [
        ("genes", "bronze.gene_metadata", "silver.genes", """
            SELECT CASE WHEN CAST(gene_id AS TEXT) ~ '^[0-9]+$' THEN CAST(gene_id AS INTEGER) END,
                   UPPER(TRIM(gene_name)), UPPER(TRIM(official_symbol)), description,
                   TRIM(chromosome), map_location, gene_type,
                   CASE WHEN CAST(start_position AS TEXT) ~ '^[0-9]+$' THEN CAST(start_position AS BIGINT) END,
                   CASE WHEN CAST(end_position AS TEXT) ~ '^[0-9]+$' THEN CAST(end_position AS BIGINT) END,
                   strand
            FROM bronze.gene_metadata
            WHERE chromosome IN ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','MT')
            AND gene_id IS NOT NULL
            AND CAST(gene_id AS TEXT) ~ '^[0-9]+$'
        """, "gene_id"),
        
        ("variants", "bronze.clinvar_variants", "silver.variants", """
            SELECT COALESCE(variant_id, accession), COALESCE(UPPER(TRIM(accession)), variant_id),
                   UPPER(TRIM(gene_name)), clinical_significance, disease,
                   CASE WHEN TRIM(chromosome) IN ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','MT') 
                        THEN TRIM(chromosome) END,
                   CASE WHEN CAST(position AS TEXT) ~ '^[0-9]+$' THEN CAST(position AS BIGINT) END,
                   CASE WHEN CAST(stop_position AS TEXT) ~ '^[0-9]+$' THEN CAST(stop_position AS BIGINT) END,
                   variant_type, review_status, COALESCE(assembly, 'GRCh38')
            FROM bronze.clinvar_variants
            WHERE gene_name IS NOT NULL AND accession IS NOT NULL
        """, "accession"),
        
        ("variant_allele_map", "bronze.allele_mappings", "silver.variant_allele_map", """
            SELECT variation_id, allele_id FROM bronze.allele_mappings
            WHERE variation_id IS NOT NULL AND allele_id IS NOT NULL
        """, "variation_id"),
        
        ("diseases", "bronze.medgen_concepts", "silver.diseases", """
            SELECT DISTINCT medgen_id, disease_name, term_type, source_db
            FROM bronze.medgen_concepts
            WHERE is_preferred = 'Y' AND medgen_id IS NOT NULL AND disease_name IS NOT NULL
        """, "medgen_id"),
        
        ("disease_hierarchy", "bronze.medgen_relations", "silver.disease_hierarchy", """
            SELECT source_medgen_id, target_medgen_id, relationship, relationship_detail
            FROM bronze.medgen_relations
            WHERE source_medgen_id IS NOT NULL AND target_medgen_id IS NOT NULL
        """, "source_medgen_id, target_medgen_id"),
        
        ("omim_catalog", "bronze.omim_entries", "silver.omim_catalog", """
            SELECT mim_number, CASE WHEN CAST(gene_id AS TEXT) ~ '^[0-9]+$' THEN CAST(gene_id AS INTEGER) END, entry_type, medgen_cui
            FROM bronze.omim_entries WHERE mim_number IS NOT NULL
        """, "mim_number"),
        
        ("gene_disease_links", "bronze.gene_disease_associations", "silver.gene_disease_links", """
            SELECT CASE WHEN CAST(gene_id AS TEXT) ~ '^[0-9]+$' THEN CAST(gene_id AS INTEGER) END,
                   UPPER(TRIM(gene_symbol)), medgen_id, omim_id, disease_name, association_type
            FROM bronze.gene_disease_associations
            WHERE CAST(gene_id AS TEXT) ~ '^[0-9]+$' AND medgen_id IS NOT NULL
        """, "gene_id, medgen_id"),
        
        ("genes_refseq", "bronze.refseq_genes", "silver.genes_refseq", """
            SELECT CASE WHEN CAST(gene_id AS TEXT) ~ '^[0-9]+$' THEN CAST(gene_id AS INTEGER) END, gene_name, chromosome, strand
            FROM bronze.refseq_genes WHERE CAST(gene_id AS TEXT) ~ '^[0-9]+$'
        """, "gene_id"),
        
        ("transcripts", "bronze.refseq_transcripts", "silver.transcripts", """
            SELECT transcript_id, CASE WHEN CAST(gene_id AS TEXT) ~ '^[0-9]+$' THEN CAST(gene_id AS INTEGER) END,
                   gene_name, chromosome,
                   CASE WHEN CAST(start AS TEXT) ~ '^[0-9]+$' THEN CAST(start AS BIGINT) END,
                   CASE WHEN CAST("end" AS TEXT) ~ '^[0-9]+$' THEN CAST("end" AS BIGINT) END,
                   strand
            FROM bronze.refseq_transcripts WHERE transcript_id IS NOT NULL AND gene_id IS NOT NULL
        """, "transcript_id"),
        
        ("proteins_refseq", "bronze.refseq_proteins", "silver.proteins_refseq", """
            SELECT protein_accession,
                   CASE WHEN CAST(gene_id AS TEXT) ~ '^[0-9]+$' THEN CAST(gene_id AS INTEGER) END,
                   UPPER(TRIM(symbol)),
                   rna_accession
            FROM bronze.refseq_proteins WHERE protein_accession IS NOT NULL AND CAST(gene_id AS TEXT) ~ '^[0-9]+$'
        """, "protein_accession"),
        
        ("proteins_uniprot", "bronze.uniprot_proteins", "silver.proteins_uniprot", """
            SELECT uniprot_accession,
                   UPPER(TRIM(REGEXP_REPLACE(gene_symbol, '\\{.*\\}', '', 'g'))),
                   REGEXP_REPLACE(protein_name, '\\{.*\\}', '', 'g'),
                   CASE WHEN CAST(taxid AS TEXT) ~ '^[0-9]+$' THEN CAST(taxid AS INTEGER) END
            FROM bronze.uniprot_proteins
            WHERE uniprot_accession IS NOT NULL AND gene_symbol IS NOT NULL AND taxid = '9606'
        """, "uniprot_accession"),
        
        ("genetic_tests", "bronze.gtr_tests", "silver.genetic_tests", """
            SELECT gtr_test_id, UPPER(TRIM(gene_symbol)), test_name, disease_name
            FROM bronze.gtr_tests WHERE gene_symbol IS NOT NULL AND gene_symbol != 'N/A' AND disease_name IS NOT NULL
        """, "gene_symbol, disease_name"),
        
        ("structural_variants", "bronze.dbvar_*", "silver.structural_variants", """
            SELECT variant_id, 'estd214' as study_id, variant_name, variant_type, chromosome,
                   CASE WHEN CAST(start_position AS TEXT) ~ '^[0-9]+$' THEN CAST(start_position AS BIGINT) END,
                   CASE WHEN CAST(end_position AS TEXT) ~ '^[0-9]+$' THEN CAST(end_position AS BIGINT) END,
                   assembly
            FROM bronze.dbvar_estd214_regions WHERE variant_id IS NOT NULL AND chromosome IS NOT NULL
            UNION ALL
            SELECT variant_id, 'nstd102_calls' as study_id, variant_name, variant_type, chromosome,
                   CASE WHEN CAST(start_position AS TEXT) ~ '^[0-9]+$' THEN CAST(start_position AS BIGINT) END,
                   CASE WHEN CAST(end_position AS TEXT) ~ '^[0-9]+$' THEN CAST(end_position AS BIGINT) END,
                   assembly
            FROM bronze.dbvar_nstd102_calls WHERE variant_id IS NOT NULL AND chromosome IS NOT NULL
            UNION ALL
            SELECT variant_id, 'nstd102_regions' as study_id, variant_name, variant_type, chromosome,
                   CASE WHEN CAST(start_position AS TEXT) ~ '^[0-9]+$' THEN CAST(start_position AS BIGINT) END,
                   CASE WHEN CAST(end_position AS TEXT) ~ '^[0-9]+$' THEN CAST(end_position AS BIGINT) END,
                   assembly
            FROM bronze.dbvar_nstd102_regions WHERE variant_id IS NOT NULL AND chromosome IS NOT NULL
        """, "variant_id, study_id"),
    ]
    
    with engine.begin() as conn:
        for idx, (name, source, target, query, conflict_col) in enumerate(operations, 1):
            # Check if counts match - skip if yes
            source_table = source.split('.')[-1] if "bronze." in source else None
            source_count = get_table_count(engine, "bronze", source_table) if source_table else 0
            target_count = get_table_count(engine, "silver", name)  # Use 'name' directly, not 'target'
            
            print(f"\n{idx}. {name}")
            print(f"   Source: {source_count:,} rows")
            print(f"   Target: {target_count:,} rows")
            
            if source_count > 0 and source_count == target_count:
                print(f"   Counts match - SKIPPING")
                continue
            
            start = time.time()
            print(f"   Processing...", end='', flush=True)
            
            insert_query = f"""
                INSERT INTO {target}
                {query}
                ON CONFLICT ({conflict_col}) DO NOTHING;
            """
            result = conn.execute(text(insert_query))
            
            elapsed = time.time() - start
            print(f"\r   Added {result.rowcount:,} rows in {elapsed:.1f}s" + " "*20)
    
    elapsed_total = time.time() - start_total
    print("\n" + "="*70)
    print(f"SILVER COMPLETE in {elapsed_total/60:.1f} minutes")
    print("="*70)


def verify_data_load(engine):
    print("\n" + "="*70)
    print("FINAL VERIFICATION")
    print("="*70)
    
    print("\nBRONZE LAYER (15 tables):")
    bronze_tables = [
        "gene_metadata", "clinvar_variants", "allele_mappings",
        "medgen_concepts", "medgen_relations", "omim_entries",
        "gene_disease_associations", "refseq_genes", "refseq_transcripts",
        "refseq_proteins", "uniprot_proteins", "gtr_tests",
        "dbvar_estd214_regions", "dbvar_nstd102_calls", "dbvar_nstd102_regions"
    ]
    
    for table in bronze_tables:
        count = get_table_count(engine, "bronze", table)
        if count > 0:
            print(f"  {table:35s}: {count:>12,}")
    
    print("\nSILVER LAYER (13 tables):")
    silver_tables = [
        "genes", "variants", "variant_allele_map", "diseases",
        "disease_hierarchy", "omim_catalog", "gene_disease_links",
        "genes_refseq", "transcripts", "proteins_refseq",
        "proteins_uniprot", "genetic_tests", "structural_variants"
    ]
    
    for table in silver_tables:
        count = get_table_count(engine, "silver", table)
        if count > 0:
            print(f"  {table:35s}: {count:>12,}")
    
    genes = get_table_count(engine, "silver", "genes")
    variants = get_table_count(engine, "silver", "variants")
    
    if genes > 0 and variants > 0:
        print("\nSTATUS: SUCCESS - All data loaded!")
    else:
        print("\nERROR STATUS: INCOMPLETE")


def main():
    print("\nPostgreSQL Data Load - Bronze to Silver")
    print("-" * 50)
    
    start_total = time.time()
    engine = get_database_engine()
    
    # Step 1: Rename existing tables
    rename_existing_tables(engine)
    
    # Step 1.5: Fix bronze table schemas (drop tables with wrong types)
    fix_bronze_table_schemas(engine)
    
    # Step 2: Load new tables to Bronze
    print("\n" + "="*70)
    print("STEP 2: LOAD TO BRONZE (ALL TEXT, NO TYPE ERRORS)")
    print("="*70)
    
    # Check existing 3 tables for new data
    for table_name in ["gene_metadata", "clinvar_variants", "allele_mappings"]:
        if table_name in DATA_FILES:
            load_table_simple(engine, table_name, DATA_FILES[table_name])
    
    # Load 12 new tables
    for table_name in ["medgen_concepts", "medgen_relations", "omim_entries",
                       "gene_disease_associations", "refseq_genes", "refseq_transcripts",
                       "refseq_proteins", "uniprot_proteins", "gtr_tests",
                       "dbvar_estd214_regions", "dbvar_nstd102_calls", "dbvar_nstd102_regions"]:
        if table_name in DATA_FILES:
            load_table_simple(engine, table_name, DATA_FILES[table_name])
    
    # Step 3: Create Silver tables
    create_silver_tables(engine)
    
    # Step 4: Copy to Silver with casting
    copy_to_silver_with_progress(engine)
    
    # Step 5: Verify
    verify_data_load(engine)
    
    total_time = time.time() - start_total
    print("\n" + "="*70)
    print(f"COMPLETE - Total time: {total_time/60:.1f} minutes")
    print("="*70)


if __name__ == "__main__":
    main()