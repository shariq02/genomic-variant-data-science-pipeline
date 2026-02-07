"""
SMART GOLD TABLE LOADER WITH AUTO-CHUNKING
- Splits large CSVs (>150MB) into chunks
- Loads small CSVs directly
- Uses psql \COPY (no memory issues)
- Drops/recreates tables
- Deletes chunks after successful load
- One table at a time with progress
"""

import os
import subprocess
from pathlib import Path
import time
from dotenv import load_dotenv

load_dotenv()

# ====================================================================
# CONFIGURATION
# ====================================================================

PSQL_PATH = r"C:\Program Files\PostgreSQL\18\bin\psql.exe"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "genome_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

BASE_DIR = Path(r"C:\Users\Sharique\Desktop\Self_Project\GitHub_Project\genomic-variant-data-science-pipeline\data\processed")
CHUNKS_DIR = BASE_DIR / "chunks"
CHUNKS_DIR.mkdir(exist_ok=True)

CHUNK_SIZE_MB = 150
CHUNK_ROWS = 500000  # ~150MB per chunk for typical data

# Table schemas
TABLES = {
    "clinical_ml_features": {
        "primary_key": "variant_id",
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT,
            gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT,
            target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT,
            clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT,
            has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT,
            combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT,
            is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT,
            is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT,
            conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT,
            is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT,
            is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT,
            has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT,
            x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT,
            is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT,
            gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT,
            gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT,
            gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT,
            gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, gene_avg_review_quality TEXT,
            gene_has_quality_annotations TEXT, gene_missense_count TEXT, gene_frameshift_count TEXT,
            gene_nonsense_count TEXT, gene_splice_count TEXT, gene_lof_variant_ratio TEXT"""
    },
    "disease_ml_features": {
        "primary_key": "variant_id",
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT,
            is_vus TEXT, clinical_significance_simple TEXT, disease_enriched TEXT, primary_disease TEXT,
            disease_name_enriched TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT,
            has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT,
            disease_is_well_annotated TEXT, disease_name_is_generic TEXT, disease_count TEXT,
            omim_disease_count TEXT, disease_count_category TEXT, is_disease_associated TEXT,
            is_multi_disease_gene TEXT, disease_association_strength TEXT, is_omim_gene TEXT,
            variant_disease_link_quality TEXT, disease_total_variants TEXT, disease_pathogenic_variants TEXT,
            disease_benign_variants TEXT, disease_vus_variants TEXT, disease_pathogenic_ratio TEXT,
            disease_gene_count TEXT, is_polygenic_disease TEXT, disease_complexity TEXT,
            disease_complexity_score TEXT, polygenic_risk_contribution TEXT,
            disease_has_high_pathogenic_burden TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT,
            gene_benign_count TEXT, gene_high_quality_count TEXT, gene_disease_diversity TEXT,
            gene_clinical_utility_score TEXT, gene_priority_tier TEXT, is_clinically_actionable TEXT,
            is_research_candidate TEXT, has_drug_development_potential TEXT, gene_annotation_score TEXT,
            has_excellent_annotation TEXT, annotation_priority_level TEXT, gene_omim_variants TEXT,
            gene_mondo_variants TEXT, gene_well_annotated_variants TEXT, disease_gene_relevance_score TEXT,
            enhanced_gene_priority_tier TEXT, is_cancer_gene_variant TEXT, is_neurological_gene_variant TEXT,
            is_cardiovascular_gene_variant TEXT, is_metabolic_gene_variant TEXT,
            is_rare_disease_gene_variant TEXT, is_highly_actionable TEXT"""
    },
    "pharmacogene_ml_features": {
        "primary_key": "variant_id",
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_symbol TEXT,
            validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_mentions_drug TEXT,
            is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT,
            variant_type TEXT, is_missense_variant TEXT, is_loss_of_function TEXT, protein_impact_category TEXT,
            mutation_severity_score TEXT, pathogenicity_score TEXT, is_pharmacogene TEXT,
            pharmacogene_category TEXT, pharmacogene_evidence_level TEXT, drug_metabolism_role TEXT,
            is_drug_target TEXT, is_metabolizing_enzyme TEXT, metabolizing_enzyme_type TEXT, is_enzyme TEXT,
            is_drug_transporter TEXT, is_kinase TEXT, is_phosphatase TEXT, is_receptor TEXT, is_gpcr TEXT,
            is_transporter TEXT, drug_target_category TEXT, druggability_score TEXT,
            enhanced_druggability_score TEXT, drug_response_impact TEXT, is_metabolizer_variant TEXT,
            metabolizer_phenotype_risk TEXT, is_transporter_variant TEXT, transporter_impact_level TEXT,
            is_kinase_inhibitor_target TEXT, kinase_variant_therapeutic_relevance TEXT, pharmgkb_source TEXT,
            pharmgkb_evidence TEXT, pharmgkb_source_count TEXT, has_pharmgkb_annotation TEXT,
            gene_pharmacogene_variants TEXT, gene_drug_interaction_variants TEXT, gene_metabolizer_variants TEXT,
            gene_transporter_variants TEXT, gene_pharmacogene_pathogenic TEXT,
            gene_has_multiple_drug_variants TEXT, gene_pharmacogene_priority TEXT, gene_pharmacogene_burden TEXT,
            gene_avg_druggability TEXT"""
    },
    "structural_variant_ml_features": {
        "primary_key": "sv_id",
        "columns": """sv_id TEXT, study_id TEXT, variant_name TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT,
            assembly TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT,
            has_gene_overlap TEXT, affected_gene_count TEXT, affected_genes TEXT, complete_overlap_genes TEXT,
            major_overlap_genes TEXT, is_multi_gene_sv TEXT, pharmacogenes_affected TEXT, kinases_affected TEXT,
            receptors_affected TEXT, omim_genes_affected TEXT, affects_pharmacogenes TEXT,
            affects_omim_genes TEXT, genes_lost TEXT, genes_gained TEXT, avg_gene_overlap_pct TEXT,
            max_gene_overlap_pct TEXT, gene_impact_severity TEXT, size_impact_score TEXT, type_impact_score TEXT,
            gene_impact_score TEXT, sv_pathogenicity_score TEXT, predicted_sv_pathogenicity TEXT,
            is_high_risk_sv TEXT, is_autosomal TEXT, chromosome_impact_modifier TEXT, chr_total_svs TEXT,
            chr_gene_affecting_svs TEXT, chr_high_risk_svs TEXT, chr_avg_sv_size TEXT, chr_avg_genes_per_sv TEXT,
            chr_gene_disruption_rate TEXT, study_total_svs TEXT, study_chr_count TEXT,
            study_gene_affecting_svs TEXT, study_quality TEXT"""
    },
    "variant_impact_ml_features": {
        "primary_key": "variant_id",
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, validated_gene_symbol TEXT,
            gene_is_validated TEXT, gene_description_length TEXT, is_pathogenic TEXT, is_benign TEXT,
            is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT,
            is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, is_snv TEXT,
            alters_protein_length TEXT, protein_change TEXT, cdna_change TEXT, has_functional_domain TEXT,
            domain_count TEXT, domain_type_count TEXT, has_multiple_domain_types TEXT, has_zinc_finger TEXT,
            has_kinase_domain TEXT, has_receptor_domain TEXT, has_sh2_domain TEXT, has_sh3_domain TEXT,
            has_ph_domain TEXT, affects_functional_domain TEXT, domain_impact_severity TEXT,
            is_missense_in_conserved_domain TEXT, phylop_score TEXT, phastcons_score TEXT, gerp_score TEXT,
            cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT,
            is_likely_deleterious TEXT, conservation_impact_class TEXT, mutation_severity_score TEXT,
            pathogenicity_score TEXT, functional_impact_score TEXT, protein_impact_category TEXT,
            is_high_impact TEXT, is_very_high_impact TEXT, is_conservation_constrained TEXT,
            is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT,
            has_protein_annotation TEXT, has_conservation_scores TEXT, annotation_completeness_score TEXT,
            splice_variant_type TEXT, splice_impact_severity TEXT, predicted_splicing_outcome TEXT,
            splice_risk_score TEXT, is_critical_splice_variant TEXT, splice_site_is_well_defined TEXT,
            gene_total_variants TEXT, gene_domain_affecting_variants TEXT, gene_splice_variants TEXT,
            gene_critical_splice_variants TEXT, gene_lof_variants TEXT, gene_conserved_domain_missense TEXT,
            gene_avg_functional_impact TEXT, gene_max_functional_impact TEXT, gene_avg_phylop TEXT,
            gene_avg_cadd TEXT, gene_has_high_impact_burden TEXT, gene_variant_impact_class TEXT,
            gene_annotation_quality TEXT, gene_is_well_annotated TEXT"""
    }
}

# ML dataset tables (same structure as variant_impact)
ML_TABLES = [
    "ml_dataset_variants_train",
    "ml_dataset_variants_validation", 
    "ml_dataset_variants_test"
]

ML_SV_TABLES = [
    "ml_dataset_structural_variants_train",
    "ml_dataset_structural_variants_validation",
    "ml_dataset_structural_variants_test"
]

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def run_psql_command(sql_command):
    """Execute SQL via psql"""
    cmd = [
        PSQL_PATH,
        "-h", POSTGRES_HOST,
        "-p", POSTGRES_PORT,
        "-U", POSTGRES_USER,
        "-d", POSTGRES_DB,
        "-c", sql_command
    ]
    
    # Set password via environment variable
    env = os.environ.copy()
    if POSTGRES_PASSWORD:
        env['PGPASSWORD'] = POSTGRES_PASSWORD
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise Exception(f"psql error: {result.stderr}")
    return result.stdout

def run_psql_file(sql_file):
    """Execute SQL file via psql"""
    cmd = [
        PSQL_PATH,
        "-h", POSTGRES_HOST,
        "-p", POSTGRES_PORT,
        "-U", POSTGRES_USER,
        "-d", POSTGRES_DB,
        "-f", str(sql_file)
    ]
    
    # Set password via environment variable
    env = os.environ.copy()
    if POSTGRES_PASSWORD:
        env['PGPASSWORD'] = POSTGRES_PASSWORD
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise Exception(f"psql error: {result.stderr}")
    return result.stdout

def get_file_size_mb(file_path):
    """Get file size in MB"""
    return file_path.stat().st_size / (1024 * 1024)

def split_csv_into_chunks(csv_file, table_name):
    """Split large CSV into chunks, return list of chunk files"""
    file_size = get_file_size_mb(csv_file)
    
    if file_size <= CHUNK_SIZE_MB:
        print(f"    File is small ({file_size:.1f}MB) - no chunking needed")
        return [csv_file]
    
    print(f"    File is large ({file_size:.1f}MB) - splitting into chunks...")
    
    chunks = []
    chunk_num = 0
    
    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
        header = f.readline()
        
        chunk_file = None
        rows_in_chunk = 0
        
        for line in f:
            if rows_in_chunk == 0:
                chunk_num += 1
                chunk_file = CHUNKS_DIR / f"{table_name}_chunk_{chunk_num}.csv"
                chunks.append(chunk_file)
                
                chunk_handle = open(chunk_file, 'w', encoding='utf-8')
                chunk_handle.write(header)
            
            chunk_handle.write(line)
            rows_in_chunk += 1
            
            if rows_in_chunk >= CHUNK_ROWS:
                chunk_handle.close()
                chunk_size = get_file_size_mb(chunk_file)
                print(f"      Chunk {chunk_num}: {rows_in_chunk:,} rows ({chunk_size:.1f}MB)")
                rows_in_chunk = 0
        
        if rows_in_chunk > 0:
            chunk_handle.close()
            chunk_size = get_file_size_mb(chunk_file)
            print(f"      Chunk {chunk_num}: {rows_in_chunk:,} rows ({chunk_size:.1f}MB)")
    
    print(f"    Created {len(chunks)} chunks")
    return chunks

def drop_and_create_table(table_name, schema_def):
    """Drop and recreate table"""
    print(f"    Dropping and recreating table...")
    run_psql_command(f"DROP TABLE IF EXISTS gold.{table_name} CASCADE")
    run_psql_command(f"CREATE TABLE gold.{table_name} ({schema_def})")

def load_chunk_with_psql(chunk_file, table_name):
    """Load single chunk using psql \copy"""
    # Create temp SQL file
    temp_sql = CHUNKS_DIR / "temp_load.sql"
    with open(temp_sql, 'w') as f:
        # Use forward slashes for Windows paths
        chunk_path = str(chunk_file).replace('\\', '/')
        f.write(f"\\copy gold.{table_name} FROM '{chunk_path}' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')")
    
    run_psql_file(temp_sql)
    temp_sql.unlink()

def get_table_count(table_name):
    """Get row count from table"""
    try:
        result = run_psql_command(f"SELECT COUNT(*) FROM gold.{table_name}")
        # Parse output
        lines = result.strip().split('\n')
        for line in lines:
            if line.strip().isdigit():
                return int(line.strip())
        return 0
    except:
        return 0

def create_index(table_name, primary_key):
    """Create index on primary key"""
    index_name = f"idx_{table_name}_{primary_key}"
    print(f"    Creating index on {primary_key}...")
    try:
        run_psql_command(f"CREATE INDEX {index_name} ON gold.{table_name}({primary_key})")
    except:
        print(f"    Index already exists")

# ====================================================================
# MAIN LOAD FUNCTION
# ====================================================================

def load_single_table(table_name, table_config):
    """Load single table with auto-chunking"""
    
    csv_file = BASE_DIR / f"{table_name}.csv"
    
    print(f"\n{'='*80}")
    print(f"TABLE: {table_name}")
    print(f"{'='*80}")
    
    if not csv_file.exists():
        print(f"  ERROR: CSV not found")
        return False
    
    file_size = get_file_size_mb(csv_file)
    print(f"  File size: {file_size:.1f}MB")
    
    start_time = time.time()
    
    try:
        # Step 1: Drop and recreate table
        drop_and_create_table(table_name, table_config["columns"])
        
        # Step 2: Split into chunks if needed
        chunks = split_csv_into_chunks(csv_file, table_name)
        
        # Step 3: Load each chunk
        print(f"  Loading {len(chunks)} chunk(s)...")
        for i, chunk in enumerate(chunks, 1):
            chunk_size = get_file_size_mb(chunk)
            print(f"    Chunk {i}/{len(chunks)}: {chunk_size:.1f}MB...", end=" ", flush=True)
            
            load_chunk_with_psql(chunk, table_name)
            
            # Delete chunk immediately after successful load
            if chunk != csv_file:  # Don't delete original file
                chunk.unlink()
                print("loaded and deleted")
            else:
                print("loaded")
        
        # Step 4: Verify
        final_count = get_table_count(table_name)
        elapsed = time.time() - start_time
        
        print(f"  Loaded {final_count:,} rows in {elapsed:.1f}s ({int(final_count/elapsed):,} rows/s)")
        
        # Step 5: Create index
        create_index(table_name, table_config["primary_key"])
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        return False

# ====================================================================
# MAIN
# ====================================================================

def main():
    print("="*80)
    print("SMART GOLD TABLE LOADER")
    print("="*80)
    print(f"Database: {POSTGRES_DB}.gold")
    print(f"Chunk size: {CHUNK_SIZE_MB}MB")
    print(f"Method: psql with \\copy")
    print("="*80)
    
    if not POSTGRES_PASSWORD:
        print("\nERROR: PostgreSQL password not found in .env file")
        print("Please add: POSTGRES_PASSWORD=your-password")
        return
    
    # Ensure gold schema exists
    run_psql_command("CREATE SCHEMA IF NOT EXISTS gold")
    print("Gold schema: OK\n")
    
    total_start = time.time()
    successful = []
    
    # Load main tables
    for table_name, table_config in TABLES.items():
        success = load_single_table(table_name, table_config)
        if success:
            successful.append(table_name)
    
    # Load ML dataset tables (reuse structures)
    for ml_table in ML_TABLES:
        config = {"primary_key": "variant_id", "columns": TABLES["variant_impact_ml_features"]["columns"]}
        success = load_single_table(ml_table, config)
        if success:
            successful.append(ml_table)
    
    for ml_sv_table in ML_SV_TABLES:
        config = {"primary_key": "sv_id", "columns": TABLES["structural_variant_ml_features"]["columns"]}
        success = load_single_table(ml_sv_table, config)
        if success:
            successful.append(ml_sv_table)
    
    total_elapsed = time.time() - total_start
    
    # Summary
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"Total time: {int(total_elapsed)}s ({total_elapsed/60:.1f} minutes)")
    print(f"Tables completed: {len(successful)}/11")
    
    if len(successful) == 11:
        print("\nSUCCESS! All 11 tables loaded to GOLD schema")

    else:
        print(f"\n{11 - len(successful)} tables failed")
    
    print("="*80)

if __name__ == "__main__":
    main()
