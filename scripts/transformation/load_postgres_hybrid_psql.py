"""
HYBRID POSTGRES LOADER - PSQL WITH SMART CHANGE DETECTION
Uses psql copy for speed + smart logic for change detection
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import os
import subprocess
import csv
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

CHUNK_SIZE = 100000

load_dotenv()

PSQL_PATH = r"C:\Program Files\PostgreSQL\18\bin\psql.exe"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "genome_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
POSTGRES_CHECKPOINT = PROCESSED_DIR / ".postgres_checkpoint.json"



TABLES = {
    "clinical_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, gene_avg_review_quality TEXT, gene_has_quality_annotations TEXT, gene_missense_count TEXT, gene_frameshift_count TEXT, gene_nonsense_count TEXT, gene_splice_count TEXT, gene_lof_variant_ratio TEXT"""
    },
    "disease_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, disease_enriched TEXT, primary_disease TEXT, disease_name_enriched TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_is_well_annotated TEXT, disease_name_is_generic TEXT, disease_count TEXT, omim_disease_count TEXT, disease_count_category TEXT, is_disease_associated TEXT, is_multi_disease_gene TEXT, disease_association_strength TEXT, is_omim_gene TEXT, variant_disease_link_quality TEXT, disease_total_variants TEXT, disease_pathogenic_variants TEXT, disease_benign_variants TEXT, disease_vus_variants TEXT, disease_pathogenic_ratio TEXT, disease_gene_count TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, disease_complexity_score TEXT, polygenic_risk_contribution TEXT, disease_has_high_pathogenic_burden TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_high_quality_count TEXT, gene_disease_diversity TEXT, gene_clinical_utility_score TEXT, gene_priority_tier TEXT, is_clinically_actionable TEXT, is_research_candidate TEXT, has_drug_development_potential TEXT, gene_annotation_score TEXT, has_excellent_annotation TEXT, annotation_priority_level TEXT, gene_omim_variants TEXT, gene_mondo_variants TEXT, gene_well_annotated_variants TEXT, disease_gene_relevance_score TEXT, enhanced_gene_priority_tier TEXT, is_cancer_gene_variant TEXT, is_neurological_gene_variant TEXT, is_cardiovascular_gene_variant TEXT, is_metabolic_gene_variant TEXT, is_rare_disease_gene_variant TEXT, is_highly_actionable TEXT"""
    },
    "pharmacogene_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_symbol TEXT, validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_mentions_drug TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT, is_loss_of_function TEXT, protein_impact_category TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_evidence_level TEXT, drug_metabolism_role TEXT, is_drug_target TEXT, is_metabolizing_enzyme TEXT, metabolizing_enzyme_type TEXT, is_enzyme TEXT, is_drug_transporter TEXT, is_kinase TEXT, is_phosphatase TEXT, is_receptor TEXT, is_gpcr TEXT, is_transporter TEXT, drug_target_category TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, drug_response_impact TEXT, is_metabolizer_variant TEXT, metabolizer_phenotype_risk TEXT, is_transporter_variant TEXT, transporter_impact_level TEXT, is_kinase_inhibitor_target TEXT, kinase_variant_therapeutic_relevance TEXT, pharmgkb_source TEXT, pharmgkb_evidence TEXT, pharmgkb_source_count TEXT, has_pharmgkb_annotation TEXT, gene_pharmacogene_variants TEXT, gene_drug_interaction_variants TEXT, gene_metabolizer_variants TEXT, gene_transporter_variants TEXT, gene_pharmacogene_pathogenic TEXT, gene_has_multiple_drug_variants TEXT, gene_pharmacogene_priority TEXT, gene_pharmacogene_burden TEXT, gene_avg_druggability TEXT"""
    },
    "structural_variant_ml_features": {
        "columns": """sv_id TEXT, study_id TEXT, variant_name TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, assembly TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, has_gene_overlap TEXT, affected_gene_count TEXT, affected_genes TEXT, complete_overlap_genes TEXT, major_overlap_genes TEXT, is_multi_gene_sv TEXT, pharmacogenes_affected TEXT, kinases_affected TEXT, receptors_affected TEXT, omim_genes_affected TEXT, affects_pharmacogenes TEXT, affects_omim_genes TEXT, genes_lost TEXT, genes_gained TEXT, avg_gene_overlap_pct TEXT, max_gene_overlap_pct TEXT, gene_impact_severity TEXT, size_impact_score TEXT, type_impact_score TEXT, gene_impact_score TEXT, sv_pathogenicity_score TEXT, predicted_sv_pathogenicity TEXT, is_high_risk_sv TEXT, is_autosomal TEXT, chromosome_impact_modifier TEXT, chr_total_svs TEXT, chr_gene_affecting_svs TEXT, chr_high_risk_svs TEXT, chr_avg_sv_size TEXT, chr_avg_genes_per_sv TEXT, chr_gene_disruption_rate TEXT, study_total_svs TEXT, study_chr_count TEXT, study_gene_affecting_svs TEXT, study_quality TEXT"""
    },
    "variant_impact_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_length TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, is_snv TEXT, alters_protein_length TEXT, protein_change TEXT, cdna_change TEXT, has_functional_domain TEXT, domain_count TEXT, domain_type_count TEXT, has_multiple_domain_types TEXT, has_zinc_finger TEXT, has_kinase_domain TEXT, has_receptor_domain TEXT, has_sh2_domain TEXT, has_sh3_domain TEXT, has_ph_domain TEXT, affects_functional_domain TEXT, domain_impact_severity TEXT, is_missense_in_conserved_domain TEXT, phylop_score TEXT, phastcons_score TEXT, gerp_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, conservation_impact_class TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, functional_impact_score TEXT, protein_impact_category TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_conservation_constrained TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_protein_annotation TEXT, has_conservation_scores TEXT, annotation_completeness_score TEXT, splice_variant_type TEXT, splice_impact_severity TEXT, predicted_splicing_outcome TEXT, splice_risk_score TEXT, is_critical_splice_variant TEXT, splice_site_is_well_defined TEXT, gene_total_variants TEXT, gene_domain_affecting_variants TEXT, gene_splice_variants TEXT, gene_critical_splice_variants TEXT, gene_lof_variants TEXT, gene_conserved_domain_missense TEXT, gene_avg_functional_impact TEXT, gene_max_functional_impact TEXT, gene_avg_phylop TEXT, gene_avg_cadd TEXT, gene_has_high_impact_burden TEXT, gene_variant_impact_class TEXT, gene_annotation_quality TEXT, gene_is_well_annotated TEXT"""
    },
    "ml_dataset_variants_train": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, variant_type TEXT, affects_functional_domain TEXT, has_kinase_domain TEXT, phastcons_score TEXT, gerp_score TEXT, functional_impact_score TEXT, domain_type_count TEXT, domain_impact_severity TEXT, conservation_impact_class TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, is_drug_target TEXT, is_kinase TEXT, is_receptor TEXT, is_transporter TEXT, is_metabolizing_enzyme TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, gene_pharmacogene_priority TEXT, gene_avg_druggability TEXT, disease_enriched TEXT, primary_disease TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_count TEXT, disease_gene_count TEXT, disease_pathogenic_ratio TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, is_clinically_actionable TEXT, has_drug_development_potential TEXT, is_cancer_gene_variant TEXT, is_neurological_gene_variant TEXT, is_cardiovascular_gene_variant TEXT, is_metabolic_gene_variant TEXT, is_rare_disease_gene_variant TEXT, disease_gene_relevance_score TEXT, disease_complexity_score TEXT"""
    },
    "ml_dataset_variants_validation": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, variant_type TEXT, affects_functional_domain TEXT, has_kinase_domain TEXT, phastcons_score TEXT, gerp_score TEXT, functional_impact_score TEXT, domain_type_count TEXT, domain_impact_severity TEXT, conservation_impact_class TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, is_drug_target TEXT, is_kinase TEXT, is_receptor TEXT, is_transporter TEXT, is_metabolizing_enzyme TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, gene_pharmacogene_priority TEXT, gene_avg_druggability TEXT, disease_enriched TEXT, primary_disease TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_count TEXT, disease_gene_count TEXT, disease_pathogenic_ratio TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, is_clinically_actionable TEXT, has_drug_development_potential TEXT, is_cancer_gene_variant TEXT, is_neurological_gene_variant TEXT, is_cardiovascular_gene_variant TEXT, is_metabolic_gene_variant TEXT, is_rare_disease_gene_variant TEXT, disease_gene_relevance_score TEXT, disease_complexity_score TEXT"""
    },
    "ml_dataset_variants_test": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, variant_type TEXT, affects_functional_domain TEXT, has_kinase_domain TEXT, phastcons_score TEXT, gerp_score TEXT, functional_impact_score TEXT, domain_type_count TEXT, domain_impact_severity TEXT, conservation_impact_class TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, is_drug_target TEXT, is_kinase TEXT, is_receptor TEXT, is_transporter TEXT, is_metabolizing_enzyme TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, gene_pharmacogene_priority TEXT, gene_avg_druggability TEXT, disease_enriched TEXT, primary_disease TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_count TEXT, disease_gene_count TEXT, disease_pathogenic_ratio TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, is_clinically_actionable TEXT, has_drug_development_potential TEXT, is_cancer_gene_variant TEXT, is_neurological_gene_variant TEXT, is_cardiovascular_gene_variant TEXT, is_metabolic_gene_variant TEXT, is_rare_disease_gene_variant TEXT, disease_gene_relevance_score TEXT, disease_complexity_score TEXT"""
    },
    "ml_dataset_structural_variants_train": {
        "columns": """sv_id TEXT, study_id TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, has_gene_overlap TEXT, affected_gene_count TEXT, is_multi_gene_sv TEXT, affects_pharmacogenes TEXT, affects_omim_genes TEXT, gene_impact_severity TEXT, size_impact_score TEXT, type_impact_score TEXT, gene_impact_score TEXT, sv_pathogenicity_score TEXT, predicted_sv_pathogenicity TEXT, is_high_risk_sv TEXT"""
    },
    "ml_dataset_structural_variants_validation": {
        "columns": """sv_id TEXT, study_id TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, has_gene_overlap TEXT, affected_gene_count TEXT, is_multi_gene_sv TEXT, affects_pharmacogenes TEXT, affects_omim_genes TEXT, gene_impact_severity TEXT, size_impact_score TEXT, type_impact_score TEXT, gene_impact_score TEXT, sv_pathogenicity_score TEXT, predicted_sv_pathogenicity TEXT, is_high_risk_sv TEXT"""
    },
    "ml_dataset_structural_variants_test": {
        "columns": """sv_id TEXT, study_id TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, has_gene_overlap TEXT, affected_gene_count TEXT, is_multi_gene_sv TEXT, affects_pharmacogenes TEXT, affects_omim_genes TEXT, gene_impact_severity TEXT, size_impact_score TEXT, type_impact_score TEXT, gene_impact_score TEXT, sv_pathogenicity_score TEXT, predicted_sv_pathogenicity TEXT, is_high_risk_sv TEXT"""
    }
}

def load_checkpoint():
    if POSTGRES_CHECKPOINT.exists():
        with open(POSTGRES_CHECKPOINT, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint):
    with open(POSTGRES_CHECKPOINT, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def get_csv_info(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        row_count = sum(1 for _ in f)
    file_size_mb = csv_file.stat().st_size / (1024 * 1024)
    return headers, row_count, file_size_mb

def get_csv_hash(csv_file, sample_size=10000):
    hash_md5 = hashlib.md5()
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            hash_md5.update(','.join(row).encode('utf-8'))
    return hash_md5.hexdigest()

def run_psql_command(sql):
    env = os.environ.copy()
    env['PGPASSWORD'] = POSTGRES_PASSWORD
    
    cmd = [
        PSQL_PATH,
        '-h', POSTGRES_HOST,
        '-p', POSTGRES_PORT,
        '-U', POSTGRES_USER,
        '-d', POSTGRES_DB,
        '-c', sql,
        '-t'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise Exception(f"psql error: {result.stderr}")
    return result.stdout

def run_psql_file(sql_file):
    env = os.environ.copy()
    env['PGPASSWORD'] = POSTGRES_PASSWORD
    
    cmd = [
        PSQL_PATH,
        '-h', POSTGRES_HOST,
        '-p', POSTGRES_PORT,
        '-U', POSTGRES_USER,
        '-d', POSTGRES_DB,
        '-f', str(sql_file)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise Exception(f"psql error: {result.stderr} | stdout: {result.stdout}")
    if result.stderr and "ERROR" in result.stderr.upper():
        raise Exception(f"psql ERROR in copy: {result.stderr}")

def table_exists(table_name):
    try:
        result = run_psql_command(f"SELECT COUNT(*) FROM gold.{table_name}")
        return True
    except:
        return False

def get_table_count(table_name):
    try:
        result = run_psql_command(f"SELECT COUNT(*) FROM gold.{table_name}")
        return int(result.strip())
    except:
        return 0

def load_csv_psql(csv_file, table_name):
    temp_sql = PROCESSED_DIR / "temp_load.sql"
    csv_path = str(csv_file).replace("\\", "/")
    
    with open(temp_sql, "w") as f:
        f.write(f"\\copy gold.{table_name} FROM '{csv_path}' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')")
    
    run_psql_file(temp_sql)
    temp_sql.unlink()

def load_csv_chunked_psycopg2(csv_file, table_name, headers):
    print(f"  Falling back to psycopg2 chunked insert ({CHUNK_SIZE:,} rows/chunk)...")
    
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = False
    cursor = conn.cursor()
    
    placeholders = ','.join(['%s'] * len(headers))
    insert_sql = f'INSERT INTO gold.{table_name} VALUES ({placeholders})'
    
    total = 0
    chunk_num = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        
        batch = []
        chunk_start = time.time()
        
        for row in reader:
            batch.append(row)
            
            if len(batch) >= CHUNK_SIZE:
                chunk_num += 1
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                chunk_time = time.time() - chunk_start
                rows_sec = len(batch) / chunk_time if chunk_time > 0 else 0
                print(f"    Chunk {chunk_num}: {total:,} rows ({chunk_time:.1f}s, {rows_sec:,.0f} rows/sec)")
                batch = []
                chunk_start = time.time()
        
        if batch:
            chunk_num += 1
            cursor.executemany(insert_sql, batch)
            conn.commit()
            total += len(batch)
            chunk_time = time.time() - chunk_start
            rows_sec = len(batch) / chunk_time if chunk_time > 0 else 0
            print(f"    Chunk {chunk_num}: {total:,} rows ({chunk_time:.1f}s, {rows_sec:,.0f} rows/sec)")
    
    cursor.close()
    conn.close()
    return total

def load_table(table_name, table_config, checkpoint):
    csv_file = PROCESSED_DIR / f"{table_name}.csv"
    
    print(f"\n{table_name}:")
    table_start = datetime.now()
    print(f"  Start: {table_start.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not csv_file.exists():
        print(f"  ERROR: CSV not found")
        return False
    
    csv_headers, csv_rows, csv_size_mb = get_csv_info(csv_file)
    csv_hash = get_csv_hash(csv_file)
    
    print(f"  CSV: {csv_rows:,} rows, {len(csv_headers)} cols, {csv_size_mb:.1f}MB")
    
    prev_info = checkpoint.get(table_name, {})
    
    if table_exists(table_name):
        table_rows = get_table_count(table_name)
        print(f"  Existing: {table_rows:,} rows")
        
        if table_rows == csv_rows and csv_hash == prev_info.get('hash'):
            table_end = datetime.now()
            duration = (table_end - table_start).total_seconds()
            print(f"  End: {table_end.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Duration: {duration:.1f}s")
            print(f"  Status: UNCHANGED")
            return True
        
        if table_rows < csv_rows:
            print(f"  Incomplete: {table_rows/csv_rows*100:.1f}% loaded")
        
        print(f"  Dropping and rebuilding...")
        run_psql_command(f"DROP TABLE IF EXISTS gold.{table_name}")
    
    print(f"  Creating table...")
    run_psql_command(f"CREATE TABLE gold.{table_name} ({table_config['columns']})")

    try:
        print(f"  Loading {csv_size_mb:.1f}MB via psql copy...")
        load_start = time.time()
        load_csv_psql(csv_file, table_name)
        load_time = time.time() - load_start
        print(f"  Loaded in {load_time:.1f}s")
    except Exception as e:
        if 'string buffer exceeds' in str(e) or '1073741823' in str(e):
            print(f"  psql buffer limit hit - switching to chunked psycopg2 insert...")
            run_psql_command(f"DROP TABLE IF EXISTS gold.{table_name}")
            run_psql_command(f"CREATE TABLE gold.{table_name} ({table_config['columns']})")
            try:
                load_start = time.time()
                load_csv_chunked_psycopg2(csv_file, table_name, csv_headers)
                load_time = time.time() - load_start
                print(f"  Chunked load completed in {load_time:.1f}s")
            except Exception as e2:
                print(f"  ERROR during chunked load: {e2}")
                return False
        else:
            print(f"  ERROR during copy: {e}")
            return False
    
    
    final_count = get_table_count(table_name)
    table_end = datetime.now()
    duration = (table_end - table_start).total_seconds()
    
    print(f"  Loaded: {final_count:,} rows")
    print(f"  End: {table_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Duration: {duration:.1f}s ({duration/60:.1f} min)")
    
    if final_count != csv_rows:
        print(f"  WARNING: Row count mismatch!")
        return False
    
    checkpoint[table_name] = {
        "rows": csv_rows,
        "columns": len(csv_headers),
        "hash": csv_hash
    }
    save_checkpoint(checkpoint)
    
    print(f"  Status: OK")
    return True

    


def main():
    print("="*80)
    print("HYBRID POSTGRES LOADER - PSQL WITH SMART CHANGE DETECTION")
    print("="*80)
    print(f"Database: {POSTGRES_DB}.gold")
    print(f"Method: psql with \\copy")
    print("="*80)
    
    if not POSTGRES_PASSWORD:
        print("\nERROR: PostgreSQL password not found in .env")
        return
    
    run_psql_command("CREATE SCHEMA IF NOT EXISTS gold")
    print("\nGold schema: OK")
    
    checkpoint = load_checkpoint()
    print(f"Previously loaded: {len(checkpoint)} tables\n")
    
    overall_start = datetime.now()
    results = {}
    
    for table_name, table_config in TABLES.items():
        success = load_table(table_name, table_config, checkpoint)
        results[table_name] = success
    
    overall_end = datetime.now()
    total_duration = (overall_end - overall_start).total_seconds()
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total time: {total_duration:.1f}s ({total_duration/60:.1f} min)")
    
    successful = [t for t, s in results.items() if s]
    failed = [t for t, s in results.items() if not s]
    
    print(f"\nSuccessful: {len(successful)}/11")
    for t in successful:
        print(f"  - {t}")
    
    if failed:
        print(f"\nFailed: {len(failed)}")
        for t in failed:
            print(f"  - {t}")
    
    print("\n" + "="*80)
    if failed:
        print("NEXT: Fix errors and re-run")
    else:
        print("NEXT: Run fix_postgres_types_fast.py")
    print("="*80)

if __name__ == "__main__":
    main()
