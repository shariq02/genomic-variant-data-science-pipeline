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

# Tables to never load - temp/intermediate tables that should not exist in gold
SKIP_TABLES = {
    "temp_df_impact",
}

TABLES = {
    "cancer_variant_ml_features": {
        "columns": """gene_symbol TEXT, gene_name TEXT, variant_key TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, sample_count TEXT, total_mutation_count TEXT, missense_sample_count TEXT, truncating_sample_count TEXT, silent_sample_count TEXT, snv_sample_count TEXT, indel_sample_count TEXT, is_recurrent_mutation TEXT, is_hotspot_mutation TEXT, is_high_impact_cancer_variant TEXT, is_driver_candidate TEXT, mutation_frequency_category TEXT, gene_total_samples TEXT, gene_unique_sites TEXT, is_cancer_gene TEXT, is_tumor_suppressor_candidate TEXT, is_oncogene_candidate TEXT, gene_cancer_role TEXT, cancer_mutation_burden_score TEXT, cancer_priority_score TEXT"""
    },
    "clinical_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, is_pharmacogene TEXT, druggability_score TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, gene_avg_review_quality TEXT, gene_has_quality_annotations TEXT, gene_missense_count TEXT, gene_frameshift_count TEXT, gene_nonsense_count TEXT, gene_splice_count TEXT, gene_lof_variant_ratio TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant TEXT, population_allele_frequency TEXT, is_common_in_population TEXT, is_rare_in_population TEXT, frequency_pathogenicity_conflict TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, is_disease_gene TEXT"""
    },
    "disease_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, disease_enriched TEXT, primary_disease TEXT, disease_name_enriched TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_is_well_annotated TEXT, disease_name_is_generic TEXT, disease_count TEXT, omim_disease_count TEXT, disease_count_category TEXT, is_disease_associated TEXT, is_multi_disease_gene TEXT, disease_association_strength TEXT, is_omim_gene TEXT, variant_disease_link_quality TEXT, disease_total_variants TEXT, disease_pathogenic_variants TEXT, disease_benign_variants TEXT, disease_vus_variants TEXT, disease_pathogenic_ratio TEXT, disease_gene_count TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, disease_complexity_score TEXT, polygenic_risk_contribution TEXT, disease_has_high_pathogenic_burden TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_high_quality_count TEXT, gene_disease_diversity TEXT, gene_clinical_utility_score TEXT, gene_priority_tier TEXT, is_clinically_actionable TEXT, is_research_candidate TEXT, gene_annotation_score TEXT, has_excellent_annotation TEXT, annotation_priority_level TEXT, gene_omim_variants TEXT, gene_mondo_variants TEXT, gene_well_annotated_variants TEXT, tissues_expressed_count TEXT, is_broadly_expressed TEXT, cancer_mutation_count TEXT, is_cancer_hotspot_gene TEXT, phylop_score TEXT, cadd_phred TEXT, is_highly_conserved TEXT, has_high_conservation TEXT, gene_domain_count TEXT, is_complex_protein TEXT"""
    },
    "drug_response_ml_features": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, pathogenicity_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT"""
    },
    "gene_expression_ml_features": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, is_pharmacogene TEXT, druggability_score TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, is_disease_gene TEXT, disease_category_count TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_cancer_gene TEXT, cancer_expression_relevance TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, has_functional_domain TEXT, domain_expression_correlation TEXT, total_gene_variants TEXT, splice_variants TEXT, expression_affecting_variants TEXT, has_expression_variants TEXT, disease_expression_score TEXT, cancer_expression_score TEXT, functional_expression_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT, disease_specific_expression_pattern TEXT, expression_function_correlation TEXT"""
    },
    "gene_pharmacogene_ml_features": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, pharmgkb_name TEXT, description TEXT, chromosome TEXT, source_count TEXT, has_pharmgkb_annotation TEXT, is_drug_metabolizer TEXT, is_drug_transporter_gene TEXT, is_drug_target_gene TEXT, has_high_druggability TEXT, is_pharmacogene TEXT, is_hepatic_metabolizer TEXT, is_renal_transporter TEXT, is_validated_cancer_target TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transporter TEXT, is_metabolic TEXT, druggability_score TEXT, total_relationships TEXT, entity_type_count TEXT, drug_relationships TEXT, disease_relationships TEXT, variant_relationships TEXT, evidence_count TEXT, total_gene_variants TEXT, pathogenic_variants TEXT, missense_variants TEXT, lof_variants TEXT, domain_affecting_variants TEXT, avg_pathogenicity_score TEXT, has_pharmacogene_variants TEXT, variant_impact_burden TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_tissue_expression TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_oncology_drug_target TEXT, cancer_mutation_burden TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, primary_indication_category TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, is_complex_drug_target TEXT, pharmacogene_evidence_score TEXT, drug_interaction_score TEXT, clinical_utility_score TEXT, pharmacogene_variant_impact_score TEXT, metabolism_context_score TEXT, pharmacogene_priority TEXT, is_high_priority_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_category_enhanced TEXT, drug_metabolism_role TEXT, clinical_actionability_tier TEXT"""
    },
    "gene_protein_family_ml_features": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, protein_family TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, protein_count TEXT, max_domain_count TEXT, proteins_with_kinase TEXT, proteins_with_receptor TEXT, proteins_with_zinc_finger TEXT, proteins_with_sh2 TEXT, proteins_with_sh3 TEXT, proteins_with_ph TEXT, proteins_with_death TEXT, proteins_with_leucine_zipper TEXT, proteins_with_helix_loop TEXT, proteins_with_ig TEXT, proteins_with_functional_domain TEXT, has_signaling_domain TEXT, has_dna_binding_domain TEXT, has_membrane_domain TEXT, has_apoptosis_domain TEXT, has_immune_domain TEXT, is_multi_domain_protein TEXT, domain_diversity_score TEXT, functional_complexity_score TEXT, druggability_potential_score TEXT, domain_affecting_variants TEXT, domain_pathogenic_variants TEXT, critical_domain_variants TEXT, has_domain_variants TEXT, protein_family_expression_breadth TEXT, protein_max_expression TEXT, tissue_specific_protein_expression TEXT, cancer_missense_mutations TEXT, cancer_truncating_mutations TEXT, cancer_samples_affected TEXT, cancer_relevant_protein_family TEXT, oncogenic_domain_alterations TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, disease_associated_protein_family TEXT, disease_specific_domains TEXT, variant_domain_impact_score TEXT, cancer_protein_family_score TEXT, disease_protein_family_score TEXT, protein_family_priority TEXT, is_high_value_protein_family TEXT, protein_functional_category TEXT, variant_disease_domain_correlation TEXT, cancer_protein_classification TEXT"""
    },
    "gene_test_availability_ml_features": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, total_test_count TEXT, unique_test_count TEXT, disease_count TEXT, genetic_test_count TEXT, tests_with_gene_info TEXT, tests_with_disease_info TEXT, complete_test_count TEXT, frequent_test_count TEXT, has_clinical_test TEXT, has_multiple_tests TEXT, has_comprehensive_testing TEXT, is_well_tested_gene TEXT, test_availability_category TEXT, test_accessibility_score TEXT, clinical_utility_score TEXT, test_quality_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, disease_test_correlation TEXT, multi_disease_testing TEXT, pathogenic_variants_in_tested_gene TEXT, test_covered_variants TEXT, variant_test_coverage_level TEXT, cancer_mutation_count TEXT, cancer_samples TEXT, is_cancer_panel_gene TEXT, hereditary_cancer_testing TEXT, rare_pathogenic_variants TEXT, carrier_screening_relevant TEXT, population_test_priority TEXT, clinical_test_utility_score TEXT, variant_test_coverage_score TEXT, population_test_relevance_score TEXT, test_priority TEXT, is_high_priority_test_gene TEXT, primary_test_type TEXT, test_recommendation_tier TEXT"""
    },
    "genetic_test_ml_features": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, total_test_count TEXT, unique_test_count TEXT, disease_count TEXT, genetic_test_count TEXT, tests_with_gene_info TEXT, tests_with_disease_info TEXT, complete_test_count TEXT, frequent_test_count TEXT, has_clinical_test TEXT, has_multiple_tests TEXT, has_comprehensive_testing TEXT, is_well_tested_gene TEXT, test_availability_category TEXT, test_accessibility_score TEXT, clinical_utility_score TEXT, test_quality_score TEXT, test_priority TEXT, is_high_priority_test_gene TEXT"""
    },
    "ml_dataset_cancer_variant_test": {
        "columns": """gene_symbol TEXT, gene_name TEXT, variant_key TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, sample_count TEXT, total_mutation_count TEXT, missense_sample_count TEXT, truncating_sample_count TEXT, silent_sample_count TEXT, snv_sample_count TEXT, indel_sample_count TEXT, is_recurrent_mutation TEXT, is_hotspot_mutation TEXT, is_high_impact_cancer_variant TEXT, is_driver_candidate TEXT, mutation_frequency_category TEXT, gene_total_samples TEXT, gene_unique_sites TEXT, is_cancer_gene TEXT, is_tumor_suppressor_candidate TEXT, is_oncogene_candidate TEXT, gene_cancer_role TEXT, cancer_mutation_burden_score TEXT, cancer_priority_score TEXT, clinvar_pathogenicity TEXT, clinvar_is_pathogenic TEXT, conservation_score TEXT, cadd_phred TEXT, functional_impact_prediction TEXT, tissue_expression_in_tumors TEXT, max_tumor_expression TEXT, expression_change_relevance TEXT, cancer_disease_associations TEXT, hereditary_cancer_syndrome TEXT, has_kinase_domain_count TEXT, affected_oncogenic_domains TEXT, kinase_domain_mutations TEXT, germline_variant_frequency TEXT, is_rare TEXT, somatic_vs_germline_classification TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, driver_likelihood_score TEXT, therapeutic_target_score TEXT, prognostic_value_score TEXT"""
    },
    "ml_dataset_cancer_variant_train": {
        "columns": """gene_symbol TEXT, gene_name TEXT, variant_key TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, sample_count TEXT, total_mutation_count TEXT, missense_sample_count TEXT, truncating_sample_count TEXT, silent_sample_count TEXT, snv_sample_count TEXT, indel_sample_count TEXT, is_recurrent_mutation TEXT, is_hotspot_mutation TEXT, is_high_impact_cancer_variant TEXT, is_driver_candidate TEXT, mutation_frequency_category TEXT, gene_total_samples TEXT, gene_unique_sites TEXT, is_cancer_gene TEXT, is_tumor_suppressor_candidate TEXT, is_oncogene_candidate TEXT, gene_cancer_role TEXT, cancer_mutation_burden_score TEXT, cancer_priority_score TEXT, clinvar_pathogenicity TEXT, clinvar_is_pathogenic TEXT, conservation_score TEXT, cadd_phred TEXT, functional_impact_prediction TEXT, tissue_expression_in_tumors TEXT, max_tumor_expression TEXT, expression_change_relevance TEXT, cancer_disease_associations TEXT, hereditary_cancer_syndrome TEXT, has_kinase_domain_count TEXT, affected_oncogenic_domains TEXT, kinase_domain_mutations TEXT, germline_variant_frequency TEXT, is_rare TEXT, somatic_vs_germline_classification TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, driver_likelihood_score TEXT, therapeutic_target_score TEXT, prognostic_value_score TEXT"""
    },
    "ml_dataset_cancer_variant_validation": {
        "columns": """gene_symbol TEXT, gene_name TEXT, variant_key TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, sample_count TEXT, total_mutation_count TEXT, missense_sample_count TEXT, truncating_sample_count TEXT, silent_sample_count TEXT, snv_sample_count TEXT, indel_sample_count TEXT, is_recurrent_mutation TEXT, is_hotspot_mutation TEXT, is_high_impact_cancer_variant TEXT, is_driver_candidate TEXT, mutation_frequency_category TEXT, gene_total_samples TEXT, gene_unique_sites TEXT, is_cancer_gene TEXT, is_tumor_suppressor_candidate TEXT, is_oncogene_candidate TEXT, gene_cancer_role TEXT, cancer_mutation_burden_score TEXT, cancer_priority_score TEXT, clinvar_pathogenicity TEXT, clinvar_is_pathogenic TEXT, conservation_score TEXT, cadd_phred TEXT, functional_impact_prediction TEXT, tissue_expression_in_tumors TEXT, max_tumor_expression TEXT, expression_change_relevance TEXT, cancer_disease_associations TEXT, hereditary_cancer_syndrome TEXT, has_kinase_domain_count TEXT, affected_oncogenic_domains TEXT, kinase_domain_mutations TEXT, germline_variant_frequency TEXT, is_rare TEXT, somatic_vs_germline_classification TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, driver_likelihood_score TEXT, therapeutic_target_score TEXT, prognostic_value_score TEXT"""
    },
    "ml_dataset_carrier_screening_test": {
        "columns": """variant_id TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT"""
    },
    "ml_dataset_carrier_screening_train": {
        "columns": """variant_id TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT"""
    },
    "ml_dataset_carrier_screening_validation": {
        "columns": """variant_id TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT"""
    },
    "ml_dataset_clinical_test": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, is_pharmacogene TEXT, druggability_score TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, gene_avg_review_quality TEXT, gene_has_quality_annotations TEXT, gene_missense_count TEXT, gene_frameshift_count TEXT, gene_nonsense_count TEXT, gene_splice_count TEXT, gene_lof_variant_ratio TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant TEXT, population_allele_frequency TEXT, is_common_in_population TEXT, is_rare_in_population TEXT, frequency_pathogenicity_conflict TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, is_disease_gene TEXT"""
    },
    "ml_dataset_clinical_train": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, is_pharmacogene TEXT, druggability_score TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, gene_avg_review_quality TEXT, gene_has_quality_annotations TEXT, gene_missense_count TEXT, gene_frameshift_count TEXT, gene_nonsense_count TEXT, gene_splice_count TEXT, gene_lof_variant_ratio TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant TEXT, population_allele_frequency TEXT, is_common_in_population TEXT, is_rare_in_population TEXT, frequency_pathogenicity_conflict TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, is_disease_gene TEXT"""
    },
    "ml_dataset_clinical_validation": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_gene_symbol TEXT, gene_is_validated TEXT, gene_has_omim TEXT, gene_has_ensembl TEXT, gene_is_well_characterized TEXT, is_pharmacogene TEXT, druggability_score TEXT, target_is_pathogenic TEXT, target_is_benign TEXT, target_is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, clinical_sig_is_uncertain TEXT, review_quality_score TEXT, has_strong_evidence TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, combined_pathogenicity_risk TEXT, protein_impact_category TEXT, is_coding_variant TEXT, is_regulatory_variant TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_deleterious_by_cadd TEXT, has_functional_domain TEXT, domain_count TEXT, has_conservation_data TEXT, has_complete_annotation TEXT, inheritance_pattern TEXT, x_linked_risk_modifier TEXT, inheritance_pathogenicity_modifier TEXT, is_mitochondrial_variant TEXT, is_y_linked_variant TEXT, is_x_linked_variant TEXT, is_autosomal_variant TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_vus_count TEXT, gene_pathogenic_ratio TEXT, gene_benign_ratio TEXT, gene_vus_ratio TEXT, gene_mutation_burden TEXT, gene_is_pathogenic_enriched TEXT, gene_is_benign_enriched TEXT, gene_is_vus_enriched TEXT, gene_variant_profile TEXT, gene_has_high_lof_burden TEXT, gene_avg_review_quality TEXT, gene_has_quality_annotations TEXT, gene_missense_count TEXT, gene_frameshift_count TEXT, gene_nonsense_count TEXT, gene_splice_count TEXT, gene_lof_variant_ratio TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant TEXT, population_allele_frequency TEXT, is_common_in_population TEXT, is_rare_in_population TEXT, frequency_pathogenicity_conflict TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, is_disease_gene TEXT"""
    },
    "ml_dataset_disease_test": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, disease_enriched TEXT, primary_disease TEXT, disease_name_enriched TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_is_well_annotated TEXT, disease_name_is_generic TEXT, disease_count TEXT, omim_disease_count TEXT, disease_count_category TEXT, is_disease_associated TEXT, is_multi_disease_gene TEXT, disease_association_strength TEXT, is_omim_gene TEXT, variant_disease_link_quality TEXT, disease_total_variants TEXT, disease_pathogenic_variants TEXT, disease_benign_variants TEXT, disease_vus_variants TEXT, disease_pathogenic_ratio TEXT, disease_gene_count TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, disease_complexity_score TEXT, polygenic_risk_contribution TEXT, disease_has_high_pathogenic_burden TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_high_quality_count TEXT, gene_disease_diversity TEXT, gene_clinical_utility_score TEXT, gene_priority_tier TEXT, is_clinically_actionable TEXT, is_research_candidate TEXT, gene_annotation_score TEXT, has_excellent_annotation TEXT, annotation_priority_level TEXT, gene_omim_variants TEXT, gene_mondo_variants TEXT, gene_well_annotated_variants TEXT, tissues_expressed_count TEXT, is_broadly_expressed TEXT, cancer_mutation_count TEXT, is_cancer_hotspot_gene TEXT, phylop_score TEXT, cadd_phred TEXT, is_highly_conserved TEXT, has_high_conservation TEXT, gene_domain_count TEXT, is_complex_protein TEXT"""
    },
    "ml_dataset_disease_train": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, disease_enriched TEXT, primary_disease TEXT, disease_name_enriched TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_is_well_annotated TEXT, disease_name_is_generic TEXT, disease_count TEXT, omim_disease_count TEXT, disease_count_category TEXT, is_disease_associated TEXT, is_multi_disease_gene TEXT, disease_association_strength TEXT, is_omim_gene TEXT, variant_disease_link_quality TEXT, disease_total_variants TEXT, disease_pathogenic_variants TEXT, disease_benign_variants TEXT, disease_vus_variants TEXT, disease_pathogenic_ratio TEXT, disease_gene_count TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, disease_complexity_score TEXT, polygenic_risk_contribution TEXT, disease_has_high_pathogenic_burden TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_high_quality_count TEXT, gene_disease_diversity TEXT, gene_clinical_utility_score TEXT, gene_priority_tier TEXT, is_clinically_actionable TEXT, is_research_candidate TEXT, gene_annotation_score TEXT, has_excellent_annotation TEXT, annotation_priority_level TEXT, gene_omim_variants TEXT, gene_mondo_variants TEXT, gene_well_annotated_variants TEXT, tissues_expressed_count TEXT, is_broadly_expressed TEXT, cancer_mutation_count TEXT, is_cancer_hotspot_gene TEXT, phylop_score TEXT, cadd_phred TEXT, is_highly_conserved TEXT, has_high_conservation TEXT, gene_domain_count TEXT, is_complex_protein TEXT"""
    },
    "ml_dataset_disease_validation": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, disease_enriched TEXT, primary_disease TEXT, disease_name_enriched TEXT, omim_id TEXT, mondo_id TEXT, orphanet_id TEXT, has_omim_disease TEXT, has_mondo_disease TEXT, has_orphanet_disease TEXT, disease_db_coverage TEXT, disease_is_well_annotated TEXT, disease_name_is_generic TEXT, disease_count TEXT, omim_disease_count TEXT, disease_count_category TEXT, is_disease_associated TEXT, is_multi_disease_gene TEXT, disease_association_strength TEXT, is_omim_gene TEXT, variant_disease_link_quality TEXT, disease_total_variants TEXT, disease_pathogenic_variants TEXT, disease_benign_variants TEXT, disease_vus_variants TEXT, disease_pathogenic_ratio TEXT, disease_gene_count TEXT, is_polygenic_disease TEXT, disease_complexity TEXT, disease_complexity_score TEXT, polygenic_risk_contribution TEXT, disease_has_high_pathogenic_burden TEXT, gene_total_variants TEXT, gene_pathogenic_count TEXT, gene_benign_count TEXT, gene_high_quality_count TEXT, gene_disease_diversity TEXT, gene_clinical_utility_score TEXT, gene_priority_tier TEXT, is_clinically_actionable TEXT, is_research_candidate TEXT, gene_annotation_score TEXT, has_excellent_annotation TEXT, annotation_priority_level TEXT, gene_omim_variants TEXT, gene_mondo_variants TEXT, gene_well_annotated_variants TEXT, tissues_expressed_count TEXT, is_broadly_expressed TEXT, cancer_mutation_count TEXT, is_cancer_hotspot_gene TEXT, phylop_score TEXT, cadd_phred TEXT, is_highly_conserved TEXT, has_high_conservation TEXT, gene_domain_count TEXT, is_complex_protein TEXT"""
    },
    "ml_dataset_drug_response_test": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, pathogenicity_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT"""
    },
    "ml_dataset_drug_response_train": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, pathogenicity_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT"""
    },
    "ml_dataset_drug_response_validation": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, pathogenicity_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT"""
    },
    "ml_dataset_expression_test": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT"""
    },
    "ml_dataset_expression_train": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT"""
    },
    "ml_dataset_expression_validation": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT"""
    },
    "ml_dataset_gene_expression_test": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, is_pharmacogene TEXT, druggability_score TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, is_disease_gene TEXT, disease_category_count TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_cancer_gene TEXT, cancer_expression_relevance TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, has_functional_domain TEXT, domain_expression_correlation TEXT, total_gene_variants TEXT, splice_variants TEXT, expression_affecting_variants TEXT, has_expression_variants TEXT, disease_expression_score TEXT, cancer_expression_score TEXT, functional_expression_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT, disease_specific_expression_pattern TEXT, expression_function_correlation TEXT"""
    },
    "ml_dataset_gene_expression_train": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, is_pharmacogene TEXT, druggability_score TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, is_disease_gene TEXT, disease_category_count TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_cancer_gene TEXT, cancer_expression_relevance TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, has_functional_domain TEXT, domain_expression_correlation TEXT, total_gene_variants TEXT, splice_variants TEXT, expression_affecting_variants TEXT, has_expression_variants TEXT, disease_expression_score TEXT, cancer_expression_score TEXT, functional_expression_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT, disease_specific_expression_pattern TEXT, expression_function_correlation TEXT"""
    },
    "ml_dataset_gene_expression_validation": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, is_pharmacogene TEXT, druggability_score TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, is_disease_gene TEXT, disease_category_count TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_cancer_gene TEXT, cancer_expression_relevance TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, has_functional_domain TEXT, domain_expression_correlation TEXT, total_gene_variants TEXT, splice_variants TEXT, expression_affecting_variants TEXT, has_expression_variants TEXT, disease_expression_score TEXT, cancer_expression_score TEXT, functional_expression_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT, disease_specific_expression_pattern TEXT, expression_function_correlation TEXT"""
    },
    "ml_dataset_gene_pharmacogene_test": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, chromosome TEXT, source_count TEXT, has_pharmgkb_annotation TEXT, is_drug_metabolizer TEXT, is_drug_transporter_gene TEXT, is_drug_target_gene TEXT, has_high_druggability TEXT, is_pharmacogene TEXT, is_hepatic_metabolizer TEXT, is_renal_transporter TEXT, is_validated_cancer_target TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transporter TEXT, is_metabolic TEXT, druggability_score TEXT, total_relationships TEXT, entity_type_count TEXT, drug_relationships TEXT, disease_relationships TEXT, variant_relationships TEXT, evidence_count TEXT, total_gene_variants TEXT, pathogenic_variants TEXT, missense_variants TEXT, lof_variants TEXT, domain_affecting_variants TEXT, avg_pathogenicity_score TEXT, has_pharmacogene_variants TEXT, variant_impact_burden TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_tissue_expression TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_oncology_drug_target TEXT, cancer_mutation_burden TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, primary_indication_category TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, is_complex_drug_target TEXT, pharmacogene_evidence_score TEXT, drug_interaction_score TEXT, clinical_utility_score TEXT, pharmacogene_variant_impact_score TEXT, metabolism_context_score TEXT, pharmacogene_priority TEXT, is_high_priority_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_category_enhanced TEXT, drug_metabolism_role TEXT, clinical_actionability_tier TEXT"""
    },
    "ml_dataset_gene_pharmacogene_train": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, chromosome TEXT, source_count TEXT, has_pharmgkb_annotation TEXT, is_drug_metabolizer TEXT, is_drug_transporter_gene TEXT, is_drug_target_gene TEXT, has_high_druggability TEXT, is_pharmacogene TEXT, is_hepatic_metabolizer TEXT, is_renal_transporter TEXT, is_validated_cancer_target TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transporter TEXT, is_metabolic TEXT, druggability_score TEXT, total_relationships TEXT, entity_type_count TEXT, drug_relationships TEXT, disease_relationships TEXT, variant_relationships TEXT, evidence_count TEXT, total_gene_variants TEXT, pathogenic_variants TEXT, missense_variants TEXT, lof_variants TEXT, domain_affecting_variants TEXT, avg_pathogenicity_score TEXT, has_pharmacogene_variants TEXT, variant_impact_burden TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_tissue_expression TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_oncology_drug_target TEXT, cancer_mutation_burden TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, primary_indication_category TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, is_complex_drug_target TEXT, pharmacogene_evidence_score TEXT, drug_interaction_score TEXT, clinical_utility_score TEXT, pharmacogene_variant_impact_score TEXT, metabolism_context_score TEXT, pharmacogene_priority TEXT, is_high_priority_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_category_enhanced TEXT, drug_metabolism_role TEXT, clinical_actionability_tier TEXT"""
    },
    "ml_dataset_gene_pharmacogene_validation": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, chromosome TEXT, source_count TEXT, has_pharmgkb_annotation TEXT, is_drug_metabolizer TEXT, is_drug_transporter_gene TEXT, is_drug_target_gene TEXT, has_high_druggability TEXT, is_pharmacogene TEXT, is_hepatic_metabolizer TEXT, is_renal_transporter TEXT, is_validated_cancer_target TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transporter TEXT, is_metabolic TEXT, druggability_score TEXT, total_relationships TEXT, entity_type_count TEXT, drug_relationships TEXT, disease_relationships TEXT, variant_relationships TEXT, evidence_count TEXT, total_gene_variants TEXT, pathogenic_variants TEXT, missense_variants TEXT, lof_variants TEXT, domain_affecting_variants TEXT, avg_pathogenicity_score TEXT, has_pharmacogene_variants TEXT, variant_impact_burden TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_tissue_expression TEXT, cancer_mutation_count TEXT, unique_tumor_samples TEXT, is_oncology_drug_target TEXT, cancer_mutation_burden TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, primary_indication_category TEXT, max_domain_count TEXT, has_kinase_domain_count TEXT, is_complex_drug_target TEXT, pharmacogene_evidence_score TEXT, drug_interaction_score TEXT, clinical_utility_score TEXT, pharmacogene_variant_impact_score TEXT, metabolism_context_score TEXT, pharmacogene_priority TEXT, is_high_priority_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_category_enhanced TEXT, drug_metabolism_role TEXT, clinical_actionability_tier TEXT"""
    },
    "ml_dataset_gene_test_test": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, total_test_count TEXT, unique_test_count TEXT, disease_count TEXT, genetic_test_count TEXT, tests_with_gene_info TEXT, tests_with_disease_info TEXT, complete_test_count TEXT, frequent_test_count TEXT, has_clinical_test TEXT, has_multiple_tests TEXT, has_comprehensive_testing TEXT, is_well_tested_gene TEXT, test_availability_category TEXT, test_accessibility_score TEXT, clinical_utility_score TEXT, test_quality_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, disease_test_correlation TEXT, multi_disease_testing TEXT, pathogenic_variants_in_tested_gene TEXT, test_covered_variants TEXT, variant_test_coverage_level TEXT, cancer_mutation_count TEXT, cancer_samples TEXT, is_cancer_panel_gene TEXT, hereditary_cancer_testing TEXT, rare_pathogenic_variants TEXT, carrier_screening_relevant TEXT, population_test_priority TEXT, clinical_test_utility_score TEXT, variant_test_coverage_score TEXT, population_test_relevance_score TEXT, test_priority TEXT, is_high_priority_test_gene TEXT, primary_test_type TEXT, test_recommendation_tier TEXT"""
    },
    "ml_dataset_gene_test_train": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, total_test_count TEXT, unique_test_count TEXT, disease_count TEXT, genetic_test_count TEXT, tests_with_gene_info TEXT, tests_with_disease_info TEXT, complete_test_count TEXT, frequent_test_count TEXT, has_clinical_test TEXT, has_multiple_tests TEXT, has_comprehensive_testing TEXT, is_well_tested_gene TEXT, test_availability_category TEXT, test_accessibility_score TEXT, clinical_utility_score TEXT, test_quality_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, disease_test_correlation TEXT, multi_disease_testing TEXT, pathogenic_variants_in_tested_gene TEXT, test_covered_variants TEXT, variant_test_coverage_level TEXT, cancer_mutation_count TEXT, cancer_samples TEXT, is_cancer_panel_gene TEXT, hereditary_cancer_testing TEXT, rare_pathogenic_variants TEXT, carrier_screening_relevant TEXT, population_test_priority TEXT, clinical_test_utility_score TEXT, variant_test_coverage_score TEXT, population_test_relevance_score TEXT, test_priority TEXT, is_high_priority_test_gene TEXT, primary_test_type TEXT, test_recommendation_tier TEXT"""
    },
    "ml_dataset_gene_test_validation": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, total_test_count TEXT, unique_test_count TEXT, disease_count TEXT, genetic_test_count TEXT, tests_with_gene_info TEXT, tests_with_disease_info TEXT, complete_test_count TEXT, frequent_test_count TEXT, has_clinical_test TEXT, has_multiple_tests TEXT, has_comprehensive_testing TEXT, is_well_tested_gene TEXT, test_availability_category TEXT, test_accessibility_score TEXT, clinical_utility_score TEXT, test_quality_score TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, disease_test_correlation TEXT, multi_disease_testing TEXT, pathogenic_variants_in_tested_gene TEXT, test_covered_variants TEXT, variant_test_coverage_level TEXT, cancer_mutation_count TEXT, cancer_samples TEXT, is_cancer_panel_gene TEXT, hereditary_cancer_testing TEXT, rare_pathogenic_variants TEXT, carrier_screening_relevant TEXT, population_test_priority TEXT, clinical_test_utility_score TEXT, variant_test_coverage_score TEXT, population_test_relevance_score TEXT, test_priority TEXT, is_high_priority_test_gene TEXT, primary_test_type TEXT, test_recommendation_tier TEXT"""
    },
    "ml_dataset_pharmacogene_test": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_symbol TEXT, validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_mentions_drug TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT, is_loss_of_function TEXT, protein_impact_category TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_evidence_level TEXT, drug_metabolism_role TEXT, is_drug_target TEXT, is_metabolizing_enzyme TEXT, metabolizing_enzyme_type TEXT, is_enzyme TEXT, is_drug_transporter TEXT, is_kinase TEXT, is_phosphatase TEXT, is_receptor TEXT, is_gpcr TEXT, is_transporter TEXT, drug_target_category TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, drug_response_impact TEXT, is_metabolizer_variant TEXT, metabolizer_phenotype_risk TEXT, is_transporter_variant TEXT, transporter_impact_level TEXT, is_kinase_inhibitor_target TEXT, kinase_variant_therapeutic_relevance TEXT, pharmgkb_source_count TEXT, has_pharmgkb_annotation TEXT, gene_pharmacogene_variants TEXT, gene_drug_interaction_variants TEXT, gene_metabolizer_variants TEXT, gene_transporter_variants TEXT, gene_pharmacogene_pathogenic TEXT, gene_has_multiple_drug_variants TEXT, gene_pharmacogene_priority TEXT, gene_pharmacogene_burden TEXT, gene_avg_druggability TEXT, tissues_expressed_count TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_context TEXT, cancer_mutation_count TEXT, is_oncology_target TEXT, is_cancer_drug_target TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT"""
    },
    "ml_dataset_pharmacogene_train": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_symbol TEXT, validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_mentions_drug TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT, is_loss_of_function TEXT, protein_impact_category TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_evidence_level TEXT, drug_metabolism_role TEXT, is_drug_target TEXT, is_metabolizing_enzyme TEXT, metabolizing_enzyme_type TEXT, is_enzyme TEXT, is_drug_transporter TEXT, is_kinase TEXT, is_phosphatase TEXT, is_receptor TEXT, is_gpcr TEXT, is_transporter TEXT, drug_target_category TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, drug_response_impact TEXT, is_metabolizer_variant TEXT, metabolizer_phenotype_risk TEXT, is_transporter_variant TEXT, transporter_impact_level TEXT, is_kinase_inhibitor_target TEXT, kinase_variant_therapeutic_relevance TEXT, pharmgkb_source_count TEXT, has_pharmgkb_annotation TEXT, gene_pharmacogene_variants TEXT, gene_drug_interaction_variants TEXT, gene_metabolizer_variants TEXT, gene_transporter_variants TEXT, gene_pharmacogene_pathogenic TEXT, gene_has_multiple_drug_variants TEXT, gene_pharmacogene_priority TEXT, gene_pharmacogene_burden TEXT, gene_avg_druggability TEXT, tissues_expressed_count TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_context TEXT, cancer_mutation_count TEXT, is_oncology_target TEXT, is_cancer_drug_target TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT"""
    },
    "ml_dataset_pharmacogene_validation": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_symbol TEXT, validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_mentions_drug TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT, is_loss_of_function TEXT, protein_impact_category TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_evidence_level TEXT, drug_metabolism_role TEXT, is_drug_target TEXT, is_metabolizing_enzyme TEXT, metabolizing_enzyme_type TEXT, is_enzyme TEXT, is_drug_transporter TEXT, is_kinase TEXT, is_phosphatase TEXT, is_receptor TEXT, is_gpcr TEXT, is_transporter TEXT, drug_target_category TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, drug_response_impact TEXT, is_metabolizer_variant TEXT, metabolizer_phenotype_risk TEXT, is_transporter_variant TEXT, transporter_impact_level TEXT, is_kinase_inhibitor_target TEXT, kinase_variant_therapeutic_relevance TEXT, pharmgkb_source_count TEXT, has_pharmgkb_annotation TEXT, gene_pharmacogene_variants TEXT, gene_drug_interaction_variants TEXT, gene_metabolizer_variants TEXT, gene_transporter_variants TEXT, gene_pharmacogene_pathogenic TEXT, gene_has_multiple_drug_variants TEXT, gene_pharmacogene_priority TEXT, gene_pharmacogene_burden TEXT, gene_avg_druggability TEXT, tissues_expressed_count TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_context TEXT, cancer_mutation_count TEXT, is_oncology_target TEXT, is_cancer_drug_target TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT"""
    },
    "ml_dataset_protein_family_test": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, protein_family TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, protein_count TEXT, max_domain_count TEXT, proteins_with_kinase TEXT, proteins_with_receptor TEXT, proteins_with_zinc_finger TEXT, proteins_with_sh2 TEXT, proteins_with_sh3 TEXT, proteins_with_ph TEXT, proteins_with_death TEXT, proteins_with_leucine_zipper TEXT, proteins_with_helix_loop TEXT, proteins_with_ig TEXT, proteins_with_functional_domain TEXT, has_signaling_domain TEXT, has_dna_binding_domain TEXT, has_membrane_domain TEXT, has_apoptosis_domain TEXT, has_immune_domain TEXT, is_multi_domain_protein TEXT, domain_diversity_score TEXT, functional_complexity_score TEXT, druggability_potential_score TEXT, domain_affecting_variants TEXT, domain_pathogenic_variants TEXT, critical_domain_variants TEXT, has_domain_variants TEXT, protein_family_expression_breadth TEXT, protein_max_expression TEXT, tissue_specific_protein_expression TEXT, cancer_missense_mutations TEXT, cancer_truncating_mutations TEXT, cancer_samples_affected TEXT, cancer_relevant_protein_family TEXT, oncogenic_domain_alterations TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, disease_associated_protein_family TEXT, disease_specific_domains TEXT, variant_domain_impact_score TEXT, cancer_protein_family_score TEXT, disease_protein_family_score TEXT, protein_family_priority TEXT, is_high_value_protein_family TEXT, protein_functional_category TEXT, variant_disease_domain_correlation TEXT, cancer_protein_classification TEXT"""
    },
    "ml_dataset_protein_family_train": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, protein_family TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, protein_count TEXT, max_domain_count TEXT, proteins_with_kinase TEXT, proteins_with_receptor TEXT, proteins_with_zinc_finger TEXT, proteins_with_sh2 TEXT, proteins_with_sh3 TEXT, proteins_with_ph TEXT, proteins_with_death TEXT, proteins_with_leucine_zipper TEXT, proteins_with_helix_loop TEXT, proteins_with_ig TEXT, proteins_with_functional_domain TEXT, has_signaling_domain TEXT, has_dna_binding_domain TEXT, has_membrane_domain TEXT, has_apoptosis_domain TEXT, has_immune_domain TEXT, is_multi_domain_protein TEXT, domain_diversity_score TEXT, functional_complexity_score TEXT, druggability_potential_score TEXT, domain_affecting_variants TEXT, domain_pathogenic_variants TEXT, critical_domain_variants TEXT, has_domain_variants TEXT, protein_family_expression_breadth TEXT, protein_max_expression TEXT, tissue_specific_protein_expression TEXT, cancer_missense_mutations TEXT, cancer_truncating_mutations TEXT, cancer_samples_affected TEXT, cancer_relevant_protein_family TEXT, oncogenic_domain_alterations TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, disease_associated_protein_family TEXT, disease_specific_domains TEXT, variant_domain_impact_score TEXT, cancer_protein_family_score TEXT, disease_protein_family_score TEXT, protein_family_priority TEXT, is_high_value_protein_family TEXT, protein_functional_category TEXT, variant_disease_domain_correlation TEXT, cancer_protein_classification TEXT"""
    },
    "ml_dataset_protein_family_validation": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, protein_family TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, protein_count TEXT, max_domain_count TEXT, proteins_with_kinase TEXT, proteins_with_receptor TEXT, proteins_with_zinc_finger TEXT, proteins_with_sh2 TEXT, proteins_with_sh3 TEXT, proteins_with_ph TEXT, proteins_with_death TEXT, proteins_with_leucine_zipper TEXT, proteins_with_helix_loop TEXT, proteins_with_ig TEXT, proteins_with_functional_domain TEXT, has_signaling_domain TEXT, has_dna_binding_domain TEXT, has_membrane_domain TEXT, has_apoptosis_domain TEXT, has_immune_domain TEXT, is_multi_domain_protein TEXT, domain_diversity_score TEXT, functional_complexity_score TEXT, druggability_potential_score TEXT, domain_affecting_variants TEXT, domain_pathogenic_variants TEXT, critical_domain_variants TEXT, has_domain_variants TEXT, protein_family_expression_breadth TEXT, protein_max_expression TEXT, tissue_specific_protein_expression TEXT, cancer_missense_mutations TEXT, cancer_truncating_mutations TEXT, cancer_samples_affected TEXT, cancer_relevant_protein_family TEXT, oncogenic_domain_alterations TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, disease_associated_protein_family TEXT, disease_specific_domains TEXT, variant_domain_impact_score TEXT, cancer_protein_family_score TEXT, disease_protein_family_score TEXT, protein_family_priority TEXT, is_high_value_protein_family TEXT, protein_functional_category TEXT, variant_disease_domain_correlation TEXT, cancer_protein_classification TEXT"""
    },
    "ml_dataset_structural_variant_test": {
        "columns": """sv_id TEXT, study_id TEXT, variant_name TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, assembly TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, sv_pathogenicity_risk TEXT, genes_overlapped TEXT, gene_list TEXT, gene_count_category TEXT, pharmacogenes_affected TEXT, omim_genes_affected TEXT, kinase_genes_affected TEXT, receptor_genes_affected TEXT, max_gene_disruption_fraction TEXT, avg_druggability_affected_genes TEXT, has_critical_gene_disruption TEXT, total_disease_associations TEXT, cancer_genes_affected TEXT, neuro_genes_affected TEXT, has_disease_associated_genes TEXT, disease_sv_priority TEXT, broadly_expressed_genes_affected TEXT, affects_essential_genes TEXT, sv_clinical_priority TEXT, sv_combined_impact_score TEXT, sv_impact_tier TEXT, sv_classification TEXT"""
    },
    "ml_dataset_structural_variant_train": {
        "columns": """sv_id TEXT, study_id TEXT, variant_name TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, assembly TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, sv_pathogenicity_risk TEXT, genes_overlapped TEXT, gene_list TEXT, gene_count_category TEXT, pharmacogenes_affected TEXT, omim_genes_affected TEXT, kinase_genes_affected TEXT, receptor_genes_affected TEXT, max_gene_disruption_fraction TEXT, avg_druggability_affected_genes TEXT, has_critical_gene_disruption TEXT, total_disease_associations TEXT, cancer_genes_affected TEXT, neuro_genes_affected TEXT, has_disease_associated_genes TEXT, disease_sv_priority TEXT, broadly_expressed_genes_affected TEXT, affects_essential_genes TEXT, sv_clinical_priority TEXT, sv_combined_impact_score TEXT, sv_impact_tier TEXT, sv_classification TEXT"""
    },
    "ml_dataset_structural_variant_validation": {
        "columns": """sv_id TEXT, study_id TEXT, variant_name TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, assembly TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, sv_pathogenicity_risk TEXT, genes_overlapped TEXT, gene_list TEXT, gene_count_category TEXT, pharmacogenes_affected TEXT, omim_genes_affected TEXT, kinase_genes_affected TEXT, receptor_genes_affected TEXT, max_gene_disruption_fraction TEXT, avg_druggability_affected_genes TEXT, has_critical_gene_disruption TEXT, total_disease_associations TEXT, cancer_genes_affected TEXT, neuro_genes_affected TEXT, has_disease_associated_genes TEXT, disease_sv_priority TEXT, broadly_expressed_genes_affected TEXT, affects_essential_genes TEXT, sv_clinical_priority TEXT, sv_combined_impact_score TEXT, sv_impact_tier TEXT, sv_classification TEXT"""
    },
    "ml_dataset_variant_drug_response_test": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, pathogenicity_score TEXT, mutation_severity_score TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, is_hepatic_drug_metabolism_variant TEXT, is_common_pharmacogene_variant TEXT, is_potential_resistance_variant TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_liver_expressed TEXT, expression_breadth TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_pharmacogene TEXT, druggability_score TEXT, pharmacogene_category TEXT, drug_metabolism_role TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, population_adjusted_score TEXT, tissue_specific_response_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT, indication_specific_actionability TEXT"""
    },
    "ml_dataset_variant_drug_response_train": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, pathogenicity_score TEXT, mutation_severity_score TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, is_hepatic_drug_metabolism_variant TEXT, is_common_pharmacogene_variant TEXT, is_potential_resistance_variant TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_liver_expressed TEXT, expression_breadth TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_pharmacogene TEXT, druggability_score TEXT, pharmacogene_category TEXT, drug_metabolism_role TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, population_adjusted_score TEXT, tissue_specific_response_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT, indication_specific_actionability TEXT"""
    },
    "ml_dataset_variant_drug_response_validation": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, pathogenicity_score TEXT, mutation_severity_score TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, is_hepatic_drug_metabolism_variant TEXT, is_common_pharmacogene_variant TEXT, is_potential_resistance_variant TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_liver_expressed TEXT, expression_breadth TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_pharmacogene TEXT, druggability_score TEXT, pharmacogene_category TEXT, drug_metabolism_role TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, population_adjusted_score TEXT, tissue_specific_response_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT, indication_specific_actionability TEXT"""
    },
    "ml_dataset_variant_impact_test": {
        "columns": """variant_id TEXT, gene_name TEXT, official_symbol TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, review_status TEXT, review_quality_score TEXT, variant_type TEXT, variant_name TEXT, reference_allele TEXT, alternate_allele TEXT, protein_change TEXT, cdna_change TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, is_snv TEXT, is_insertion TEXT, is_deletion TEXT, has_functional_domain TEXT, domain_count TEXT, has_zinc_finger TEXT, has_kinase_domain TEXT, has_receptor_domain TEXT, has_sh2_domain TEXT, has_sh3_domain TEXT, has_ph_domain TEXT, affects_functional_domain TEXT, domain_impact_severity TEXT, domain_type_count TEXT, has_multiple_domain_types TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, protein_impact_category TEXT, combined_impact_score TEXT, variant_impact_tier TEXT, phylop_score TEXT, phastcons_score TEXT, gerp_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, conservation_impact_class TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_conservation_constrained TEXT, is_highly_conserved_region TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_splice_affecting TEXT, has_cadd_score TEXT, is_deleterious_by_cadd TEXT, is_splice_site_variant TEXT, splice_impact_severity TEXT, lof_category TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, is_druggable_gene TEXT, is_key_protein_type TEXT, is_well_annotated TEXT, clinical_impact_priority TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_impact_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant_variant TEXT, cancer_variant_priority TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, has_cardiovascular_disease TEXT, is_disease_associated_gene TEXT, disease_impact_category TEXT, disease_specific_priority TEXT, gene_total_variants TEXT, gene_high_impact_count TEXT, gene_very_high_impact_count TEXT, gene_lof_count TEXT, gene_splice_variant_count TEXT, gene_domain_affecting_count TEXT, gene_avg_impact_score TEXT, gene_max_impact_score TEXT, gene_impact_burden TEXT, gene_lof_tolerance TEXT, gene_variant_impact_priority TEXT"""
    },
    "ml_dataset_variant_impact_train": {
        "columns": """variant_id TEXT, gene_name TEXT, official_symbol TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, review_status TEXT, review_quality_score TEXT, variant_type TEXT, variant_name TEXT, reference_allele TEXT, alternate_allele TEXT, protein_change TEXT, cdna_change TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, is_snv TEXT, is_insertion TEXT, is_deletion TEXT, has_functional_domain TEXT, domain_count TEXT, has_zinc_finger TEXT, has_kinase_domain TEXT, has_receptor_domain TEXT, has_sh2_domain TEXT, has_sh3_domain TEXT, has_ph_domain TEXT, affects_functional_domain TEXT, domain_impact_severity TEXT, domain_type_count TEXT, has_multiple_domain_types TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, protein_impact_category TEXT, combined_impact_score TEXT, variant_impact_tier TEXT, phylop_score TEXT, phastcons_score TEXT, gerp_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, conservation_impact_class TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_conservation_constrained TEXT, is_highly_conserved_region TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_splice_affecting TEXT, has_cadd_score TEXT, is_deleterious_by_cadd TEXT, is_splice_site_variant TEXT, splice_impact_severity TEXT, lof_category TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, is_druggable_gene TEXT, is_key_protein_type TEXT, is_well_annotated TEXT, clinical_impact_priority TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_impact_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant_variant TEXT, cancer_variant_priority TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, has_cardiovascular_disease TEXT, is_disease_associated_gene TEXT, disease_impact_category TEXT, disease_specific_priority TEXT, gene_total_variants TEXT, gene_high_impact_count TEXT, gene_very_high_impact_count TEXT, gene_lof_count TEXT, gene_splice_variant_count TEXT, gene_domain_affecting_count TEXT, gene_avg_impact_score TEXT, gene_max_impact_score TEXT, gene_impact_burden TEXT, gene_lof_tolerance TEXT, gene_variant_impact_priority TEXT"""
    },
    "ml_dataset_variant_impact_validation": {
        "columns": """variant_id TEXT, gene_name TEXT, official_symbol TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, review_status TEXT, review_quality_score TEXT, variant_type TEXT, variant_name TEXT, reference_allele TEXT, alternate_allele TEXT, protein_change TEXT, cdna_change TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, is_snv TEXT, is_insertion TEXT, is_deletion TEXT, has_functional_domain TEXT, domain_count TEXT, has_zinc_finger TEXT, has_kinase_domain TEXT, has_receptor_domain TEXT, has_sh2_domain TEXT, has_sh3_domain TEXT, has_ph_domain TEXT, affects_functional_domain TEXT, domain_impact_severity TEXT, domain_type_count TEXT, has_multiple_domain_types TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, protein_impact_category TEXT, combined_impact_score TEXT, variant_impact_tier TEXT, phylop_score TEXT, phastcons_score TEXT, gerp_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, conservation_impact_class TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_conservation_constrained TEXT, is_highly_conserved_region TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_splice_affecting TEXT, has_cadd_score TEXT, is_deleterious_by_cadd TEXT, is_splice_site_variant TEXT, splice_impact_severity TEXT, lof_category TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, is_druggable_gene TEXT, is_key_protein_type TEXT, is_well_annotated TEXT, clinical_impact_priority TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_impact_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant_variant TEXT, cancer_variant_priority TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, has_cardiovascular_disease TEXT, is_disease_associated_gene TEXT, disease_impact_category TEXT, disease_specific_priority TEXT, gene_total_variants TEXT, gene_high_impact_count TEXT, gene_very_high_impact_count TEXT, gene_lof_count TEXT, gene_splice_variant_count TEXT, gene_domain_affecting_count TEXT, gene_avg_impact_score TEXT, gene_max_impact_score TEXT, gene_impact_burden TEXT, gene_lof_tolerance TEXT, gene_variant_impact_priority TEXT"""
    },
    "ml_dataset_variant_population_test": {
        "columns": """variant_id TEXT, variant_key TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, clinvar_pathogenic TEXT, clinvar_benign TEXT, pathogenicity_score TEXT, conservation_level TEXT, pathogenicity_frequency_conflict TEXT, rare_pathogenic_variant TEXT, common_benign_validation TEXT, total_gene_variants TEXT, lof_variants TEXT, gene_mutation_tolerance TEXT, gene_constraint_score TEXT, total_disease_count TEXT, disease_allele_frequency TEXT, carrier_frequency_by_disease TEXT, somatic_frequency TEXT, germline_cancer_predisposition TEXT, expression_tissues TEXT, expression_frequency_correlation TEXT, tissue_specific_allele_effects TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT, clinical_significance_frequency_score TEXT, carrier_risk_score_adjusted TEXT, pathogenicity_likelihood_refined TEXT"""
    },
    "ml_dataset_variant_population_train": {
        "columns": """variant_id TEXT, variant_key TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, clinvar_pathogenic TEXT, clinvar_benign TEXT, pathogenicity_score TEXT, conservation_level TEXT, pathogenicity_frequency_conflict TEXT, rare_pathogenic_variant TEXT, common_benign_validation TEXT, total_gene_variants TEXT, lof_variants TEXT, gene_mutation_tolerance TEXT, gene_constraint_score TEXT, total_disease_count TEXT, disease_allele_frequency TEXT, carrier_frequency_by_disease TEXT, somatic_frequency TEXT, germline_cancer_predisposition TEXT, expression_tissues TEXT, expression_frequency_correlation TEXT, tissue_specific_allele_effects TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT, clinical_significance_frequency_score TEXT, carrier_risk_score_adjusted TEXT, pathogenicity_likelihood_refined TEXT"""
    },
    "ml_dataset_variant_population_validation": {
        "columns": """variant_id TEXT, variant_key TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, clinvar_pathogenic TEXT, clinvar_benign TEXT, pathogenicity_score TEXT, conservation_level TEXT, pathogenicity_frequency_conflict TEXT, rare_pathogenic_variant TEXT, common_benign_validation TEXT, total_gene_variants TEXT, lof_variants TEXT, gene_mutation_tolerance TEXT, gene_constraint_score TEXT, total_disease_count TEXT, disease_allele_frequency TEXT, carrier_frequency_by_disease TEXT, somatic_frequency TEXT, germline_cancer_predisposition TEXT, expression_tissues TEXT, expression_frequency_correlation TEXT, tissue_specific_allele_effects TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT, clinical_significance_frequency_score TEXT, carrier_risk_score_adjusted TEXT, pathogenicity_likelihood_refined TEXT"""
    },
    "pharmacogene_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, chromosome TEXT, position TEXT, official_symbol TEXT, validated_gene_symbol TEXT, gene_is_validated TEXT, gene_description_mentions_drug TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, variant_type TEXT, is_missense_variant TEXT, is_loss_of_function TEXT, protein_impact_category TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, is_pharmacogene TEXT, pharmacogene_category TEXT, pharmacogene_evidence_level TEXT, drug_metabolism_role TEXT, is_drug_target TEXT, is_metabolizing_enzyme TEXT, metabolizing_enzyme_type TEXT, is_enzyme TEXT, is_drug_transporter TEXT, is_kinase TEXT, is_phosphatase TEXT, is_receptor TEXT, is_gpcr TEXT, is_transporter TEXT, drug_target_category TEXT, druggability_score TEXT, enhanced_druggability_score TEXT, drug_response_impact TEXT, is_metabolizer_variant TEXT, metabolizer_phenotype_risk TEXT, is_transporter_variant TEXT, transporter_impact_level TEXT, is_kinase_inhibitor_target TEXT, kinase_variant_therapeutic_relevance TEXT, pharmgkb_sources TEXT, pharmgkb_evidence TEXT, pharmgkb_source_count TEXT, has_pharmgkb_annotation TEXT, gene_pharmacogene_variants TEXT, gene_drug_interaction_variants TEXT, gene_metabolizer_variants TEXT, gene_transporter_variants TEXT, gene_pharmacogene_pathogenic TEXT, gene_has_multiple_drug_variants TEXT, gene_pharmacogene_priority TEXT, gene_pharmacogene_burden TEXT, gene_avg_druggability TEXT, tissues_expressed_count TEXT, is_liver_expressed TEXT, is_kidney_expressed TEXT, expression_breadth TEXT, drug_metabolism_context TEXT, cancer_mutation_count TEXT, is_oncology_target TEXT, is_cancer_drug_target TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT"""
    },
    "population_frequency_ml_features": {
        "columns": """variant_id TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT"""
    },
    "protein_family_ml_features": {
        "columns": """gene_symbol TEXT, gene_name TEXT, description TEXT, chromosome TEXT, protein_family TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, protein_count TEXT, max_domain_count TEXT, proteins_with_kinase TEXT, proteins_with_receptor TEXT, proteins_with_zinc_finger TEXT, proteins_with_sh2 TEXT, proteins_with_sh3 TEXT, proteins_with_ph TEXT, proteins_with_death TEXT, proteins_with_leucine_zipper TEXT, proteins_with_helix_loop TEXT, proteins_with_ig TEXT, proteins_with_functional_domain TEXT, has_signaling_domain TEXT, has_dna_binding_domain TEXT, has_membrane_domain TEXT, has_apoptosis_domain TEXT, has_immune_domain TEXT, is_multi_domain_protein TEXT, domain_diversity_score TEXT, functional_complexity_score TEXT, druggability_potential_score TEXT, gene_druggability_score TEXT, protein_family_priority TEXT, is_high_value_protein_family TEXT, protein_functional_category TEXT"""
    },
    "structural_variant_ml_features": {
        "columns": """sv_id TEXT, study_id TEXT, variant_name TEXT, chromosome TEXT, start_pos TEXT, end_pos TEXT, assembly TEXT, variant_type TEXT, sv_type_class TEXT, sv_size TEXT, sv_size_category TEXT, sv_pathogenicity_risk TEXT, genes_overlapped TEXT, gene_list TEXT, gene_count_category TEXT, pharmacogenes_affected TEXT, omim_genes_affected TEXT, kinase_genes_affected TEXT, receptor_genes_affected TEXT, max_gene_disruption_fraction TEXT, avg_druggability_affected_genes TEXT, has_critical_gene_disruption TEXT, total_disease_associations TEXT, cancer_genes_affected TEXT, neuro_genes_affected TEXT, has_disease_associated_genes TEXT, disease_sv_priority TEXT, broadly_expressed_genes_affected TEXT, affects_essential_genes TEXT, sv_clinical_priority TEXT, sv_combined_impact_score TEXT, sv_impact_tier TEXT, sv_classification TEXT"""
    },
    "transcript_expression_ml_features": {
        "columns": """gene_symbol TEXT, gene_full_name TEXT, description TEXT, chromosome TEXT, gene_length TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_transcription_factor TEXT, max_expression_tpm TEXT, avg_expression_tpm TEXT, peak_expression_tpm TEXT, total_tissues_expressed TEXT, tissue_type_count TEXT, primary_tissue_count TEXT, is_ubiquitously_expressed TEXT, is_tissue_specific TEXT, is_highly_expressed TEXT, is_lowly_expressed TEXT, expression_breadth_category TEXT, expression_level_category TEXT, tissue_specificity_score TEXT, expression_significance_score TEXT, clinical_relevance_score TEXT, expression_priority TEXT, is_clinically_relevant_expression TEXT"""
    },
    "variant_cancer_ml_features": {
        "columns": """gene_symbol TEXT, gene_name TEXT, variant_key TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, sample_count TEXT, total_mutation_count TEXT, missense_sample_count TEXT, truncating_sample_count TEXT, silent_sample_count TEXT, snv_sample_count TEXT, indel_sample_count TEXT, is_recurrent_mutation TEXT, is_hotspot_mutation TEXT, is_high_impact_cancer_variant TEXT, is_driver_candidate TEXT, mutation_frequency_category TEXT, gene_total_samples TEXT, gene_unique_sites TEXT, is_cancer_gene TEXT, is_tumor_suppressor_candidate TEXT, is_oncogene_candidate TEXT, gene_cancer_role TEXT, cancer_mutation_burden_score TEXT, cancer_priority_score TEXT, clinvar_pathogenicity TEXT, clinvar_is_pathogenic TEXT, conservation_score TEXT, cadd_phred TEXT, functional_impact_prediction TEXT, tissue_expression_in_tumors TEXT, max_tumor_expression TEXT, expression_change_relevance TEXT, cancer_disease_associations TEXT, hereditary_cancer_syndrome TEXT, has_kinase_domain_count TEXT, affected_oncogenic_domains TEXT, kinase_domain_mutations TEXT, germline_variant_frequency TEXT, is_rare TEXT, somatic_vs_germline_classification TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, driver_likelihood_score TEXT, therapeutic_target_score TEXT, prognostic_value_score TEXT"""
    },
    "variant_drug_response_ml_features": {
        "columns": """variant_pharmgkb_id TEXT, variant_name TEXT, variant_id TEXT, gene_symbol TEXT, variant_location TEXT, clinical_significance_simple TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, has_functional_domain TEXT, affects_functional_domain TEXT, phylop_score TEXT, cadd_phred TEXT, conservation_level TEXT, pathogenicity_score TEXT, mutation_severity_score TEXT, has_pharmgkb_annotation TEXT, has_high_conservation TEXT, affects_drug_metabolism TEXT, affects_drug_efficacy TEXT, is_high_impact_variant TEXT, is_hepatic_drug_metabolism_variant TEXT, is_common_pharmacogene_variant TEXT, is_potential_resistance_variant TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_liver_expressed TEXT, expression_breadth TEXT, allele_frequency TEXT, is_common_variant TEXT, is_rare_variant TEXT, drug_response_frequency_context TEXT, total_disease_count TEXT, has_cancer_disease TEXT, has_cardiovascular_disease TEXT, has_neurological_disease TEXT, primary_indication_category TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_pharmacogene TEXT, druggability_score TEXT, pharmacogene_category TEXT, drug_metabolism_role TEXT, pharmacogene_annotation_score TEXT, functional_impact_score TEXT, population_adjusted_score TEXT, tissue_specific_response_score TEXT, drug_response_priority_score TEXT, drug_response_priority TEXT, is_actionable_pharmacogene_variant TEXT, drug_response_category TEXT, clinical_actionability TEXT, indication_specific_actionability TEXT"""
    },
    "variant_impact_ml_features": {
        "columns": """variant_id TEXT, gene_name TEXT, official_symbol TEXT, chromosome TEXT, position TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, clinical_significance_simple TEXT, clinvar_pathogenicity_class TEXT, review_status TEXT, review_quality_score TEXT, variant_type TEXT, variant_name TEXT, reference_allele TEXT, alternate_allele TEXT, protein_change TEXT, cdna_change TEXT, is_missense_variant TEXT, is_frameshift_variant TEXT, is_nonsense_variant TEXT, is_splice_variant TEXT, is_snv TEXT, is_insertion TEXT, is_deletion TEXT, refseq_protein_accession TEXT, uniprot_accession TEXT, protein_name TEXT, has_functional_domain TEXT, domain_count TEXT, has_zinc_finger TEXT, has_kinase_domain TEXT, has_receptor_domain TEXT, has_sh2_domain TEXT, has_sh3_domain TEXT, has_ph_domain TEXT, affects_functional_domain TEXT, domain_impact_severity TEXT, domain_type_count TEXT, has_multiple_domain_types TEXT, mutation_severity_score TEXT, pathogenicity_score TEXT, protein_impact_category TEXT, combined_impact_score TEXT, variant_impact_tier TEXT, phylop_score TEXT, phastcons_score TEXT, gerp_score TEXT, cadd_phred TEXT, conservation_level TEXT, is_highly_conserved TEXT, is_constrained TEXT, is_likely_deleterious TEXT, conservation_impact_class TEXT, is_high_impact TEXT, is_very_high_impact TEXT, is_conservation_constrained TEXT, is_highly_conserved_region TEXT, is_domain_affecting TEXT, is_loss_of_function TEXT, is_splice_affecting TEXT, has_cadd_score TEXT, is_deleterious_by_cadd TEXT, is_splice_site_variant TEXT, splice_impact_severity TEXT, lof_category TEXT, is_kinase TEXT, is_receptor TEXT, is_enzyme TEXT, is_pharmacogene TEXT, druggability_score TEXT, is_druggable_gene TEXT, is_key_protein_type TEXT, is_well_annotated TEXT, clinical_impact_priority TEXT, tissues_expressed_count TEXT, max_expression_tpm TEXT, is_broadly_expressed TEXT, is_highly_expressed TEXT, expression_impact_context TEXT, cancer_mutation_count TEXT, is_cancer_gene TEXT, is_cancer_relevant_variant TEXT, cancer_variant_priority TEXT, disease_count TEXT, has_cancer_disease TEXT, has_neurological_disease TEXT, has_metabolic_disease TEXT, has_cardiovascular_disease TEXT, is_disease_associated_gene TEXT, disease_impact_category TEXT, disease_specific_priority TEXT, gene_total_variants TEXT, gene_high_impact_count TEXT, gene_very_high_impact_count TEXT, gene_lof_count TEXT, gene_splice_variant_count TEXT, gene_domain_affecting_count TEXT, gene_avg_impact_score TEXT, gene_max_impact_score TEXT, gene_impact_burden TEXT, gene_lof_tolerance TEXT, gene_variant_impact_priority TEXT"""
    },
    "variant_population_ml_features": {
        "columns": """variant_id TEXT, variant_key TEXT, gene_symbol TEXT, gene_name TEXT, chromosome TEXT, position TEXT, reference_allele TEXT, alternate_allele TEXT, allele_frequency TEXT, frequency_category TEXT, is_ultra_rare_variant TEXT, is_very_rare_variant TEXT, is_rare_variant TEXT, is_low_frequency_variant TEXT, is_common_variant TEXT, frequency_tier TEXT, clinical_significance TEXT, is_pathogenic TEXT, is_benign TEXT, is_vus TEXT, is_germline TEXT, is_somatic TEXT, rarity_score TEXT, carrier_risk_score TEXT, pathogenicity_likelihood_score TEXT, clinvar_pathogenic TEXT, clinvar_benign TEXT, pathogenicity_score TEXT, conservation_level TEXT, pathogenicity_frequency_conflict TEXT, rare_pathogenic_variant TEXT, common_benign_validation TEXT, total_gene_variants TEXT, lof_variants TEXT, gene_mutation_tolerance TEXT, gene_constraint_score TEXT, total_disease_count TEXT, disease_allele_frequency TEXT, carrier_frequency_by_disease TEXT, somatic_frequency TEXT, germline_cancer_predisposition TEXT, expression_tissues TEXT, expression_frequency_correlation TEXT, tissue_specific_allele_effects TEXT, is_clinically_actionable_rare_variant TEXT, is_carrier_screening_candidate TEXT, population_priority TEXT, screening_recommendation TEXT, clinical_significance_frequency_score TEXT, carrier_risk_score_adjusted TEXT, pathogenicity_likelihood_refined TEXT"""
    },
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
        run_psql_command(f"SELECT COUNT(*) FROM gold.{table_name}")
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
        print(f"  SKIP: CSV not found (not yet downloaded)")
        return None  # None = skipped, not failed

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
    print(f"Tables configured: {len(TABLES)}")
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
        if table_name in SKIP_TABLES:
            print(f"\n{table_name}: SKIP (excluded temp table)")
            continue
        success = load_table(table_name, table_config, checkpoint)
        if success is not None:  # None means CSV not found = skipped
            results[table_name] = success

    overall_end = datetime.now()
    total_duration = (overall_end - overall_start).total_seconds()

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total time: {total_duration:.1f}s ({total_duration/60:.1f} min)")

    successful = [t for t, s in results.items() if s]
    failed = [t for t, s in results.items() if not s]
    skipped_no_csv = len(TABLES) - len(results) - len(SKIP_TABLES)

    print(f"\nSuccessful: {len(successful)}/{len(TABLES) - len(SKIP_TABLES)}")
    for t in successful:
        print(f"  - {t}")

    if skipped_no_csv > 0:
        print(f"\nSkipped (CSV not downloaded yet): {skipped_no_csv}")

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
