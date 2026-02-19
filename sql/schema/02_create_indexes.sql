-- ====================================================================
-- CREATE INDEXES - ULTRA-ENRICHED GOLD LAYER
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: January 14, 2026
-- ====================================================================
-- Purpose: Create optimized indexes for ultra-enriched gold layer tables
--          Supports 100+ columns with functional flags, disease categories,
--          cellular locations, and clinical utility scores
-- ====================================================================
-- ====================================================================
-- GENE FEATURES TABLE INDEXES (105 columns)
-- ====================================================================
-- Primary indexes (core identifiers)
CREATE INDEX IF NOT EXISTS idx_gene_features_gene_name ON gold.gene_features(gene_name);
CREATE INDEX IF NOT EXISTS idx_gene_features_gene_id ON gold.gene_features(gene_id);
CREATE INDEX IF NOT EXISTS idx_gene_features_chromosome ON gold.gene_features(chromosome);
-- Database cross-references (NEW - ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_features_mim_id ON gold.gene_features(mim_id);
CREATE INDEX IF NOT EXISTS idx_gene_features_hgnc_id ON gold.gene_features(hgnc_id);
CREATE INDEX IF NOT EXISTS idx_gene_features_ensembl_id ON gold.gene_features(ensembl_id);
-- Mutation count and severity indexes
CREATE INDEX IF NOT EXISTS idx_gene_features_mutation_count ON gold.gene_features(mutation_count);
CREATE INDEX IF NOT EXISTS idx_gene_features_pathogenic_count ON gold.gene_features(pathogenic_count);
CREATE INDEX IF NOT EXISTS idx_gene_features_pathogenic_ratio ON gold.gene_features(pathogenic_ratio);
-- Risk scoring indexes
CREATE INDEX IF NOT EXISTS idx_gene_features_risk_level ON gold.gene_features(risk_level);
CREATE INDEX IF NOT EXISTS idx_gene_features_risk_score ON gold.gene_features(risk_score);
-- Clinical utility indexes (NEW - ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_features_avg_clinical_actionability ON gold.gene_features(avg_clinical_actionability);
CREATE INDEX IF NOT EXISTS idx_gene_features_avg_clinical_utility ON gold.gene_features(avg_clinical_utility);
CREATE INDEX IF NOT EXISTS idx_gene_features_avg_mutation_severity ON gold.gene_features(avg_mutation_severity);
-- Functional classification indexes (NEW - ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_features_primary_function ON gold.gene_features(primary_function);
CREATE INDEX IF NOT EXISTS idx_gene_features_biological_process ON gold.gene_features(biological_process);
CREATE INDEX IF NOT EXISTS idx_gene_features_cellular_location ON gold.gene_features(cellular_location);
CREATE INDEX IF NOT EXISTS idx_gene_features_druggability_score ON gold.gene_features(druggability_score);
-- Functional flag indexes (17 protein types - NEW)
CREATE INDEX IF NOT EXISTS idx_gene_features_is_kinase ON gold.gene_features(is_kinase)
WHERE is_kinase = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_phosphatase ON gold.gene_features(is_phosphatase)
WHERE is_phosphatase = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_receptor ON gold.gene_features(is_receptor)
WHERE is_receptor = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_gpcr ON gold.gene_features(is_gpcr)
WHERE is_gpcr = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_transcription_factor ON gold.gene_features(is_transcription_factor)
WHERE is_transcription_factor = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_enzyme ON gold.gene_features(is_enzyme)
WHERE is_enzyme = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_transporter ON gold.gene_features(is_transporter)
WHERE is_transporter = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_is_channel ON gold.gene_features(is_channel)
WHERE is_channel = true;
-- Disease category indexes (9 categories - NEW)
CREATE INDEX IF NOT EXISTS idx_gene_features_cancer_related ON gold.gene_features(cancer_related)
WHERE cancer_related = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_immune_related ON gold.gene_features(immune_related)
WHERE immune_related = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_neurological_related ON gold.gene_features(neurological_related)
WHERE neurological_related = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_cardiovascular_related ON gold.gene_features(cardiovascular_related)
WHERE cardiovascular_related = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_metabolic_related ON gold.gene_features(metabolic_related)
WHERE metabolic_related = true;
-- Cellular location indexes (partial - for most common locations)
CREATE INDEX IF NOT EXISTS idx_gene_features_nuclear ON gold.gene_features(nuclear)
WHERE nuclear = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_mitochondrial ON gold.gene_features(mitochondrial)
WHERE mitochondrial = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_membrane ON gold.gene_features(membrane)
WHERE membrane = true;
-- Mutation type count indexes (NEW - ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_features_frameshift_count ON gold.gene_features(frameshift_count);
CREATE INDEX IF NOT EXISTS idx_gene_features_nonsense_count ON gold.gene_features(nonsense_count);
CREATE INDEX IF NOT EXISTS idx_gene_features_missense_count ON gold.gene_features(missense_count);
-- Ratio indexes (NEW)
CREATE INDEX IF NOT EXISTS idx_gene_features_severe_mutation_ratio ON gold.gene_features(severe_mutation_ratio);
CREATE INDEX IF NOT EXISTS idx_gene_features_germline_ratio ON gold.gene_features(germline_ratio);
-- Quality indexes (NEW)
CREATE INDEX IF NOT EXISTS idx_gene_features_avg_review_quality ON gold.gene_features(avg_review_quality);
CREATE INDEX IF NOT EXISTS idx_gene_features_is_well_characterized ON gold.gene_features(is_well_characterized)
WHERE is_well_characterized = true;
-- Composite indexes for common queries (ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_features_kinase_cancer ON gold.gene_features(is_kinase, cancer_related, druggability_score)
WHERE is_kinase = true
    AND cancer_related = true;
CREATE INDEX IF NOT EXISTS idx_gene_features_druggable_high_risk ON gold.gene_features(druggability_score, risk_level, pathogenic_ratio)
WHERE druggability_score >= 2;
CREATE INDEX IF NOT EXISTS idx_gene_features_function_risk ON gold.gene_features(primary_function, risk_level, mutation_count);
CREATE INDEX IF NOT EXISTS idx_gene_features_clinical_utility ON gold.gene_features(avg_clinical_utility, avg_clinical_actionability)
WHERE avg_clinical_utility >= 3;
-- ====================================================================
-- CHROMOSOME FEATURES TABLE INDEXES (20 columns)
-- ====================================================================
CREATE INDEX IF NOT EXISTS idx_chromosome_features_chromosome ON gold.chromosome_features(chromosome);
CREATE INDEX IF NOT EXISTS idx_chromosome_features_gene_count ON gold.chromosome_features(gene_count);
CREATE INDEX IF NOT EXISTS idx_chromosome_features_variant_count ON gold.chromosome_features(variant_count);
CREATE INDEX IF NOT EXISTS idx_chromosome_features_pathogenic_percentage ON gold.chromosome_features(pathogenic_percentage);
-- NEW: Clinical metrics
CREATE INDEX IF NOT EXISTS idx_chromosome_features_avg_actionability ON gold.chromosome_features(avg_actionability);
CREATE INDEX IF NOT EXISTS idx_chromosome_features_avg_severity ON gold.chromosome_features(avg_severity);
-- ====================================================================
-- GENE-DISEASE ASSOCIATION TABLE INDEXES (25 columns)
-- ====================================================================
-- Core relationship indexes
CREATE INDEX IF NOT EXISTS idx_gene_disease_gene_name ON gold.gene_disease_association(gene_name);
CREATE INDEX IF NOT EXISTS idx_gene_disease_disease ON gold.gene_disease_association(disease);
CREATE INDEX IF NOT EXISTS idx_gene_disease_mutation_count ON gold.gene_disease_association(mutation_count);
CREATE INDEX IF NOT EXISTS idx_gene_disease_pathogenic_ratio ON gold.gene_disease_association(pathogenic_ratio);
CREATE INDEX IF NOT EXISTS idx_gene_disease_association_strength ON gold.gene_disease_association(association_strength);
-- NEW: Disease database IDs (ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_disease_omim_id ON gold.gene_disease_association(omim_disease_id);
CREATE INDEX IF NOT EXISTS idx_gene_disease_orphanet_id ON gold.gene_disease_association(orphanet_disease_id);
CREATE INDEX IF NOT EXISTS idx_gene_disease_mondo_id ON gold.gene_disease_association(mondo_disease_id);
-- NEW: Disease characteristic flags
CREATE INDEX IF NOT EXISTS idx_gene_disease_cancer_flag ON gold.gene_disease_association(cancer_disease_flag)
WHERE cancer_disease_flag > 0;
CREATE INDEX IF NOT EXISTS idx_gene_disease_syndrome_flag ON gold.gene_disease_association(syndrome_flag)
WHERE syndrome_flag > 0;
CREATE INDEX IF NOT EXISTS idx_gene_disease_hereditary_flag ON gold.gene_disease_association(hereditary_flag)
WHERE hereditary_flag > 0;
CREATE INDEX IF NOT EXISTS idx_gene_disease_rare_flag ON gold.gene_disease_association(rare_disease_flag)
WHERE rare_disease_flag > 0;
-- NEW: Clinical utility indexes
CREATE INDEX IF NOT EXISTS idx_gene_disease_avg_actionability ON gold.gene_disease_association(avg_actionability);
CREATE INDEX IF NOT EXISTS idx_gene_disease_avg_clinical_utility ON gold.gene_disease_association(avg_clinical_utility);
CREATE INDEX IF NOT EXISTS idx_gene_disease_avg_severity ON gold.gene_disease_association(avg_severity);
-- Composite indexes for common queries (ultra-enriched)
CREATE INDEX IF NOT EXISTS idx_gene_disease_gene_strength ON gold.gene_disease_association(
    gene_name,
    association_strength,
    pathogenic_ratio
);
CREATE INDEX IF NOT EXISTS idx_gene_disease_cancer_high_utility ON gold.gene_disease_association(cancer_disease_flag, avg_clinical_utility)
WHERE cancer_disease_flag > 0
    AND avg_clinical_utility >= 3;
CREATE INDEX IF NOT EXISTS idx_gene_disease_omim_strong ON gold.gene_disease_association(omim_disease_id, association_strength)
WHERE omim_disease_id IS NOT NULL;
-- ====================================================================
-- ML FEATURES TABLE INDEXES (100 columns)
-- ====================================================================
-- Primary identifier
CREATE INDEX IF NOT EXISTS idx_ml_features_gene_name ON gold.ml_features(gene_name);
CREATE INDEX IF NOT EXISTS idx_ml_features_gene_id ON gold.ml_features(gene_id);
CREATE INDEX IF NOT EXISTS idx_ml_features_chromosome ON gold.ml_features(chromosome);
-- Risk and scoring
CREATE INDEX IF NOT EXISTS idx_ml_features_risk_level ON gold.ml_features(risk_level);
CREATE INDEX IF NOT EXISTS idx_ml_features_risk_score ON gold.ml_features(risk_score);
CREATE INDEX IF NOT EXISTS idx_ml_features_pathogenic_ratio ON gold.ml_features(pathogenic_ratio);
-- Clinical utility (NEW)
CREATE INDEX IF NOT EXISTS idx_ml_features_avg_clinical_utility ON gold.ml_features(avg_clinical_utility);
CREATE INDEX IF NOT EXISTS idx_ml_features_druggability_score ON gold.ml_features(druggability_score);
-- Functional flags (subset for ML)
CREATE INDEX IF NOT EXISTS idx_ml_features_is_kinase ON gold.ml_features(is_kinase)
WHERE is_kinase = true;
CREATE INDEX IF NOT EXISTS idx_ml_features_cancer_related ON gold.ml_features(cancer_related)
WHERE cancer_related = true;
CREATE INDEX IF NOT EXISTS idx_ml_features_primary_function ON gold.ml_features(primary_function);
-- Quality
CREATE INDEX IF NOT EXISTS idx_ml_features_is_well_characterized ON gold.ml_features(is_well_characterized)
WHERE is_well_characterized = true;
-- Composite index for ML model training
CREATE INDEX IF NOT EXISTS idx_ml_features_training_set ON gold.ml_features(
    risk_level,
    is_well_characterized,
    mutation_count
)
WHERE is_well_characterized = true;
-- ====================================================================
-- ANALYSIS COMPLETE
-- ====================================================================
-- Verify indexes created
SELECT schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'gold'
ORDER BY tablename,
    indexname;
-- Index statistics
SELECT schemaname,
    tablename,
    COUNT(*) as index_count
FROM pg_indexes
WHERE schemaname = 'gold'
GROUP BY schemaname,
    tablename
ORDER BY tablename;