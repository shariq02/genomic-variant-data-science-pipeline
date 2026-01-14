-- ====================================================================
-- CREATE VIEWS - ULTRA-ENRICHED GOLD LAYER
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: January 14, 2026
-- ====================================================================
-- Purpose: Create analytical views for ultra-enriched gold layer
--          Leverages 100+ columns including functional flags, disease
--          categories, cellular locations, and clinical utility scores
-- ====================================================================
-- Drop existing views first (CASCADE will be handled by load script)
-- ====================================================================
-- HIGH-RISK GENES VIEW (ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_high_risk_genes AS
SELECT gene_name,
    gene_id,
    chromosome,
    map_location,
    -- Mutation metrics
    mutation_count,
    pathogenic_count,
    likely_pathogenic_count,
    total_pathogenic,
    pathogenic_ratio,
    -- NEW: Mutation types
    frameshift_count,
    nonsense_count,
    splice_count,
    missense_count,
    severe_mutation_ratio,
    -- NEW: Clinical utility
    avg_clinical_actionability,
    avg_clinical_utility,
    avg_mutation_severity,
    drug_response_count,
    -- Risk scoring
    risk_level,
    risk_score,
    -- NEW: Functional classification
    primary_function,
    biological_process,
    cellular_location,
    druggability_score,
    -- NEW: Disease categories (9 categories)
    cancer_related,
    immune_related,
    neurological_related,
    cardiovascular_related,
    metabolic_related,
    developmental_related,
    alzheimer_related,
    diabetes_related,
    breast_cancer_related,
    -- NEW: Functional protein types (key ones)
    is_kinase,
    is_receptor,
    is_gpcr,
    is_transcription_factor,
    is_enzyme,
    -- NEW: Database IDs
    mim_id,
    hgnc_id,
    ensembl_id,
    -- Quality
    avg_review_quality,
    is_well_characterized,
    disease_count
FROM gold.gene_features
WHERE risk_level IN ('High', 'Medium')
    AND mutation_count >= 10
ORDER BY risk_score DESC,
    pathogenic_ratio DESC;
COMMENT ON VIEW gold.v_high_risk_genes IS 'Ultra-enriched high-risk genes with clinical utility scores, functional classifications, and disease categories';
-- ====================================================================
-- TOP GENES BY MUTATIONS (ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_top_genes_by_mutations AS
SELECT gene_name,
    gene_id,
    chromosome,
    -- Mutation counts
    mutation_count,
    total_pathogenic,
    pathogenic_ratio,
    -- NEW: Mutation type breakdown
    frameshift_count,
    nonsense_count,
    splice_count,
    missense_count,
    deletion_count,
    insertion_count,
    -- NEW: Origin breakdown
    germline_count,
    somatic_count,
    de_novo_count,
    germline_ratio,
    -- Risk
    risk_level,
    risk_score,
    -- NEW: Clinical metrics
    avg_clinical_utility,
    avg_clinical_actionability,
    drug_response_count,
    -- Function
    primary_function,
    druggability_score,
    -- Disease
    disease_count,
    cancer_variant_count,
    -- Quality
    avg_review_quality,
    recently_evaluated_count
FROM gold.gene_features
ORDER BY mutation_count DESC
LIMIT 100;
COMMENT ON VIEW gold.v_top_genes_by_mutations IS 'Top 100 genes by mutation count with ultra-enriched mutation type breakdown and clinical metrics';
-- ====================================================================
-- DRUGGABLE TARGETS VIEW (NEW - ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_druggable_targets AS
SELECT gene_name,
    gene_id,
    chromosome,
    -- Functional classification
    primary_function,
    biological_process,
    cellular_location,
    druggability_score,
    -- Specific protein types
    is_kinase,
    is_phosphatase,
    is_receptor,
    is_gpcr,
    is_enzyme,
    is_channel,
    is_transporter,
    -- Mutation metrics
    mutation_count,
    pathogenic_ratio,
    severe_mutation_ratio,
    -- Clinical utility
    avg_clinical_utility,
    avg_clinical_actionability,
    drug_response_count,
    -- Disease associations
    cancer_related,
    immune_related,
    neurological_related,
    cancer_variant_count,
    -- Risk
    risk_level,
    risk_score,
    -- Database IDs
    mim_id,
    hgnc_id,
    ensembl_id,
    -- Quality
    is_well_characterized
FROM gold.gene_features
WHERE druggability_score >= 2
    AND mutation_count >= 5
ORDER BY druggability_score DESC,
    avg_clinical_utility DESC,
    pathogenic_ratio DESC;
COMMENT ON VIEW gold.v_druggable_targets IS 'Druggable therapeutic targets with high clinical utility and disease relevance';
-- ====================================================================
-- CANCER KINASES VIEW (NEW - ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_cancer_kinases AS
SELECT gene_name,
    gene_id,
    chromosome,
    -- Kinase-specific
    primary_function,
    druggability_score,
    -- Mutation metrics
    mutation_count,
    cancer_variant_count,
    pathogenic_ratio,
    severe_mutation_ratio,
    -- Clinical utility
    avg_clinical_utility,
    avg_clinical_actionability,
    drug_response_count,
    -- Cellular location
    cellular_location,
    membrane,
    nuclear,
    cytoplasmic,
    -- Risk
    risk_level,
    risk_score,
    -- Disease specifics
    breast_cancer_related,
    -- Quality
    avg_review_quality,
    is_well_characterized,
    -- Database IDs
    mim_id,
    ensembl_id
FROM gold.gene_features
WHERE is_kinase = true
    AND cancer_related = true
    AND mutation_count >= 10
ORDER BY cancer_variant_count DESC,
    druggability_score DESC;
COMMENT ON VIEW gold.v_cancer_kinases IS 'Cancer-related kinases - key therapeutic targets with clinical utility scores';
-- ====================================================================
-- FUNCTIONAL GENE SUMMARY (NEW - ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_functional_gene_summary AS
SELECT primary_function,
    biological_process,
    -- Counts
    COUNT(*) as gene_count,
    SUM(
        CASE
            WHEN risk_level = 'High' THEN 1
            ELSE 0
        END
    ) as high_risk_count,
    SUM(
        CASE
            WHEN druggability_score >= 3 THEN 1
            ELSE 0
        END
    ) as highly_druggable_count,
    -- Averages
    ROUND(AVG(mutation_count)::numeric, 2) as avg_mutations,
    ROUND(AVG(pathogenic_ratio)::numeric, 4) as avg_pathogenic_ratio,
    ROUND(AVG(druggability_score)::numeric, 2) as avg_druggability,
    ROUND(AVG(avg_clinical_utility)::numeric, 2) as avg_clinical_utility,
    -- Disease associations
    SUM(
        CASE
            WHEN cancer_related THEN 1
            ELSE 0
        END
    ) as cancer_genes,
    SUM(
        CASE
            WHEN immune_related THEN 1
            ELSE 0
        END
    ) as immune_genes,
    SUM(
        CASE
            WHEN neurological_related THEN 1
            ELSE 0
        END
    ) as neuro_genes,
    -- Totals
    SUM(mutation_count) as total_mutations,
    SUM(cancer_variant_count) as total_cancer_variants
FROM gold.gene_features
GROUP BY primary_function,
    biological_process
ORDER BY gene_count DESC;
COMMENT ON VIEW gold.v_functional_gene_summary IS 'Summary statistics by functional category with clinical utility metrics';
-- ====================================================================
-- CHROMOSOME RISK PROFILE (ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_chromosome_risk_profile AS
SELECT chromosome,
    gene_count,
    variant_count,
    -- Pathogenic metrics
    pathogenic_count,
    likely_pathogenic_count,
    total_pathogenic,
    pathogenic_percentage,
    -- NEW: Mutation types
    frameshift_count,
    nonsense_count,
    splice_count,
    missense_count,
    -- NEW: Origin
    germline_count,
    somatic_count,
    -- NEW: Clinical metrics
    avg_actionability,
    avg_severity,
    -- Averages
    avg_mutations_per_gene,
    -- Benign
    benign_count,
    vus_count,
    -- Calculated metrics
    ROUND(
        (
            pathogenic_count::numeric / NULLIF(variant_count, 0)
        ) * 100,
        2
    ) as pathogenic_rate,
    ROUND(
        (frameshift_count + nonsense_count)::numeric / NULLIF(variant_count, 0) * 100,
        2
    ) as severe_mutation_rate
FROM gold.chromosome_features
ORDER BY CASE
        WHEN chromosome ~ '^[0-9]+$' THEN chromosome::integer
        ELSE 99
    END,
    chromosome;
COMMENT ON VIEW gold.v_chromosome_risk_profile IS 'Ultra-enriched chromosome-level risk profile with mutation types and clinical scores';
-- ====================================================================
-- GENE-DISEASE MATRIX (ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_gene_disease_matrix AS
SELECT gene_name,
    disease,
    -- Database IDs (NEW)
    omim_disease_id,
    orphanet_disease_id,
    mondo_disease_id,
    -- Mutation metrics
    mutation_count,
    pathogenic_count,
    total_pathogenic,
    pathogenic_ratio,
    association_strength,
    -- NEW: Mutation types
    frameshift_count,
    nonsense_count,
    missense_count,
    -- NEW: Disease characteristics
    cancer_disease_flag,
    syndrome_flag,
    hereditary_flag,
    rare_disease_flag,
    -- NEW: Clinical utility
    avg_quality,
    avg_actionability,
    avg_clinical_utility,
    avg_severity,
    -- Benign
    benign_count,
    total_benign
FROM gold.gene_disease_association
WHERE association_strength IN ('Strong', 'Moderate')
    AND mutation_count >= 3
ORDER BY pathogenic_ratio DESC,
    mutation_count DESC;
COMMENT ON VIEW gold.v_gene_disease_matrix IS 'Ultra-enriched gene-disease associations with database IDs and clinical utility scores';
-- ====================================================================
-- DISEASE COMPLEXITY VIEW (ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_disease_complexity AS
SELECT disease,
    -- Database IDs (NEW)
    MAX(omim_disease_id) as omim_id,
    MAX(orphanet_disease_id) as orphanet_id,
    MAX(mondo_disease_id) as mondo_id,
    -- Gene counts
    COUNT(DISTINCT gene_name) as gene_count,
    SUM(mutation_count) as total_mutations,
    -- Pathogenic metrics
    SUM(pathogenic_count) as total_pathogenic,
    ROUND(AVG(pathogenic_ratio)::numeric, 4) as avg_pathogenic_ratio,
    -- NEW: Disease characteristics
    MAX(cancer_disease_flag) as is_cancer_disease,
    MAX(syndrome_flag) as is_syndrome,
    MAX(hereditary_flag) as is_hereditary,
    MAX(rare_disease_flag) as is_rare_disease,
    -- NEW: Clinical metrics
    ROUND(AVG(avg_actionability)::numeric, 2) as avg_actionability,
    ROUND(AVG(avg_clinical_utility)::numeric, 2) as avg_clinical_utility,
    ROUND(AVG(avg_severity)::numeric, 2) as avg_severity,
    -- Association strength
    SUM(
        CASE
            WHEN association_strength = 'Strong' THEN 1
            ELSE 0
        END
    ) as strong_associations,
    SUM(
        CASE
            WHEN association_strength = 'Moderate' THEN 1
            ELSE 0
        END
    ) as moderate_associations
FROM gold.gene_disease_association
GROUP BY disease
HAVING COUNT(DISTINCT gene_name) >= 2
ORDER BY gene_count DESC,
    total_mutations DESC
LIMIT 200;
COMMENT ON VIEW gold.v_disease_complexity IS 'Disease complexity analysis with database IDs and clinical utility metrics - top 200 diseases';
-- ====================================================================
-- ML FEATURES SUMMARY (ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_ml_features_summary AS
SELECT risk_level,
    primary_function,
    -- Counts
    COUNT(*) as gene_count,
    -- Mutation averages
    ROUND(AVG(mutation_count)::numeric, 2) as avg_mutations,
    ROUND(AVG(pathogenic_ratio)::numeric, 4) as avg_pathogenic_ratio,
    ROUND(AVG(severe_mutation_ratio)::numeric, 4) as avg_severe_ratio,
    -- NEW: Clinical utility averages
    ROUND(AVG(avg_clinical_utility)::numeric, 2) as avg_clinical_utility,
    ROUND(AVG(avg_clinical_actionability)::numeric, 2) as avg_actionability,
    ROUND(AVG(druggability_score)::numeric, 2) as avg_druggability,
    -- Quality metrics
    SUM(
        CASE
            WHEN is_well_characterized THEN 1
            ELSE 0
        END
    ) as well_characterized_count,
    ROUND(AVG(avg_review_quality)::numeric, 2) as avg_review_quality,
    -- Disease associations
    SUM(
        CASE
            WHEN cancer_related THEN 1
            ELSE 0
        END
    ) as cancer_genes,
    SUM(
        CASE
            WHEN immune_related THEN 1
            ELSE 0
        END
    ) as immune_genes,
    -- Protein types
    SUM(
        CASE
            WHEN is_kinase THEN 1
            ELSE 0
        END
    ) as kinase_count,
    SUM(
        CASE
            WHEN is_receptor THEN 1
            ELSE 0
        END
    ) as receptor_count,
    SUM(
        CASE
            WHEN is_enzyme THEN 1
            ELSE 0
        END
    ) as enzyme_count
FROM gold.ml_features
GROUP BY risk_level,
    primary_function
ORDER BY CASE
        risk_level
        WHEN 'High' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low' THEN 3
        ELSE 4
    END,
    gene_count DESC;
COMMENT ON VIEW gold.v_ml_features_summary IS 'ML features summary with ultra-enriched functional categories and clinical metrics';
-- ====================================================================
-- THERAPEUTIC TARGETS VIEW (NEW - ULTRA-ENRICHED)
-- ====================================================================
CREATE OR REPLACE VIEW gold.v_therapeutic_targets AS
SELECT gene_name,
    primary_function,
    -- Druggability
    druggability_score,
    -- Clinical utility
    avg_clinical_utility,
    avg_clinical_actionability,
    drug_response_count,
    -- Mutation metrics
    mutation_count,
    pathogenic_ratio,
    cancer_variant_count,
    -- Protein types
    is_kinase,
    is_receptor,
    is_gpcr,
    is_enzyme,
    -- Disease relevance
    cancer_related,
    immune_related,
    alzheimer_related,
    diabetes_related,
    -- Cellular location (for drug delivery)
    cellular_location,
    membrane,
    extracellular,
    -- Database IDs
    mim_id,
    ensembl_id,
    -- Quality
    is_well_characterized
FROM gold.ml_features
WHERE druggability_score >= 3
    AND avg_clinical_utility >= 2
    AND mutation_count >= 10
ORDER BY druggability_score DESC,
    avg_clinical_utility DESC,
    pathogenic_ratio DESC;
COMMENT ON VIEW gold.v_therapeutic_targets IS 'High-priority therapeutic targets with druggability and clinical utility scores';
-- ====================================================================
-- VERIFICATION
-- ====================================================================
-- List all views
SELECT table_schema,
    table_name,
    view_definition
FROM information_schema.views
WHERE table_schema = 'gold'
ORDER BY table_name;
-- View row counts
SELECT 'v_high_risk_genes' as view_name,
    COUNT(*) as row_count
FROM gold.v_high_risk_genes
UNION ALL
SELECT 'v_top_genes_by_mutations',
    COUNT(*)
FROM gold.v_top_genes_by_mutations
UNION ALL
SELECT 'v_druggable_targets',
    COUNT(*)
FROM gold.v_druggable_targets
UNION ALL
SELECT 'v_cancer_kinases',
    COUNT(*)
FROM gold.v_cancer_kinases
UNION ALL
SELECT 'v_functional_gene_summary',
    COUNT(*)
FROM gold.v_functional_gene_summary
UNION ALL
SELECT 'v_chromosome_risk_profile',
    COUNT(*)
FROM gold.v_chromosome_risk_profile
UNION ALL
SELECT 'v_gene_disease_matrix',
    COUNT(*)
FROM gold.v_gene_disease_matrix
UNION ALL
SELECT 'v_disease_complexity',
    COUNT(*)
FROM gold.v_disease_complexity
UNION ALL
SELECT 'v_ml_features_summary',
    COUNT(*)
FROM gold.v_ml_features_summary
UNION ALL
SELECT 'v_therapeutic_targets',
    COUNT(*)
FROM gold.v_therapeutic_targets
ORDER BY view_name;