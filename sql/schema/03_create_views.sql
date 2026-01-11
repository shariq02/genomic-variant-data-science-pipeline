-- ====================================================================
-- DATABASE VIEW CREATION
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: 31 December 2025
-- ====================================================================
-- FILE 3: sql/schema/03_create_views.sql
-- Purpose: Create useful views for analysis
-- ====================================================================
-- CREATE VIEWS FOR ANALYSIS
-- DNA Gene Mapping Project
-- ====================================================================
-- SILVER LAYER VIEWS
-- ====================================================================
-- View: v_gene_with_stats
-- Purpose: Genes with basic variant statistics
CREATE OR REPLACE VIEW silver.v_gene_with_stats AS
SELECT g.gene_id,
    g.gene_name,
    g.official_symbol,
    g.chromosome,
    g.gene_type,
    g.gene_length,
    COUNT(v.accession) AS variant_count,
    COUNT(
        CASE
            WHEN v.clinical_significance ILIKE '%pathogenic%' THEN 1
        END
    ) AS pathogenic_count,
    COUNT(
        CASE
            WHEN v.clinical_significance ILIKE '%benign%' THEN 1
        END
    ) AS benign_count
FROM silver.genes_clean g
    LEFT JOIN silver.variants_clean v ON g.gene_name = v.gene_name
GROUP BY g.gene_id,
    g.gene_name,
    g.official_symbol,
    g.chromosome,
    g.gene_type,
    g.gene_length;
COMMENT ON VIEW silver.v_gene_with_stats IS 'Genes with basic variant statistics';
-- --------------------------------------------------------------------
-- View: v_pathogenic_variants
-- Purpose: Only pathogenic and likely pathogenic variants
-- --------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_pathogenic_variants AS
SELECT variant_id,
    accession,
    gene_name,
    clinical_significance,
    disease,
    chromosome,
    position,
    variant_type,
    molecular_consequence,
    protein_change
FROM silver.variants_clean
WHERE clinical_significance ILIKE '%pathogenic%'
ORDER BY gene_name,
    position;
COMMENT ON VIEW silver.v_pathogenic_variants IS 'Only pathogenic and likely pathogenic variants';
-- ====================================================================
-- GOLD LAYER VIEWS - Based on actual gold tables from Databricks
-- ====================================================================
-- View: v_gene_disease_matrix
-- Purpose: Gene-disease association matrix for analysis
CREATE OR REPLACE VIEW gold.v_gene_disease_matrix AS
SELECT gene_name,
    disease,
    mutation_count,
    pathogenic_count,
    pathogenic_ratio,
    association_strength
FROM gold.gene_disease_association
WHERE mutation_count >= 3
ORDER BY gene_name,
    pathogenic_ratio DESC;
COMMENT ON VIEW gold.v_gene_disease_matrix IS 'Gene-disease association matrix';
-- View: v_disease_complexity
-- Purpose: Diseases ranked by complexity (number of genes involved)
CREATE OR REPLACE VIEW gold.v_disease_complexity AS
SELECT disease,
    COUNT(DISTINCT gene_name) as genes_affected,
    SUM(mutation_count) as total_mutations,
    AVG(pathogenic_ratio) as avg_pathogenic_ratio
FROM gold.gene_disease_association
WHERE disease IS NOT NULL
GROUP BY disease
HAVING COUNT(DISTINCT gene_name) >= 1
ORDER BY genes_affected DESC,
    total_mutations DESC;
COMMENT ON VIEW gold.v_disease_complexity IS 'Diseases ranked by complexity (multiple genes)';
-- View: v_high_risk_genes
-- Purpose: Genes with high pathogenic ratio
CREATE OR REPLACE VIEW gold.v_high_risk_genes AS
SELECT gene_name,
    chromosome,
    mutation_count,
    pathogenic_count,
    pathogenic_ratio,
    risk_level,
    risk_score,
    disease_count
FROM gold.gene_features
WHERE risk_level = 'High'
ORDER BY risk_score DESC;
COMMENT ON VIEW gold.v_high_risk_genes IS 'High-risk genes sorted by risk score';
-- View: v_top_genes_by_mutations
-- Purpose: Top genes by mutation count
CREATE OR REPLACE VIEW gold.v_top_genes_by_mutations AS
SELECT gene_name,
    chromosome,
    mutation_count,
    pathogenic_count,
    pathogenic_ratio,
    disease_count,
    risk_level
FROM gold.gene_features
ORDER BY mutation_count DESC
LIMIT 20;
COMMENT ON VIEW gold.v_top_genes_by_mutations IS 'Top 20 genes by total mutation count';
-- View: v_chromosome_risk_profile
-- Purpose: Chromosome risk profile with detailed stats
CREATE OR REPLACE VIEW gold.v_chromosome_risk_profile AS
SELECT chromosome,
    gene_count,
    variant_count,
    pathogenic_count,
    pathogenic_percentage,
    avg_mutations_per_gene,
    total_pathogenic,
    CASE
        WHEN pathogenic_percentage > 50 THEN 'High Risk'
        WHEN pathogenic_percentage > 30 THEN 'Moderate Risk'
        ELSE 'Low Risk'
    END as chromosome_risk_level
FROM gold.chromosome_features
ORDER BY CASE
        chromosome
        WHEN 'X' THEN 23
        WHEN 'Y' THEN 24
        WHEN 'MT' THEN 25
        ELSE chromosome::INTEGER
    END;
COMMENT ON VIEW gold.v_chromosome_risk_profile IS 'Chromosome risk profile with classification';
-- View: v_ml_features_summary
-- Purpose: ML features with risk classification
CREATE OR REPLACE VIEW gold.v_ml_features_summary AS
SELECT gene_name,
    chromosome,
    mutation_count,
    pathogenic_count,
    total_pathogenic,
    pathogenic_ratio,
    disease_count,
    variant_type_count,
    mutation_density,
    risk_level
FROM gold.ml_features
WHERE mutation_count > 0
ORDER BY pathogenic_ratio DESC;
COMMENT ON VIEW gold.v_ml_features_summary IS 'ML features summary with risk levels';
-- ====================================================================
-- COMPLETION MESSAGE
-- ====================================================================
DO $$ BEGIN RAISE NOTICE '';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'VIEWS CREATED SUCCESSFULLY';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Created 6 analytical views:';
RAISE NOTICE '  - v_gene_disease_matrix';
RAISE NOTICE '  - v_disease_complexity';
RAISE NOTICE '  - v_high_risk_genes';
RAISE NOTICE '  - v_top_genes_by_mutations';
RAISE NOTICE '  - v_chromosome_risk_profile';
RAISE NOTICE '  - v_ml_features_summary';
RAISE NOTICE '====================================================================';
END $$;