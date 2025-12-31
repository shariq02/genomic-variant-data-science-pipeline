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
    COUNT(v.variant_id) as variant_count,
    COUNT(
        CASE
            WHEN v.clinical_significance ILIKE '%pathogenic%' THEN 1
        END
    ) as pathogenic_count,
    COUNT(
        CASE
            WHEN v.clinical_significance ILIKE '%benign%' THEN 1
        END
    ) as benign_count
FROM silver.genes g
    LEFT JOIN silver.variants v ON g.gene_id = v.gene_id
GROUP BY g.gene_id,
    g.gene_name,
    g.official_symbol,
    g.chromosome,
    g.gene_type,
    g.gene_length;
COMMENT ON VIEW silver.v_gene_with_stats IS 'Genes with basic variant statistics';
-- View: v_pathogenic_variants
-- Purpose: Only pathogenic and likely pathogenic variants
CREATE OR REPLACE VIEW silver.v_pathogenic_variants AS
SELECT variant_id,
    accession,
    gene_name,
    gene_id,
    clinical_significance,
    disease,
    chromosome,
    position,
    variant_type,
    molecular_consequence,
    protein_change
FROM silver.variants
WHERE clinical_significance ILIKE '%pathogenic%'
ORDER BY gene_name,
    position;
COMMENT ON VIEW silver.v_pathogenic_variants IS 'Only pathogenic and likely pathogenic variants';
-- ====================================================================
-- GOLD LAYER VIEWS
-- ====================================================================
-- View: v_high_risk_genes
-- Purpose: Genes with high risk level
CREATE OR REPLACE VIEW gold.v_high_risk_genes AS
SELECT gs.gene_id,
    gs.gene_name,
    gs.chromosome,
    gs.total_variants,
    gs.pathogenic_variants,
    gs.pathogenic_ratio,
    gs.mutation_density,
    gs.disease_count,
    gs.risk_score,
    g.description,
    g.gene_type
FROM gold.gene_summary gs
    JOIN silver.genes g ON gs.gene_id = g.gene_id
WHERE gs.risk_level = 'High'
ORDER BY gs.risk_score DESC;
COMMENT ON VIEW gold.v_high_risk_genes IS 'High-risk genes sorted by risk score';
-- View: v_gene_disease_matrix
-- Purpose: Gene-disease association matrix for analysis
CREATE OR REPLACE VIEW gold.v_gene_disease_matrix AS
SELECT gene_name,
    disease,
    mutation_count,
    pathogenic_count,
    pathogenic_ratio,
    risk_level
FROM gold.gene_disease_association
WHERE mutation_count >= 3 -- Filter out sparse data
ORDER BY gene_name,
    pathogenic_ratio DESC;
COMMENT ON VIEW gold.v_gene_disease_matrix IS 'Gene-disease association matrix';
-- View: v_chromosome_risk_profile
-- Purpose: Chromosome risk profile with detailed stats
CREATE OR REPLACE VIEW gold.v_chromosome_risk_profile AS
SELECT cs.chromosome,
    cs.gene_count,
    cs.variant_count,
    cs.pathogenic_count,
    cs.pathogenic_percentage,
    cs.avg_mutations_per_gene,
    CASE
        WHEN cs.pathogenic_percentage > 50 THEN 'High Risk'
        WHEN cs.pathogenic_percentage > 30 THEN 'Moderate Risk'
        ELSE 'Low Risk'
    END as chromosome_risk_level
FROM gold.chromosome_summary cs
ORDER BY CASE
        chromosome
        WHEN 'X' THEN 23
        WHEN 'Y' THEN 24
        WHEN 'MT' THEN 25
        ELSE chromosome::INTEGER
    END;
COMMENT ON VIEW gold.v_chromosome_risk_profile IS 'Chromosome risk profile with classification';
-- View: v_ml_predictions_summary
-- Purpose: ML predictions with gene context
CREATE OR REPLACE VIEW gold.v_ml_predictions_summary AS
SELECT p.prediction_id,
    p.gene_name,
    p.predicted_risk,
    p.confidence,
    g.chromosome,
    g.gene_type,
    gs.total_variants,
    gs.pathogenic_ratio as actual_pathogenic_ratio,
    p.prediction_date
FROM gold.ml_disease_predictions p
    JOIN silver.genes g ON p.gene_id = g.gene_id
    LEFT JOIN gold.gene_summary gs ON p.gene_id = gs.gene_id
ORDER BY p.confidence DESC;
COMMENT ON VIEW gold.v_ml_predictions_summary IS 'ML predictions with gene context';
-- ====================================================================
-- ANALYSIS VIEWS
-- ====================================================================
-- View: v_top_genes_by_mutations
-- Purpose: Top genes by mutation count
CREATE OR REPLACE VIEW gold.v_top_genes_by_mutations AS
SELECT gs.gene_name,
    gs.chromosome,
    gs.total_variants,
    gs.pathogenic_variants,
    gs.pathogenic_ratio,
    gs.disease_count,
    g.description
FROM gold.gene_summary gs
    JOIN silver.genes g ON gs.gene_id = g.gene_id
ORDER BY gs.total_variants DESC
LIMIT 20;
COMMENT ON VIEW gold.v_top_genes_by_mutations IS 'Top 20 genes by total mutation count';
-- View: v_disease_complexity
-- Purpose: Diseases ranked by complexity (number of genes involved)
CREATE OR REPLACE VIEW gold.v_disease_complexity AS
SELECT disease,
    COUNT(DISTINCT gene_name) as genes_affected,
    SUM(mutation_count) as total_mutations,
    AVG(pathogenic_ratio) as avg_pathogenic_ratio
FROM gold.gene_disease_association
WHERE disease IS NOT NULL
    AND disease != 'Unknown'
GROUP BY disease
HAVING COUNT(DISTINCT gene_name) >= 2
ORDER BY genes_affected DESC,
    total_mutations DESC;
COMMENT ON VIEW gold.v_disease_complexity IS 'Diseases ranked by complexity (multiple genes)';
-- ====================================================================
-- COMPLETION MESSAGE
-- ====================================================================
DO $$ BEGIN RAISE NOTICE '';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'VIEWS CREATED SUCCESSFULLY';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Created 9 analytical views:';
RAISE NOTICE '  - v_gene_with_stats';
RAISE NOTICE '  - v_pathogenic_variants';
RAISE NOTICE '  - v_high_risk_genes';
RAISE NOTICE '  - v_gene_disease_matrix';
RAISE NOTICE '  - v_chromosome_risk_profile';
RAISE NOTICE '  - v_ml_predictions_summary';
RAISE NOTICE '  - v_top_genes_by_mutations';
RAISE NOTICE '  - v_disease_complexity';
RAISE NOTICE '====================================================================';
END $$;