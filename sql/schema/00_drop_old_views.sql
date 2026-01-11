-- ====================================================================
-- DROP ALL OLD VIEWS - CLEAN SLATE
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: 11 January 2026
-- ====================================================================
-- Drop all old views that might exist from previous schema
DROP VIEW IF EXISTS silver.v_gene_with_stats CASCADE;
DROP VIEW IF EXISTS silver.v_pathogenic_variants CASCADE;
DROP VIEW IF EXISTS gold.v_high_risk_genes CASCADE;
DROP VIEW IF EXISTS gold.v_gene_disease_matrix CASCADE;
DROP VIEW IF EXISTS gold.v_chromosome_risk_profile CASCADE;
DROP VIEW IF EXISTS gold.v_ml_predictions_summary CASCADE;
DROP VIEW IF EXISTS gold.v_top_genes_by_mutations CASCADE;
DROP VIEW IF EXISTS gold.v_disease_complexity CASCADE;
DROP VIEW IF EXISTS gold.v_ml_features_summary CASCADE;
-- ====================================================================
-- COMPLETION MESSAGE
-- ====================================================================
DO $$ BEGIN RAISE NOTICE '';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'ALL OLD VIEWS DROPPED';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Database is now ready for fresh schema creation';
RAISE NOTICE '====================================================================';
END $$;