-- ====================================================================
-- DROP OLD VIEWS AND TABLES - CLEANUP SCRIPT FOR POSTGRESQL
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: January 14, 2026
-- ====================================================================
-- Purpose: Clean up all old views and tables before running enhanced pipeline
-- Run this FIRST before any data processing!
-- ====================================================================
-- IMPORTANT: This script is for PostgreSQL database cleanup
--            For Databricks cleanup, use the companion notebook
-- ====================================================================

-- Set client encoding
SET client_encoding = 'UTF8';

-- Display start message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'DROP OLD VIEWS AND TABLES - CLEANUP';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting cleanup process...';
    RAISE NOTICE '========================================';
END $$;

-- ====================================================================
-- STEP 1: DROP ALL GOLD LAYER VIEWS
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 1: DROPPING GOLD LAYER VIEWS';
    RAISE NOTICE '========================================';
END $$;

-- Drop all views in gold schema (CASCADE will drop dependencies)
DROP VIEW IF EXISTS gold.v_high_risk_genes CASCADE;
DROP VIEW IF EXISTS gold.v_top_genes_by_mutations CASCADE;
DROP VIEW IF EXISTS gold.v_druggable_targets CASCADE;
DROP VIEW IF EXISTS gold.v_cancer_kinases CASCADE;
DROP VIEW IF EXISTS gold.v_functional_gene_summary CASCADE;
DROP VIEW IF EXISTS gold.v_chromosome_risk_profile CASCADE;
DROP VIEW IF EXISTS gold.v_gene_disease_matrix CASCADE;
DROP VIEW IF EXISTS gold.v_disease_complexity CASCADE;
DROP VIEW IF EXISTS gold.v_ml_features_summary CASCADE;
DROP VIEW IF EXISTS gold.v_therapeutic_targets CASCADE;

-- Drop any additional views that might exist
DROP VIEW IF EXISTS gold.v_pathogenic_variants CASCADE;
DROP VIEW IF EXISTS gold.v_gene_summary CASCADE;
DROP VIEW IF EXISTS gold.v_disease_genes CASCADE;
DROP VIEW IF EXISTS gold.v_clinical_variants CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Gold layer views dropped';
END $$;

-- ====================================================================
-- STEP 2: DROP ALL GOLD LAYER TABLES
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 2: DROPPING GOLD LAYER TABLES';
    RAISE NOTICE '========================================';
END $$;

-- Drop gold tables
DROP TABLE IF EXISTS gold.gene_features CASCADE;
DROP TABLE IF EXISTS gold.chromosome_features CASCADE;
DROP TABLE IF EXISTS gold.gene_disease_association CASCADE;
DROP TABLE IF EXISTS gold.ml_features CASCADE;

-- Drop any additional gold tables
DROP TABLE IF EXISTS gold.variant_features CASCADE;
DROP TABLE IF EXISTS gold.clinical_features CASCADE;
DROP TABLE IF EXISTS gold.gene_annotations CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Gold layer tables dropped';
END $$;

-- ====================================================================
-- STEP 3: DROP ALL SILVER LAYER VIEWS
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 3: DROPPING SILVER LAYER VIEWS';
    RAISE NOTICE '========================================';
END $$;

-- Drop silver views if any exist
DROP VIEW IF EXISTS silver.v_genes CASCADE;
DROP VIEW IF EXISTS silver.v_variants CASCADE;
DROP VIEW IF EXISTS silver.v_gene_variants CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Silver layer views dropped';
END $$;

-- ====================================================================
-- STEP 4: DROP ALL SILVER LAYER TABLES
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 4: DROPPING SILVER LAYER TABLES';
    RAISE NOTICE '========================================';
END $$;

-- Drop old silver tables (will be replaced by enhanced versions)
DROP TABLE IF EXISTS silver.genes_ultra_enriched CASCADE;
DROP TABLE IF EXISTS silver.variants_ultra_enriched CASCADE;
DROP TABLE IF EXISTS silver.genes_enriched CASCADE;
DROP TABLE IF EXISTS silver.variants_enriched CASCADE;
DROP TABLE IF EXISTS silver.genes_processed CASCADE;
DROP TABLE IF EXISTS silver.variants_processed CASCADE;
DROP TABLE IF EXISTS silver.genes CASCADE;
DROP TABLE IF EXISTS silver.variants CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Silver layer tables dropped';
END $$;

-- ====================================================================
-- STEP 5: DROP ALL BRONZE LAYER VIEWS
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 5: DROPPING BRONZE LAYER VIEWS';
    RAISE NOTICE '========================================';
END $$;

-- Drop bronze views if any exist
DROP VIEW IF EXISTS bronze.v_raw_genes CASCADE;
DROP VIEW IF EXISTS bronze.v_raw_variants CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Bronze layer views dropped';
END $$;

-- ====================================================================
-- STEP 6: DROP ALL BRONZE LAYER TABLES
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 6: DROPPING BRONZE LAYER TABLES';
    RAISE NOTICE '========================================';
END $$;

-- Drop bronze tables
DROP TABLE IF EXISTS bronze.variants_raw CASCADE;
DROP TABLE IF EXISTS bronze.genes_raw CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Bronze layer tables dropped';
END $$;

-- ====================================================================
-- STEP 7: DROP ALL REFERENCE/LOOKUP TABLES
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 7: DROPPING REFERENCE/LOOKUP TABLES';
    RAISE NOTICE '========================================';
END $$;

-- Drop reference schema views
DROP VIEW IF EXISTS reference.gene_search_view CASCADE;

-- Drop reference tables (disease lookups)
DROP TABLE IF EXISTS reference.omim_disease_lookup CASCADE;
DROP TABLE IF EXISTS reference.orphanet_disease_lookup CASCADE;
DROP TABLE IF EXISTS reference.mondo_disease_lookup CASCADE;

-- Drop reference tables (gene lookups) - THESE ARE FROM STEP 3!
DROP TABLE IF EXISTS reference.gene_alias_lookup CASCADE;
DROP TABLE IF EXISTS reference.gene_designation_lookup CASCADE;
DROP TABLE IF EXISTS reference.gene_universal_search CASCADE;

-- Drop any other reference tables
DROP TABLE IF EXISTS reference.gene_xrefs CASCADE;
DROP TABLE IF EXISTS reference.disease_ontology CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Reference/lookup tables dropped';
END $$;

-- ====================================================================
-- STEP 8: VERIFY CLEANUP
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 8: VERIFYING CLEANUP';
    RAISE NOTICE '========================================';
END $$;

-- Count remaining tables in each schema
DO $$
DECLARE
    bronze_count INTEGER;
    silver_count INTEGER;
    gold_count INTEGER;
    ref_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO bronze_count FROM information_schema.tables WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE';
    SELECT COUNT(*) INTO silver_count FROM information_schema.tables WHERE table_schema = 'silver' AND table_type = 'BASE TABLE';
    SELECT COUNT(*) INTO gold_count FROM information_schema.tables WHERE table_schema = 'gold' AND table_type = 'BASE TABLE';
    SELECT COUNT(*) INTO ref_count FROM information_schema.tables WHERE table_schema = 'reference' AND table_type = 'BASE TABLE';
    
    RAISE NOTICE 'Remaining tables:';
    RAISE NOTICE '  Bronze schema:    % tables', bronze_count;
    RAISE NOTICE '  Silver schema:    % tables', silver_count;
    RAISE NOTICE '  Gold schema:      % tables', gold_count;
    RAISE NOTICE '  Reference schema: % tables', ref_count;
END $$;

-- Count remaining views in each schema
DO $$
DECLARE
    bronze_views INTEGER;
    silver_views INTEGER;
    gold_views INTEGER;
    ref_views INTEGER;
BEGIN
    SELECT COUNT(*) INTO bronze_views FROM information_schema.views WHERE table_schema = 'bronze';
    SELECT COUNT(*) INTO silver_views FROM information_schema.views WHERE table_schema = 'silver';
    SELECT COUNT(*) INTO gold_views FROM information_schema.views WHERE table_schema = 'gold';
    SELECT COUNT(*) INTO ref_views FROM information_schema.views WHERE table_schema = 'reference';
    
    RAISE NOTICE '';
    RAISE NOTICE 'Remaining views:';
    RAISE NOTICE '  Bronze schema:    % views', bronze_views;
    RAISE NOTICE '  Silver schema:    % views', silver_views;
    RAISE NOTICE '  Gold schema:      % views', gold_views;
    RAISE NOTICE '  Reference schema: % views', ref_views;
END $$;

-- List any remaining objects (for manual review)
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Remaining database objects:';
    RAISE NOTICE '========================================';
END $$;

-- Show remaining tables
SELECT 
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold', 'reference')
    AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- Show remaining views
SELECT 
    table_schema,
    table_name,
    'VIEW' as object_type
FROM information_schema.views
WHERE table_schema IN ('bronze', 'silver', 'gold', 'reference')
ORDER BY table_schema, table_name;

-- ====================================================================
-- FINAL SUMMARY
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ CLEANUP COMPLETE!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'All old views and tables have been dropped.';
    RAISE NOTICE 'Database is ready for enhanced pipeline.';
    RAISE NOTICE '';
    RAISE NOTICE 'NEXT STEPS:';
    RAISE NOTICE '1. Run enhanced processing scripts in Databricks:';
    RAISE NOTICE '   - 02_gene_data_processing_ENHANCED.py';
    RAISE NOTICE '   - 03_variant_data_processing_ENHANCED.py';
    RAISE NOTICE '   - 06_create_gene_alias_mapper_COMPLETE.py';
    RAISE NOTICE '   - 04_feature_engineering.py';
    RAISE NOTICE '';
    RAISE NOTICE '2. Load new data to PostgreSQL:';
    RAISE NOTICE '   - Use load_gold_to_postgres.py';
    RAISE NOTICE '';
    RAISE NOTICE '3. Create new views:';
    RAISE NOTICE '   - Run 03_create_views.sql (updated version)';
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
END $$;

-- ====================================================================
-- OPTIONAL: RECREATE SCHEMAS IF COMPLETELY EMPTY
-- ====================================================================
-- Uncomment these if you want to recreate the schemas from scratch

-- DROP SCHEMA IF EXISTS bronze CASCADE;
-- DROP SCHEMA IF EXISTS silver CASCADE;
-- DROP SCHEMA IF EXISTS gold CASCADE;
-- DROP SCHEMA IF EXISTS reference CASCADE;

-- CREATE SCHEMA IF NOT EXISTS bronze;
-- CREATE SCHEMA IF NOT EXISTS silver;
-- CREATE SCHEMA IF NOT EXISTS gold;
-- CREATE SCHEMA IF NOT EXISTS reference;

-- COMMENT ON SCHEMA bronze IS 'Raw data layer';
-- COMMENT ON SCHEMA silver IS 'Cleaned and enriched data layer';
-- COMMENT ON SCHEMA gold IS 'Analytical and aggregated data layer';
-- COMMENT ON SCHEMA reference IS 'Reference and lookup tables';

-- ====================================================================
-- END OF CLEANUP SCRIPT
-- ====================================================================
