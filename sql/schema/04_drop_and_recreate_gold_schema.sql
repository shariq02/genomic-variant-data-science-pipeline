-- ====================================================================
-- DROP AND RECREATE GOLD SCHEMA
-- DNA Gene Mapping Project
-- Date: 17 January 2026
-- ====================================================================

-- This script completely removes the old gold schema and creates a fresh one
-- Use this BEFORE loading new data from Databricks

-- Step 1: Drop all views that depend on gold tables
-- (This prevents CASCADE errors)

DROP VIEW IF EXISTS gold.high_risk_genes CASCADE;
DROP VIEW IF EXISTS gold.cancer_genes CASCADE;
DROP VIEW IF EXISTS gold.kinase_summary CASCADE;
DROP VIEW IF EXISTS gold.clinical_summary CASCADE;

-- Step 2: Drop all tables in gold schema with CASCADE
-- (This removes any remaining dependencies)

DROP TABLE IF EXISTS gold.gene_features CASCADE;
DROP TABLE IF EXISTS gold.chromosome_features CASCADE;
DROP TABLE IF EXISTS gold.gene_disease_association CASCADE;
DROP TABLE IF EXISTS gold.ml_features CASCADE;

-- Step 3: Drop and recreate the entire gold schema
-- (This ensures a completely clean slate)

DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;

-- Step 4: Grant permissions (adjust as needed)

GRANT USAGE ON SCHEMA gold TO postgres;
GRANT CREATE ON SCHEMA gold TO postgres;

-- Step 5: Verify schema is empty

SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'gold';

-- Should return 0 rows

-- ====================================================================
-- VERIFICATION QUERIES
-- ====================================================================

-- Check all schemas
SELECT schema_name 
FROM information_schema.schemata 
ORDER BY schema_name;

-- Should see: gold (but empty)

-- Check gold schema is empty
SELECT 
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'gold';

-- Should return 0 rows

-- ====================================================================
-- READY FOR FRESH LOAD
-- ====================================================================

-- Now run: python load_gold_to_postgres_COMPLETE.py
-- This will create fresh tables with the new 65-column schema

-- Expected tables after load:
--   gold.gene_features (65 columns)
--   gold.chromosome_features
--   gold.gene_disease_association
--   gold.ml_features
