-- ====================================================================
-- DATABASE SCHEMA CREATION - UPDATED FOR DATABRICKS UNITY CATALOG
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: 11 January 2026
-- ====================================================================
-- CREATE SCHEMAS (Data Lake Layers)
-- ====================================================================
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
COMMENT ON SCHEMA bronze IS 'Raw data layer - unprocessed data from sources';
COMMENT ON SCHEMA silver IS 'Cleaned data layer - validated and standardized';
COMMENT ON SCHEMA gold IS 'Analytics layer - aggregated and business-ready';
-- ====================================================================
-- BRONZE LAYER TABLES (Raw Data)
-- ====================================================================
CREATE TABLE bronze.genes_raw (
    gene_id TEXT,
    gene_name TEXT NOT NULL,
    official_symbol TEXT,
    description TEXT,
    chromosome VARCHAR(10),
    map_location TEXT,
    gene_type TEXT,
    summary TEXT,
    start_position BIGINT,
    end_position BIGINT,
    strand VARCHAR(10),
    gene_length BIGINT,
    other_aliases TEXT,
    other_designations TEXT,
    full_name TEXT,
    nomenclature_status TEXT,
    db_xrefs TEXT,
    modification_date VARCHAR(50),
    feature_type TEXT,
    download_date VARCHAR(50),
    data_source VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE bronze.genes_raw IS 'Raw gene metadata from NCBI FTP';
CREATE TABLE bronze.variants_raw (
    variant_id TEXT,
    accession TEXT,
    allele_id TEXT,
    gene_id BIGINT,
    gene_name TEXT,
    clinical_significance TEXT,
    clinical_significance_simple TEXT,
    disease TEXT,
    phenotype_ids TEXT,
    chromosome VARCHAR(10),
    position BIGINT,
    stop_position BIGINT,
    cytogenetic TEXT,
    variant_type TEXT,
    variant_name TEXT,
    reference_allele TEXT,
    alternate_allele TEXT,
    position_vcf BIGINT,
    reference_allele_vcf TEXT,
    alternate_allele_vcf TEXT,
    review_status TEXT,
    number_submitters INTEGER,
    rs_id BIGINT,
    dbvar_id TEXT,
    origin TEXT,
    origin_simple TEXT,
    last_evaluated VARCHAR(50),
    assembly VARCHAR(50),
    tested_in_gtr VARCHAR(10),
    guidelines TEXT,
    submitter_categories TEXT,
    download_date VARCHAR(50),
    data_source VARCHAR(100),
    molecular_consequence TEXT,
    protein_change TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE bronze.variants_raw IS 'Raw variant data from ClinVar FTP';
-- ====================================================================
-- SILVER LAYER TABLES (Cleaned Data)
-- ====================================================================
CREATE TABLE silver.genes_clean (
    gene_id TEXT PRIMARY KEY,
    gene_name TEXT NOT NULL,
    official_symbol TEXT,
    description TEXT,
    chromosome VARCHAR(10) NOT NULL,
    map_location TEXT,
    gene_type TEXT,
    summary TEXT,
    start_position BIGINT,
    end_position BIGINT,
    strand VARCHAR(10),
    gene_length BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_chromosome CHECK (
        chromosome IN (
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8',
            '9',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            'X',
            'Y',
            'MT'
        )
    )
);
COMMENT ON TABLE silver.genes_clean IS 'Cleaned and validated gene information';
CREATE TABLE silver.variants_clean (
    variant_id TEXT,
    accession TEXT PRIMARY KEY,
    gene_name TEXT NOT NULL,
    clinical_significance TEXT,
    disease TEXT,
    chromosome VARCHAR(10),
    position BIGINT,
    stop_position BIGINT,
    variant_type TEXT,
    molecular_consequence TEXT,
    protein_change TEXT,
    review_status TEXT,
    assembly VARCHAR(50),
    data_quality_score INTEGER,
    quality_tier VARCHAR(50),
    position_was_enriched BOOLEAN,
    variant_type_was_enriched BOOLEAN,
    clinical_significance_was_enriched BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_variant_chromosome CHECK (
        chromosome IS NULL
        OR chromosome IN (
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8',
            '9',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            'X',
            'Y',
            'MT'
        )
    ),
    CONSTRAINT valid_variant_positions CHECK (
        position IS NULL
        OR stop_position IS NULL
        OR position <= stop_position
    )
);
COMMENT ON TABLE silver.variants_clean IS 'Cleaned and enriched variant information';
-- ====================================================================
-- GOLD LAYER TABLES (Analytics-Ready)
-- ====================================================================
CREATE TABLE gold.gene_features (
    gene_name TEXT PRIMARY KEY,
    gene_id TEXT,
    chromosome VARCHAR(10),
    gene_type TEXT,
    gene_length BIGINT,
    mutation_count INTEGER,
    pathogenic_count INTEGER,
    likely_pathogenic_count INTEGER,
    total_pathogenic INTEGER,
    benign_count INTEGER,
    likely_benign_count INTEGER,
    total_benign INTEGER,
    pathogenic_ratio DECIMAL(10, 4),
    benign_ratio DECIMAL(10, 4),
    disease_count INTEGER,
    variant_type_count INTEGER,
    avg_position DECIMAL(20, 6),
    mutation_density DECIMAL(20, 6),
    risk_level VARCHAR(20),
    risk_score DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.gene_features IS 'Per-gene aggregated features';
CREATE TABLE gold.chromosome_features (
    chromosome VARCHAR(10) PRIMARY KEY,
    gene_count INTEGER,
    variant_count INTEGER,
    pathogenic_count INTEGER,
    likely_pathogenic_count INTEGER,
    benign_count INTEGER,
    likely_benign_count INTEGER,
    total_pathogenic INTEGER,
    total_benign INTEGER,
    pathogenic_percentage DECIMAL(10, 2),
    avg_mutations_per_gene DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.chromosome_features IS 'Chromosome-level aggregated statistics';
CREATE TABLE gold.gene_disease_association (
    gene_name TEXT,
    disease TEXT,
    mutation_count INTEGER,
    pathogenic_count INTEGER,
    likely_pathogenic_count INTEGER,
    benign_count INTEGER,
    likely_benign_count INTEGER,
    total_pathogenic INTEGER,
    total_benign INTEGER,
    pathogenic_ratio DECIMAL(10, 4),
    association_strength VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (gene_name, disease)
);
COMMENT ON TABLE gold.gene_disease_association IS 'Gene-disease association with mutation statistics';
CREATE TABLE gold.ml_features (
    gene_name TEXT PRIMARY KEY,
    chromosome VARCHAR(10),
    mutation_count INTEGER,
    pathogenic_count INTEGER,
    likely_pathogenic_count INTEGER,
    total_pathogenic INTEGER,
    benign_count INTEGER,
    likely_benign_count INTEGER,
    total_benign INTEGER,
    pathogenic_ratio DECIMAL(10, 4),
    disease_count INTEGER,
    variant_type_count INTEGER,
    mutation_density DECIMAL(20, 6),
    risk_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.ml_features IS 'ML-ready features for model training';
CREATE TABLE gold.ml_disease_predictions (
    prediction_id SERIAL PRIMARY KEY,
    gene_name TEXT,
    predicted_risk VARCHAR(20),
    confidence DECIMAL(5, 4),
    model_version VARCHAR(50),
    prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.ml_disease_predictions IS 'ML model predictions for disease risk';
CREATE TABLE gold.ml_gene_clusters (
    gene_name TEXT PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    cluster_label VARCHAR(50),
    distance_to_centroid DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.ml_gene_clusters IS 'Gene clustering results';
-- ====================================================================
-- GRANT PERMISSIONS
-- ====================================================================
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO postgres;
-- ====================================================================
-- COMPLETION MESSAGE
-- ====================================================================
DO $$ BEGIN RAISE NOTICE '';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'DATABASE SCHEMA CREATION COMPLETED';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Schemas created: bronze, silver, gold';
RAISE NOTICE 'Using TEXT fields to handle real-world data (no truncation)';
RAISE NOTICE 'Ready for 190K genes and 4M+ variants';
RAISE NOTICE '';
RAISE NOTICE '====================================================================';
END $$;