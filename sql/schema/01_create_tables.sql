-- ====================================================================
-- DATABASE SCHEMA CREATION
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: 30 December 2025
-- ====================================================================
-- FILE 1: sql/schema/01_create_tables.sql
-- Purpose: Create all database tables (Bronze/Silver/Gold layers)
-- ====================================================================
-- Connect to database first
-- psql -U postgres -d genome_db
-- ====================================================================
-- CREATE SCHEMAS (Data Lake Layers)
-- ====================================================================
-- Drop schemas if they exist (for clean start)
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
-- Create schemas
CREATE SCHEMA bronze;
-- Raw data (as-is from source)
CREATE SCHEMA silver;
-- Cleaned and validated data
CREATE SCHEMA gold;
-- Aggregated analytical data
COMMENT ON SCHEMA bronze IS 'Raw data layer - unprocessed data from sources';
COMMENT ON SCHEMA silver IS 'Cleaned data layer - validated and standardized';
COMMENT ON SCHEMA gold IS 'Analytics layer - aggregated and business-ready';
-- ====================================================================
-- BRONZE LAYER TABLES (Raw Data)
-- ====================================================================
-- Table: bronze.genes_raw
-- Purpose: Raw gene data from NCBI
CREATE TABLE bronze.genes_raw (
    gene_id VARCHAR(50) PRIMARY KEY,
    gene_name VARCHAR(100) NOT NULL,
    official_symbol VARCHAR(100),
    description TEXT,
    chromosome VARCHAR(10),
    map_location VARCHAR(100),
    gene_type VARCHAR(50),
    summary TEXT,
    start_position BIGINT,
    end_position BIGINT,
    strand VARCHAR(10),
    gene_length BIGINT,
    other_aliases TEXT,
    other_designations TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'NCBI'
);
COMMENT ON TABLE bronze.genes_raw IS 'Raw gene metadata from NCBI GenBank';
-- Table: bronze.variants_raw
-- Purpose: Raw variant data from ClinVar
CREATE TABLE bronze.variants_raw (
    variant_id VARCHAR(50) PRIMARY KEY,
    accession VARCHAR(50),
    gene_name VARCHAR(100),
    clinical_significance VARCHAR(100),
    disease TEXT,
    chromosome VARCHAR(10),
    position BIGINT,
    stop_position BIGINT,
    variant_type VARCHAR(100),
    molecular_consequence VARCHAR(200),
    protein_change VARCHAR(200),
    allele_id VARCHAR(50),
    review_status VARCHAR(100),
    assembly VARCHAR(50),
    cytogenetic VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'ClinVar'
);
COMMENT ON TABLE bronze.variants_raw IS 'Raw variant data from ClinVar';
-- ====================================================================
-- SILVER LAYER TABLES (Cleaned Data)
-- ====================================================================
-- Table: silver.genes
-- Purpose: Cleaned and validated gene data
CREATE TABLE silver.genes (
    gene_id VARCHAR(50) PRIMARY KEY,
    gene_name VARCHAR(100) NOT NULL,
    official_symbol VARCHAR(100),
    description TEXT,
    chromosome VARCHAR(10) NOT NULL,
    map_location VARCHAR(100),
    gene_type VARCHAR(50),
    summary TEXT,
    start_position BIGINT,
    end_position BIGINT,
    strand VARCHAR(10),
    gene_length BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    ),
    CONSTRAINT valid_positions CHECK (
        start_position IS NULL
        OR end_position IS NULL
        OR start_position < end_position
    )
);
COMMENT ON TABLE silver.genes IS 'Cleaned and validated gene information';
-- Table: silver.variants
-- Purpose: Cleaned and validated variant data
CREATE TABLE silver.variants (
    variant_id VARCHAR(50) PRIMARY KEY,
    accession VARCHAR(50),
    gene_name VARCHAR(100) NOT NULL,
    gene_id VARCHAR(50),
    clinical_significance VARCHAR(100),
    disease TEXT,
    chromosome VARCHAR(10),
    position BIGINT,
    stop_position BIGINT,
    variant_type VARCHAR(100),
    molecular_consequence VARCHAR(200),
    protein_change VARCHAR(200),
    review_status VARCHAR(100),
    assembly VARCHAR(50) DEFAULT 'GRCh38',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gene FOREIGN KEY (gene_id) REFERENCES silver.genes(gene_id),
    CONSTRAINT valid_variant_chromosome CHECK (
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
    ),
    CONSTRAINT valid_variant_positions CHECK (
        position IS NULL
        OR stop_position IS NULL
        OR position <= stop_position
    )
);
COMMENT ON TABLE silver.variants IS 'Cleaned and validated variant information';
-- ====================================================================
-- GOLD LAYER TABLES (Analytics-Ready)
-- ====================================================================
-- Table: gold.gene_disease_association
-- Purpose: Gene-disease mutation statistics
CREATE TABLE gold.gene_disease_association (
    association_id SERIAL PRIMARY KEY,
    gene_name VARCHAR(100) NOT NULL,
    gene_id VARCHAR(50),
    disease TEXT NOT NULL,
    mutation_count INTEGER NOT NULL,
    pathogenic_count INTEGER DEFAULT 0,
    benign_count INTEGER DEFAULT 0,
    uncertain_count INTEGER DEFAULT 0,
    pathogenic_ratio DECIMAL(5, 4),
    risk_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gene_assoc FOREIGN KEY (gene_id) REFERENCES silver.genes(gene_id),
    CONSTRAINT valid_counts CHECK (mutation_count > 0),
    CONSTRAINT valid_ratio CHECK (
        pathogenic_ratio BETWEEN 0 AND 1
    ),
    CONSTRAINT valid_risk CHECK (
        risk_level IN ('Low', 'Medium', 'High', 'Unknown')
    )
);
COMMENT ON TABLE gold.gene_disease_association IS 'Gene-disease association with mutation statistics';
-- Table: gold.chromosome_summary
-- Purpose: Chromosome-level statistics
CREATE TABLE gold.chromosome_summary (
    chromosome VARCHAR(10) PRIMARY KEY,
    gene_count INTEGER NOT NULL,
    variant_count INTEGER NOT NULL,
    pathogenic_count INTEGER DEFAULT 0,
    pathogenic_percentage DECIMAL(5, 2),
    avg_mutations_per_gene DECIMAL(10, 2),
    total_gene_length BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_chrom_chromosome CHECK (
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
COMMENT ON TABLE gold.chromosome_summary IS 'Chromosome-level aggregated statistics';
-- Table: gold.gene_summary
-- Purpose: Per-gene aggregated statistics
CREATE TABLE gold.gene_summary (
    gene_id VARCHAR(50) PRIMARY KEY,
    gene_name VARCHAR(100) NOT NULL,
    chromosome VARCHAR(10),
    total_variants INTEGER DEFAULT 0,
    pathogenic_variants INTEGER DEFAULT 0,
    benign_variants INTEGER DEFAULT 0,
    uncertain_variants INTEGER DEFAULT 0,
    pathogenic_ratio DECIMAL(5, 4),
    mutation_density DECIMAL(10, 6),
    disease_count INTEGER DEFAULT 0,
    risk_score DECIMAL(5, 2),
    risk_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gene_summary FOREIGN KEY (gene_id) REFERENCES silver.genes(gene_id)
);
COMMENT ON TABLE gold.gene_summary IS 'Per-gene aggregated statistics and risk scores';
-- ====================================================================
-- ML TABLES (For Data Science Phase)
-- ====================================================================
-- Table: gold.ml_disease_predictions
-- Purpose: Store ML model predictions
CREATE TABLE gold.ml_disease_predictions (
    prediction_id SERIAL PRIMARY KEY,
    gene_id VARCHAR(50),
    gene_name VARCHAR(100),
    predicted_risk VARCHAR(20),
    confidence DECIMAL(5, 4),
    model_version VARCHAR(50),
    top_feature_1 VARCHAR(100),
    top_feature_2 VARCHAR(100),
    top_feature_3 VARCHAR(100),
    prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gene_pred FOREIGN KEY (gene_id) REFERENCES silver.genes(gene_id)
);
COMMENT ON TABLE gold.ml_disease_predictions IS 'ML model predictions for disease risk';
-- Table: gold.ml_feature_importance
-- Purpose: Store feature importance from ML models
CREATE TABLE gold.ml_feature_importance (
    feature_name VARCHAR(100) PRIMARY KEY,
    importance_score DECIMAL(10, 6) NOT NULL,
    rank INTEGER,
    model_version VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.ml_feature_importance IS 'Feature importance scores from ML models';
-- Table: gold.ml_gene_clusters
-- Purpose: Store clustering results
CREATE TABLE gold.ml_gene_clusters (
    gene_id VARCHAR(50) PRIMARY KEY,
    gene_name VARCHAR(100),
    cluster_id INTEGER NOT NULL,
    cluster_label VARCHAR(50),
    distance_to_centroid DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gene_cluster FOREIGN KEY (gene_id) REFERENCES silver.genes(gene_id)
);
COMMENT ON TABLE gold.ml_gene_clusters IS 'Gene clustering results from unsupervised learning';
-- ====================================================================
-- GRANT PERMISSIONS
-- ====================================================================
-- Grant permissions to postgres user (adjust as needed)
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
RAISE NOTICE 'DATABASE SCHEMA CREATION COMPLETED SUCCESSFULLY';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Schemas created: bronze, silver, gold';
RAISE NOTICE 'Bronze tables: genes_raw, variants_raw';
RAISE NOTICE 'Silver tables: genes, variants';
RAISE NOTICE 'Gold tables: gene_disease_association, chromosome_summary, gene_summary';
RAISE NOTICE 'ML tables: ml_disease_predictions, ml_feature_importance, ml_gene_clusters';
RAISE NOTICE '====================================================================';
END $$;