-- ====================================================================
-- DATABASE INDEX CREATION
-- DNA Gene Mapping Project
-- Author: Sharique Mohammad
-- Date: 30 December 2025
-- ====================================================================
-- FILE 2: sql/schema/02_create_indexes.sql
-- Purpose: Create indexes for query optimization
-- ====================================================================
-- CREATE INDEXES FOR QUERY OPTIMIZATION
-- DNA Gene Mapping Project
-- ====================================================================
-- BRONZE LAYER INDEXES
-- ====================================================================
-- Genes indexes
CREATE INDEX idx_bronze_genes_name ON bronze.genes_raw(gene_name);
CREATE INDEX idx_bronze_genes_chromosome ON bronze.genes_raw(chromosome);
CREATE INDEX idx_bronze_genes_type ON bronze.genes_raw(gene_type);
-- Variants indexes
CREATE INDEX idx_bronze_variants_gene ON bronze.variants_raw(gene_name);
CREATE INDEX idx_bronze_variants_chromosome ON bronze.variants_raw(chromosome);
CREATE INDEX idx_bronze_variants_significance ON bronze.variants_raw(clinical_significance);
-- ====================================================================
-- SILVER LAYER INDEXES
-- ====================================================================
-- Genes indexes
CREATE INDEX idx_silver_genes_name ON silver.genes(gene_name);
CREATE INDEX idx_silver_genes_chromosome ON silver.genes(chromosome);
CREATE INDEX idx_silver_genes_type ON silver.genes(gene_type);
CREATE INDEX idx_silver_genes_position ON silver.genes(start_position, end_position);
-- Variants indexes
CREATE INDEX idx_silver_variants_gene ON silver.variants(gene_name);
CREATE INDEX idx_silver_variants_gene_id ON silver.variants(gene_id);
CREATE INDEX idx_silver_variants_chromosome ON silver.variants(chromosome);
CREATE INDEX idx_silver_variants_significance ON silver.variants(clinical_significance);
CREATE INDEX idx_silver_variants_position ON silver.variants(position);
CREATE INDEX idx_silver_variants_disease ON silver.variants(disease);
CREATE INDEX idx_silver_variants_type ON silver.variants(variant_type);
-- Composite indexes for common queries
CREATE INDEX idx_silver_variants_gene_chrom ON silver.variants(gene_name, chromosome);
CREATE INDEX idx_silver_variants_chrom_sig ON silver.variants(chromosome, clinical_significance);
-- ====================================================================
-- GOLD LAYER INDEXES
-- ====================================================================
-- Gene-disease association indexes
CREATE INDEX idx_gold_assoc_gene ON gold.gene_disease_association(gene_name);
CREATE INDEX idx_gold_assoc_disease ON gold.gene_disease_association(disease);
CREATE INDEX idx_gold_assoc_risk ON gold.gene_disease_association(risk_level);
CREATE INDEX idx_gold_assoc_ratio ON gold.gene_disease_association(pathogenic_ratio DESC);
-- Gene summary indexes
CREATE INDEX idx_gold_summary_gene ON gold.gene_summary(gene_name);
CREATE INDEX idx_gold_summary_chromosome ON gold.gene_summary(chromosome);
CREATE INDEX idx_gold_summary_risk ON gold.gene_summary(risk_level);
CREATE INDEX idx_gold_summary_score ON gold.gene_summary(risk_score DESC);
-- ML predictions indexes
CREATE INDEX idx_gold_pred_gene ON gold.ml_disease_predictions(gene_name);
CREATE INDEX idx_gold_pred_risk ON gold.ml_disease_predictions(predicted_risk);
CREATE INDEX idx_gold_pred_confidence ON gold.ml_disease_predictions(confidence DESC);
CREATE INDEX idx_gold_pred_date ON gold.ml_disease_predictions(prediction_date DESC);
-- ====================================================================
-- COMPLETION MESSAGE
-- ====================================================================
DO $$ BEGIN RAISE NOTICE '';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'INDEXES CREATED SUCCESSFULLY';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Created 25+ indexes for query optimization';
RAISE NOTICE 'Database is now optimized for analytical queries';
RAISE NOTICE '====================================================================';
END $$;