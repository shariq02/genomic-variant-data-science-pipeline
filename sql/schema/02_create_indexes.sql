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
CREATE INDEX idx_silver_genes_name ON silver.genes_clean(gene_name);
CREATE INDEX idx_silver_genes_chromosome ON silver.genes_clean(chromosome);
CREATE INDEX idx_silver_genes_type ON silver.genes_clean(gene_type);
CREATE INDEX idx_silver_genes_position ON silver.genes_clean(start_position, end_position);
-- Variants indexes
CREATE INDEX idx_silver_variants_gene ON silver.variants_clean(gene_name);
CREATE INDEX idx_silver_variants_chromosome ON silver.variants_clean(chromosome);
CREATE INDEX idx_silver_variants_significance ON silver.variants_clean(clinical_significance);
CREATE INDEX idx_silver_variants_position ON silver.variants_clean(position);
CREATE INDEX idx_silver_variants_disease ON silver.variants_clean(disease);
CREATE INDEX idx_silver_variants_type ON silver.variants_clean(variant_type);
-- Composite indexes for common queries
CREATE INDEX idx_silver_variants_gene_chrom ON silver.variants_clean(gene_name, chromosome);
CREATE INDEX idx_silver_variants_chrom_sig ON silver.variants_clean(chromosome, clinical_significance);
-- ====================================================================
-- GOLD LAYER INDEXES - Based on actual gold tables from Databricks
-- ====================================================================
-- Gene Features indexes
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_name ON gold.gene_features(gene_name);
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_chromosome ON gold.gene_features(chromosome);
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_risk ON gold.gene_features(risk_level);
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_score ON gold.gene_features(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_mutations ON gold.gene_features(mutation_count DESC);
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_pathogenic ON gold.gene_features(pathogenic_count DESC);
-- Chromosome Features indexes
CREATE INDEX IF NOT EXISTS idx_gold_chromosome_features_chrom ON gold.chromosome_features(chromosome);
CREATE INDEX IF NOT EXISTS idx_gold_chromosome_features_variants ON gold.chromosome_features(variant_count DESC);
CREATE INDEX IF NOT EXISTS idx_gold_chromosome_features_pathogenic ON gold.chromosome_features(pathogenic_count DESC);
-- Gene-Disease Association indexes
CREATE INDEX IF NOT EXISTS idx_gold_gene_disease_gene ON gold.gene_disease_association(gene_name);
CREATE INDEX IF NOT EXISTS idx_gold_gene_disease_disease ON gold.gene_disease_association(disease);
CREATE INDEX IF NOT EXISTS idx_gold_gene_disease_ratio ON gold.gene_disease_association(pathogenic_ratio DESC);
CREATE INDEX IF NOT EXISTS idx_gold_gene_disease_strength ON gold.gene_disease_association(association_strength);
CREATE INDEX IF NOT EXISTS idx_gold_gene_disease_mutations ON gold.gene_disease_association(mutation_count DESC);
-- ML Features indexes
CREATE INDEX IF NOT EXISTS idx_gold_ml_features_gene ON gold.ml_features(gene_name);
CREATE INDEX IF NOT EXISTS idx_gold_ml_features_chromosome ON gold.ml_features(chromosome);
CREATE INDEX IF NOT EXISTS idx_gold_ml_features_risk ON gold.ml_features(risk_level);
CREATE INDEX IF NOT EXISTS idx_gold_ml_features_mutations ON gold.ml_features(mutation_count DESC);
CREATE INDEX IF NOT EXISTS idx_gold_ml_features_pathogenic ON gold.ml_features(pathogenic_ratio DESC);
-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_gold_gene_features_chrom_risk ON gold.gene_features(chromosome, risk_level);
CREATE INDEX IF NOT EXISTS idx_gold_gene_disease_gene_disease ON gold.gene_disease_association(gene_name, disease);
-- ====================================================================
-- COMPLETION MESSAGE
-- ====================================================================
DO $$ BEGIN RAISE NOTICE '';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'INDEXES CREATED SUCCESSFULLY';
RAISE NOTICE '====================================================================';
RAISE NOTICE 'Created 20+ indexes for query optimization on gold tables';
RAISE NOTICE 'Database is now optimized for analytical queries';
RAISE NOTICE '====================================================================';
END $$;