# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - TRANSCRIPT EXPRESSION ANALYSIS (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Gene-Level Expression Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive expression profiling
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 6: Transcript Isoform Impact
# MAGIC - Use Case 16: Gene Expression Analysis
# MAGIC
# MAGIC **Creates:** gold.gene_expression_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD TRANSCRIPT EXPRESSION FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core tables
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # ENHANCED: enriched genes

# Additional enrichment tables
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")

print(f"GTEx expression: {df_gtex.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,Calculate Gene Expression Statistics
print("\nCALCULATING GENE EXPRESSION STATISTICS")
print("="*80)

df_gene_expression = (
    df_gtex
    .groupBy("gene_name")
    .agg(
        spark_max("max_tpm").alias("max_expression_tpm"),
        avg("expression_tpm").alias("avg_expression_tpm"),
        spark_max("tissues_expressed").alias("total_tissues_expressed"),
        countDistinct("tissue_type").alias("tissue_type_count"),
        spark_sum(when(col("is_primary_tissue"), 1).otherwise(0)).alias("primary_tissue_count"),
        spark_max("expression_tpm").alias("peak_expression_tpm")
    )
)

print(f"Genes with expression data: {df_gene_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Tissue Specificity Classification
print("\nADDING TISSUE SPECIFICITY CLASSIFICATION")
print("="*80)

df_expression_classified = (
    df_gene_expression
    .withColumn("is_ubiquitously_expressed",
                when(col("total_tissues_expressed") >= 40, True).otherwise(False))
    
    .withColumn("is_tissue_specific",
                when(col("total_tissues_expressed") <= 5, True).otherwise(False))
    
    .withColumn("is_highly_expressed",
                when(col("max_expression_tpm") >= 100, True).otherwise(False))
    
    .withColumn("is_lowly_expressed",
                when(col("max_expression_tpm") < 1, True).otherwise(False))
    
    .withColumn("expression_breadth_category",
                when(col("total_tissues_expressed") <= 5, lit("tissue_specific"))
                .when(col("total_tissues_expressed") <= 20, lit("moderately_specific"))
                .when(col("total_tissues_expressed") <= 40, lit("broadly_expressed"))
                .otherwise(lit("ubiquitous")))
    
    .withColumn("expression_level_category",
                when(col("max_expression_tpm") >= 100, lit("high"))
                .when(col("max_expression_tpm") >= 10, lit("medium"))
                .when(col("max_expression_tpm") >= 1, lit("low"))
                .otherwise(lit("very_low")))
)

print("Classification added")

# COMMAND ----------

# DBTITLE 1,Calculate Expression Scores
print("\nCALCULATING EXPRESSION SCORES")
print("="*80)

df_scored = (
    df_expression_classified
    .withColumn("tissue_specificity_score",
                100.0 / (col("total_tissues_expressed") + 1))
    
    .withColumn("expression_significance_score",
                when(col("max_expression_tpm") >= 100, 10)
                .when(col("max_expression_tpm") >= 50, 8)
                .when(col("max_expression_tpm") >= 10, 6)
                .when(col("max_expression_tpm") >= 1, 4)
                .otherwise(2))
    
    .withColumn("clinical_relevance_score",
                when(col("is_tissue_specific"), 8).otherwise(0) +
                when(col("is_highly_expressed"), 5).otherwise(0) +
                (col("primary_tissue_count") * 2))
)

print("Scores calculated")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Context
print("\nENRICHING WITH DISEASE CONTEXT")
print("="*80)

disease_expression = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease")
    )
    .withColumn("is_disease_gene",
                col("total_disease_count") >= 1)
    .withColumn("disease_category_count",
                when(col("has_cancer_disease"), 1).otherwise(0) +
                when(col("has_neurological_disease"), 1).otherwise(0) +
                when(col("has_metabolic_disease"), 1).otherwise(0))
)

print(f"Disease genes: {disease_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

cancer_expression = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("unique_tumor_samples")
    )
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
    .withColumn("cancer_expression_relevance",
                when(col("unique_tumor_samples") >= 100, lit("High"))
                .when(col("unique_tumor_samples") >= 10, lit("Medium"))
                .otherwise(lit("Low")))
)

print(f"Cancer genes: {cancer_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Protein Domain Context
print("\nENRICHING WITH PROTEIN DOMAIN CONTEXT")
print("="*80)

protein_expression = (
    df_protein_domains
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("has_kinase_domain_count"),
        spark_sum(when(col("has_functional_domain"), 1).otherwise(0)).alias("has_functional_domain_count")
    )
    .withColumn("has_functional_domain",
                col("has_functional_domain_count") > 0)
    .withColumn("domain_expression_correlation",
                when(col("max_domain_count") >= 5, lit("Complex"))
                .otherwise(lit("Simple")))
)

print(f"Genes with protein domains: {protein_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Variant Context
print("\nENRICHING WITH VARIANT CONTEXT")
print("="*80)

variant_expression = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("total_gene_variants"),
        spark_sum(when(col("is_splice_variant"), 1).otherwise(0)).alias("splice_variants"),
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("expression_affecting_variants")
    )
    .withColumn("has_expression_variants",
                (col("splice_variants") > 0) | (col("expression_affecting_variants") > 0))
)

print(f"Genes with variants: {variant_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data and All Enrichments
print("\nJOINING WITH GENE MASTER DATA AND ENRICHMENTS")
print("="*80)

df_with_genes = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name").alias("gene_full_name"),
        col("description"),
        col("chromosome"),
        col("gene_length"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transcription_factor"),
        col("is_pharmacogene"),
        col("druggability_score")
    )
    .join(
        df_scored.withColumn("gene_symbol", upper(trim(col("gene_name")))),
        on="gene_symbol",
        how="left"
    )
    
    # Join all enrichment tables
    .join(disease_expression, on="gene_symbol", how="left")
    .join(cancer_expression, on="gene_symbol", how="left")
    .join(protein_expression, on="gene_symbol", how="left")
    .join(variant_expression, on="gene_symbol", how="left")
    
    # Fill nulls
    .fillna({
        "max_expression_tpm": 0.0,
        "avg_expression_tpm": 0.0,
        "peak_expression_tpm": 0.0,
        "total_tissues_expressed": 0,
        "tissue_type_count": 0,
        "primary_tissue_count": 0,
        "is_ubiquitously_expressed": False,
        "is_tissue_specific": False,
        "is_highly_expressed": False,
        "is_lowly_expressed": True,
        "expression_breadth_category": "unknown",
        "expression_level_category": "very_low",
        "tissue_specificity_score": 0.0,
        "expression_significance_score": 0,
        "clinical_relevance_score": 0,
        "total_disease_count": 0,
        "has_cancer_disease": False,
        "has_neurological_disease": False,
        "has_metabolic_disease": False,
        "is_disease_gene": False,
        "disease_category_count": 0,
        "cancer_mutation_count": 0,
        "unique_tumor_samples": 0,
        "is_cancer_gene": False,
        "cancer_expression_relevance": "None",
        "max_domain_count": 0,
        "has_kinase_domain_count": 0,
        "has_functional_domain": False,
        "domain_expression_correlation": "Unknown",
        "total_gene_variants": 0,
        "splice_variants": 0,
        "expression_affecting_variants": 0,
        "has_expression_variants": False
    })
)

print(f"Genes with expression features: {df_with_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Scores
print("\nADDING ENHANCED SCORES")
print("="*80)

df_enhanced_scores = (
    df_with_genes
    # Disease-expression score
    .withColumn("disease_expression_score",
                when(col("is_disease_gene") & col("is_highly_expressed"), 10).otherwise(0) +
                when(col("is_disease_gene") & col("is_tissue_specific"), 8).otherwise(0) +
                (col("disease_category_count") * 3))
    
    # Cancer-expression score
    .withColumn("cancer_expression_score",
                when(col("is_cancer_gene") & col("is_highly_expressed"), 10).otherwise(0) +
                when(col("cancer_expression_relevance") == "High", 5).otherwise(0))
    
    # Functional expression score
    .withColumn("functional_expression_score",
                when(col("has_functional_domain") & col("is_highly_expressed"), 8).otherwise(0) +
                when(col("has_expression_variants"), 5).otherwise(0) +
                when(col("is_pharmacogene") & col("is_highly_expressed"), 7).otherwise(0))
)

print("Enhanced scores calculated")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Priority Classification
print("\nADDING ENHANCED PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_enhanced_scores
    .withColumn("expression_priority",
                when(col("clinical_relevance_score") + col("disease_expression_score") >= 20, lit("critical"))
                .when(col("clinical_relevance_score") >= 15, lit("high"))
                .when(col("clinical_relevance_score") >= 8, lit("medium"))
                .otherwise(lit("low")))
    
    .withColumn("is_clinically_relevant_expression",
                when((col("is_tissue_specific")) & (col("is_highly_expressed")), True).otherwise(False))
    
    # Enhanced: Disease-specific expression pattern
    .withColumn("disease_specific_expression_pattern",
                when(col("has_cancer_disease") & col("is_highly_expressed"), lit("Cancer_Overexpressed"))
                .when(col("has_neurological_disease") & col("is_tissue_specific"), lit("Neuro_Specific"))
                .when(col("has_metabolic_disease") & col("is_ubiquitously_expressed"), lit("Metabolic_Ubiquitous"))
                .when(col("is_disease_gene"), lit("Disease_Associated"))
                .otherwise(lit("Standard")))
    
    # Enhanced: Expression-function correlation
    .withColumn("expression_function_correlation",
                when((col("is_pharmacogene")) & (col("is_highly_expressed")), lit("High_Drug_Target"))
                .when((col("is_kinase") | col("is_receptor")) & (col("is_highly_expressed")), lit("High_Signaling"))
                .when((col("is_transcription_factor")) & (col("is_tissue_specific")), lit("TF_Specific"))
                .otherwise(lit("Standard")))
)

print("Enhanced priority classification added")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        # Gene identifiers
        col("gene_symbol"),
        col("gene_full_name"),
        col("description"),
        col("chromosome"),
        col("gene_length"),
        
        # Gene type flags
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transcription_factor"),
        col("is_pharmacogene"),
        col("druggability_score"),
        
        # Expression statistics
        col("max_expression_tpm"),
        col("avg_expression_tpm"),
        col("peak_expression_tpm"),
        col("total_tissues_expressed"),
        col("tissue_type_count"),
        col("primary_tissue_count"),
        
        # Expression classifications
        col("is_ubiquitously_expressed"),
        col("is_tissue_specific"),
        col("is_highly_expressed"),
        col("is_lowly_expressed"),
        col("expression_breadth_category"),
        col("expression_level_category"),
        
        # Base scores
        col("tissue_specificity_score"),
        col("expression_significance_score"),
        col("clinical_relevance_score"),
        
        # Disease context
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease"),
        col("is_disease_gene"),
        col("disease_category_count"),
        
        # Cancer context
        col("cancer_mutation_count"),
        col("unique_tumor_samples"),
        col("is_cancer_gene"),
        col("cancer_expression_relevance"),
        
        # Protein domain context
        col("max_domain_count"),
        col("has_kinase_domain_count"),
        col("has_functional_domain"),
        col("domain_expression_correlation"),
        
        # Variant context
        col("total_gene_variants"),
        col("splice_variants"),
        col("expression_affecting_variants"),
        col("has_expression_variants"),
        
        # Enhanced scores
        col("disease_expression_score"),
        col("cancer_expression_score"),
        col("functional_expression_score"),
        
        # Classifications
        col("expression_priority"),
        col("is_clinically_relevant_expression"),
        col("disease_specific_expression_pattern"),
        col("expression_function_correlation")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} genes")
print(f"Total columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by Gene Symbol
print("\nDEDUPLICATING BY GENE_SYMBOL")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["gene_symbol"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Transcript Expression Features
print("\nSAVING GOLD TRANSCRIPT EXPRESSION FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_expression_ml_features")

print(f"Saved: {catalog_name}.gold.gene_expression_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nTRANSCRIPT EXPRESSION FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.gene_expression_ml_features").count()
print(f"\nTable created: {result_count:,} genes")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Base expression: 15 features")
print("  - Disease context: 6 features")
print("  - Cancer context: 4 features")
print("  - Protein domain context: 4 features")
print("  - Variant context: 4 features")
print("  - Enhanced scores: 3 features")
print("  - Classifications: 4 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.gene_expression_ml_features") \
    .groupBy("expression_priority") \
    .count() \
    .orderBy("expression_priority") \
    .show()

print("\nDisease-specific expression:")
spark.table(f"{catalog_name}.gold.gene_expression_ml_features") \
    .groupBy("disease_specific_expression_pattern") \
    .count() \
    .orderBy("disease_specific_expression_pattern") \
    .show()

print("\nProcessing complete")
