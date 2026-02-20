# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - TRANSCRIPT EXPRESSION ANALYSIS
# MAGIC ##### Module: Gene-Level Expression Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 6: Transcript Isoform Impact
# MAGIC - Use Case 16: Gene Expression Analysis
# MAGIC
# MAGIC **Input:**
# MAGIC - silver.gtex_tissue_expression
# MAGIC - silver.genes_ultra_enriched
# MAGIC
# MAGIC **Output:** gold.transcript_expression_ml_features

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

print("GOLD TRANSCRIPT EXPRESSION FEATURES")
print("Gene-level expression feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"GTEx expression: {df_gtex.count():,}")
print(f"Genes: {df_genes.count():,}")

print("\nGTEx schema:")
df_gtex.printSchema()

print("\nSample GTEx data:")
df_gtex.show(5, truncate=60)

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
print("\nExpression statistics:")
df_gene_expression.describe().show()

print("\nTop 10 genes by max expression:")
df_gene_expression.orderBy(col("max_expression_tpm").desc()).show(10, truncate=False)

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
                when(col("total_tissues_expressed") <= 5, "tissue_specific")
                .when(col("total_tissues_expressed") <= 20, "moderately_specific")
                .when(col("total_tissues_expressed") <= 40, "broadly_expressed")
                .otherwise("ubiquitous"))
    
    .withColumn("expression_level_category",
                when(col("max_expression_tpm") >= 100, "high")
                .when(col("max_expression_tpm") >= 10, "medium")
                .when(col("max_expression_tpm") >= 1, "low")
                .otherwise("very_low"))
)

print("Added classification")
print("\nTissue specificity distribution:")
df_expression_classified.groupBy("expression_breadth_category").count().orderBy("expression_breadth_category").show()

print("\nExpression level distribution:")
df_expression_classified.groupBy("expression_level_category").count().orderBy("expression_level_category").show()

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

print("Added scores")
print("\nScore distribution:")
df_scored.select("tissue_specificity_score", "expression_significance_score", "clinical_relevance_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data
print("\nJOINING WITH GENE MASTER DATA")
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
        col("is_transcription_factor")
    )
    .join(
        df_scored.withColumn("gene_symbol", upper(trim(col("gene_name")))),
        on="gene_symbol",
        how="left"
    )
)

print(f"Genes with expression features: {df_with_genes.count():,}")
print("\nSample with genes:")
df_with_genes.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Expression Priority Classification
print("\nADDING EXPRESSION PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_with_genes
    .withColumn("expression_priority",
                when(col("clinical_relevance_score") >= 15, "high")
                .when(col("clinical_relevance_score") >= 8, "medium")
                .otherwise("low"))
    
    .withColumn("is_clinically_relevant_expression",
                when((col("is_tissue_specific")) & (col("is_highly_expressed")), True).otherwise(False))
)

print("Added priority classification")
print("\nPriority distribution:")
df_priority.groupBy("expression_priority").count().orderBy("expression_priority").show()

print("\nClinically relevant:")
clinically_relevant = df_priority.filter(col("is_clinically_relevant_expression")).count()
print(f"  Count: {clinically_relevant:,}")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        col("gene_symbol"),
        col("gene_full_name"),
        col("description"),
        col("chromosome"),
        col("gene_length"),
        
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transcription_factor"),
        
        coalesce(col("max_expression_tpm"), lit(0.0)).alias("max_expression_tpm"),
        coalesce(col("avg_expression_tpm"), lit(0.0)).alias("avg_expression_tpm"),
        coalesce(col("peak_expression_tpm"), lit(0.0)).alias("peak_expression_tpm"),
        coalesce(col("total_tissues_expressed"), lit(0)).alias("total_tissues_expressed"),
        coalesce(col("tissue_type_count"), lit(0)).alias("tissue_type_count"),
        coalesce(col("primary_tissue_count"), lit(0)).alias("primary_tissue_count"),
        
        coalesce(col("is_ubiquitously_expressed"), lit(False)).alias("is_ubiquitously_expressed"),
        coalesce(col("is_tissue_specific"), lit(False)).alias("is_tissue_specific"),
        coalesce(col("is_highly_expressed"), lit(False)).alias("is_highly_expressed"),
        coalesce(col("is_lowly_expressed"), lit(True)).alias("is_lowly_expressed"),
        
        coalesce(col("expression_breadth_category"), lit("unknown")).alias("expression_breadth_category"),
        coalesce(col("expression_level_category"), lit("very_low")).alias("expression_level_category"),
        
        coalesce(col("tissue_specificity_score"), lit(0.0)).alias("tissue_specificity_score"),
        coalesce(col("expression_significance_score"), lit(0)).alias("expression_significance_score"),
        coalesce(col("clinical_relevance_score"), lit(0)).alias("clinical_relevance_score"),
        
        coalesce(col("expression_priority"), lit("low")).alias("expression_priority"),
        coalesce(col("is_clinically_relevant_expression"), lit(False)).alias("is_clinically_relevant_expression")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} genes")

print("\nFeature columns:")
for col_name in df_final.columns:
    print(f"  - {col_name}")

print("\nSample final features:")
df_final.show(5, truncate=60)

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
    .saveAsTable(f"{catalog_name}.gold.transcript_expression_ml_features")

print(f"Saved: {catalog_name}.gold.transcript_expression_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nTRANSCRIPT EXPRESSION FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.transcript_expression_ml_features").count()
print(f"\nTable created:")
print(f"  gold.transcript_expression_ml_features: {result_count:,} genes")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.transcript_expression_ml_features") \
    .groupBy("expression_priority") \
    .count() \
    .orderBy("expression_priority") \
    .show()

print("\nExpression breadth breakdown:")
spark.table(f"{catalog_name}.gold.transcript_expression_ml_features") \
    .groupBy("expression_breadth_category") \
    .count() \
    .orderBy("expression_breadth_category") \
    .show()

print("\nProcessing complete")
