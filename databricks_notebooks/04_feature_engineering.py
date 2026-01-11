# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Feature Engineering  
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** 11 January 2026  
# MAGIC
# MAGIC **Purpose:**  Create analytical and machine-learning features from cleaned gene and variant data.
# MAGIC
# MAGIC **Input Tables (Silver Layer):**  
# MAGIC - `workspace.silver.genes_clean`  
# MAGIC - `workspace.silver.variants_clean`
# MAGIC
# MAGIC **Output Tables (Gold Layer):**  
# MAGIC - `workspace.gold.gene_features`  
# MAGIC - `workspace.gold.chromosome_features`  
# MAGIC - `workspace.gold.gene_disease_association`  
# MAGIC - `workspace.gold.ml_features`
# MAGIC
# MAGIC

# COMMAND ----------

# ====================================================================
# FILE: databricks_notebooks/03_feature_engineering.py
#
# PYSPARK DATA PROCESSING WITH UNITY CATALOG
# Feature Engineering
#
# Project: DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 11 January 2026
#
# Purpose:
#   Generate gene-level, chromosome-level, gene-disease,
#   and machine-learning features from cleaned Silver layer data.
#
# Input (Silver Layer):
#   workspace.silver.genes_clean
#   workspace.silver.variants_clean
#
# Output (Gold Layer):
#   workspace.gold.gene_features
#   workspace.gold.chromosome_features
#   workspace.gold.gene_disease_association
#   workspace.gold.ml_features
#
# Notes:
#   - Aggregates variant data at gene and chromosome levels
#   - Computes pathogenicity ratios and risk scores
#   - Prepares ML-ready feature tables
# ====================================================================


# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    when, round as spark_round, countDistinct
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("\n" + "="*40)
print("FEATURE ENGINEERING")
print("="*40)
print(f"Catalog: {catalog_name}")
print("Creating ML features from Silver layer")
print("="*40)

# COMMAND ----------

# DBTITLE 1,Read Silver Layer Tables
print("\nReading cleaned data...")

df_genes = spark.table(f"{catalog_name}.silver.genes_clean")
df_variants = spark.table(f"{catalog_name}.silver.variants_clean")

print(f"Loaded {df_genes.count():,} genes")
print(f"Loaded {df_variants.count():,} variants")

# COMMAND ----------

# DBTITLE 1,Create Gene-Level Aggregations
df_gene_features = (
    df_variants
    .groupBy("gene_name")
    .agg(
        count("*").alias("mutation_count"),
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        countDistinct("disease").alias("disease_count"),
        countDistinct("variant_type").alias("variant_type_count"),
        avg(when(col("position").isNotNull(), col("position"))).alias("avg_position")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(col("total_pathogenic") / col("mutation_count"), 4))
    .withColumn("benign_ratio",
                spark_round(col("total_benign") / col("mutation_count"), 4))
    .withColumn("risk_level",
                when(col("pathogenic_ratio") >= 0.7, "High")
                .when(col("pathogenic_ratio") >= 0.3, "Medium")
                .otherwise("Low"))
    .withColumn("risk_score",
                spark_round(
                    (col("pathogenic_ratio") * 50) +
                    (col("mutation_count") / 100) +
                    (col("disease_count") * 2),
                    2
                ))
)


# COMMAND ----------

# DBTITLE 1,Join Gene Metadata
df_gene_features = df_gene_features.join(
    df_genes.select("gene_name", "gene_id", "chromosome", "gene_type", "gene_length"),
    on="gene_name",
    how="left"
)

# COMMAND ----------

# DBTITLE 1,Mutation Density Calculation
df_gene_features = df_gene_features.withColumn("mutation_density",
    when(col("gene_length").isNotNull() & (col("gene_length") > 0),
         spark_round(col("mutation_count") / col("gene_length") * 1000000, 6))
    .otherwise(None)
)

print(f"Created {df_gene_features.count():,} gene features")

# COMMAND ----------

# DBTITLE 1,Chromosome-Level Features
df_chromosome_features = (
    df_variants
    .filter(col("chromosome").isNotNull())
    .groupBy("chromosome")
    .agg(
        countDistinct("gene_name").alias("gene_count"),
        count("*").alias("variant_count"),
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_percentage",
                spark_round((col("total_pathogenic") / col("variant_count")) * 100, 2))
    .withColumn("avg_mutations_per_gene",
                spark_round(col("variant_count") / col("gene_count"), 2))
    .orderBy("chromosome")
)

print(f"Created chromosome features for {df_chromosome_features.count()} chromosomes")


# COMMAND ----------

# DBTITLE 1,Gene–Disease Associations
df_gene_disease = (
    df_variants
    .filter(col("disease").isNotNull())
    .filter(col("disease") != "Unknown")
    .groupBy("gene_name", "disease")
    .agg(
        count("*").alias("mutation_count"),
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(col("total_pathogenic") / col("mutation_count"), 4))
    .withColumn("association_strength",
                when(col("pathogenic_ratio") >= 0.8, "Strong")
                .when(col("pathogenic_ratio") >= 0.5, "Moderate")
                .otherwise("Weak"))
    .filter(col("mutation_count") >= 3)
    .orderBy(col("pathogenic_ratio").desc())
)

print(f"Created {df_gene_disease.count():,} gene-disease associations")


# COMMAND ----------

# DBTITLE 1,Save Gene Features
print("\n" + "="*40)
print("SAVING TO GOLD LAYER")
print("="*40)

df_gene_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_features")

print(f"Saved: {catalog_name}.gold.gene_features")

# COMMAND ----------

# DBTITLE 1,Save Chromosome Features
print("\n" + "="*40)
print("SAVING TO GOLD LAYER")
print("="*40)

df_chromosome_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.chromosome_features")

print(f"Saved: {catalog_name}.gold.chromosome_features")


# COMMAND ----------

# DBTITLE 1,Save Gene–Disease Associations
print("\n" + "="*40)
print("SAVING TO GOLD LAYER")
print("="*40)

df_gene_disease.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_disease_association")

print(f"Saved: {catalog_name}.gold.gene_disease_association")


# COMMAND ----------

# DBTITLE 1,Prepare ML Features
df_ml_features = df_gene_features.select(
    "gene_name", 
    "chromosome", 
    "mutation_count", 
    "pathogenic_count",
    "likely_pathogenic_count",
    "total_pathogenic",
    "benign_count",
    "likely_benign_count",
    "total_benign",
    "pathogenic_ratio", 
    "disease_count",
    "variant_type_count", 
    "mutation_density", 
    "risk_level"
)

# COMMAND ----------

# DBTITLE 1,Save ML Features
df_ml_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_features")

print(f"Saved: {catalog_name}.gold.ml_features")

# COMMAND ----------

# DBTITLE 1,Completion Message
print("\n" + "="*70)
print("FEATURE ENGINEERING COMPLETE!")
print("="*70)
print("Gold Layer Tables Created:")
print(f"  - gene_features: {df_gene_features.count():,} rows")
print(f"  - chromosome_features: {df_chromosome_features.count():,} rows")
print(f"  - gene_disease_association: {df_gene_disease.count():,} rows")
print(f"  - ml_features: {df_ml_features.count():,} rows")


# COMMAND ----------

df_variants = spark.table("workspace.silver.variants_clean")

# Check disease distribution
df_variants.groupBy("disease").count().orderBy(col("count").desc()).show(20, truncate=False)

# Check how many non-Unknown diseases exist
non_unknown_count = df_variants.filter(
    (col("disease").isNotNull()) & 
    (col("disease") != "Unknown")
).count()

print(f"Variants with non-Unknown disease: {non_unknown_count}")
