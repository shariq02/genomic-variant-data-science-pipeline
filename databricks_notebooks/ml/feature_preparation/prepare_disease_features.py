# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - DISEASE FEATURES
# MAGIC ##### Module: Disease ML Features Preparation
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 05, 2026
# MAGIC
# MAGIC **Input:** disease_ml_features (16 columns)
# MAGIC **Output:** ml_disease_features
# MAGIC **Primary Key:** disease_id

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Load table data
print("Loading disease_ml_features table...")
df_disease = spark.table(f"{catalog_name}.gold.disease_ml_features")

print(f"Total records: {df_disease.count():,}")
df_disease.printSchema()

# COMMAND ----------

# DBTITLE 1,Selecting and preparing disease features
print("Selecting and preparing disease features...")

df_ml_disease = df_disease.select(
    "variant_id",
    "gene_name",
    "chromosome",
    "position",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "clinical_significance_simple",
    "disease_enriched",
    "primary_disease",
    "disease_name_enriched",
    "omim_id",
    "mondo_id",
    "orphanet_id",
    "has_omim_disease",
    "has_mondo_disease",
    "has_orphanet_disease",
    "disease_db_coverage",
    "disease_count",
    "disease_gene_count",
    "disease_total_variants",
    "disease_pathogenic_variants",
    "disease_benign_variants",
    "disease_vus_variants",
    "disease_pathogenic_ratio",
    "is_polygenic_disease",
    "disease_complexity",
    "is_clinically_actionable",
    "has_drug_development_potential",
    "is_cancer_gene_variant",
    "is_neurological_gene_variant",
    "is_cardiovascular_gene_variant",
    "is_metabolic_gene_variant",
    "is_rare_disease_gene_variant"
)

# COMMAND ----------

# DBTITLE 1,Handling missing values
print("Handling missing values...")

df_ml_disease = df_ml_disease.fillna({
    "is_pathogenic": False,
    "is_benign": False,
    "is_vus": False,
    "has_omim_disease": False,
    "has_mondo_disease": False,
    "has_orphanet_disease": False,
    "is_polygenic_disease": False,
    "is_clinically_actionable": False,
    "has_drug_development_potential": False,
    "is_cancer_gene_variant": False,
    "is_neurological_gene_variant": False,
    "is_cardiovascular_gene_variant": False,
    "is_metabolic_gene_variant": False,
    "is_rare_disease_gene_variant": False,
    "disease_db_coverage": 0,
    "disease_count": 0,
    "disease_gene_count": 0,
    "disease_total_variants": 0,
    "disease_pathogenic_variants": 0,
    "disease_benign_variants": 0,
    "disease_vus_variants": 0,
    "disease_pathogenic_ratio": 0.0
})

# COMMAND ----------

# DBTITLE 1,Creating derived features
print("Creating derived features...")

df_ml_disease = df_ml_disease.withColumn(
    "disease_category_count",
    (F.col("is_cancer_gene_variant").cast("int") +
     F.col("is_neurological_gene_variant").cast("int") +
     F.col("is_cardiovascular_gene_variant").cast("int") +
     F.col("is_metabolic_gene_variant").cast("int") +
     F.col("is_rare_disease_gene_variant").cast("int"))
)

df_ml_disease = df_ml_disease.withColumn(
    "database_coverage_score",
    (F.col("has_omim_disease").cast("int") +
     F.col("has_mondo_disease").cast("int") +
     F.col("has_orphanet_disease").cast("int"))
)

df_ml_disease = df_ml_disease.withColumn(
    "variants_per_gene",
    F.when(F.col("disease_gene_count") > 0,
           F.col("disease_total_variants") / F.col("disease_gene_count"))
     .otherwise(0.0)
)

df_ml_disease = df_ml_disease.withColumn(
    "disease_priority_score",
    (F.col("is_clinically_actionable").cast("int") * 3 +
     F.col("has_drug_development_potential").cast("int") * 2 +
     F.col("database_coverage_score"))
)

df_ml_disease = df_ml_disease.withColumn(
    "disease_burden_score",
    F.least(F.col("disease_gene_count") / 10.0, F.lit(1.0)) * 
    F.least(F.col("disease_total_variants") / 100.0, F.lit(1.0)) * 10
)

# COMMAND ----------

# DBTITLE 1,Final disease ML features schema
print("Final disease ML features schema:")
df_ml_disease.printSchema()

print(f"\nTotal features prepared: {df_ml_disease.count():,}")

print("\nSample records:")
df_ml_disease.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Writing to ML table
print(f"Writing ml_disease_features to {catalog_name}.gold.ml_disease_features...")

df_ml_disease.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_disease_features")

print("Disease features preparation complete!")

# COMMAND ----------

# DBTITLE 1,Verification
print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_disease_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
