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

from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

print("Loading disease_ml_features table...")
df_disease = spark.table(f"{catalog_name}.gold.disease_ml_features")

print(f"Total records: {df_disease.count():,}")
df_disease.printSchema()

# COMMAND ----------

print("Selecting and preparing disease features...")

df_ml_disease = df_disease.select(
    "disease_id",
    "disease_name",
    "omim_id",
    "mondo_id",
    "orphanet_id",
    "is_cancer",
    "is_neurological",
    "is_cardiovascular",
    "is_metabolic",
    "is_rare_disease",
    "is_immune_disorder",
    "associated_gene_count",
    "associated_variant_count",
    "clinically_actionable",
    "has_drug_potential"
)

# COMMAND ----------

print("Handling missing values...")

df_ml_disease = df_ml_disease.fillna({
    "is_cancer": False,
    "is_neurological": False,
    "is_cardiovascular": False,
    "is_metabolic": False,
    "is_rare_disease": False,
    "is_immune_disorder": False,
    "clinically_actionable": False,
    "has_drug_potential": False,
    "associated_gene_count": 0,
    "associated_variant_count": 0
})

# COMMAND ----------

print("Creating derived features...")

df_ml_disease = df_ml_disease.withColumn(
    "disease_category_count",
    (F.col("is_cancer").cast("int") +
     F.col("is_neurological").cast("int") +
     F.col("is_cardiovascular").cast("int") +
     F.col("is_metabolic").cast("int") +
     F.col("is_rare_disease").cast("int") +
     F.col("is_immune_disorder").cast("int"))
)

df_ml_disease = df_ml_disease.withColumn(
    "database_coverage_score",
    (F.col("omim_id").isNotNull().cast("int") +
     F.col("mondo_id").isNotNull().cast("int") +
     F.col("orphanet_id").isNotNull().cast("int"))
)

df_ml_disease = df_ml_disease.withColumn(
    "variants_per_gene",
    F.when(F.col("associated_gene_count") > 0,
           F.col("associated_variant_count") / F.col("associated_gene_count"))
     .otherwise(0.0)
)

df_ml_disease = df_ml_disease.withColumn(
    "disease_complexity",
    F.when(F.col("associated_gene_count") > 10, "polygenic")
     .when(F.col("associated_gene_count") > 1, "oligogenic")
     .when(F.col("associated_gene_count") == 1, "monogenic")
     .otherwise("unknown")
)

df_ml_disease = df_ml_disease.withColumn(
    "disease_priority_score",
    (F.col("clinically_actionable").cast("int") * 3 +
     F.col("has_drug_potential").cast("int") * 2 +
     F.col("database_coverage_score"))
)

df_ml_disease = df_ml_disease.withColumn(
    "disease_burden_score",
    F.least(F.col("associated_gene_count") / 10.0, F.lit(1.0)) * 
    F.least(F.col("associated_variant_count") / 100.0, F.lit(1.0)) * 10
)

# COMMAND ----------

print("Final disease ML features schema:")
df_ml_disease.printSchema()

print(f"\nTotal features prepared: {df_ml_disease.count():,}")

print("\nSample records:")
df_ml_disease.show(5, truncate=False)

# COMMAND ----------

print(f"Writing ml_disease_features to {catalog_name}.gold.ml_disease_features...")

df_ml_disease.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_disease_features")

print("Disease features preparation complete!")

# COMMAND ----------

print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_disease_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
