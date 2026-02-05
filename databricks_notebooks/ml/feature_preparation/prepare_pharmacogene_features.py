# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - PHARMACOGENE FEATURES
# MAGIC ##### Module: Pharmacogene ML Features Preparation
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 05, 2026
# MAGIC
# MAGIC **Input:** pharmacogene_ml_features (21 columns)
# MAGIC **Output:** ml_pharmacogene_features
# MAGIC **Primary Key:** variant_id

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

print("Loading pharmacogene_ml_features table...")
df_pharmacogene = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features")

print(f"Total records: {df_pharmacogene.count():,}")
df_pharmacogene.printSchema()

# COMMAND ----------

print("Selecting and preparing pharmacogene features...")

df_ml_pharmacogene = df_pharmacogene.select(
    "variant_id",
    "gene_symbol",
    "drug_name",
    "drug_class",
    "is_cyp_gene",
    "is_drug_target",
    "is_kinase",
    "is_receptor",
    "is_transporter",
    "is_metabolic_enzyme",
    "affects_drug_metabolism",
    "affects_drug_response",
    "affects_drug_toxicity",
    "domain_affecting",
    "domain_type",
    "interaction_score",
    "evidence_level",
    "clinical_annotation_exists",
    "fda_label_exists",
    "guideline_exists"
)

# COMMAND ----------

print("Handling missing values...")

df_ml_pharmacogene = df_ml_pharmacogene.fillna({
    "is_cyp_gene": False,
    "is_drug_target": False,
    "is_kinase": False,
    "is_receptor": False,
    "is_transporter": False,
    "is_metabolic_enzyme": False,
    "affects_drug_metabolism": False,
    "affects_drug_response": False,
    "affects_drug_toxicity": False,
    "domain_affecting": False,
    "clinical_annotation_exists": False,
    "fda_label_exists": False,
    "guideline_exists": False,
    "interaction_score": 0.0
})

# COMMAND ----------

print("Creating derived features...")

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "protein_function_count",
    (F.col("is_cyp_gene").cast("int") +
     F.col("is_drug_target").cast("int") +
     F.col("is_kinase").cast("int") +
     F.col("is_receptor").cast("int") +
     F.col("is_transporter").cast("int") +
     F.col("is_metabolic_enzyme").cast("int"))
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "drug_impact_count",
    (F.col("affects_drug_metabolism").cast("int") +
     F.col("affects_drug_response").cast("int") +
     F.col("affects_drug_toxicity").cast("int"))
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "clinical_evidence_score",
    (F.col("clinical_annotation_exists").cast("int") * 3 +
     F.col("fda_label_exists").cast("int") * 2 +
     F.col("guideline_exists").cast("int") * 1)
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "pharmacogene_priority",
    F.when(
        (F.col("is_cyp_gene") == True) & 
        (F.col("clinical_evidence_score") >= 3), 
        "high"
    ).when(
        (F.col("is_drug_target") == True) | 
        (F.col("clinical_evidence_score") >= 2), 
        "medium"
    ).otherwise("low")
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "druggability_score",
    (F.col("protein_function_count") * 0.3 +
     F.col("drug_impact_count") * 0.4 +
     F.col("clinical_evidence_score") * 0.3)
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "high_impact_pharmacogene",
    F.when(
        (F.col("is_cyp_gene") == True) & 
        (F.col("affects_drug_metabolism") == True), 
        True
    ).when(
        (F.col("is_drug_target") == True) & 
        (F.col("fda_label_exists") == True), 
        True
    ).otherwise(False)
)

# COMMAND ----------

print("Final pharmacogene ML features schema:")
df_ml_pharmacogene.printSchema()

print(f"\nTotal features prepared: {df_ml_pharmacogene.count():,}")

print("\nSample records:")
df_ml_pharmacogene.show(5, truncate=False)

# COMMAND ----------

print(f"Writing ml_pharmacogene_features to {catalog_name}.gold.ml_pharmacogene_features...")

df_ml_pharmacogene.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_pharmacogene_features")

print("Pharmacogene features preparation complete!")

# COMMAND ----------

print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_pharmacogene_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
