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

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Load table data
print("Loading pharmacogene_ml_features table...")
df_pharmacogene = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features")

print(f"Total records: {df_pharmacogene.count():,}")
df_pharmacogene.printSchema()

# COMMAND ----------

# DBTITLE 1,Selecting and preparing pharmacogene features
print("Selecting and preparing pharmacogene features...")

df_ml_pharmacogene = df_pharmacogene.select(
    "variant_id",
    "gene_name",
    "official_symbol",
    "validated_gene_symbol",
    "chromosome",
    "position",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "clinical_significance_simple",
    "variant_type",
    "is_pharmacogene",
    "pharmacogene_category",
    "pharmacogene_evidence_level",
    "is_cyp_gene",
    "is_drug_target",
    "is_kinase",
    "is_receptor",
    "is_transporter",
    "is_metabolizing_enzyme",
    "is_enzyme",
    "drug_metabolism_role",
    "drug_target_category",
    "druggability_score",
    "enhanced_druggability_score",
    "has_drug_interaction_potential",
    "drug_response_impact",
    "has_pharmgkb_annotation",
    "is_clinical_pharmacogene"
)

# COMMAND ----------

# DBTITLE 1,Handling missing values
print("Handling missing values...")

df_ml_pharmacogene = df_ml_pharmacogene.fillna({
    "is_pathogenic": False,
    "is_benign": False,
    "is_vus": False,
    "is_pharmacogene": False,
    "is_cyp_gene": False,
    "is_drug_target": False,
    "is_kinase": False,
    "is_receptor": False,
    "is_transporter": False,
    "is_metabolizing_enzyme": False,
    "is_enzyme": False,
    "has_drug_interaction_potential": False,
    "has_pharmgkb_annotation": False,
    "is_clinical_pharmacogene": False,
    "druggability_score": 0.0,
    "enhanced_druggability_score": 0.0
})

# COMMAND ----------

# DBTITLE 1,Creating derived features
print("Creating derived features...")

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "protein_function_count",
    (F.col("is_cyp_gene").cast("int") +
     F.col("is_drug_target").cast("int") +
     F.col("is_kinase").cast("int") +
     F.col("is_receptor").cast("int") +
     F.col("is_transporter").cast("int") +
     F.col("is_metabolizing_enzyme").cast("int"))
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "pharmacogene_evidence_score",
    F.when(F.col("pharmacogene_evidence_level") == "high", 3)
     .when(F.col("pharmacogene_evidence_level") == "medium", 2)
     .when(F.col("pharmacogene_evidence_level") == "low", 1)
     .otherwise(0)
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "clinical_evidence_score",
    (F.col("is_clinical_pharmacogene").cast("int") * 3 +
     F.col("has_pharmgkb_annotation").cast("int") * 2 +
     F.col("pharmacogene_evidence_score"))
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "pharmacogene_priority",
    F.when(
        (F.col("is_cyp_gene") == True) & 
        (F.col("clinical_evidence_score") >= 4), 
        "high"
    ).when(
        (F.col("is_drug_target") == True) | 
        (F.col("clinical_evidence_score") >= 2), 
        "medium"
    ).otherwise("low")
)

df_ml_pharmacogene = df_ml_pharmacogene.withColumn(
    "high_impact_pharmacogene",
    F.when(
        (F.col("is_cyp_gene") == True) & 
        (F.col("has_drug_interaction_potential") == True), 
        True
    ).when(
        (F.col("is_drug_target") == True) & 
        (F.col("is_clinical_pharmacogene") == True), 
        True
    ).otherwise(False)
)

# COMMAND ----------

# DBTITLE 1,Final pharmacogene ML features schema
print("Final pharmacogene ML features schema:")
df_ml_pharmacogene.printSchema()

print(f"\nTotal features prepared: {df_ml_pharmacogene.count():,}")

print("\nSample records:")
df_ml_pharmacogene.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Writing to ML table
print(f"Writing ml_pharmacogene_features to {catalog_name}.gold.ml_pharmacogene_features...")

df_ml_pharmacogene.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_pharmacogene_features")

print("Pharmacogene features preparation complete!")

# COMMAND ----------

# DBTITLE 1,Verification
print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_pharmacogene_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
