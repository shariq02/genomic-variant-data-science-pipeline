# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - CLINICAL FEATURES
# MAGIC ##### Module: Clinical ML Features Preparation
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 05, 2026
# MAGIC
# MAGIC **Input:** clinical_ml_features (23 columns)
# MAGIC **Output:** ml_clinical_features
# MAGIC **Primary Key:** variant_id

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

print("Loading clinical_ml_features table...")
df_clinical = spark.table(f"{catalog_name}.gold.clinical_ml_features")

print(f"Total records: {df_clinical.count():,}")
df_clinical.printSchema()

# COMMAND ----------

print("Selecting and preparing clinical features...")

df_ml_clinical = df_clinical.select(
    "variant_id",
    "gene_symbol",
    "clinical_significance",
    "pathogenic_flag",
    "benign_flag",
    "vus_flag",
    "conflicting_flag",
    "risk_factor_flag",
    "drug_response_flag",
    "review_status",
    "stars",
    "has_assertion_criteria",
    "no_assertion_provided",
    "expert_panel_review",
    "practice_guideline_review",
    "submission_count",
    "submitter_count",
    "first_in_clinvar_date",
    "last_updated_clinvar_date",
    "conservation_phylop",
    "conservation_cadd",
    "has_multiple_genes",
    "gene_count"
)

# COMMAND ----------

print("Handling missing values...")

df_ml_clinical = df_ml_clinical.fillna({
    "pathogenic_flag": False,
    "benign_flag": False,
    "vus_flag": False,
    "conflicting_flag": False,
    "risk_factor_flag": False,
    "drug_response_flag": False,
    "has_assertion_criteria": False,
    "no_assertion_provided": False,
    "expert_panel_review": False,
    "practice_guideline_review": False,
    "has_multiple_genes": False,
    "stars": 0,
    "submission_count": 0,
    "submitter_count": 0,
    "conservation_phylop": 0.0,
    "conservation_cadd": 0.0,
    "gene_count": 1
})

# COMMAND ----------

print("Creating derived features...")

df_ml_clinical = df_ml_clinical.withColumn(
    "clinical_confidence_score",
    F.when(F.col("expert_panel_review") == True, 4)
     .when(F.col("practice_guideline_review") == True, 3)
     .when(F.col("stars") >= 2, 2)
     .when(F.col("has_assertion_criteria") == True, 1)
     .otherwise(0)
)

df_ml_clinical = df_ml_clinical.withColumn(
    "evidence_strength",
    (F.col("submission_count") * 0.3 + F.col("submitter_count") * 0.7).cast("int")
)

df_ml_clinical = df_ml_clinical.withColumn(
    "conservation_composite",
    (F.col("conservation_phylop") + F.col("conservation_cadd")) / 2
)

df_ml_clinical = df_ml_clinical.withColumn(
    "clinical_actionability",
    F.when(
        (F.col("pathogenic_flag") == True) & 
        (F.col("clinical_confidence_score") >= 2), 
        1
    ).otherwise(0)
)

# COMMAND ----------

print("Final clinical ML features schema:")
df_ml_clinical.printSchema()

print(f"\nTotal features prepared: {df_ml_clinical.count():,}")

print("\nSample records:")
df_ml_clinical.show(5, truncate=False)

# COMMAND ----------

print(f"Writing ml_clinical_features to {catalog_name}.gold.ml_clinical_features...")

df_ml_clinical.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_clinical_features")

print("Clinical features preparation complete!")

# COMMAND ----------

print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_clinical_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
