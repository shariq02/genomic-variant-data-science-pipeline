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

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Load Table Data
print("Loading clinical_ml_features table...")
df_clinical = spark.table(f"{catalog_name}.gold.clinical_ml_features")

print(f"Total records: {df_clinical.count():,}")
df_clinical.printSchema()

# COMMAND ----------

# DBTITLE 1,Preparing clinical features
print("Selecting and preparing clinical features...")

df_ml_clinical = df_clinical.select(
    "variant_id",
    "gene_name",
    "official_gene_symbol",
    "chromosome",
    "position",
    "clinical_significance_simple",
    "clinvar_pathogenicity_class",
    "target_is_pathogenic",
    "target_is_benign",
    "target_is_vus",
    "clinical_sig_is_uncertain",
    "review_quality_score",
    "has_strong_evidence",
    "mutation_severity_score",
    "pathogenicity_score",
    "combined_pathogenicity_risk",
    "phylop_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_benign_count",
    "gene_vus_count",
    "gene_pathogenic_ratio",
    "gene_benign_ratio",
    "gene_vus_ratio"
)

# COMMAND ----------

# DBTITLE 1,Handling missing values
print("Handling missing values...")

df_ml_clinical = df_ml_clinical.fillna({
    "target_is_pathogenic": False,
    "target_is_benign": False,
    "target_is_vus": False,
    "clinical_sig_is_uncertain": False,
    "has_strong_evidence": False,
    "is_highly_conserved": False,
    "is_constrained": False,
    "review_quality_score": 0,
    "mutation_severity_score": 0,
    "pathogenicity_score": 0,
    "combined_pathogenicity_risk": 0,
    "conservation_level": 0,
    "phylop_score": 0.0,
    "cadd_phred": 0.0,
    "gene_total_variants": 0,
    "gene_pathogenic_count": 0,
    "gene_benign_count": 0,
    "gene_vus_count": 0,
    "gene_pathogenic_ratio": 0.0,
    "gene_benign_ratio": 0.0,
    "gene_vus_ratio": 0.0
})

# COMMAND ----------

# DBTITLE 1,Creating derived features
print("Creating derived features...")

df_ml_clinical = df_ml_clinical.withColumn(
    "clinical_confidence_score",
    F.when(F.col("has_strong_evidence") == True, 3)
     .when(F.col("review_quality_score") >= 2, 2)
     .when(F.col("review_quality_score") >= 1, 1)
     .otherwise(0)
)

df_ml_clinical = df_ml_clinical.withColumn(
    "evidence_strength",
    F.col("review_quality_score")
)

df_ml_clinical = df_ml_clinical.withColumn(
    "conservation_composite",
    (F.col("phylop_score") + F.col("cadd_phred")) / 2
)

df_ml_clinical = df_ml_clinical.withColumn(
    "clinical_actionability",
    F.when(
        (F.col("target_is_pathogenic") == True) & 
        (F.col("clinical_confidence_score") >= 2), 
        1
    ).otherwise(0)
)

# COMMAND ----------

# DBTITLE 1,Final clinical ML features schema
print("Final clinical ML features schema:")
df_ml_clinical.printSchema()

print(f"\nTotal features prepared: {df_ml_clinical.count():,}")

print("\nSample records:")
df_ml_clinical.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Writing to ML table
print(f"Writing ml_clinical_features to {catalog_name}.gold.ml_clinical_features...")

df_ml_clinical.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_clinical_features")

print("Clinical features preparation complete!")

# COMMAND ----------

# DBTITLE 1,Verification
print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_clinical_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
