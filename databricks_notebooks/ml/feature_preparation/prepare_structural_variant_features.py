# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - STRUCTURAL VARIANT FEATURES
# MAGIC ##### Module: Structural Variant ML Features Preparation
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 05, 2026
# MAGIC
# MAGIC **Input:** structural_variant_ml_features (20 columns)
# MAGIC **Output:** ml_structural_variant_features
# MAGIC **Primary Key:** sv_id

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Load table data
print("Loading structural_variant_ml_features table...")
df_structural = spark.table(f"{catalog_name}.gold.structural_variant_ml_features")

print(f"Total records: {df_structural.count():,}")
df_structural.printSchema()

# COMMAND ----------

# DBTITLE 1,Selecting and preparing structural variant features
print("Selecting and preparing structural variant features...")

df_ml_structural = df_structural.select(
    "sv_id",
    "study_id",
    "variant_name",
    "chromosome",
    "start_pos",
    "end_pos",
    "variant_type",
    "sv_type_class",
    "sv_size",
    "sv_size_category",
    "has_gene_overlap",
    "affected_gene_count",
    "affected_genes",
    "complete_overlap_genes",
    "major_overlap_genes",
    "is_multi_gene_sv",
    "pharmacogenes_affected",
    "affects_pharmacogenes",
    "affects_omim_genes",
    "gene_impact_severity",
    "size_impact_score",
    "type_impact_score",
    "gene_impact_score",
    "sv_pathogenicity_score",
    "predicted_sv_pathogenicity",
    "is_high_risk_sv"
)

# COMMAND ----------

# DBTITLE 1,Handling missing values
print("Handling missing values...")

df_ml_structural = df_ml_structural.fillna({
    "has_gene_overlap": False,
    "is_multi_gene_sv": False,
    "affects_pharmacogenes": False,
    "affects_omim_genes": False,
    "is_high_risk_sv": False,
    "affected_gene_count": 0,
    "complete_overlap_genes": 0,
    "major_overlap_genes": 0,
    "pharmacogenes_affected": 0,
    "sv_size": 0,
    "size_impact_score": 0,
    "type_impact_score": 0,
    "gene_impact_score": 0,
    "sv_pathogenicity_score": 0
})

# COMMAND ----------

# DBTITLE 1,Creating derived features
print("Creating derived features...")

df_ml_structural = df_ml_structural.withColumn(
    "total_impact_score",
    F.col("size_impact_score") + F.col("type_impact_score") + F.col("gene_impact_score")
)

df_ml_structural = df_ml_structural.withColumn(
    "gene_disruption_potential",
    F.when(
        (F.col("complete_overlap_genes") > 0), 
        "high"
    ).when(
        (F.col("major_overlap_genes") > 0), 
        "medium"
    ).when(
        (F.col("has_gene_overlap") == True), 
        "low"
    ).otherwise("none")
)

df_ml_structural = df_ml_structural.withColumn(
    "sv_priority_score",
    (F.col("sv_pathogenicity_score") * 0.4 +
     F.col("gene_impact_score") * 0.3 +
     F.col("size_impact_score") * 0.2 +
     F.col("type_impact_score") * 0.1)
)

df_ml_structural = df_ml_structural.withColumn(
    "is_high_confidence_pathogenic",
    F.when(
        (F.col("predicted_sv_pathogenicity") == "pathogenic") & 
        (F.col("is_high_risk_sv") == True) & 
        (F.col("complete_overlap_genes") > 0), 
        True
    ).otherwise(False)
)

df_ml_structural = df_ml_structural.withColumn(
    "clinical_relevance_flag",
    F.when(
        (F.col("affects_pharmacogenes") == True) | 
        (F.col("affects_omim_genes") == True), 
        True
    ).otherwise(False)
)

df_ml_structural = df_ml_structural.withColumn(
    "dosage_sensitivity_flag",
    F.when(
        ((F.col("sv_type_class") == "deletion") | (F.col("sv_type_class") == "duplication")) &
        (F.col("affected_gene_count") > 0), 
        True
    ).otherwise(False)
)

# COMMAND ----------

# DBTITLE 1,Final structural variant ML features schema
print("Final structural variant ML features schema:")
df_ml_structural.printSchema()

print(f"\nTotal features prepared: {df_ml_structural.count():,}")

print("\nSample records:")
df_ml_structural.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Writing to ML table
print(f"Writing ml_structural_variant_features to {catalog_name}.gold.ml_structural_variant_features...")

df_ml_structural.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_structural_variant_features")

print("Structural variant features preparation complete!")

# COMMAND ----------

# DBTITLE 1,Verification
print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_structural_variant_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
