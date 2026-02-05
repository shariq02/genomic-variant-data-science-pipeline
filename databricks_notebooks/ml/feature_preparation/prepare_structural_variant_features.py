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

from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

print("Loading structural_variant_ml_features table...")
df_structural = spark.table(f"{catalog_name}.gold.structural_variant_ml_features")

print(f"Total records: {df_structural.count():,}")
df_structural.printSchema()

# COMMAND ----------

print("Selecting and preparing structural variant features...")

df_ml_structural = df_structural.select(
    "sv_id",
    "sv_type",
    "chromosome",
    "start_position",
    "end_position",
    "sv_length",
    "genes_affected_count",
    "genes_affected_list",
    "is_deletion",
    "is_duplication",
    "is_insertion",
    "is_inversion",
    "population_frequency",
    "is_rare_sv",
    "predicted_high_impact",
    "predicted_moderate_impact",
    "quality_score",
    "has_gene_overlap",
    "overlaps_exon"
)

# COMMAND ----------

print("Handling missing values...")

df_ml_structural = df_ml_structural.fillna({
    "is_deletion": False,
    "is_duplication": False,
    "is_insertion": False,
    "is_inversion": False,
    "is_rare_sv": False,
    "predicted_high_impact": False,
    "predicted_moderate_impact": False,
    "has_gene_overlap": False,
    "overlaps_exon": False,
    "genes_affected_count": 0,
    "sv_length": 0,
    "population_frequency": 0.0,
    "quality_score": 0.0
})

# COMMAND ----------

print("Creating derived features...")

df_ml_structural = df_ml_structural.withColumn(
    "sv_size_category",
    F.when(F.col("sv_length") >= 1000000, "large")
     .when(F.col("sv_length") >= 10000, "medium")
     .when(F.col("sv_length") >= 1000, "small")
     .otherwise("very_small")
)

df_ml_structural = df_ml_structural.withColumn(
    "sv_impact_score",
    F.when(F.col("predicted_high_impact") == True, 3)
     .when(F.col("predicted_moderate_impact") == True, 2)
     .otherwise(1)
)

df_ml_structural = df_ml_structural.withColumn(
    "gene_disruption_potential",
    F.when(
        (F.col("genes_affected_count") > 0) & (F.col("overlaps_exon") == True), 
        "high"
    ).when(
        (F.col("genes_affected_count") > 0) & (F.col("has_gene_overlap") == True), 
        "medium"
    ).otherwise("low")
)

df_ml_structural = df_ml_structural.withColumn(
    "sv_priority_score",
    (F.col("sv_impact_score") * 0.4 +
     F.col("genes_affected_count") * 0.3 +
     F.col("quality_score") / 100 * 0.3)
)

df_ml_structural = df_ml_structural.withColumn(
    "is_high_confidence_pathogenic",
    F.when(
        (F.col("predicted_high_impact") == True) & 
        (F.col("is_rare_sv") == True) & 
        (F.col("overlaps_exon") == True) &
        (F.col("quality_score") >= 50), 
        True
    ).otherwise(False)
)

df_ml_structural = df_ml_structural.withColumn(
    "frequency_category",
    F.when(F.col("population_frequency") == 0, "novel")
     .when(F.col("population_frequency") < 0.01, "rare")
     .when(F.col("population_frequency") < 0.05, "uncommon")
     .otherwise("common")
)

df_ml_structural = df_ml_structural.withColumn(
    "dosage_sensitivity_flag",
    F.when(
        ((F.col("is_deletion") == True) | (F.col("is_duplication") == True)) &
        (F.col("genes_affected_count") > 0), 
        True
    ).otherwise(False)
)

# COMMAND ----------

print("Final structural variant ML features schema:")
df_ml_structural.printSchema()

print(f"\nTotal features prepared: {df_ml_structural.count():,}")

print("\nSample records:")
df_ml_structural.show(5, truncate=False)

# COMMAND ----------

print(f"Writing ml_structural_variant_features to {catalog_name}.gold.ml_structural_variant_features...")

df_ml_structural.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_structural_variant_features")

print("Structural variant features preparation complete!")

# COMMAND ----------

print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_structural_variant_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
