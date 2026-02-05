# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - VARIANT IMPACT FEATURES
# MAGIC ##### Module: Variant Impact ML Features Preparation
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 05, 2026
# MAGIC
# MAGIC **Input:** variant_impact_ml_features (18 columns)
# MAGIC **Output:** ml_variant_impact_features
# MAGIC **Primary Key:** variant_id

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

print("Loading variant_impact_ml_features table...")
df_variant_impact = spark.table(f"{catalog_name}.gold.variant_impact_ml_features")

print(f"Total records: {df_variant_impact.count():,}")
df_variant_impact.printSchema()

# COMMAND ----------

print("Selecting and preparing variant impact features...")

df_ml_variant_impact = df_variant_impact.select(
    "variant_id",
    "gene_symbol",
    "consequence_type",
    "is_high_impact",
    "is_moderate_impact",
    "is_low_impact",
    "is_lof",
    "affects_protein_domain",
    "affects_dna_binding",
    "affects_kinase_domain",
    "affects_signal_peptide",
    "conservation_phylop",
    "conservation_phastcons",
    "conservation_gerp",
    "conservation_cadd",
    "in_conserved_region",
    "high_conservation_flag"
)

# COMMAND ----------

print("Handling missing values...")

df_ml_variant_impact = df_ml_variant_impact.fillna({
    "is_high_impact": False,
    "is_moderate_impact": False,
    "is_low_impact": False,
    "is_lof": False,
    "affects_protein_domain": False,
    "affects_dna_binding": False,
    "affects_kinase_domain": False,
    "affects_signal_peptide": False,
    "in_conserved_region": False,
    "high_conservation_flag": False,
    "conservation_phylop": 0.0,
    "conservation_phastcons": 0.0,
    "conservation_gerp": 0.0,
    "conservation_cadd": 0.0
})

# COMMAND ----------

print("Creating derived features...")

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "domain_impact_count",
    (F.col("affects_protein_domain").cast("int") +
     F.col("affects_dna_binding").cast("int") +
     F.col("affects_kinase_domain").cast("int") +
     F.col("affects_signal_peptide").cast("int"))
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "conservation_composite",
    (F.col("conservation_phylop") + 
     F.col("conservation_phastcons") + 
     F.col("conservation_gerp") + 
     F.col("conservation_cadd")) / 4
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "impact_severity_score",
    F.when(F.col("is_high_impact") == True, 3)
     .when(F.col("is_moderate_impact") == True, 2)
     .when(F.col("is_low_impact") == True, 1)
     .otherwise(0)
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "functional_impact_score",
    (F.col("impact_severity_score") * 0.4 +
     F.col("domain_impact_count") * 0.3 +
     F.col("conservation_composite") * 0.3)
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "high_confidence_deleterious",
    F.when(
        (F.col("is_lof") == True) | 
        ((F.col("is_high_impact") == True) & (F.col("high_conservation_flag") == True)), 
        True
    ).otherwise(False)
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "conservation_category",
    F.when(F.col("conservation_composite") >= 5, "highly_conserved")
     .when(F.col("conservation_composite") >= 2, "moderately_conserved")
     .when(F.col("conservation_composite") > 0, "weakly_conserved")
     .otherwise("not_conserved")
)

# COMMAND ----------

print("Final variant impact ML features schema:")
df_ml_variant_impact.printSchema()

print(f"\nTotal features prepared: {df_ml_variant_impact.count():,}")

print("\nSample records:")
df_ml_variant_impact.show(5, truncate=False)

# COMMAND ----------

print(f"Writing ml_variant_impact_features to {catalog_name}.gold.ml_variant_impact_features...")

df_ml_variant_impact.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_variant_impact_features")

print("Variant impact features preparation complete!")

# COMMAND ----------

print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_variant_impact_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
