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

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Laod table data
print("Loading variant_impact_ml_features table...")
df_variant_impact = spark.table(f"{catalog_name}.gold.variant_impact_ml_features")

print(f"Total records: {df_variant_impact.count():,}")
df_variant_impact.printSchema()

# COMMAND ----------

# DBTITLE 1,Preparing variant impact features
print("Selecting and preparing variant impact features...")

df_ml_variant_impact = df_variant_impact.select(
    "variant_id",
    "gene_name",
    "validated_gene_symbol",
    "chromosome",
    "position",
    "variant_type",
    "clinical_significance_simple",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "is_high_impact",
    "is_very_high_impact",
    "is_loss_of_function",
    "affects_functional_domain",
    "is_domain_affecting",
    "has_kinase_domain",
    "phylop_score",
    "phastcons_score",
    "gerp_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "is_likely_deleterious",
    "mutation_severity_score",
    "pathogenicity_score",
    "functional_impact_score"
)

# COMMAND ----------

# DBTITLE 1,Handling missing values
print("Handling missing values...")

df_ml_variant_impact = df_ml_variant_impact.fillna({
    "is_pathogenic": False,
    "is_benign": False,
    "is_vus": False,
    "is_high_impact": False,
    "is_very_high_impact": False,
    "is_loss_of_function": False,
    "affects_functional_domain": False,
    "is_domain_affecting": False,
    "has_kinase_domain": False,
    "is_highly_conserved": False,
    "is_constrained": False,
    "is_likely_deleterious": False,
    "phylop_score": 0.0,
    "phastcons_score": 0.0,
    "gerp_score": 0.0,
    "cadd_phred": 0.0,
    "conservation_level": 0,
    "mutation_severity_score": 0,
    "pathogenicity_score": 0,
    "functional_impact_score": 0
})

# COMMAND ----------

# DBTITLE 1,Creating derived features
print("Creating derived features...")

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "domain_impact_count",
    (F.col("affects_functional_domain").cast("int") +
     F.col("is_domain_affecting").cast("int") +
     F.col("has_kinase_domain").cast("int"))
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "conservation_composite",
    (F.col("phylop_score") + 
     F.col("phastcons_score") + 
     F.col("gerp_score") + 
     F.col("cadd_phred")) / 4
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "impact_severity_score",
    F.when(F.col("is_very_high_impact") == True, 3)
     .when(F.col("is_high_impact") == True, 2)
     .otherwise(1)
)

df_ml_variant_impact = df_ml_variant_impact.withColumn(
    "high_confidence_deleterious",
    F.when(
        (F.col("is_loss_of_function") == True) | 
        ((F.col("is_very_high_impact") == True) & (F.col("is_highly_conserved") == True)), 
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

# DBTITLE 1,Final variant impact ML features schema
print("Final variant impact ML features schema:")
df_ml_variant_impact.printSchema()

print(f"\nTotal features prepared: {df_ml_variant_impact.count():,}")

print("\nSample records:")
df_ml_variant_impact.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Writing to ML table
print(f"Writing ml_variant_impact_features to {catalog_name}.gold.ml_variant_impact_features...")

df_ml_variant_impact.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_variant_impact_features")

print("Variant impact features preparation complete!")

# COMMAND ----------

# DBTITLE 1,Verification
print("Verification:")
df_verify = spark.table(f"{catalog_name}.gold.ml_variant_impact_features")
print(f"Records written: {df_verify.count():,}")
print(f"Columns: {len(df_verify.columns)}")
