# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - STRUCTURAL VARIANT IMPACT
# MAGIC ##### Module: Prepare Structural Variant ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 15: Structural Variant Impact
# MAGIC
# MAGIC **Input:** gold.structural_variant_ml_features (217K SVs)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_structural_variant_train
# MAGIC - gold.ml_dataset_structural_variant_validation
# MAGIC - gold.ml_dataset_structural_variant_test
# MAGIC
# MAGIC **Target:** sv_classification / sv_impact_tier

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - STRUCTURAL VARIANT IMPACT")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load structural_variant_ml_features
print("\nLOADING structural_variant_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.structural_variant_ml_features")
print(f"Total records: {df.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by sv_id
print("\nDEDUPLICATING BY sv_id")
print("=" * 80)

before = df.count()
df = df.dropDuplicates(["sv_id"])
after = df.count()
print(f"Before: {before:,}")
print(f"After:  {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from structural_variant_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "sv_id",
    "study_id",
    "variant_name",
    "chromosome",
    "start_pos",
    "end_pos",
    "assembly",
    "variant_type",
    "sv_type_class",
    "sv_size",
    "sv_size_category",
    "sv_pathogenicity_risk",
    "genes_overlapped",
    "gene_list",
    "pharmacogenes_affected",
    "omim_genes_affected",
    "kinase_genes_affected",
    "receptor_genes_affected",
    "max_gene_disruption_fraction",
    "avg_druggability_affected_genes",
    "has_critical_gene_disruption",
    "total_disease_associations",
    "cancer_genes_affected",
    "neuro_genes_affected",
    "has_disease_associated_genes",
    "broadly_expressed_genes_affected",
    "affects_essential_genes",
    "sv_combined_impact_score",
    "sv_impact_tier",
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("sv_impact_tier:")
df_ml.groupBy("sv_impact_tier").count().orderBy("sv_impact_tier").show()

print("sv_type_class:")
df_ml.groupBy("sv_type_class").count().orderBy("sv_type_class").show()

print("sv_pathogenicity_risk:")
df_ml.groupBy("sv_pathogenicity_risk").count().orderBy("sv_pathogenicity_risk").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "sv_size": 0,
    "genes_overlapped": 0,
    "pharmacogenes_affected": 0,
    "omim_genes_affected": 0,
    "kinase_genes_affected": 0,
    "receptor_genes_affected": 0,
    "max_gene_disruption_fraction": 0.0,
    "avg_druggability_affected_genes": 0.0,
    "total_disease_associations": 0,
    "cancer_genes_affected": 0,
    "neuro_genes_affected": 0,
    "broadly_expressed_genes_affected": 0,
    "sv_combined_impact_score": 0,
    "sv_type_class": "unknown",
    "sv_impact_tier": "unknown",
    "study_id": "unknown",
    "variant_name": "unknown",
    "assembly": "unknown"
})

print("Missing values filled")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Create splits
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df_ml = df_ml.withColumn("random_split", rand(seed=42))

df_train = df_ml.filter(col("random_split") < 0.70).drop("random_split")
df_validation = df_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df_test = df_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save structural variant splits
print("\nSAVING STRUCTURAL VARIANT SPLITS")
print("=" * 80)

df_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variant_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_structural_variant_train")

df_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variant_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_structural_variant_validation")

df_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variant_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_structural_variant_test")

# COMMAND ----------

# DBTITLE 1,Verify structural variant splits
print("\nVERIFYING STRUCTURAL VARIANT SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variant_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variant_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variant_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain sv_impact_tier distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_structural_variant_train") \
    .groupBy("sv_impact_tier").count().orderBy("sv_impact_tier").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - STRUCTURAL VARIANT ML DATASETS")
print("=" * 80)
print(f"structural_variant: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - STRUCTURAL VARIANT COMPLETE")
