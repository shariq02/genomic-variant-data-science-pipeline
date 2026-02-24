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
# MAGIC **Target:** is_high_risk_sv

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
    "chromosome",
    "start_pos",
    "end_pos",
    "variant_type",
    "sv_type_class",
    "sv_size",
    "sv_size_category",
    "has_gene_overlap",
    "affected_gene_count",
    "complete_overlap_genes",
    "major_overlap_genes",
    "is_multi_gene_sv",
    "affects_pharmacogenes",
    "affects_omim_genes",
    "pharmacogenes_affected",
    "gene_impact_severity",
    "size_impact_score",
    "type_impact_score",
    "gene_impact_score",
    "sv_pathogenicity_score",
    "predicted_sv_pathogenicity",
    "is_high_risk_sv"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_risk_sv:")
df_ml.groupBy("is_high_risk_sv").count().orderBy("is_high_risk_sv").show()

print("sv_type_class:")
df_ml.groupBy("sv_type_class").count().orderBy("sv_type_class").show()

print("sv_size_category:")
df_ml.groupBy("sv_size_category").count().orderBy("sv_size_category").show()

print("predicted_sv_pathogenicity:")
df_ml.groupBy("predicted_sv_pathogenicity").count().orderBy("predicted_sv_pathogenicity").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "sv_size": 0,
    "affected_gene_count": 0,
    "complete_overlap_genes": 0,
    "major_overlap_genes": 0,
    "pharmacogenes_affected": 0,
    "size_impact_score": 0,
    "type_impact_score": 0,
    "gene_impact_score": 0,
    "sv_pathogenicity_score": 0,
    "sv_type_class": "unknown",
    "sv_size_category": "unknown",
    "gene_impact_severity": "unknown",
    "predicted_sv_pathogenicity": "unknown",
    "study_id": "unknown"
})

print("Missing values filled")

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

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_structural_variant_train") \
    .groupBy("is_high_risk_sv").count().show()

print("\nTrain sv_type_class distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_structural_variant_train") \
    .groupBy("sv_type_class").count().orderBy("sv_type_class").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - STRUCTURAL VARIANT ML DATASETS")
print("=" * 80)
print(f"structural_variant: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - STRUCTURAL VARIANT COMPLETE")
