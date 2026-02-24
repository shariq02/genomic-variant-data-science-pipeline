# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - CANCER VARIANT CLASSIFICATION
# MAGIC ##### Module: Prepare Cancer Driver/Passenger ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 12: Cancer Variant Classification
# MAGIC
# MAGIC **Input:** gold.variant_cancer_ml_features (3.1M variants)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_cancer_variant_train
# MAGIC - gold.ml_dataset_cancer_variant_validation
# MAGIC - gold.ml_dataset_cancer_variant_test
# MAGIC
# MAGIC **Target:** is_driver_candidate

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - CANCER VARIANT CLASSIFICATION")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load variant_cancer_ml_features
print("\nLOADING variant_cancer_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.variant_cancer_ml_features")
print(f"Total records: {df.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_key
print("\nDEDUPLICATING BY variant_key")
print("=" * 80)

before = df.count()
df = df.dropDuplicates(["variant_key"])
after = df.count()
print(f"Before: {before:,}")
print(f"After:  {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from variant_cancer_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "gene_symbol",
    "gene_name",
    "variant_key",
    "chromosome",
    "position",
    "reference_allele",
    "alternate_allele",
    "sample_count",
    "total_mutation_count",
    "missense_sample_count",
    "truncating_sample_count",
    "silent_sample_count",
    "snv_sample_count",
    "indel_sample_count",
    "is_recurrent_mutation",
    "is_hotspot_mutation",
    "is_high_impact_cancer_variant",
    "is_driver_candidate",
    "mutation_frequency_category",
    "gene_total_samples",
    "gene_unique_sites",
    "is_cancer_gene",
    "is_tumor_suppressor_candidate",
    "is_oncogene_candidate",
    "gene_cancer_role",
    "cancer_mutation_burden_score",
    "cancer_priority_score",
    "clinvar_pathogenicity",
    "clinvar_is_pathogenic",
    "conservation_score",
    "cadd_phred",
    "functional_impact_prediction",
    "tissue_expression_in_tumors",
    "max_tumor_expression",
    "expression_change_relevance",
    "cancer_disease_associations",
    "hereditary_cancer_syndrome",
    "has_kinase_domain_count",
    "affected_oncogenic_domains",
    "kinase_domain_mutations",
    "germline_variant_frequency",
    "is_rare",
    "somatic_vs_germline_classification",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_pharmacogene",
    "driver_likelihood_score",
    "therapeutic_target_score",
    "prognostic_value_score"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_driver_candidate:")
df_ml.groupBy("is_driver_candidate").count().orderBy("is_driver_candidate").show()

print("gene_cancer_role:")
df_ml.groupBy("gene_cancer_role").count().orderBy("gene_cancer_role").show()

print("mutation_frequency_category:")
df_ml.groupBy("mutation_frequency_category").count().orderBy("mutation_frequency_category").show()

print("somatic_vs_germline_classification:")
df_ml.groupBy("somatic_vs_germline_classification").count().orderBy("somatic_vs_germline_classification").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "sample_count": 0,
    "total_mutation_count": 0,
    "missense_sample_count": 0,
    "truncating_sample_count": 0,
    "silent_sample_count": 0,
    "snv_sample_count": 0,
    "indel_sample_count": 0,
    "gene_total_samples": 0,
    "gene_unique_sites": 0,
    "cancer_mutation_burden_score": 0,
    "cancer_priority_score": 0.0,
    "conservation_score": 0.0,
    "cadd_phred": 0.0,
    "functional_impact_prediction": 0,
    "tissue_expression_in_tumors": 0,
    "max_tumor_expression": 0.0,
    "has_kinase_domain_count": 0,
    "germline_variant_frequency": 0.0,
    "driver_likelihood_score": 0,
    "therapeutic_target_score": 0,
    "prognostic_value_score": 0.0,
    "mutation_frequency_category": "rare",
    "gene_cancer_role": "other",
    "clinvar_pathogenicity": "Unknown",
    "expression_change_relevance": "unknown",
    "somatic_vs_germline_classification": "unknown"
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

# DBTITLE 1,Save cancer variant splits
print("\nSAVING CANCER VARIANT SPLITS")
print("=" * 80)

df_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_cancer_variant_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_cancer_variant_train")

df_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_cancer_variant_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_cancer_variant_validation")

df_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_cancer_variant_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_cancer_variant_test")

# COMMAND ----------

# DBTITLE 1,Verify cancer variant splits
print("\nVERIFYING CANCER VARIANT SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_cancer_variant_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_cancer_variant_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_cancer_variant_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_cancer_variant_train") \
    .groupBy("is_driver_candidate").count().show()

print("\nTrain gene cancer role distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_cancer_variant_train") \
    .groupBy("gene_cancer_role").count().orderBy("gene_cancer_role").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - CANCER VARIANT ML DATASETS")
print("=" * 80)
print(f"cancer_variant: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - CANCER VARIANT COMPLETE")
