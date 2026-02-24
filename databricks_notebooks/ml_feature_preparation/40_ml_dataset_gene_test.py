# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - GENETIC TEST AVAILABILITY
# MAGIC ##### Module: Prepare Gene Test Availability ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 5: Genetic Test Availability
# MAGIC
# MAGIC **Input:** gold.gene_test_availability_ml_features (gene-level, ~19K genes)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_gene_test_train
# MAGIC - gold.ml_dataset_gene_test_validation
# MAGIC - gold.ml_dataset_gene_test_test
# MAGIC
# MAGIC **Target:** is_high_priority_test_gene

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - GENETIC TEST AVAILABILITY")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load gene_test_availability_ml_features
print("\nLOADING gene_test_availability_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.gene_test_availability_ml_features")
print(f"Total records: {df.count():,}")

# COMMAND ----------

# DBTITLE 1,Select features from gene_test_availability_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "gene_symbol",
    "gene_name",
    "description",
    "chromosome",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_pharmacogene",
    "total_test_count",
    "unique_test_count",
    "disease_count",
    "genetic_test_count",
    "tests_with_gene_info",
    "tests_with_disease_info",
    "complete_test_count",
    "frequent_test_count",
    "has_clinical_test",
    "has_multiple_tests",
    "has_comprehensive_testing",
    "is_well_tested_gene",
    "test_availability_category",
    "test_accessibility_score",
    "clinical_utility_score",
    "test_quality_score",
    "total_disease_count",
    "has_cancer_disease",
    "has_cardiovascular_disease",
    "has_neurological_disease",
    "disease_test_correlation",
    "multi_disease_testing",
    "pathogenic_variants_in_tested_gene",
    "test_covered_variants",
    "variant_test_coverage_level",
    "cancer_mutation_count",
    "cancer_samples",
    "is_cancer_panel_gene",
    "hereditary_cancer_testing",
    "rare_pathogenic_variants",
    "carrier_screening_relevant",
    "population_test_priority",
    "clinical_test_utility_score",
    "variant_test_coverage_score",
    "population_test_relevance_score",
    "test_priority",
    "is_high_priority_test_gene",
    "primary_test_type",
    "test_recommendation_tier"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_priority_test_gene:")
df_ml.groupBy("is_high_priority_test_gene").count().orderBy("is_high_priority_test_gene").show()

print("test_priority:")
df_ml.groupBy("test_priority").count().orderBy("test_priority").show()

print("test_recommendation_tier:")
df_ml.groupBy("test_recommendation_tier").count().orderBy("test_recommendation_tier").show()

print("test_availability_category:")
df_ml.groupBy("test_availability_category").count().orderBy("test_availability_category").show()

print("primary_test_type:")
df_ml.groupBy("primary_test_type").count().orderBy("primary_test_type").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "total_test_count": 0,
    "unique_test_count": 0,
    "disease_count": 0,
    "genetic_test_count": 0,
    "tests_with_gene_info": 0,
    "tests_with_disease_info": 0,
    "complete_test_count": 0,
    "frequent_test_count": 0,
    "test_accessibility_score": 0,
    "clinical_utility_score": 0,
    "test_quality_score": 0,
    "total_disease_count": 0,
    "pathogenic_variants_in_tested_gene": 0,
    "test_covered_variants": 0,
    "cancer_mutation_count": 0,
    "cancer_samples": 0,
    "rare_pathogenic_variants": 0,
    "clinical_test_utility_score": 0,
    "variant_test_coverage_score": 0,
    "population_test_relevance_score": 0,
    "test_availability_category": "unknown",
    "disease_test_correlation": "unknown",
    "variant_test_coverage_level": "unknown",
    "population_test_priority": "unknown",
    "test_priority": "low",
    "primary_test_type": "unknown",
    "test_recommendation_tier": "unknown",
    "description": "unknown"
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

# DBTITLE 1,Save gene test splits
print("\nSAVING GENE TEST SPLITS")
print("=" * 80)

df_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_test_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_test_train")

df_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_test_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_test_validation")

df_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_test_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_test_test")

# COMMAND ----------

# DBTITLE 1,Verify gene test splits
print("\nVERIFYING GENE TEST SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_train") \
    .groupBy("is_high_priority_test_gene").count().show()

print("\nTrain test priority distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_train") \
    .groupBy("test_priority").count().orderBy("test_priority").show()

print("\nTrain test recommendation tier distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_train") \
    .groupBy("test_recommendation_tier").count().orderBy("test_recommendation_tier").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - GENE TEST ML DATASETS")
print("=" * 80)
print(f"gene_test: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - GENE TEST COMPLETE")
