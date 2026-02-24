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
    "gene_full_name",
    "chromosome",
    "total_gene_variants",
    "pathogenic_variants",
    "vus_variants",
    "benign_variants",
    "lof_variants",
    "has_pathogenic_variants",
    "pathogenic_variant_burden",
    "gene_is_omim_disease_gene",
    "gene_omim_disease_count",
    "gene_has_rare_disease",
    "gene_has_cancer_disease",
    "gene_has_cardiovascular_disease",
    "gene_has_neurological_disease",
    "gene_disease_diversity",
    "gene_clinical_actionability",
    "is_pharmacogene",
    "druggability_score",
    "is_high_priority_pharmacogene",
    "is_validated_cancer_target",
    "cancer_mutation_count",
    "is_cancer_gene",
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_broadly_expressed",
    "total_domains",
    "has_kinase_domain",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "avg_conservation_level",
    "is_highly_conserved_gene",
    "test_utility_score",
    "clinical_actionability_score",
    "disease_burden_score",
    "pharmacogenomic_relevance_score",
    "test_priority_tier",
    "is_high_priority_test_gene",
    "test_recommendation",
    "clinical_test_category"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_priority_test_gene:")
df_ml.groupBy("is_high_priority_test_gene").count().orderBy("is_high_priority_test_gene").show()

print("test_priority_tier:")
df_ml.groupBy("test_priority_tier").count().orderBy("test_priority_tier").show()

print("test_recommendation:")
df_ml.groupBy("test_recommendation").count().orderBy("test_recommendation").show()

print("clinical_test_category:")
df_ml.groupBy("clinical_test_category").count().orderBy("clinical_test_category").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "total_gene_variants": 0,
    "pathogenic_variants": 0,
    "vus_variants": 0,
    "benign_variants": 0,
    "lof_variants": 0,
    "gene_omim_disease_count": 0,
    "gene_disease_diversity": 0,
    "druggability_score": 0.0,
    "cancer_mutation_count": 0,
    "tissues_expressed_count": 0,
    "max_expression_tpm": 0.0,
    "total_domains": 0,
    "avg_conservation_level": 0.0,
    "test_utility_score": 0,
    "clinical_actionability_score": 0,
    "disease_burden_score": 0,
    "pharmacogenomic_relevance_score": 0,
    "pathogenic_variant_burden": "none",
    "gene_clinical_actionability": "unknown",
    "test_priority_tier": "low",
    "test_recommendation": "not_recommended",
    "clinical_test_category": "unknown"
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

print("\nTrain test priority tier distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_train") \
    .groupBy("test_priority_tier").count().orderBy("test_priority_tier").show()

print("\nTrain test recommendation distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_test_train") \
    .groupBy("test_recommendation").count().orderBy("test_recommendation").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - GENE TEST ML DATASETS")
print("=" * 80)
print(f"gene_test: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - GENE TEST COMPLETE")
