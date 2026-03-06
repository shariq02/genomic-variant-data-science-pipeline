# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - POPULATION CARRIER SCREENING
# MAGIC ##### Module: Prepare Population Frequency ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Population Carrier Screening
# MAGIC - Use Case 19: Ancestry-Specific Risk
# MAGIC
# MAGIC **Input:**
# MAGIC - gold.population_frequency_ml_features (46K variants, base)
# MAGIC - gold.variant_population_ml_features (46K variants, enhanced)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_carrier_screening_train
# MAGIC - gold.ml_dataset_carrier_screening_validation
# MAGIC - gold.ml_dataset_carrier_screening_test
# MAGIC - gold.ml_dataset_variant_population_train
# MAGIC - gold.ml_dataset_variant_population_validation
# MAGIC - gold.ml_dataset_variant_population_test
# MAGIC
# MAGIC **Target:** is_clinically_actionable_rare_variant

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - POPULATION CARRIER SCREENING")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 1: population_frequency_ml_features (Base)

# COMMAND ----------

# DBTITLE 1,Load population_frequency_ml_features
print("\nLOADING population_frequency_ml_features")
print("=" * 80)

df1 = spark.table(f"{catalog_name}.gold.population_frequency_ml_features")
print(f"Total records: {df1.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY variant_id")
print("=" * 80)

before = df1.count()
df1 = df1.dropDuplicates(["variant_id"])
after = df1.count()
print(f"Before: {before:,}")
print(f"After:  {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from population_frequency_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df1_ml = df1.select(
    "variant_id",
    "gene_symbol",
    "chromosome",
    "position",
    "reference_allele",
    "alternate_allele",
    "allele_frequency",
    "frequency_category",
    "is_ultra_rare_variant",
    "is_very_rare_variant",
    "is_rare_variant",
    "is_low_frequency_variant",
    "is_common_variant",
    "frequency_tier",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "is_germline",
    "is_somatic",
    "rarity_score",
    "carrier_risk_score",
    "pathogenicity_likelihood_score",
    "is_carrier_screening_candidate",
)

print(f"Features selected: {len(df1_ml.columns)}")
print(f"Records: {df1_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - population_frequency_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)


print("frequency_tier:")
df1_ml.groupBy("frequency_tier").count().orderBy("frequency_tier").show()


# COMMAND ----------

# DBTITLE 1,Handle missing values - population_frequency_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df1_ml = df1_ml.fillna({
    "allele_frequency": 0.0,
    "rarity_score": 0,
    "carrier_risk_score": 0,
    "pathogenicity_likelihood_score": 0,
    "frequency_category": "unknown",
    "frequency_tier": "unknown",
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - population_frequency_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df1_ml = df1_ml.withColumn("random_split", rand(seed=42))

df1_train = df1_ml.filter(col("random_split") < 0.70).drop("random_split")
df1_validation = df1_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df1_test = df1_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save carrier_screening splits
print("\nSAVING CARRIER SCREENING SPLITS")
print("=" * 80)

df1_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_carrier_screening_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_carrier_screening_train")

df1_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_carrier_screening_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_carrier_screening_validation")

df1_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_carrier_screening_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_carrier_screening_test")

# COMMAND ----------

# DBTITLE 1,Verify carrier_screening splits
print("\nVERIFYING CARRIER SCREENING SPLITS")
print("=" * 80)

t1 = spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_train").count()
v1 = spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_validation").count()
te1 = spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_test").count()
total1 = t1 + v1 + te1

print(f"Train:      {t1:,} ({t1/total1*100:.1f}%)")
print(f"Validation: {v1:,} ({v1/total1*100:.1f}%)")
print(f"Test:       {te1:,} ({te1/total1*100:.1f}%)")
print(f"Total:      {total1:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_train") \
    .groupBy("is_clinically_actionable_rare_variant").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 2: variant_population_ml_features (Enhanced)

# COMMAND ----------

# DBTITLE 1,Load variant_population_ml_features
print("\nLOADING variant_population_ml_features")
print("=" * 80)

df2 = spark.table(f"{catalog_name}.gold.variant_population_ml_features")
print(f"Total records: {df2.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY variant_id")
print("=" * 80)

before = df2.count()
df2 = df2.dropDuplicates(["variant_id"])
after = df2.count()
print(f"Before: {before:,}")
print(f"After:  {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from variant_population_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df2_ml = df2.select(
    "variant_id",
    "variant_key",
    "gene_symbol",
    "chromosome",
    "position",
    "reference_allele",
    "alternate_allele",
    "allele_frequency",
    "frequency_category",
    "is_ultra_rare_variant",
    "is_very_rare_variant",
    "is_rare_variant",
    "is_low_frequency_variant",
    "is_common_variant",
    "frequency_tier",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "is_germline",
    "is_somatic",
    "rarity_score",
    "carrier_risk_score",
    "pathogenicity_likelihood_score",
    "pathogenicity_score",
    "conservation_level",
    "pathogenicity_frequency_conflict",
    "rare_pathogenic_variant",
    "common_benign_validation",
    "total_gene_variants",
    "lof_variants",
    "gene_mutation_tolerance",
    "gene_constraint_score",
    "total_disease_count",
    "disease_allele_frequency",
    "somatic_frequency",
    "germline_cancer_predisposition",
    "expression_tissues",
    "expression_frequency_correlation",
    "tissue_specific_allele_effects",
    "is_clinically_actionable_rare_variant",
    "is_carrier_screening_candidate",
    "clinical_significance_frequency_score",
    "carrier_risk_score_adjusted",
    "pathogenicity_likelihood_refined"
)

print(f"Features selected: {len(df2_ml.columns)}")
print(f"Records: {df2_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - variant_population_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("frequency_tier:")
df2_ml.groupBy("frequency_tier").count().orderBy("frequency_tier").show()


# COMMAND ----------

# DBTITLE 1,Handle missing values - variant_population_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df2_ml = df2_ml.fillna({
    "allele_frequency": 0.0,
    "rarity_score": 0,
    "carrier_risk_score": 0,
    "pathogenicity_likelihood_score": 0,
    "pathogenicity_score": 0,
    "conservation_level": 0,
    "total_gene_variants": 0,
    "lof_variants": 0,
    "gene_constraint_score": 0.0,
    "total_disease_count": 0,
    "somatic_frequency": 0,
    "expression_tissues": 0,
    "clinical_significance_frequency_score": 0.0,
    "carrier_risk_score_adjusted": 0.0,
    "pathogenicity_likelihood_refined": 0,
    "frequency_category": "unknown",
    "frequency_tier": "unknown",
    "gene_mutation_tolerance": "unknown",
    "disease_allele_frequency": "unknown",
    "expression_frequency_correlation": "unknown",
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - variant_population_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df2_ml = df2_ml.withColumn("random_split", rand(seed=42))

df2_train = df2_ml.filter(col("random_split") < 0.70).drop("random_split")
df2_validation = df2_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df2_test = df2_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save variant_population splits
print("\nSAVING VARIANT POPULATION SPLITS")
print("=" * 80)

df2_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_population_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_population_train")

df2_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_population_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_population_validation")

df2_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_population_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_population_test")

# COMMAND ----------

# DBTITLE 1,Verify variant_population splits
print("\nVERIFYING VARIANT POPULATION SPLITS")
print("=" * 80)

t2 = spark.table(f"{catalog_name}.gold.ml_dataset_variant_population_train").count()
v2 = spark.table(f"{catalog_name}.gold.ml_dataset_variant_population_validation").count()
te2 = spark.table(f"{catalog_name}.gold.ml_dataset_variant_population_test").count()
total2 = t2 + v2 + te2

print(f"Train:      {t2:,} ({t2/total2*100:.1f}%)")
print(f"Validation: {v2:,} ({v2/total2*100:.1f}%)")
print(f"Test:       {te2:,} ({te2/total2*100:.1f}%)")
print(f"Total:      {total2:,}")


# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - POPULATION ML DATASETS")
print("=" * 80)
print(f"carrier_screening:   Train={t1:,} | Validation={v1:,} | Test={te1:,}")
print(f"variant_population:  Train={t2:,} | Validation={v2:,} | Test={te2:,}")
print("\nML DATASET - POPULATION CARRIER SCREENING COMPLETE")
