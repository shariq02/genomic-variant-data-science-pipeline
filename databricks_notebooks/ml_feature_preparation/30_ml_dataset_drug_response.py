# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - DRUG RESPONSE PREDICTION
# MAGIC ##### Module: Prepare Drug Response ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 11: Treatment Response Prediction
# MAGIC
# MAGIC **Input:**
# MAGIC - gold.drug_response_ml_features (variant-level, PharmGKB variants)
# MAGIC - gold.variant_drug_response_ml_features (variant-level, enhanced drug response)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_drug_response_train
# MAGIC - gold.ml_dataset_drug_response_validation
# MAGIC - gold.ml_dataset_drug_response_test
# MAGIC - gold.ml_dataset_variant_drug_response_train
# MAGIC - gold.ml_dataset_variant_drug_response_validation
# MAGIC - gold.ml_dataset_variant_drug_response_test
# MAGIC
# MAGIC **Target:** is_actionable_pharmacogene_variant

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - DRUG RESPONSE PREDICTION")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 1: drug_response_ml_features (PharmGKB variant-level)

# COMMAND ----------

# DBTITLE 1,Load drug_response_ml_features
print("\nLOADING drug_response_ml_features")
print("=" * 80)

df1 = spark.table(f"{catalog_name}.gold.drug_response_ml_features")
print(f"Total records: {df1.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY variant_id")
print("=" * 80)

before = df1.count()
df1 = df1.dropDuplicates(["variant_id"])
after = df1.count()
print(f"Before: {before:,}")
print(f"After: {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from drug_response_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df1_ml = df1.select(
    "variant_pharmgkb_id",
    "variant_name",
    "variant_id",
    "gene_symbol",
    "variant_location",
    "clinical_significance_simple",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "has_functional_domain",
    "affects_functional_domain",
    "phylop_score",
    "cadd_phred",
    "conservation_level",
    "has_pharmgkb_annotation",
    "has_high_conservation",
    "affects_drug_metabolism",
    "affects_drug_efficacy",
    "is_high_impact_variant",
    "pharmacogene_annotation_score",
    "functional_impact_score",
    "pathogenicity_score",
    "drug_response_priority_score",
    "drug_response_priority",
    "is_actionable_pharmacogene_variant",
    "drug_response_category",
    "clinical_actionability"
)

print(f"Features selected: {len(df1_ml.columns)}")
print(f"Records: {df1_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - drug_response_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_actionable_pharmacogene_variant:")
df1_ml.groupBy("is_actionable_pharmacogene_variant").count().orderBy("is_actionable_pharmacogene_variant").show()

print("drug_response_priority:")
df1_ml.groupBy("drug_response_priority").count().orderBy("drug_response_priority").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values - drug_response_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df1_ml = df1_ml.fillna({
    "phylop_score": 0.0,
    "cadd_phred": 0.0,
    "conservation_level": 0,
    "pharmacogene_annotation_score": 0,
    "functional_impact_score": 0,
    "pathogenicity_score": 0,
    "drug_response_priority_score": 0.0,
    "clinical_significance_simple": "Unknown",
    "drug_response_priority": "low",
    "drug_response_category": "unknown",
    "clinical_actionability": "research_only",
    "variant_location": "Unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - drug_response_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df1_ml = df1_ml.withColumn("random_split", rand(seed=42))

df1_train = df1_ml.filter(col("random_split") < 0.70).drop("random_split")
df1_validation = df1_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df1_test = df1_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save drug_response splits
print("\nSAVING DRUG RESPONSE SPLITS")
print("=" * 80)

df1_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_drug_response_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_drug_response_train")

df1_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_drug_response_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_drug_response_validation")

df1_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_drug_response_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_drug_response_test")

# COMMAND ----------

# DBTITLE 1,Verify drug_response splits
print("\nVERIFYING DRUG RESPONSE SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_train") \
    .groupBy("is_actionable_pharmacogene_variant").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 2: variant_drug_response_ml_features (Enhanced drug response)

# COMMAND ----------

# DBTITLE 1,Load variant_drug_response_ml_features
print("\nLOADING variant_drug_response_ml_features")
print("=" * 80)

df2 = spark.table(f"{catalog_name}.gold.variant_drug_response_ml_features")
print(f"Total records: {df2.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY variant_id")
print("=" * 80)

before = df2.count()
df2 = df2.dropDuplicates(["variant_id"])
after = df2.count()
print(f"Before: {before:,}")
print(f"After: {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from variant_drug_response_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df2_ml = df2.select(
    "variant_pharmgkb_id",
    "variant_name",
    "variant_id",
    "gene_symbol",
    "variant_location",
    "clinical_significance_simple",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "has_functional_domain",
    "affects_functional_domain",
    "phylop_score",
    "cadd_phred",
    "conservation_level",
    "pathogenicity_score",
    "mutation_severity_score",
    "has_pharmgkb_annotation",
    "has_high_conservation",
    "affects_drug_metabolism",
    "affects_drug_efficacy",
    "is_high_impact_variant",
    "is_hepatic_drug_metabolism_variant",
    "is_common_pharmacogene_variant",
    "is_potential_resistance_variant",
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_liver_expressed",
    "expression_breadth",
    "allele_frequency",
    "is_common_variant",
    "is_rare_variant",
    "drug_response_frequency_context",
    "total_disease_count",
    "has_cancer_disease",
    "has_cardiovascular_disease",
    "has_neurological_disease",
    "primary_indication_category",
    "cancer_mutation_count",
    "is_cancer_gene",
    "is_pharmacogene",
    "druggability_score",
    "pharmacogene_category",
    "drug_metabolism_role",
    "pharmacogene_annotation_score",
    "functional_impact_score",
    "population_adjusted_score",
    "tissue_specific_response_score",
    "drug_response_priority_score",
    "drug_response_priority",
    "is_actionable_pharmacogene_variant",
    "drug_response_category",
    "clinical_actionability",
    "indication_specific_actionability"
)

print(f"Features selected: {len(df2_ml.columns)}")
print(f"Records: {df2_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - variant_drug_response_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_actionable_pharmacogene_variant:")
df2_ml.groupBy("is_actionable_pharmacogene_variant").count().orderBy("is_actionable_pharmacogene_variant").show()

print("drug_response_category:")
df2_ml.groupBy("drug_response_category").count().orderBy("drug_response_category").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values - variant_drug_response_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df2_ml = df2_ml.fillna({
    "phylop_score": 0.0,
    "cadd_phred": 0.0,
    "conservation_level": 0,
    "pathogenicity_score": 0,
    "mutation_severity_score": 0,
    "pharmacogene_annotation_score": 0,
    "functional_impact_score": 0,
    "population_adjusted_score": 0,
    "tissue_specific_response_score": 0,
    "drug_response_priority_score": 0.0,
    "allele_frequency": 0.0,
    "tissues_expressed_count": 0,
    "max_expression_tpm": 0.0,
    "cancer_mutation_count": 0,
    "total_disease_count": 0,
    "druggability_score": 0.0,
    "clinical_significance_simple": "Unknown",
    "drug_response_priority": "low",
    "drug_response_category": "unknown",
    "clinical_actionability": "research_only",
    "expression_breadth": "unknown",
    "drug_response_frequency_context": "unknown",
    "primary_indication_category": "Unknown",
    "pharmacogene_category": "unknown",
    "drug_metabolism_role": "unknown",
    "variant_location": "Unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - variant_drug_response_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df2_ml = df2_ml.withColumn("random_split", rand(seed=42))

df2_train = df2_ml.filter(col("random_split") < 0.70).drop("random_split")
df2_validation = df2_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df2_test = df2_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save variant_drug_response splits
print("\nSAVING VARIANT DRUG RESPONSE SPLITS")
print("=" * 80)

df2_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_drug_response_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_drug_response_train")

df2_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_drug_response_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_drug_response_validation")

df2_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_drug_response_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_drug_response_test")

# COMMAND ----------

# DBTITLE 1,Verify variant_drug_response splits
print("\nVERIFYING VARIANT DRUG RESPONSE SPLITS")
print("=" * 80)

t2 = spark.table(f"{catalog_name}.gold.ml_dataset_variant_drug_response_train").count()
v2 = spark.table(f"{catalog_name}.gold.ml_dataset_variant_drug_response_validation").count()
te2 = spark.table(f"{catalog_name}.gold.ml_dataset_variant_drug_response_test").count()
total2 = t2 + v2 + te2

print(f"Train:      {t2:,} ({t2/total2*100:.1f}%)")
print(f"Validation: {v2:,} ({v2/total2*100:.1f}%)")
print(f"Test:       {te2:,} ({te2/total2*100:.1f}%)")
print(f"Total:      {total2:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_variant_drug_response_train") \
    .groupBy("is_actionable_pharmacogene_variant").count().show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - DRUG RESPONSE ML DATASETS")
print("=" * 80)
print(f"drug_response:         Train={t:,} | Validation={v:,} | Test={te:,}")
print(f"variant_drug_response: Train={t2:,} | Validation={v2:,} | Test={te2:,}")
print("\nML DATASET - DRUG RESPONSE COMPLETE")
