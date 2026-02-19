# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - DRUG RESPONSE PREDICTION
# MAGIC ##### Module: Prepare Drug Response ML Dataset
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 11: Treatment Response Prediction
# MAGIC 
# MAGIC **Input:** gold.drug_response_ml_features (4.2M variants)
# MAGIC 
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_drug_response_train
# MAGIC - gold.ml_dataset_drug_response_validation
# MAGIC - gold.ml_dataset_drug_response_test
# MAGIC 
# MAGIC **Target:** Drug response priority (high/medium/low)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - DRUG RESPONSE PREDICTION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Drug Response Features
print("\nLOADING DRUG RESPONSE FEATURES")
print("="*80)

df = spark.table(f"{catalog_name}.gold.drug_response_ml_features")

print(f"Total records: {df.count():,}")

print("\nSchema:")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant ID
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = df.count()
df = df.dropDuplicates(["variant_id"])
after_count = df.count()

print(f"Before: {before_count:,}")
print(f"After: {after_count:,}")
print(f"Removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Select ML Features
print("\nSELECTING ML FEATURES")
print("="*80)

ml_features = [
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
]

df_ml = df.select(*ml_features)

print(f"Features selected: {len(ml_features)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Target Variable
print("\nADDING TARGET VARIABLE")
print("="*80)

df_ml = (
    df_ml
    .withColumn("target_high_priority",
                when(col("drug_response_priority") == "high", 1).otherwise(0))
    
    .withColumn("target_medium_priority",
                when(col("drug_response_priority") == "medium", 1).otherwise(0))
    
    .withColumn("target_low_priority",
                when(col("drug_response_priority") == "low", 1).otherwise(0))
)

print("Target variables added")
print("\nTarget distribution:")
df_ml.groupBy("drug_response_priority").count().orderBy("drug_response_priority").show()

# COMMAND ----------

# DBTITLE 1,Handle Missing Values
print("\nHANDLING MISSING VALUES")
print("="*80)

fill_dict = {
    "phylop_score": 0.0,
    "cadd_phred": 0.0,
    "conservation_level": 0,
    "pharmacogene_annotation_score": 0,
    "functional_impact_score": 0,
    "pathogenicity_score": 0,
    "drug_response_priority_score": 0.0,
    "clinical_significance_simple": "Unknown",
    "drug_response_category": "unknown",
    "clinical_actionability": "research_only"
}

df_ml = df_ml.fillna(fill_dict)

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create Train Validation Test Splits
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("="*80)

df_ml = df_ml.withColumn("random_split", rand(seed=42))

df_train = df_ml.filter(col("random_split") < 0.70).drop("random_split")
df_validation = df_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df_test = df_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save Train Set
print("\nSAVING TRAIN SET")
print("="*80)

df_train.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_drug_response_train")

print(f"Saved: {catalog_name}.gold.ml_dataset_drug_response_train")

# COMMAND ----------

# DBTITLE 1,Save Validation Set
print("\nSAVING VALIDATION SET")
print("="*80)

df_validation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_drug_response_validation")

print(f"Saved: {catalog_name}.gold.ml_dataset_drug_response_validation")

# COMMAND ----------

# DBTITLE 1,Save Test Set
print("\nSAVING TEST SET")
print("="*80)

df_test.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_drug_response_test")

print(f"Saved: {catalog_name}.gold.ml_dataset_drug_response_test")

# COMMAND ----------

# DBTITLE 1,Verify Datasets
print("\nVERIFYING DATASETS")
print("="*80)

train_count = spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_train").count()
val_count = spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_validation").count()
test_count = spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_test").count()
total = train_count + val_count + test_count

print(f"Train: {train_count:,} ({train_count/total*100:.1f}%)")
print(f"Validation: {val_count:,} ({val_count/total*100:.1f}%)")
print(f"Test: {test_count:,} ({test_count/total*100:.1f}%)")
print(f"Total: {total:,}")

print("\nTrain set distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_drug_response_train") \
    .groupBy("drug_response_priority") \
    .count() \
    .orderBy("drug_response_priority") \
    .show()

print("\nML DATASET - DRUG RESPONSE COMPLETE")
