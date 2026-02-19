# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - CARRIER SCREENING PREDICTION
# MAGIC ##### Module: Prepare Population Frequency ML Dataset
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Population Carrier Screening
# MAGIC - Use Case 19: Ancestry-Specific Risk
# MAGIC 
# MAGIC **Input:** gold.population_frequency_ml_features (46,339 variants)
# MAGIC 
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_carrier_screening_train
# MAGIC - gold.ml_dataset_carrier_screening_validation
# MAGIC - gold.ml_dataset_carrier_screening_test
# MAGIC 
# MAGIC **Target:** Carrier screening risk classification

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - CARRIER SCREENING PREDICTION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Population Frequency Features
print("\nLOADING POPULATION FREQUENCY FEATURES")
print("="*80)

df = spark.table(f"{catalog_name}.gold.population_frequency_ml_features")

print(f"Total records: {df.count():,}")

print("\nSchema:")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Select ML Features
print("\nSELECTING ML FEATURES")
print("="*80)

ml_features = [
    "variant_id",
    "gene_symbol",
    "chromosome",
    "position",
    
    "allele_frequency",
    "frequency_category",
    
    "is_ultra_rare_variant",
    "is_very_rare_variant",
    "is_rare_variant",
    "is_low_frequency_variant",
    "is_common_variant",
    
    "frequency_tier",
    
    "clinical_significance",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "is_germline",
    "is_somatic",
    
    "rarity_score",
    "carrier_risk_score",
    "pathogenicity_likelihood_score",
    
    "is_clinically_actionable_rare_variant",
    "is_carrier_screening_candidate",
    "population_priority",
    "screening_recommendation"
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
    .withColumn("target_high_risk_carrier",
                when((col("is_clinically_actionable_rare_variant")) & 
                     (col("is_germline")), 1).otherwise(0))
    
    .withColumn("target_screening_recommended",
                when(col("screening_recommendation") == "recommended", 1).otherwise(0))
    
    .withColumn("target_rare_pathogenic",
                when((col("is_rare_variant") | col("is_ultra_rare_variant")) & 
                     (col("is_pathogenic")), 1).otherwise(0))
)

print("Target variables added")
print("\nFrequency tier distribution:")
df_ml.groupBy("frequency_tier").count().orderBy("frequency_tier").show()

print("\nScreening recommendation distribution:")
df_ml.groupBy("screening_recommendation").count().orderBy("screening_recommendation").show()

# COMMAND ----------

# DBTITLE 1,Handle Missing Values
print("\nHANDLING MISSING VALUES")
print("="*80)

fill_dict = {
    "allele_frequency": 0.0,
    "rarity_score": 0,
    "carrier_risk_score": 0,
    "pathogenicity_likelihood_score": 0,
    "frequency_category": "unknown",
    "frequency_tier": "unknown",
    "clinical_significance": "Unknown",
    "population_priority": "low",
    "screening_recommendation": "not_indicated"
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
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_carrier_screening_train")

print(f"Saved: {catalog_name}.gold.ml_dataset_carrier_screening_train")

# COMMAND ----------

# DBTITLE 1,Save Validation Set
print("\nSAVING VALIDATION SET")
print("="*80)

df_validation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_carrier_screening_validation")

print(f"Saved: {catalog_name}.gold.ml_dataset_carrier_screening_validation")

# COMMAND ----------

# DBTITLE 1,Save Test Set
print("\nSAVING TEST SET")
print("="*80)

df_test.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_carrier_screening_test")

print(f"Saved: {catalog_name}.gold.ml_dataset_carrier_screening_test")

# COMMAND ----------

# DBTITLE 1,Verify Datasets
print("\nVERIFYING DATASETS")
print("="*80)

train_count = spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_train").count()
val_count = spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_validation").count()
test_count = spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_test").count()
total = train_count + val_count + test_count

print(f"Train: {train_count:,} ({train_count/total*100:.1f}%)")
print(f"Validation: {val_count:,} ({val_count/total*100:.1f}%)")
print(f"Test: {test_count:,} ({test_count/total*100:.1f}%)")
print(f"Total: {total:,}")

print("\nTrain set - Frequency tier distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_train") \
    .groupBy("frequency_tier") \
    .count() \
    .orderBy("frequency_tier") \
    .show()

print("\nTrain set - Screening recommendation distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_train") \
    .groupBy("screening_recommendation") \
    .count() \
    .orderBy("screening_recommendation") \
    .show()

print("\nTrain set - Target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_carrier_screening_train") \
    .groupBy("target_high_risk_carrier") \
    .count() \
    .show()

print("\nML DATASET - CARRIER SCREENING COMPLETE")
