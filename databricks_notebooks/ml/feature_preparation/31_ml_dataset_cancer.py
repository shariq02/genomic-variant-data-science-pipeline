# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - CANCER VARIANT CLASSIFICATION
# MAGIC ##### Module: Prepare Cancer Driver/Passenger ML Dataset
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 12: Cancer Variant Classification
# MAGIC 
# MAGIC **Input:** gold.cancer_variant_ml_features (3.1M variants)
# MAGIC 
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_cancer_train
# MAGIC - gold.ml_dataset_cancer_validation
# MAGIC - gold.ml_dataset_cancer_test
# MAGIC 
# MAGIC **Target:** Driver vs passenger mutation classification

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - CANCER VARIANT CLASSIFICATION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Cancer Variant Features
print("\nLOADING CANCER VARIANT FEATURES")
print("="*80)

df = spark.table(f"{catalog_name}.gold.cancer_variant_ml_features")

print(f"Total records: {df.count():,}")

print("\nSchema:")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant Key
print("\nDEDUPLICATING BY VARIANT_KEY")
print("="*80)

before_count = df.count()
df = df.dropDuplicates(["variant_key"])
after_count = df.count()

print(f"Before: {before_count:,}")
print(f"After: {after_count:,}")
print(f"Removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Select ML Features
print("\nSELECTING ML FEATURES")
print("="*80)

ml_features = [
    "variant_key",
    "gene_symbol",
    "chromosome",
    "position",
    
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
    "cancer_priority_score"
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
    .withColumn("target_is_driver",
                when(col("is_driver_candidate"), 1).otherwise(0))
    
    .withColumn("target_is_hotspot",
                when(col("is_hotspot_mutation"), 1).otherwise(0))
    
    .withColumn("target_is_tumor_suppressor",
                when(col("is_tumor_suppressor_candidate"), 1).otherwise(0))
    
    .withColumn("target_is_oncogene",
                when(col("is_oncogene_candidate"), 1).otherwise(0))
)

print("Target variables added")
print("\nDriver candidate distribution:")
df_ml.groupBy("is_driver_candidate").count().show()

print("\nGene cancer role distribution:")
df_ml.groupBy("gene_cancer_role").count().orderBy("gene_cancer_role").show()

# COMMAND ----------

# DBTITLE 1,Handle Missing Values
print("\nHANDLING MISSING VALUES")
print("="*80)

fill_dict = {
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
    "cancer_priority_score": 0,
    "mutation_frequency_category": "rare",
    "gene_cancer_role": "other"
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
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_cancer_train")

print(f"Saved: {catalog_name}.gold.ml_dataset_cancer_train")

# COMMAND ----------

# DBTITLE 1,Save Validation Set
print("\nSAVING VALIDATION SET")
print("="*80)

df_validation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_cancer_validation")

print(f"Saved: {catalog_name}.gold.ml_dataset_cancer_validation")

# COMMAND ----------

# DBTITLE 1,Save Test Set
print("\nSAVING TEST SET")
print("="*80)

df_test.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_cancer_test")

print(f"Saved: {catalog_name}.gold.ml_dataset_cancer_test")

# COMMAND ----------

# DBTITLE 1,Verify Datasets
print("\nVERIFYING DATASETS")
print("="*80)

train_count = spark.table(f"{catalog_name}.gold.ml_dataset_cancer_train").count()
val_count = spark.table(f"{catalog_name}.gold.ml_dataset_cancer_validation").count()
test_count = spark.table(f"{catalog_name}.gold.ml_dataset_cancer_test").count()
total = train_count + val_count + test_count

print(f"Train: {train_count:,} ({train_count/total*100:.1f}%)")
print(f"Validation: {val_count:,} ({val_count/total*100:.1f}%)")
print(f"Test: {test_count:,} ({test_count/total*100:.1f}%)")
print(f"Total: {total:,}")

print("\nTrain set - Driver candidate distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_cancer_train") \
    .groupBy("is_driver_candidate") \
    .count() \
    .show()

print("\nTrain set - Gene cancer role distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_cancer_train") \
    .groupBy("gene_cancer_role") \
    .count() \
    .orderBy("gene_cancer_role") \
    .show()

print("\nML DATASET - CANCER CLASSIFICATION COMPLETE")
