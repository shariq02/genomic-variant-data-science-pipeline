# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - PHARMACOGENE PRIORITY PREDICTION
# MAGIC ##### Module: Prepare Pharmacogene Druggability ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 14: Drug Target Identification
# MAGIC
# MAGIC **Input:** gold.gene_pharmacogene_ml_features (2,209 genes)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_gene_pharmacogene_train
# MAGIC - gold.ml_dataset_gene_pharmacogene_validation
# MAGIC - gold.ml_dataset_gene_pharmacogene_test
# MAGIC
# MAGIC **Target:** Pharmacogene priority (high/medium/low)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - PHARMACOGENE PRIORITY PREDICTION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Pharmacogene Features
print("\nLOADING PHARMACOGENE FEATURES")
print("="*80)

df = spark.table(f"{catalog_name}.gold.gene_pharmacogene_ml_features")

print(f"Total records: {df.count():,}")

print("\nSchema:")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Select ML Features
print("\nSELECTING ML FEATURES")
print("="*80)

ml_features = [
    "gene_symbol",
    "gene_full_name",
    "chromosome",
    
    "has_pharmacogene_annotation",
    "is_drug_metabolizer",
    "is_drug_transporter_gene",
    "is_drug_target_gene",
    "has_high_druggability",
    
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_transporter",
    "is_metabolic",
    
    "druggability_score",
    
    "total_relationships",
    "entity_type_count",
    "drug_relationships",
    "disease_relationships",
    "variant_relationships",
    "evidence_count",
    
    "pharmacogene_evidence_score",
    "drug_interaction_score",
    "clinical_utility_score",
    
    "pharmacogene_priority",
    "is_high_priority_pharmacogene",
    "pharmacogene_category"
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
                when(col("pharmacogene_priority") == "high", 1).otherwise(0))
    
    .withColumn("target_medium_priority",
                when(col("pharmacogene_priority") == "medium", 1).otherwise(0))
    
    .withColumn("target_low_priority",
                when(col("pharmacogene_priority") == "low", 1).otherwise(0))
)

print("Target variables added")
print("\nPriority distribution:")
df_ml.groupBy("pharmacogene_priority").count().orderBy("pharmacogene_priority").show()

print("\nCategory distribution:")
df_ml.groupBy("pharmacogene_category").count().orderBy("pharmacogene_category").show()

# COMMAND ----------

# DBTITLE 1,Handle Missing Values
print("\nHANDLING MISSING VALUES")
print("="*80)

fill_dict = {
    "druggability_score": 0.0,
    "total_relationships": 0,
    "entity_type_count": 0,
    "drug_relationships": 0,
    "disease_relationships": 0,
    "variant_relationships": 0,
    "evidence_count": 0,
    "pharmacogene_evidence_score": 0,
    "drug_interaction_score": 0,
    "clinical_utility_score": 0,
    "pharmacogene_category": "other"
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
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train")

print(f"Saved: {catalog_name}.gold.ml_dataset_gene_pharmacogene_train")

# COMMAND ----------

# DBTITLE 1,Save Validation Set
print("\nSAVING VALIDATION SET")
print("="*80)

df_validation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_validation")

print(f"Saved: {catalog_name}.gold.ml_dataset_pharmacogene_validation")

# COMMAND ----------

# DBTITLE 1,Save Test Set
print("\nSAVING TEST SET")
print("="*80)

df_test.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_test")

print(f"Saved: {catalog_name}.gold.ml_dataset_pharmacogene_test")

# COMMAND ----------

# DBTITLE 1,Verify Datasets
print("\nVERIFYING DATASETS")
print("="*80)

train_count = spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train").count()
val_count = spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_validation").count()
test_count = spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_test").count()
total = train_count + val_count + test_count

print(f"Train: {train_count:,} ({train_count/total*100:.1f}%)")
print(f"Validation: {val_count:,} ({val_count/total*100:.1f}%)")
print(f"Test: {test_count:,} ({test_count/total*100:.1f}%)")
print(f"Total: {total:,}")

print("\nTrain set - Priority distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train") \
    .groupBy("pharmacogene_priority") \
    .count() \
    .orderBy("pharmacogene_priority") \
    .show()

print("\nTrain set - Category distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train") \
    .groupBy("pharmacogene_category") \
    .count() \
    .orderBy("pharmacogene_category") \
    .show()

print("\nML DATASET - PHARMACOGENE PRIORITY COMPLETE")
