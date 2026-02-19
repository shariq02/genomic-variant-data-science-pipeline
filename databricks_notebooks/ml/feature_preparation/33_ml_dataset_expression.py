# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - GENE EXPRESSION PREDICTION
# MAGIC ##### Module: Prepare Tissue Specificity ML Dataset
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 6: Transcript Isoform Impact
# MAGIC - Use Case 16: Gene Expression Analysis
# MAGIC 
# MAGIC **Input:** gold.transcript_expression_ml_features (44,874 genes)
# MAGIC 
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_expression_train
# MAGIC - gold.ml_dataset_expression_validation
# MAGIC - gold.ml_dataset_expression_test
# MAGIC 
# MAGIC **Target:** Tissue specificity prediction

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, count

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - GENE EXPRESSION PREDICTION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Transcript Expression Features
print("\nLOADING TRANSCRIPT EXPRESSION FEATURES")
print("="*80)

df = spark.table(f"{catalog_name}.gold.transcript_expression_ml_features")

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
    "gene_length",
    
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_transcription_factor",
    
    "max_expression_tpm",
    "avg_expression_tpm",
    "peak_expression_tpm",
    "total_tissues_expressed",
    "tissue_type_count",
    "primary_tissue_count",
    
    "is_ubiquitously_expressed",
    "is_tissue_specific",
    "is_highly_expressed",
    "is_lowly_expressed",
    
    "expression_breadth_category",
    "expression_level_category",
    
    "tissue_specificity_score",
    "expression_significance_score",
    "clinical_relevance_score",
    
    "expression_priority",
    "is_clinically_relevant_expression"
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
    .withColumn("target_tissue_specific",
                when(col("is_tissue_specific"), 1).otherwise(0))
    
    .withColumn("target_ubiquitously_expressed",
                when(col("is_ubiquitously_expressed"), 1).otherwise(0))
    
    .withColumn("target_highly_expressed",
                when(col("is_highly_expressed"), 1).otherwise(0))
    
    .withColumn("target_clinically_relevant",
                when(col("is_clinically_relevant_expression"), 1).otherwise(0))
)

print("Target variables added")
print("\nExpression breadth distribution:")
df_ml.groupBy("expression_breadth_category").count().orderBy("expression_breadth_category").show()

print("\nExpression level distribution:")
df_ml.groupBy("expression_level_category").count().orderBy("expression_level_category").show()

# COMMAND ----------

# DBTITLE 1,Handle Missing Values
print("\nHANDLING MISSING VALUES")
print("="*80)

fill_dict = {
    "gene_length": 0,
    "max_expression_tpm": 0.0,
    "avg_expression_tpm": 0.0,
    "peak_expression_tpm": 0.0,
    "total_tissues_expressed": 0,
    "tissue_type_count": 0,
    "primary_tissue_count": 0,
    "tissue_specificity_score": 0.0,
    "expression_significance_score": 0,
    "clinical_relevance_score": 0,
    "expression_breadth_category": "unknown",
    "expression_level_category": "very_low",
    "expression_priority": "low"
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
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_expression_train")

print(f"Saved: {catalog_name}.gold.ml_dataset_expression_train")

# COMMAND ----------

# DBTITLE 1,Save Validation Set
print("\nSAVING VALIDATION SET")
print("="*80)

df_validation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_expression_validation")

print(f"Saved: {catalog_name}.gold.ml_dataset_expression_validation")

# COMMAND ----------

# DBTITLE 1,Save Test Set
print("\nSAVING TEST SET")
print("="*80)

df_test.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_expression_test")

print(f"Saved: {catalog_name}.gold.ml_dataset_expression_test")

# COMMAND ----------

# DBTITLE 1,Verify Datasets
print("\nVERIFYING DATASETS")
print("="*80)

train_count = spark.table(f"{catalog_name}.gold.ml_dataset_expression_train").count()
val_count = spark.table(f"{catalog_name}.gold.ml_dataset_expression_validation").count()
test_count = spark.table(f"{catalog_name}.gold.ml_dataset_expression_test").count()
total = train_count + val_count + test_count

print(f"Train: {train_count:,} ({train_count/total*100:.1f}%)")
print(f"Validation: {val_count:,} ({val_count/total*100:.1f}%)")
print(f"Test: {test_count:,} ({test_count/total*100:.1f}%)")
print(f"Total: {total:,}")

print("\nTrain set - Expression breadth distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_expression_train") \
    .groupBy("expression_breadth_category") \
    .count() \
    .orderBy("expression_breadth_category") \
    .show()

print("\nTrain set - Expression level distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_expression_train") \
    .groupBy("expression_level_category") \
    .count() \
    .orderBy("expression_level_category") \
    .show()

print("\nML DATASET - GENE EXPRESSION COMPLETE")
