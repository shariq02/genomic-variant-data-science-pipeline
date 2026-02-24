# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - GENE EXPRESSION PREDICTION
# MAGIC ##### Module: Prepare Expression ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 6: Transcript Isoform Impact
# MAGIC - Use Case 16: Gene Expression Analysis
# MAGIC
# MAGIC **Input:**
# MAGIC - gold.transcript_expression_ml_features (gene-level, 44K genes)
# MAGIC - gold.gene_expression_ml_features (gene-level, 44K genes, enhanced)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_expression_train
# MAGIC - gold.ml_dataset_expression_validation
# MAGIC - gold.ml_dataset_expression_test
# MAGIC - gold.ml_dataset_gene_expression_train
# MAGIC - gold.ml_dataset_gene_expression_validation
# MAGIC - gold.ml_dataset_gene_expression_test
# MAGIC
# MAGIC **Target:** is_clinically_relevant_expression

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - GENE EXPRESSION PREDICTION")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 1: transcript_expression_ml_features

# COMMAND ----------

# DBTITLE 1,Load transcript_expression_ml_features
print("\nLOADING transcript_expression_ml_features")
print("=" * 80)

df1 = spark.table(f"{catalog_name}.gold.transcript_expression_ml_features")
print(f"Total records: {df1.count():,}")

# COMMAND ----------

# DBTITLE 1,Select features from transcript_expression_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df1_ml = df1.select(
    "gene_symbol",
    "gene_full_name",
    "description",
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
)

print(f"Features selected: {len(df1_ml.columns)}")
print(f"Records: {df1_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - transcript_expression_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_clinically_relevant_expression:")
df1_ml.groupBy("is_clinically_relevant_expression").count().orderBy("is_clinically_relevant_expression").show()

print("expression_breadth_category:")
df1_ml.groupBy("expression_breadth_category").count().orderBy("expression_breadth_category").show()

print("expression_priority:")
df1_ml.groupBy("expression_priority").count().orderBy("expression_priority").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values - transcript_expression_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df1_ml = df1_ml.fillna({
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
    "expression_priority": "low",
    "description": "unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - transcript_expression_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df1_ml = df1_ml.withColumn("random_split", rand(seed=42))

df1_train = df1_ml.filter(col("random_split") < 0.70).drop("random_split")
df1_validation = df1_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df1_test = df1_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save transcript_expression splits
print("\nSAVING EXPRESSION SPLITS")
print("=" * 80)

df1_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_expression_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_expression_train")

df1_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_expression_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_expression_validation")

df1_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_expression_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_expression_test")

# COMMAND ----------

# DBTITLE 1,Verify transcript_expression splits
print("\nVERIFYING EXPRESSION SPLITS")
print("=" * 80)

t1 = spark.table(f"{catalog_name}.gold.ml_dataset_expression_train").count()
v1 = spark.table(f"{catalog_name}.gold.ml_dataset_expression_validation").count()
te1 = spark.table(f"{catalog_name}.gold.ml_dataset_expression_test").count()
total1 = t1 + v1 + te1

print(f"Train:      {t1:,} ({t1/total1*100:.1f}%)")
print(f"Validation: {v1:,} ({v1/total1*100:.1f}%)")
print(f"Test:       {te1:,} ({te1/total1*100:.1f}%)")
print(f"Total:      {total1:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_expression_train") \
    .groupBy("is_clinically_relevant_expression").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 2: gene_expression_ml_features (Enhanced with disease, cancer, domain context)

# COMMAND ----------

# DBTITLE 1,Load gene_expression_ml_features
print("\nLOADING gene_expression_ml_features")
print("=" * 80)

df2 = spark.table(f"{catalog_name}.gold.gene_expression_ml_features")
print(f"Total records: {df2.count():,}")

# COMMAND ----------

# DBTITLE 1,Select features from gene_expression_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df2_ml = df2.select(
    "gene_symbol",
    "gene_full_name",
    "description",
    "chromosome",
    "gene_length",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_transcription_factor",
    "is_pharmacogene",
    "druggability_score",
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
    "total_disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "has_metabolic_disease",
    "is_disease_gene",
    "disease_category_count",
    "cancer_mutation_count",
    "unique_tumor_samples",
    "is_cancer_gene",
    "cancer_expression_relevance",
    "max_domain_count",
    "has_kinase_domain_count",
    "has_functional_domain",
    "domain_expression_correlation",
    "total_gene_variants",
    "splice_variants",
    "expression_affecting_variants",
    "has_expression_variants",
    "disease_expression_score",
    "cancer_expression_score",
    "functional_expression_score",
    "expression_priority",
    "is_clinically_relevant_expression",
    "disease_specific_expression_pattern",
    "expression_function_correlation"
)

print(f"Features selected: {len(df2_ml.columns)}")
print(f"Records: {df2_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - gene_expression_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_clinically_relevant_expression:")
df2_ml.groupBy("is_clinically_relevant_expression").count().orderBy("is_clinically_relevant_expression").show()

print("expression_priority:")
df2_ml.groupBy("expression_priority").count().orderBy("expression_priority").show()

print("cancer_expression_relevance:")
df2_ml.groupBy("cancer_expression_relevance").count().orderBy("cancer_expression_relevance").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values - gene_expression_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df2_ml = df2_ml.fillna({
    "gene_length": 0,
    "druggability_score": 0.0,
    "max_expression_tpm": 0.0,
    "avg_expression_tpm": 0.0,
    "peak_expression_tpm": 0.0,
    "total_tissues_expressed": 0,
    "tissue_type_count": 0,
    "primary_tissue_count": 0,
    "tissue_specificity_score": 0.0,
    "expression_significance_score": 0,
    "clinical_relevance_score": 0,
    "total_disease_count": 0,
    "disease_category_count": 0,
    "cancer_mutation_count": 0,
    "unique_tumor_samples": 0,
    "max_domain_count": 0,
    "has_kinase_domain_count": 0,
    "total_gene_variants": 0,
    "splice_variants": 0,
    "expression_affecting_variants": 0,
    "disease_expression_score": 0,
    "cancer_expression_score": 0,
    "functional_expression_score": 0,
    "expression_breadth_category": "unknown",
    "expression_level_category": "very_low",
    "expression_priority": "low",
    "cancer_expression_relevance": "unknown",
    "domain_expression_correlation": "unknown",
    "disease_specific_expression_pattern": "unknown",
    "expression_function_correlation": "unknown",
    "description": "unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - gene_expression_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df2_ml = df2_ml.withColumn("random_split", rand(seed=42))

df2_train = df2_ml.filter(col("random_split") < 0.70).drop("random_split")
df2_validation = df2_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df2_test = df2_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save gene_expression splits
print("\nSAVING GENE EXPRESSION SPLITS")
print("=" * 80)

df2_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_expression_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_expression_train")

df2_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_expression_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_expression_validation")

df2_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_expression_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_expression_test")

# COMMAND ----------

# DBTITLE 1,Verify gene_expression splits
print("\nVERIFYING GENE EXPRESSION SPLITS")
print("=" * 80)

t2 = spark.table(f"{catalog_name}.gold.ml_dataset_gene_expression_train").count()
v2 = spark.table(f"{catalog_name}.gold.ml_dataset_gene_expression_validation").count()
te2 = spark.table(f"{catalog_name}.gold.ml_dataset_gene_expression_test").count()
total2 = t2 + v2 + te2

print(f"Train:      {t2:,} ({t2/total2*100:.1f}%)")
print(f"Validation: {v2:,} ({v2/total2*100:.1f}%)")
print(f"Test:       {te2:,} ({te2/total2*100:.1f}%)")
print(f"Total:      {total2:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_expression_train") \
    .groupBy("is_clinically_relevant_expression").count().show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - EXPRESSION ML DATASETS")
print("=" * 80)
print(f"transcript_expression: Train={t1:,} | Validation={v1:,} | Test={te1:,}")
print(f"gene_expression:       Train={t2:,} | Validation={v2:,} | Test={te2:,}")
print("\nML DATASET - EXPRESSION COMPLETE")
