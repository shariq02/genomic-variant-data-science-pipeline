# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - DISEASE GENE DISCOVERY
# MAGIC ##### Module: Prepare Disease Association ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 3: Disease Gene Discovery
# MAGIC - Use Case 8: Disease Relationship Network
# MAGIC
# MAGIC **Input:** gold.disease_ml_features (4.2M variants, ~60 cols)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_disease_train
# MAGIC - gold.ml_dataset_disease_validation
# MAGIC - gold.ml_dataset_disease_test
# MAGIC
# MAGIC **Target:** is_pathogenic

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - DISEASE GENE DISCOVERY")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load disease_ml_features
print("\nLOADING disease_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.disease_ml_features")
print(f"Total records: {df.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY variant_id")
print("=" * 80)

before = df.count()
df = df.dropDuplicates(["variant_id"])
after = df.count()
print(f"Before: {before:,}")
print(f"After:  {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from disease_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "variant_id",
    "gene_name",
    "chromosome",
    "position",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "disease_enriched",
    "primary_disease",
    "disease_name_enriched",
    "omim_id",
    "mondo_id",
    "orphanet_id",
    "has_omim_disease",
    "has_mondo_disease",
    "disease_db_coverage",
    "disease_name_is_generic",
    "disease_count",
    "omim_disease_count",
    "disease_count_category",
    "is_disease_associated",
    "is_multi_disease_gene",
    "disease_association_strength",
    "is_omim_gene",
    "variant_disease_link_quality",
    "disease_total_variants",
    "disease_pathogenic_variants",
    "disease_benign_variants",
    "disease_vus_variants",
    "disease_pathogenic_ratio",
    "disease_gene_count",
    "is_polygenic_disease",
    "disease_complexity",
    "disease_complexity_score",
    "polygenic_risk_contribution",
    "disease_has_high_pathogenic_burden",
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_benign_count",
    "gene_high_quality_count",
    "gene_disease_diversity",
    "gene_clinical_utility_score",
    "gene_priority_tier",
    "is_clinically_actionable",
    "is_research_candidate",
    "gene_annotation_score",
    "has_excellent_annotation",
    "annotation_priority_level",
    "gene_omim_variants",
    "tissues_expressed_count",
    "is_broadly_expressed",
    "cancer_mutation_count",
    "is_cancer_hotspot_gene",
    "phylop_score",
    "cadd_phred",
    "is_highly_conserved",
    "has_high_conservation",
    "gene_domain_count",
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_pathogenic:")
df_ml.groupBy("is_pathogenic").count().orderBy("is_pathogenic").show()

print("disease_complexity:")
df_ml.groupBy("disease_complexity").count().orderBy("disease_complexity").show()

print("gene_priority_tier:")
df_ml.groupBy("gene_priority_tier").count().orderBy("gene_priority_tier").show()

print("annotation_priority_level:")
df_ml.groupBy("annotation_priority_level").count().orderBy("annotation_priority_level").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "disease_db_coverage": 0,
    "disease_count": 0,
    "omim_disease_count": 0,
    "disease_association_strength": 0,
    "disease_total_variants": 0,
    "disease_pathogenic_variants": 0,
    "disease_benign_variants": 0,
    "disease_vus_variants": 0,
    "disease_pathogenic_ratio": 0.0,
    "disease_gene_count": 0,
    "disease_complexity_score": 0,
    "gene_total_variants": 0,
    "gene_pathogenic_count": 0,
    "gene_benign_count": 0,
    "gene_high_quality_count": 0,
    "gene_disease_diversity": 0,
    "gene_clinical_utility_score": 0,
    "gene_annotation_score": 0,
    "gene_omim_variants": 0,
    "tissues_expressed_count": 0,
    "cancer_mutation_count": 0,
    "phylop_score": 0.0,
    "cadd_phred": 0.0,
    "gene_domain_count": 0,
    "disease_enriched": "Unknown",
    "primary_disease": "Unknown",
    "disease_name_enriched": "Unknown",
    "disease_count_category": "none",
    "variant_disease_link_quality": "unknown",
    "disease_complexity": "unknown",
    "polygenic_risk_contribution": "unknown",
    "gene_priority_tier": "low",
    "annotation_priority_level": "low",
    "omim_id": "unknown",
    "mondo_id": "unknown",
    "orphanet_id": "unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Repartition for 4.2M rows
print("\nREPARTITIONING")
print("=" * 80)

df_ml = df_ml.repartition(10)
print("Repartitioned to 10 partitions")

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

# DBTITLE 1,Save disease splits
print("\nSAVING DISEASE SPLITS")
print("=" * 80)

df_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_disease_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_disease_train")

df_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_disease_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_disease_validation")

df_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_disease_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_disease_test")

# COMMAND ----------

# DBTITLE 1,Verify disease splits
print("\nVERIFYING DISEASE SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_disease_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_disease_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_disease_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_disease_train") \
    .groupBy("is_pathogenic").count().show()

print("\nTrain disease complexity distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_disease_train") \
    .groupBy("disease_complexity").count().orderBy("disease_complexity").show()

print("\nTrain gene priority tier distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_disease_train") \
    .groupBy("gene_priority_tier").count().orderBy("gene_priority_tier").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - DISEASE ML DATASETS")
print("=" * 80)
print(f"disease: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - DISEASE COMPLETE")
