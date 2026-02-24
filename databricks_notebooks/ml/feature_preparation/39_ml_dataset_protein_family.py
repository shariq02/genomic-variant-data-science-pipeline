# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - PROTEIN FAMILY CONSERVATION
# MAGIC ##### Module: Prepare Protein Family ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Protein Domain Analysis
# MAGIC - Use Case 7: Protein Family Conservation
# MAGIC
# MAGIC **Input:** gold.gene_protein_family_ml_features (gene-level, ~19K genes)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_protein_family_train
# MAGIC - gold.ml_dataset_protein_family_validation
# MAGIC - gold.ml_dataset_protein_family_test
# MAGIC
# MAGIC **Target:** is_high_value_protein_family

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - PROTEIN FAMILY CONSERVATION")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load gene_protein_family_ml_features
print("\nLOADING gene_protein_family_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.gene_protein_family_ml_features")
print(f"Total records: {df.count():,}")

# COMMAND ----------

# DBTITLE 1,Select features from gene_protein_family_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "gene_symbol",
    "gene_full_name",
    "chromosome",
    "total_domains",
    "unique_domain_types",
    "has_kinase_domain",
    "has_receptor_domain",
    "has_sh2_domain",
    "has_sh3_domain",
    "has_ph_domain",
    "has_zinc_finger",
    "has_dna_binding",
    "has_atp_binding",
    "has_multiple_domain_types",
    "domain_complexity_score",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_transcription_factor",
    "is_transporter",
    "protein_family_category",
    "is_druggable_family",
    "family_druggability_tier",
    "total_gene_variants",
    "pathogenic_variants",
    "domain_affecting_variants",
    "has_pathogenic_domain_variants",
    "variant_domain_impact_score",
    "druggability_score",
    "enhanced_druggability_score",
    "is_pharmacogene",
    "is_high_priority_pharmacogene",
    "total_disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "has_cardiovascular_disease",
    "cancer_mutation_count",
    "is_cancer_gene",
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_broadly_expressed",
    "is_highly_expressed",
    "avg_conservation_level",
    "max_conservation_level",
    "highly_conserved_domain_count",
    "is_highly_conserved_gene",
    "conservation_clinical_relevance",
    "domain_clinical_relevance_score",
    "family_clinical_impact_score",
    "conservation_druggability_score",
    "protein_family_priority",
    "is_high_value_protein_family",
    "family_therapeutic_potential",
    "conservation_significance"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_value_protein_family:")
df_ml.groupBy("is_high_value_protein_family").count().orderBy("is_high_value_protein_family").show()

print("protein_family_category:")
df_ml.groupBy("protein_family_category").count().orderBy("protein_family_category").show()

print("protein_family_priority:")
df_ml.groupBy("protein_family_priority").count().orderBy("protein_family_priority").show()

print("family_therapeutic_potential:")
df_ml.groupBy("family_therapeutic_potential").count().orderBy("family_therapeutic_potential").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "total_domains": 0,
    "unique_domain_types": 0,
    "domain_complexity_score": 0,
    "total_gene_variants": 0,
    "pathogenic_variants": 0,
    "domain_affecting_variants": 0,
    "variant_domain_impact_score": 0,
    "druggability_score": 0.0,
    "enhanced_druggability_score": 0.0,
    "total_disease_count": 0,
    "cancer_mutation_count": 0,
    "tissues_expressed_count": 0,
    "max_expression_tpm": 0.0,
    "avg_conservation_level": 0.0,
    "max_conservation_level": 0,
    "highly_conserved_domain_count": 0,
    "domain_clinical_relevance_score": 0,
    "family_clinical_impact_score": 0,
    "conservation_druggability_score": 0,
    "protein_family_category": "unknown",
    "family_druggability_tier": "unknown",
    "protein_family_priority": "low",
    "family_therapeutic_potential": "unknown",
    "conservation_significance": "unknown",
    "conservation_clinical_relevance": "unknown"
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

# DBTITLE 1,Save protein family splits
print("\nSAVING PROTEIN FAMILY SPLITS")
print("=" * 80)

df_train.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_protein_family_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_protein_family_train")

df_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_protein_family_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_protein_family_validation")

df_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_protein_family_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_protein_family_test")

# COMMAND ----------

# DBTITLE 1,Verify protein family splits
print("\nVERIFYING PROTEIN FAMILY SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_protein_family_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_protein_family_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_protein_family_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_protein_family_train") \
    .groupBy("is_high_value_protein_family").count().show()

print("\nTrain protein family category distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_protein_family_train") \
    .groupBy("protein_family_category").count().orderBy("protein_family_category").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - PROTEIN FAMILY ML DATASETS")
print("=" * 80)
print(f"protein_family: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - PROTEIN FAMILY COMPLETE")
