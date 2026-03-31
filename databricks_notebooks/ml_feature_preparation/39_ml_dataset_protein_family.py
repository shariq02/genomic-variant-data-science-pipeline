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
    "gene_name",
    "description",
    "chromosome",
    "protein_family",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_pharmacogene",
    "druggability_score",
    "protein_count",
    "max_domain_count",
    "proteins_with_kinase",
    "proteins_with_receptor",
    "proteins_with_zinc_finger",
    "proteins_with_sh2",
    "proteins_with_sh3",
    "proteins_with_ph",
    "proteins_with_death",
    "proteins_with_leucine_zipper",
    "proteins_with_helix_loop",
    "proteins_with_ig",
    "proteins_with_functional_domain",
    "has_dna_binding_domain",
    "has_membrane_domain",
    "has_apoptosis_domain",
    "has_immune_domain",
    "is_multi_domain_protein",
    "domain_diversity_score",
    "functional_complexity_score",
    "druggability_potential_score",
    "domain_affecting_variants",
    "domain_pathogenic_variants",
    "critical_domain_variants",
    "has_domain_variants",
    "protein_family_expression_breadth",
    "protein_max_expression",
    "tissue_specific_protein_expression",
    "cancer_missense_mutations",
    "cancer_truncating_mutations",
    "cancer_samples_affected",
    "cancer_relevant_protein_family",
    "oncogenic_domain_alterations",
    "total_disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "disease_associated_protein_family",
    "disease_specific_domains",
    "variant_domain_impact_score",
    "cancer_protein_family_score",
    "disease_protein_family_score",
    "is_high_value_protein_family",
    "variant_disease_domain_correlation",
    "cancer_protein_classification"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_value_protein_family:")
df_ml.groupBy("is_high_value_protein_family").count().orderBy("is_high_value_protein_family").show()

print("cancer_protein_classification:")
df_ml.groupBy("cancer_protein_classification").count().orderBy("cancer_protein_classification").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "protein_count": 0,
    "max_domain_count": 0,
    "proteins_with_kinase": 0,
    "proteins_with_receptor": 0,
    "proteins_with_zinc_finger": 0,
    "proteins_with_sh2": 0,
    "proteins_with_sh3": 0,
    "proteins_with_ph": 0,
    "proteins_with_death": 0,
    "proteins_with_leucine_zipper": 0,
    "proteins_with_helix_loop": 0,
    "proteins_with_ig": 0,
    "proteins_with_functional_domain": 0,
    "domain_diversity_score": 0,
    "functional_complexity_score": 0,
    "druggability_potential_score": 0,
    "domain_affecting_variants": 0,
    "domain_pathogenic_variants": 0,
    "critical_domain_variants": 0,
    "variant_domain_impact_score": 0,
    "protein_family_expression_breadth": 0,
    "protein_max_expression": 0.0,
    "cancer_missense_mutations": 0,
    "cancer_truncating_mutations": 0,
    "cancer_samples_affected": 0,
    "total_disease_count": 0,
    "cancer_protein_family_score": 0.0,
    "disease_protein_family_score": 0.0,
    "druggability_score": 0.0,
    "protein_family": "unknown",
    "oncogenic_domain_alterations": "unknown",
    "disease_specific_domains": "unknown",
    "variant_disease_domain_correlation": "unknown",
    "cancer_protein_classification": "unknown",
    "description": "unknown"
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

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - PROTEIN FAMILY ML DATASETS")
print("=" * 80)
print(f"protein_family: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - PROTEIN FAMILY COMPLETE")
