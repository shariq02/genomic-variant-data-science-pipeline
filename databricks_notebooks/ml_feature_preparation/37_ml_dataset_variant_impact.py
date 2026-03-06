# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - VARIANT IMPACT PREDICTION
# MAGIC ##### Module: Prepare Variant Impact ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 2: Variant Interpretation
# MAGIC - Use Case 7: Protein Family Conservation
# MAGIC
# MAGIC **Input:** gold.variant_impact_ml_features (4.2M variants, ~120 cols)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_variant_impact_train
# MAGIC - gold.ml_dataset_variant_impact_validation
# MAGIC - gold.ml_dataset_variant_impact_test
# MAGIC
# MAGIC **Target:** is_high_impact

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - VARIANT IMPACT PREDICTION")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load variant_impact_ml_features
print("\nLOADING variant_impact_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.variant_impact_ml_features")
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

# DBTITLE 1,Select features from variant_impact_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "variant_id",
    "gene_name",
    "official_symbol",
    "chromosome",
    "position",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "review_status",
    "review_quality_score",
    "variant_type",
    "variant_name",
    "reference_allele",
    "alternate_allele",
    "protein_change",
    "cdna_change",
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "is_snv",
    "is_insertion",
    "is_deletion",
    "domain_count",
    "has_zinc_finger",
    "has_kinase_domain",
    "has_receptor_domain",
    "has_sh2_domain",
    "has_sh3_domain",
    "has_ph_domain",
    "affects_functional_domain",
    "domain_type_count",
    "has_multiple_domain_types",
    "mutation_severity_score",
    "pathogenicity_score",
    "protein_impact_category",
    "combined_impact_score",
    "variant_impact_tier",
    "phylop_score",
    "phastcons_score",
    "gerp_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "is_likely_deleterious",
    "conservation_impact_class",
    "is_high_impact",
    "is_very_high_impact",
    "is_conservation_constrained",
    "is_loss_of_function",
    "is_splice_affecting",
    "has_cadd_score",
    "is_deleterious_by_cadd",
    "splice_impact_severity",
    "lof_category",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_pharmacogene",
    "druggability_score",
    "is_druggable_gene",
    "is_key_protein_type",
    "is_well_annotated",
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_broadly_expressed",
    "is_highly_expressed",
    "expression_impact_context",
    "cancer_mutation_count",
    "is_cancer_gene",
    "is_cancer_relevant_variant",
    "cancer_variant_priority",
    "disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "has_metabolic_disease",
    "has_cardiovascular_disease",
    "is_disease_associated_gene",
    "disease_impact_category",
    "disease_specific_priority",
    "gene_total_variants",
    "gene_high_impact_count",
    "gene_very_high_impact_count",
    "gene_lof_count",
    "gene_splice_variant_count",
    "gene_domain_affecting_count",
    "gene_avg_impact_score",
    "gene_max_impact_score",
    "gene_impact_burden",
    "gene_lof_tolerance",
    "gene_variant_impact_priority"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_impact:")
df_ml.groupBy("is_high_impact").count().orderBy("is_high_impact").show()

print("variant_impact_tier:")
df_ml.groupBy("variant_impact_tier").count().orderBy("variant_impact_tier").show()

print("lof_category:")
df_ml.groupBy("lof_category").count().orderBy("lof_category").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "review_quality_score": 0,
    "domain_count": 0,
    "domain_type_count": 0,
    "mutation_severity_score": 0,
    "pathogenicity_score": 0,
    "combined_impact_score": 0,
    "phylop_score": 0.0,
    "phastcons_score": 0.0,
    "gerp_score": 0.0,
    "cadd_phred": 0.0,
    "conservation_level": 0,
    "druggability_score": 0.0,
    "tissues_expressed_count": 0,
    "max_expression_tpm": 0.0,
    "cancer_mutation_count": 0,
    "disease_count": 0,
    "gene_total_variants": 0,
    "gene_high_impact_count": 0,
    "gene_very_high_impact_count": 0,
    "gene_lof_count": 0,
    "gene_splice_variant_count": 0,
    "gene_domain_affecting_count": 0,
    "gene_avg_impact_score": 0.0,
    "gene_max_impact_score": 0,
    "review_status": "unknown",
    "variant_type": "unknown",
    "protein_change": "unknown",
    "cdna_change": "unknown",
    "protein_impact_category": "unknown",
    "variant_impact_tier": "unknown",
    "conservation_impact_class": "unknown",
    "splice_impact_severity": "unknown",
    "lof_category": "unknown",
    "expression_impact_context": "unknown",
    "cancer_variant_priority": "unknown",
    "disease_impact_category": "unknown",
    "disease_specific_priority": "unknown",
    "gene_impact_burden": "unknown",
    "gene_lof_tolerance": "unknown",
    "gene_variant_impact_priority": "unknown",
    "official_symbol": "unknown",
    "variant_name": "unknown",
    "reference_allele": "unknown",
    "alternate_allele": "unknown"
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

# DBTITLE 1,Save variant impact splits
print("\nSAVING VARIANT IMPACT SPLITS")
print("=" * 80)

df_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_impact_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_impact_train")

df_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_impact_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_impact_validation")

df_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variant_impact_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_variant_impact_test")

# COMMAND ----------

# DBTITLE 1,Verify variant impact splits
print("\nVERIFYING VARIANT IMPACT SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_variant_impact_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_variant_impact_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_variant_impact_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_variant_impact_train") \
    .groupBy("is_high_impact").count().show()

print("\nTrain variant impact tier distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_variant_impact_train") \
    .groupBy("variant_impact_tier").count().orderBy("variant_impact_tier").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - VARIANT IMPACT ML DATASETS")
print("=" * 80)
print(f"variant_impact: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - VARIANT IMPACT COMPLETE")
