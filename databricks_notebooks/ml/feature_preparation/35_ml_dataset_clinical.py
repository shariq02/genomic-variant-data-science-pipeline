# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - CLINICAL VARIANT INTERPRETATION
# MAGIC ##### Module: Prepare Clinical Pathogenicity ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 2: Variant Interpretation
# MAGIC
# MAGIC **Input:** gold.clinical_ml_features (4.2M variants, ~95 cols)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_clinical_train
# MAGIC - gold.ml_dataset_clinical_validation
# MAGIC - gold.ml_dataset_clinical_test
# MAGIC
# MAGIC **Target:** target_is_pathogenic

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - CLINICAL VARIANT INTERPRETATION")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Load clinical_ml_features
print("\nLOADING clinical_ml_features")
print("=" * 80)

df = spark.table(f"{catalog_name}.gold.clinical_ml_features")
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

# DBTITLE 1,Select features from clinical_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df_ml = df.select(
    "variant_id",
    "gene_name",
    "chromosome",
    "position",
    "official_gene_symbol",
    "gene_is_validated",
    "gene_has_omim",
    "gene_has_ensembl",
    "gene_is_well_characterized",
    "is_pharmacogene",
    "druggability_score",
    "target_is_pathogenic",
    "target_is_benign",
    "target_is_vus",
    "clinical_significance_simple",
    "clinvar_pathogenicity_class",
    "clinical_sig_is_uncertain",
    "review_quality_score",
    "has_strong_evidence",
    "mutation_severity_score",
    "pathogenicity_score",
    "combined_pathogenicity_risk",
    "protein_impact_category",
    "is_coding_variant",
    "is_regulatory_variant",
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "phylop_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "is_likely_deleterious",
    "is_high_impact",
    "is_very_high_impact",
    "is_domain_affecting",
    "is_loss_of_function",
    "is_deleterious_by_cadd",
    "has_functional_domain",
    "domain_count",
    "has_conservation_data",
    "has_complete_annotation",
    "inheritance_pattern",
    "x_linked_risk_modifier",
    "inheritance_pathogenicity_modifier",
    "is_mitochondrial_variant",
    "is_y_linked_variant",
    "is_x_linked_variant",
    "is_autosomal_variant",
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_benign_count",
    "gene_vus_count",
    "gene_pathogenic_ratio",
    "gene_benign_ratio",
    "gene_vus_ratio",
    "gene_mutation_burden",
    "gene_is_pathogenic_enriched",
    "gene_is_benign_enriched",
    "gene_is_vus_enriched",
    "gene_variant_profile",
    "gene_has_high_lof_burden",
    "gene_avg_review_quality",
    "gene_has_quality_annotations",
    "gene_missense_count",
    "gene_frameshift_count",
    "gene_nonsense_count",
    "gene_splice_count",
    "gene_lof_variant_ratio",
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_broadly_expressed",
    "is_highly_expressed",
    "expression_context",
    "cancer_mutation_count",
    "is_cancer_gene",
    "is_cancer_relevant",
    "population_allele_frequency",
    "is_common_in_population",
    "is_rare_in_population",
    "frequency_pathogenicity_conflict",
    "disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "is_disease_gene"
)

print(f"Features selected: {len(df_ml.columns)}")
print(f"Records: {df_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("target_is_pathogenic:")
df_ml.groupBy("target_is_pathogenic").count().orderBy("target_is_pathogenic").show()

print("clinical_significance_simple:")
df_ml.groupBy("clinical_significance_simple").count().orderBy("clinical_significance_simple").show()

print("clinvar_pathogenicity_class:")
df_ml.groupBy("clinvar_pathogenicity_class").count().orderBy("clinvar_pathogenicity_class").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df_ml = df_ml.fillna({
    "druggability_score": 0.0,
    "review_quality_score": 0,
    "mutation_severity_score": 0,
    "pathogenicity_score": 0,
    "combined_pathogenicity_risk": 0,
    "phylop_score": 0.0,
    "cadd_phred": 0.0,
    "conservation_level": 0,
    "domain_count": 0,
    "gene_total_variants": 0,
    "gene_pathogenic_count": 0,
    "gene_benign_count": 0,
    "gene_vus_count": 0,
    "gene_pathogenic_ratio": 0.0,
    "gene_benign_ratio": 0.0,
    "gene_vus_ratio": 0.0,
    "gene_avg_review_quality": 0.0,
    "gene_missense_count": 0,
    "gene_frameshift_count": 0,
    "gene_nonsense_count": 0,
    "gene_splice_count": 0,
    "gene_lof_variant_ratio": 0.0,
    "tissues_expressed_count": 0,
    "max_expression_tpm": 0.0,
    "cancer_mutation_count": 0,
    "population_allele_frequency": 0.0,
    "disease_count": 0,
    "clinical_significance_simple": "Unknown",
    "clinvar_pathogenicity_class": "Unknown",
    "protein_impact_category": "unknown",
    "inheritance_pattern": "unknown",
    "x_linked_risk_modifier": "unknown",
    "inheritance_pathogenicity_modifier": "unknown",
    "gene_mutation_burden": "unknown",
    "gene_variant_profile": "unknown",
    "expression_context": "unknown"
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

# DBTITLE 1,Save clinical splits
print("\nSAVING CLINICAL SPLITS")
print("=" * 80)

df_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_clinical_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_clinical_train")

df_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_clinical_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_clinical_validation")

df_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_clinical_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_clinical_test")

# COMMAND ----------

# DBTITLE 1,Verify clinical splits
print("\nVERIFYING CLINICAL SPLITS")
print("=" * 80)

t = spark.table(f"{catalog_name}.gold.ml_dataset_clinical_train").count()
v = spark.table(f"{catalog_name}.gold.ml_dataset_clinical_validation").count()
te = spark.table(f"{catalog_name}.gold.ml_dataset_clinical_test").count()
total = t + v + te

print(f"Train:      {t:,} ({t/total*100:.1f}%)")
print(f"Validation: {v:,} ({v/total*100:.1f}%)")
print(f"Test:       {te:,} ({te/total*100:.1f}%)")
print(f"Total:      {total:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_clinical_train") \
    .groupBy("target_is_pathogenic").count().show()

print("\nTrain clinical significance distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_clinical_train") \
    .groupBy("clinical_significance_simple").count().orderBy("clinical_significance_simple").show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - CLINICAL ML DATASETS")
print("=" * 80)
print(f"clinical: Train={t:,} | Validation={v:,} | Test={te:,}")
print("\nML DATASET - CLINICAL COMPLETE")
