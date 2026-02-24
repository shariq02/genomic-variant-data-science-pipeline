# Databricks notebook source
# MAGIC %md
# MAGIC #### ML DATASET - PHARMACOGENE PRIORITY PREDICTION
# MAGIC ##### Module: Prepare Pharmacogene ML Dataset
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 23, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 14: Drug Target Identification
# MAGIC
# MAGIC **Input:**
# MAGIC - gold.gene_pharmacogene_ml_features (gene-level, ~2K genes)
# MAGIC - gold.pharmacogene_ml_features (variant-level, 4.2M variants)
# MAGIC
# MAGIC **Output:**
# MAGIC - gold.ml_dataset_gene_pharmacogene_train
# MAGIC - gold.ml_dataset_gene_pharmacogene_validation
# MAGIC - gold.ml_dataset_gene_pharmacogene_test
# MAGIC - gold.ml_dataset_pharmacogene_train
# MAGIC - gold.ml_dataset_pharmacogene_validation
# MAGIC - gold.ml_dataset_pharmacogene_test
# MAGIC
# MAGIC **Targets:**
# MAGIC - gene_pharmacogene_ml_features: is_high_priority_pharmacogene
# MAGIC - pharmacogene_ml_features: is_pharmacogene

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("ML DATASET - PHARMACOGENE PRIORITY PREDICTION")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 1: gene_pharmacogene_ml_features (Gene-level)

# COMMAND ----------

# DBTITLE 1,Load gene_pharmacogene_ml_features
print("\nLOADING gene_pharmacogene_ml_features")
print("=" * 80)

df1 = spark.table(f"{catalog_name}.gold.gene_pharmacogene_ml_features")
print(f"Total records: {df1.count():,}")

# COMMAND ----------

# DBTITLE 1,Select features from gene_pharmacogene_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df1_ml = df1.select(
    "gene_symbol",
    "gene_full_name",
    "chromosome",
    "source_count",
    "has_pharmgkb_annotation",
    "is_drug_metabolizer",
    "is_drug_transporter_gene",
    "is_drug_target_gene",
    "has_high_druggability",
    "is_pharmacogene",
    "is_hepatic_metabolizer",
    "is_renal_transporter",
    "is_validated_cancer_target",
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
    "total_gene_variants",
    "pathogenic_variants",
    "missense_variants",
    "lof_variants",
    "domain_affecting_variants",
    "avg_pathogenicity_score",
    "has_pharmacogene_variants",
    "variant_impact_burden",
    "tissues_expressed_count",
    "max_expression_tpm",
    "avg_expression_tpm",
    "is_liver_expressed",
    "is_kidney_expressed",
    "expression_breadth",
    "drug_metabolism_tissue_expression",
    "cancer_mutation_count",
    "unique_tumor_samples",
    "is_oncology_drug_target",
    "cancer_mutation_burden",
    "total_disease_count",
    "has_cancer_disease",
    "has_cardiovascular_disease",
    "has_neurological_disease",
    "has_metabolic_disease",
    "primary_indication_category",
    "max_domain_count",
    "has_kinase_domain_count",
    "is_complex_drug_target",
    "pharmacogene_evidence_score",
    "drug_interaction_score",
    "clinical_utility_score",
    "pharmacogene_variant_impact_score",
    "metabolism_context_score",
    "pharmacogene_priority",
    "is_high_priority_pharmacogene",
    "pharmacogene_category",
    "pharmacogene_category_enhanced",
    "drug_metabolism_role",
    "clinical_actionability_tier"
)

print(f"Features selected: {len(df1_ml.columns)}")
print(f"Records: {df1_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - gene_pharmacogene_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_high_priority_pharmacogene:")
df1_ml.groupBy("is_high_priority_pharmacogene").count().orderBy("is_high_priority_pharmacogene").show()

print("pharmacogene_priority:")
df1_ml.groupBy("pharmacogene_priority").count().orderBy("pharmacogene_priority").show()

print("clinical_actionability_tier:")
df1_ml.groupBy("clinical_actionability_tier").count().orderBy("clinical_actionability_tier").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values - gene_pharmacogene_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df1_ml = df1_ml.fillna({
    "source_count": 0,
    "druggability_score": 0.0,
    "total_relationships": 0,
    "entity_type_count": 0,
    "drug_relationships": 0,
    "disease_relationships": 0,
    "variant_relationships": 0,
    "evidence_count": 0,
    "total_gene_variants": 0,
    "pathogenic_variants": 0,
    "missense_variants": 0,
    "lof_variants": 0,
    "domain_affecting_variants": 0,
    "avg_pathogenicity_score": 0.0,
    "tissues_expressed_count": 0,
    "max_expression_tpm": 0.0,
    "avg_expression_tpm": 0.0,
    "cancer_mutation_count": 0,
    "unique_tumor_samples": 0,
    "total_disease_count": 0,
    "max_domain_count": 0,
    "has_kinase_domain_count": 0,
    "pharmacogene_evidence_score": 0,
    "drug_interaction_score": 0,
    "clinical_utility_score": 0.0,
    "pharmacogene_variant_impact_score": 0,
    "metabolism_context_score": 0,
    "pharmacogene_priority": "low",
    "pharmacogene_category": "other",
    "pharmacogene_category_enhanced": "other",
    "drug_metabolism_role": "unknown",
    "clinical_actionability_tier": "Tier 4",
    "expression_breadth": "unknown",
    "drug_metabolism_tissue_expression": "unknown",
    "cancer_mutation_burden": "low",
    "variant_impact_burden": "Low",
    "primary_indication_category": "Unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - gene_pharmacogene_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df1_ml = df1_ml.withColumn("random_split", rand(seed=42))

df1_train = df1_ml.filter(col("random_split") < 0.70).drop("random_split")
df1_validation = df1_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df1_test = df1_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save gene_pharmacogene splits
print("\nSAVING GENE PHARMACOGENE SPLITS")
print("=" * 80)

df1_train.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_pharmacogene_train")

df1_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_pharmacogene_validation")

df1_test.coalesce(1).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_gene_pharmacogene_test")

# COMMAND ----------

# DBTITLE 1,Verify gene_pharmacogene splits
print("\nVERIFYING GENE PHARMACOGENE SPLITS")
print("=" * 80)

t1 = spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train").count()
v1 = spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_validation").count()
te1 = spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_test").count()
total1 = t1 + v1 + te1

print(f"Train:      {t1:,} ({t1/total1*100:.1f}%)")
print(f"Validation: {v1:,} ({v1/total1*100:.1f}%)")
print(f"Test:       {te1:,} ({te1/total1*100:.1f}%)")
print(f"Total:      {total1:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_gene_pharmacogene_train") \
    .groupBy("is_high_priority_pharmacogene").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PART 2: pharmacogene_ml_features (Variant-level)

# COMMAND ----------

# DBTITLE 1,Load pharmacogene_ml_features
print("\nLOADING pharmacogene_ml_features")
print("=" * 80)

df2 = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features")
print(f"Total records: {df2.count():,}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY variant_id")
print("=" * 80)

before = df2.count()
df2 = df2.dropDuplicates(["variant_id"])
after = df2.count()
print(f"Before: {before:,}")
print(f"After: {after:,}")
print(f"Removed: {before - after:,}")

# COMMAND ----------

# DBTITLE 1,Select features from pharmacogene_ml_features
print("\nSELECTING FEATURES")
print("=" * 80)

df2_ml = df2.select(
    "variant_id",
    "gene_name",
    "chromosome",
    "position",
    "official_symbol",
    "validated_gene_symbol",
    "gene_is_validated",
    "gene_description_mentions_drug",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "clinical_significance_simple",
    "variant_type",
    "is_missense_variant",
    "is_loss_of_function",
    "protein_impact_category",
    "mutation_severity_score",
    "pathogenicity_score",
    "is_pharmacogene",
    "pharmacogene_category",
    "pharmacogene_evidence_level",
    "drug_metabolism_role",
    "is_drug_target",
    "is_metabolizing_enzyme",
    "metabolizing_enzyme_type",
    "is_enzyme",
    "is_drug_transporter",
    "is_kinase",
    "is_phosphatase",
    "is_receptor",
    "is_gpcr",
    "is_transporter",
    "drug_target_category",
    "druggability_score",
    "enhanced_druggability_score",
    "drug_response_impact",
    "is_metabolizer_variant",
    "metabolizer_phenotype_risk",
    "is_transporter_variant",
    "transporter_impact_level",
    "is_kinase_inhibitor_target",
    "kinase_variant_therapeutic_relevance",
    "pharmgkb_source_count",
    "has_pharmgkb_annotation",
    "gene_pharmacogene_variants",
    "gene_drug_interaction_variants",
    "gene_metabolizer_variants",
    "gene_transporter_variants",
    "gene_pharmacogene_pathogenic",
    "gene_has_multiple_drug_variants",
    "gene_pharmacogene_priority",
    "gene_pharmacogene_burden",
    "gene_avg_druggability",
    "tissues_expressed_count",
    "is_liver_expressed",
    "is_kidney_expressed",
    "expression_breadth",
    "drug_metabolism_context",
    "cancer_mutation_count",
    "is_oncology_target",
    "is_cancer_drug_target",
    "allele_frequency",
    "is_common_variant",
    "is_rare_variant",
    "drug_response_frequency_context",
    "disease_count",
    "has_cancer_disease",
    "has_cardiovascular_disease",
    "has_neurological_disease",
    "primary_indication_category"
)

print(f"Features selected: {len(df2_ml.columns)}")
print(f"Records: {df2_ml.count():,}")

# COMMAND ----------

# DBTITLE 1,Check target distribution - pharmacogene_ml_features
print("\nTARGET DISTRIBUTION")
print("=" * 80)

print("is_pharmacogene:")
df2_ml.groupBy("is_pharmacogene").count().orderBy("is_pharmacogene").show()

print("pharmacogene_category:")
df2_ml.groupBy("pharmacogene_category").count().orderBy("pharmacogene_category").show()

# COMMAND ----------

# DBTITLE 1,Handle missing values - pharmacogene_ml_features
print("\nHANDLING MISSING VALUES")
print("=" * 80)

df2_ml = df2_ml.fillna({
    "mutation_severity_score": 0,
    "pathogenicity_score": 0,
    "druggability_score": 0.0,
    "enhanced_druggability_score": 0.0,
    "pharmgkb_source_count": 0,
    "gene_pharmacogene_variants": 0,
    "gene_drug_interaction_variants": 0,
    "gene_metabolizer_variants": 0,
    "gene_transporter_variants": 0,
    "gene_pharmacogene_pathogenic": 0,
    "gene_avg_druggability": 0.0,
    "tissues_expressed_count": 0,
    "cancer_mutation_count": 0,
    "allele_frequency": 0.0,
    "disease_count": 0,
    "clinical_significance_simple": "Unknown",
    "pharmacogene_category": "unknown",
    "pharmacogene_evidence_level": "unknown",
    "drug_metabolism_role": "unknown",
    "drug_target_category": "unknown",
    "drug_response_impact": "unknown",
    "metabolizer_phenotype_risk": "unknown",
    "transporter_impact_level": "unknown",
    "kinase_variant_therapeutic_relevance": "unknown",
    "gene_pharmacogene_priority": "low",
    "gene_pharmacogene_burden": "low",
    "expression_breadth": "unknown",
    "drug_metabolism_context": "unknown",
    "drug_response_frequency_context": "unknown",
    "primary_indication_category": "Unknown",
    "metabolizing_enzyme_type": "unknown",
    "variant_type": "unknown",
    "protein_impact_category": "unknown"
})

print("Missing values filled")

# COMMAND ----------

# DBTITLE 1,Create splits - pharmacogene_ml_features
print("\nCREATING TRAIN/VALIDATION/TEST SPLITS (70/15/15)")
print("=" * 80)

df2_ml = df2_ml.withColumn("random_split", rand(seed=42))

df2_train = df2_ml.filter(col("random_split") < 0.70).drop("random_split")
df2_validation = df2_ml.filter((col("random_split") >= 0.70) & (col("random_split") < 0.85)).drop("random_split")
df2_test = df2_ml.filter(col("random_split") >= 0.85).drop("random_split")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Save pharmacogene splits
print("\nSAVING PHARMACOGENE SPLITS")
print("=" * 80)

df2_train.coalesce(5).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_pharmacogene_train")
print(f"Saved: {catalog_name}.gold.ml_dataset_pharmacogene_train")

df2_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_pharmacogene_validation")
print(f"Saved: {catalog_name}.gold.ml_dataset_pharmacogene_validation")

df2_test.coalesce(2).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_pharmacogene_test")
print(f"Saved: {catalog_name}.gold.ml_dataset_pharmacogene_test")

# COMMAND ----------

# DBTITLE 1,Verify pharmacogene splits
print("\nVERIFYING PHARMACOGENE SPLITS")
print("=" * 80)

t2 = spark.table(f"{catalog_name}.gold.ml_dataset_pharmacogene_train").count()
v2 = spark.table(f"{catalog_name}.gold.ml_dataset_pharmacogene_validation").count()
te2 = spark.table(f"{catalog_name}.gold.ml_dataset_pharmacogene_test").count()
total2 = t2 + v2 + te2

print(f"Train:      {t2:,} ({t2/total2*100:.1f}%)")
print(f"Validation: {v2:,} ({v2/total2*100:.1f}%)")
print(f"Test:       {te2:,} ({te2/total2*100:.1f}%)")
print(f"Total:      {total2:,}")

print("\nTrain target distribution:")
spark.table(f"{catalog_name}.gold.ml_dataset_pharmacogene_train") \
    .groupBy("is_pharmacogene").count().show()

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY - PHARMACOGENE ML DATASETS")
print("=" * 80)
print(f"gene_pharmacogene: Train={t1:,} | Validation={v1:,} | Test={te1:,}")
print(f"pharmacogene:      Train={t2:,} | Validation={v2:,} | Test={te2:,}")
print("\nML DATASET - PHARMACOGENE COMPLETE")
