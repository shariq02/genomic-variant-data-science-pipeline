# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - COMBINED ML DATASET (DEDUPLICATION FIX)
# MAGIC ##### Module: Combine All ML Features for Training
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 06, 2026
# MAGIC
# MAGIC **CRITICAL FIX:**
# MAGIC - Deduplication added to prevent cartesian explosion
# MAGIC - 69M duplicates -> 4.1M unique variants
# MAGIC - Optimized for small data (1.9 GB total)
# MAGIC
# MAGIC **Input:** 
# MAGIC - ml_clinical_features (variant_id)
# MAGIC - ml_disease_features (variant_id)
# MAGIC - ml_pharmacogene_features (variant_id)
# MAGIC - ml_variant_impact_features (variant_id)
# MAGIC - ml_structural_variant_features (sv_id)
# MAGIC
# MAGIC **Output:** 
# MAGIC - ml_dataset_variants_train
# MAGIC - ml_dataset_variants_validation
# MAGIC - ml_dataset_variants_test
# MAGIC - ml_dataset_structural_variants_train
# MAGIC - ml_dataset_structural_variants_validation
# MAGIC - ml_dataset_structural_variants_test

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Load table data
print("Loading all ML feature tables...")

df_clinical = spark.table(f"{catalog_name}.gold.ml_clinical_features")
df_disease = spark.table(f"{catalog_name}.gold.ml_disease_features")
df_pharmacogene = spark.table(f"{catalog_name}.gold.ml_pharmacogene_features")
df_variant_impact = spark.table(f"{catalog_name}.gold.ml_variant_impact_features")
df_structural = spark.table(f"{catalog_name}.gold.ml_structural_variant_features")

print("All tables loaded successfully")

# COMMAND ----------

# DBTITLE 1,CRITICAL: Deduplicate tables to prevent cartesian explosion
print("\n=== DEDUPLICATION (CRITICAL FIX) ===\n")
print("Removing duplicate variant_ids to prevent 213M row explosion...\n")

# Deduplicate clinical (69M -> 4.1M unique)
print("Deduplicating ml_clinical_features...")
df_clinical_deduped = df_clinical.dropDuplicates(["variant_id"])
print(f"  Before: {df_clinical.count():,} rows")
print(f"  After: {df_clinical_deduped.count():,} rows")

# Deduplicate variant_impact
print("\nDeduplicating ml_variant_impact_features...")
df_impact_deduped = df_variant_impact.dropDuplicates(["variant_id"])
print(f"  Before: {df_variant_impact.count():,} rows")
print(f"  After: {df_impact_deduped.count():,} rows")

# Deduplicate pharmacogene
print("\nDeduplicating ml_pharmacogene_features...")
df_pharmacogene_deduped = df_pharmacogene.dropDuplicates(["variant_id"])
print(f"  Before: {df_pharmacogene.count():,} rows")
print(f"  After: {df_pharmacogene_deduped.count():,} rows")

# Deduplicate disease
print("\nDeduplicating ml_disease_features...")
df_disease_deduped = df_disease.dropDuplicates(["variant_id"])
print(f"  Before: {df_disease.count():,} rows")
print(f"  After: {df_disease_deduped.count():,} rows")

print("\nDeduplication complete - ready for joins")

# COMMAND ----------

# DBTITLE 1,PART 1: VARIANT DATASET
print("\n=== PART 1: VARIANT DATASET (SNV/INDELS) ===\n")

print("Preparing variant-level features for joining...")

# COMMAND ----------

# DBTITLE 1,Selecting columns from deduplicated variant_impact table
print("Selecting columns from variant_impact table...")

df_impact_selected = df_impact_deduped.select(
    "variant_id",
    "variant_type",
    "is_high_impact",
    "is_very_high_impact",
    "is_loss_of_function",
    "affects_functional_domain",
    "is_domain_affecting",
    "has_kinase_domain",
    "phastcons_score",
    "gerp_score",
    "functional_impact_score",
    "domain_impact_count",
    "impact_severity_score",
    "high_confidence_deleterious",
    "conservation_category"
)

print("Variant impact columns selected")

# COMMAND ----------

# DBTITLE 1,Selecting columns from deduplicated pharmacogene table
print("Selecting columns from pharmacogene table...")

df_pharmacogene_selected = df_pharmacogene_deduped.select(
    "variant_id",
    "is_pharmacogene",
    "pharmacogene_category",
    "is_cyp_gene",
    "is_drug_target",
    "is_kinase",
    "is_receptor",
    "is_transporter",
    "is_metabolizing_enzyme",
    "druggability_score",
    "enhanced_druggability_score",
    "has_drug_interaction_potential",
    "is_clinical_pharmacogene",
    "protein_function_count",
    "pharmacogene_evidence_score",
    "clinical_evidence_score",
    "pharmacogene_priority",
    "high_impact_pharmacogene"
)

print("Pharmacogene columns selected")

# COMMAND ----------

# DBTITLE 1,Selecting columns from deduplicated disease table
print("Selecting columns from disease table...")

df_disease_selected = df_disease_deduped.select(
    "variant_id",
    "disease_enriched",
    "primary_disease",
    "omim_id",
    "mondo_id",
    "orphanet_id",
    "has_omim_disease",
    "has_mondo_disease",
    "has_orphanet_disease",
    "disease_db_coverage",
    "disease_count",
    "disease_gene_count",
    "disease_pathogenic_ratio",
    "is_polygenic_disease",
    "disease_complexity",
    "is_clinically_actionable",
    "has_drug_development_potential",
    "is_cancer_gene_variant",
    "is_neurological_gene_variant",
    "is_cardiovascular_gene_variant",
    "is_metabolic_gene_variant",
    "is_rare_disease_gene_variant",
    "disease_category_count",
    "database_coverage_score",
    "disease_priority_score",
    "disease_burden_score"
)

print("Disease columns selected")

# COMMAND ----------

# DBTITLE 1,Performing joins on deduplicated data (disease broadcasted)
print("Performing joins on deduplicated data...")

df_variants = df_clinical_deduped.join(
    df_impact_selected,
    on="variant_id",
    how="left"
).join(
    df_pharmacogene_selected,
    on="variant_id",
    how="left"
).join(
    F.broadcast(df_disease_selected),
    on="variant_id",
    how="left"
)

print("Variant features joined successfully")

# Verify no cartesian explosion
result_count = df_variants.count()
print(f"\nJoin result: {result_count:,} rows (should be ~4.1M)")

# COMMAND ----------

# DBTITLE 1,Handling missing values in variant dataset
print("Handling missing values in variant dataset...")

numeric_cols = [
    "review_quality_score", "mutation_severity_score", "pathogenicity_score",
    "combined_pathogenicity_risk", "phylop_score", "cadd_phred", "conservation_level",
    "gene_total_variants", "gene_pathogenic_count", "gene_benign_count", "gene_vus_count",
    "gene_pathogenic_ratio", "gene_benign_ratio", "gene_vus_ratio",
    "clinical_confidence_score", "evidence_strength", "conservation_composite",
    "clinical_actionability", 
    "phastcons_score", "gerp_score",
    "functional_impact_score", "domain_impact_count", "impact_severity_score",
    "druggability_score", "enhanced_druggability_score", "protein_function_count",
    "pharmacogene_evidence_score", "clinical_evidence_score",
    "disease_db_coverage", "disease_count", "disease_gene_count",
    "disease_pathogenic_ratio", "disease_category_count", "database_coverage_score",
    "disease_priority_score", "disease_burden_score"
]

boolean_cols = [
    "target_is_pathogenic", "target_is_benign", "target_is_vus",
    "clinical_sig_is_uncertain", "has_strong_evidence", "is_highly_conserved",
    "is_constrained", "is_high_impact", "is_very_high_impact",
    "is_loss_of_function", "affects_functional_domain", "is_domain_affecting",
    "has_kinase_domain", "high_confidence_deleterious",
    "is_pharmacogene", "is_cyp_gene", "is_drug_target", "is_kinase",
    "is_receptor", "is_transporter", "is_metabolizing_enzyme",
    "has_drug_interaction_potential", "is_clinical_pharmacogene",
    "high_impact_pharmacogene", "has_omim_disease", "has_mondo_disease",
    "has_orphanet_disease", "is_polygenic_disease", "is_clinically_actionable",
    "has_drug_development_potential", "is_cancer_gene_variant",
    "is_neurological_gene_variant", "is_cardiovascular_gene_variant",
    "is_metabolic_gene_variant", "is_rare_disease_gene_variant"
]

fill_dict = {}
for col in numeric_cols:
    fill_dict[col] = 0.0
for col in boolean_cols:
    fill_dict[col] = False

df_variants = df_variants.fillna(fill_dict)

print("Missing values handled")

# COMMAND ----------

# DBTITLE 1,Selecting ML-ready features for variants
print("Selecting ML-ready features for variants...")

variant_ml_features = [
    "variant_id",
    "gene_name",
    "official_gene_symbol",
    "chromosome",
    "position",
    "variant_type",
    "target_is_pathogenic",
    "target_is_benign",
    "target_is_vus",
    "clinical_confidence_score",
    "phylop_score",
    "cadd_phred",
    "conservation_composite",
    "is_high_impact",
    "is_very_high_impact",
    "is_loss_of_function",
    "affects_functional_domain",
    "domain_impact_count",
    "functional_impact_score",
    "high_confidence_deleterious",
    "is_pharmacogene",
    "is_cyp_gene",
    "is_drug_target",
    "druggability_score",
    "is_clinical_pharmacogene",
    "high_impact_pharmacogene",
    "has_omim_disease",
    "is_clinically_actionable",
    "disease_complexity",
    "disease_priority_score",
    "is_cancer_gene_variant",
    "is_neurological_gene_variant",
    "is_rare_disease_gene_variant"
]

df_variants_ml = df_variants.select(*variant_ml_features)

print(f"Features selected: {len(variant_ml_features)}")

# COMMAND ----------

# DBTITLE 1,Optimizing partition count for small dataset (4.1M rows)
print("Optimizing partition count for small dataset...")
print("Dataset: ~4.1M rows, ~1.9 GB compressed")

# For 4.1M rows, use 10 partitions (~410K rows each)
df_variants_ml = df_variants_ml.repartition(10)

print("Data repartitioned to 10 partitions")

# COMMAND ----------

# DBTITLE 1,Creating train/validation/test splits
print("Creating train/validation/test splits for variants (70/15/15)...")

df_variants_ml = df_variants_ml.withColumn("random_id", F.rand(seed=42))

df_variants_train = df_variants_ml.filter(F.col("random_id") < 0.70).drop("random_id")
df_variants_validation = df_variants_ml.filter((F.col("random_id") >= 0.70) & (F.col("random_id") < 0.85)).drop("random_id")
df_variants_test = df_variants_ml.filter(F.col("random_id") >= 0.85).drop("random_id")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Writing train set (optimized for small data)
print(f"\nWriting variant train set to {catalog_name}.gold.ml_dataset_variants_train...")

df_variants_train.coalesce(5).write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variants_train")

print("Train set written successfully")

# COMMAND ----------

# DBTITLE 1,Writing validation set
print(f"\nWriting variant validation set to {catalog_name}.gold.ml_dataset_variants_validation...")

df_variants_validation.coalesce(2).write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variants_validation")

print("Validation set written successfully")

# COMMAND ----------

# DBTITLE 1,Writing test set
print(f"\nWriting variant test set to {catalog_name}.gold.ml_dataset_variants_test...")

df_variants_test.coalesce(2).write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_variants_test")

print("Test set written successfully")

print("\nVariant ML datasets created successfully!")

# COMMAND ----------

# DBTITLE 1,Verifying variant datasets
print("\nVerifying variant datasets...")
train_count = spark.table(f"{catalog_name}.gold.ml_dataset_variants_train").count()
val_count = spark.table(f"{catalog_name}.gold.ml_dataset_variants_validation").count()
test_count = spark.table(f"{catalog_name}.gold.ml_dataset_variants_test").count()

print(f"Train: {train_count:,} records")
print(f"Validation: {val_count:,} records")
print(f"Test: {test_count:,} records")
print(f"Total: {train_count + val_count + test_count:,} records")

# COMMAND ----------

# DBTITLE 1,Checking class distribution in train set
print("Checking class distribution in train set...")
spark.table(f"{catalog_name}.gold.ml_dataset_variants_train").groupBy("target_is_pathogenic").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2: Structural Variants (Already unique - no deduplication needed)

# COMMAND ----------

# DBTITLE 1,PART 2: STRUCTURAL VARIANT DATASET
print("\n=== PART 2: STRUCTURAL VARIANT DATASET ===\n")

print("Preparing structural variant dataset...")

# COMMAND ----------

# DBTITLE 1,Handling missing values in structural variant dataset
print("Handling missing values in structural variant dataset...")

sv_numeric_cols = [
    "sv_size", "affected_gene_count", "complete_overlap_genes", "major_overlap_genes",
    "pharmacogenes_affected", "size_impact_score", "type_impact_score",
    "gene_impact_score", "sv_pathogenicity_score", "total_impact_score",
    "sv_priority_score"
]

sv_boolean_cols = [
    "has_gene_overlap", "is_multi_gene_sv", "affects_pharmacogenes",
    "affects_omim_genes", "is_high_risk_sv", "is_high_confidence_pathogenic",
    "clinical_relevance_flag", "dosage_sensitivity_flag"
]

sv_fill_dict = {}
for col in sv_numeric_cols:
    sv_fill_dict[col] = 0.0
for col in sv_boolean_cols:
    sv_fill_dict[col] = False

df_structural_clean = df_structural.fillna(sv_fill_dict)

print("Missing values handled")

# COMMAND ----------

# DBTITLE 1,Selecting ML-ready features for structural variants
print("Selecting ML-ready features for structural variants...")

sv_ml_features = [
    "sv_id",
    "study_id",
    "chromosome",
    "start_pos",
    "end_pos",
    "variant_type",
    "sv_type_class",
    "sv_size",
    "sv_size_category",
    "has_gene_overlap",
    "affected_gene_count",
    "is_multi_gene_sv",
    "affects_pharmacogenes",
    "affects_omim_genes",
    "gene_impact_severity",
    "size_impact_score",
    "type_impact_score",
    "gene_impact_score",
    "sv_pathogenicity_score",
    "predicted_sv_pathogenicity",
    "is_high_risk_sv",
    "total_impact_score",
    "gene_disruption_potential",
    "sv_priority_score",
    "is_high_confidence_pathogenic",
    "clinical_relevance_flag",
    "dosage_sensitivity_flag"
]

df_sv_ml = df_structural_clean.select(*sv_ml_features)

print(f"Features selected: {len(sv_ml_features)}")

# COMMAND ----------

# DBTITLE 1,Optimizing partition count for structural variants (217K rows)
print("Optimizing partition count for structural variants...")

# For 217K rows, use 5 partitions
df_sv_ml = df_sv_ml.repartition(5)

print("Data repartitioned to 5 partitions")

# COMMAND ----------

# DBTITLE 1,Creating train/validation/test splits for structural variants
print("Creating train/validation/test splits for structural variants (70/15/15)...")

df_sv_ml = df_sv_ml.withColumn("random_id", F.rand(seed=42))

df_sv_train = df_sv_ml.filter(F.col("random_id") < 0.70).drop("random_id")
df_sv_validation = df_sv_ml.filter((F.col("random_id") >= 0.70) & (F.col("random_id") < 0.85)).drop("random_id")
df_sv_test = df_sv_ml.filter(F.col("random_id") >= 0.85).drop("random_id")

print("Splits created")

# COMMAND ----------

# DBTITLE 1,Writing SV datasets
print(f"\nWriting SV train set to {catalog_name}.gold.ml_dataset_structural_variants_train...")
df_sv_train.coalesce(1).write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variants_train")
print("SV train set written")

print(f"\nWriting SV validation set to {catalog_name}.gold.ml_dataset_structural_variants_validation...")
df_sv_validation.coalesce(1).write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variants_validation")
print("SV validation set written")

print(f"\nWriting SV test set to {catalog_name}.gold.ml_dataset_structural_variants_test...")
df_sv_test.coalesce(1).write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variants_test")
print("SV test set written")

print("\nStructural variant ML datasets created successfully!")

# COMMAND ----------

# DBTITLE 1,Verifying structural variant datasets
print("\nVerifying structural variant datasets...")
sv_train_count = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_train").count()
sv_val_count = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_validation").count()
sv_test_count = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_test").count()

print(f"SV Train: {sv_train_count:,} records")
print(f"SV Validation: {sv_val_count:,} records")
print(f"SV Test: {sv_test_count:,} records")
print(f"SV Total: {sv_train_count + sv_val_count + sv_test_count:,} records")

# COMMAND ----------

# DBTITLE 1,Checking class distribution in SV train set
print("Checking class distribution in SV train set...")
spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_train").groupBy("is_high_risk_sv").count().show()

# COMMAND ----------

# DBTITLE 1,FINAL SUMMARY
print("\n=== FINAL SUMMARY ===\n")

print("VARIANT DATASETS (SNV/INDELS):")
print(f"  Train: {train_count:,}")
print(f"  Validation: {val_count:,}")
print(f"  Test: {test_count:,}")
print(f"  Features: {len(variant_ml_features)}")

print("\nSTRUCTURAL VARIANT DATASETS:")
print(f"  Train: {sv_train_count:,}")
print(f"  Validation: {sv_val_count:,}")
print(f"  Test: {sv_test_count:,}")
print(f"  Features: {len(sv_ml_features)}")

print("\n=== CRITICAL FIXES APPLIED ===")
print("  - Deduplication: 69M -> 4.1M unique variants (NO cartesian explosion)")
print("  - Broadcast join for disease table")
print("  - Optimal partitions: 10 for variants, 5 for SVs")
print("  - OptimizeWrite enabled")
print("\nAll ML datasets created successfully!")
print("Expected completion time: 5-8 minutes total")
