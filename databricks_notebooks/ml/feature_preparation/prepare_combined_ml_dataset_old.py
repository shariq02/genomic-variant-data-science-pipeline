# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE PREPARATION - COMBINED ML DATASET
# MAGIC ##### Module: Combine All ML Features for Training
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 05, 2026
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
import random

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

# DBTITLE 1,PART 1: VARIANT DATASET
print("\n=== PART 1: VARIANT DATASET (SNV/INDELS) - OPTIMIZED ===\n")

print("Preparing variant-level features for joining...")

# COMMAND ----------

# DBTITLE 1,Selecting columns from variant_impact table
print("Selecting columns from variant_impact table...")

df_impact_selected = df_variant_impact.select(
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

print("Variant impact columns selected (duplicates removed)")

# COMMAND ----------

# DBTITLE 1,Selecting columns from pharmacogene table
print("Selecting columns from pharmacogene table...")

df_pharmacogene_selected = df_pharmacogene.select(
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

# DBTITLE 1,Selecting columns from disease table
print("Selecting columns from disease table...")

df_disease_selected = df_disease.select(
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

# DBTITLE 1,Performing optimized joins (disease table broadcasted)
print("Performing optimized joins (disease table broadcasted)...")

# Drop duplicate columns from variant_impact (keep clinical versions)
df_variants = df_clinical.join(
    df_impact_selected.drop("conservation_composite", "phylop_score", 
                            "mutation_severity_score", "pathogenicity_score"),
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

print("Variant features joined successfully (computation deferred)")

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
    # From impact table (unique ones only):
    "phastcons_score", "gerp_score",
    "functional_impact_score", "domain_impact_count", "impact_severity_score",
    # From pharmacogene:
    "druggability_score", "enhanced_druggability_score", "protein_function_count",
    "pharmacogene_evidence_score", "clinical_evidence_score",
    # From disease:
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

# DBTITLE 1,Optimizing partition count before creating splits
print("Optimizing partition count before creating splits...")

df_variants_ml = df_variants_ml.coalesce(200)

print("Partitions coalesced to 200")

# COMMAND ----------

# DBTITLE 1,Creating train/validation/test splits for variants
print("Creating train/validation/test splits for variants (70/15/15)...")

df_variants_ml = df_variants_ml.withColumn("random_id", F.rand(seed=42))

df_variants_train = df_variants_ml.filter(F.col("random_id") < 0.70).drop("random_id")
df_variants_validation = df_variants_ml.filter((F.col("random_id") >= 0.70) & (F.col("random_id") < 0.85)).drop("random_id")
df_variants_test = df_variants_ml.filter(F.col("random_id") >= 0.85).drop("random_id")

print("Splits created (computation deferred until write)")

# COMMAND ----------

# DBTITLE 1,Variant ML dataset creation
print(f"\nWriting variant train set to {catalog_name}.gold.ml_dataset_variants_train...")
df_variants_train.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_variants_train")
print("Train set written")

print(f"\nWriting variant validation set to {catalog_name}.gold.ml_dataset_variants_validation...")
df_variants_validation.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_variants_validation")
print("Validation set written")

print(f"\nWriting variant test set to {catalog_name}.gold.ml_dataset_variants_test...")
df_variants_test.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_variants_test")
print("Test set written")

print("\nVariant ML datasets created successfully!")

# COMMAND ----------

# DBTITLE 1,Verifying class distribution in variant splits
print("\nVerifying variant datasets (counting after write)...")
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
# MAGIC no change below

# COMMAND ----------

# DBTITLE 1,PART 2: STRUCTURAL VARIANT DATASET
print("\n=== PART 2: STRUCTURAL VARIANT DATASET - OPTIMIZED ===\n")

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

# DBTITLE 1,Optimizing partition count for structural variants
print("Optimizing partition count for structural variants...")

df_sv_ml = df_sv_ml.coalesce(50)

print("Partitions coalesced to 50 (smaller dataset)")

# COMMAND ----------

# DBTITLE 1,Creating train/validation/test splits for structural variants
print("Creating train/validation/test splits for structural variants (70/15/15)...")

df_sv_ml = df_sv_ml.withColumn("random_id", F.rand(seed=42))

df_sv_train = df_sv_ml.filter(F.col("random_id") < 0.70).drop("random_id")
df_sv_validation = df_sv_ml.filter((F.col("random_id") >= 0.70) & (F.col("random_id") < 0.85)).drop("random_id")
df_sv_test = df_sv_ml.filter(F.col("random_id") >= 0.85).drop("random_id")

print("Splits created (computation deferred until write)")

# COMMAND ----------

# DBTITLE 1,Structural variant ML dataset creation
print(f"\nWriting SV train set to {catalog_name}.gold.ml_dataset_structural_variants_train...")
df_sv_train.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variants_train")
print("SV train set written")

print(f"\nWriting SV validation set to {catalog_name}.gold.ml_dataset_structural_variants_validation...")
df_sv_validation.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variants_validation")
print("SV validation set written")

print(f"\nWriting SV test set to {catalog_name}.gold.ml_dataset_structural_variants_test...")
df_sv_test.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_structural_variants_test")
print("SV test set written")

print("\nStructural variant ML datasets created successfully!")

# COMMAND ----------

# DBTITLE 1,Verifying class distribution in structural variant splits
print("\nVerifying structural variant datasets (counting after write)...")
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

print("\nAll ML datasets created successfully with optimizations!")
print("\nOptimizations applied:")
print("  - Broadcast join for disease table")
print("  - Deferred count operations")
print("  - Coalesced partitions (200 for variants, 50 for SVs)")
print("  - Early column selection")
print("  - Single fillna operations")
