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
# MAGIC - ml_disease_features (disease_id)
# MAGIC - ml_pharmacogene_features (variant_id)
# MAGIC - ml_variant_impact_features (variant_id)
# MAGIC - ml_structural_variant_features (sv_id)
# MAGIC
# MAGIC **Output:** 
# MAGIC - ml_dataset_train
# MAGIC - ml_dataset_validation
# MAGIC - ml_dataset_test

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

catalog_name = "workspace"

# COMMAND ----------

print("Loading all ML feature tables...")

df_clinical = spark.table(f"{catalog_name}.gold.ml_clinical_features")
df_disease = spark.table(f"{catalog_name}.gold.ml_disease_features")
df_pharmacogene = spark.table(f"{catalog_name}.gold.ml_pharmacogene_features")
df_variant_impact = spark.table(f"{catalog_name}.gold.ml_variant_impact_features")
df_structural = spark.table(f"{catalog_name}.gold.ml_structural_variant_features")

print(f"Clinical features: {df_clinical.count():,} records")
print(f"Disease features: {df_disease.count():,} records")
print(f"Pharmacogene features: {df_pharmacogene.count():,} records")
print(f"Variant impact features: {df_variant_impact.count():,} records")
print(f"Structural variant features: {df_structural.count():,} records")

# COMMAND ----------

print("Combining variant-level features (clinical, pharmacogene, variant_impact)...")

df_variants = df_clinical.join(
    df_variant_impact.drop("gene_symbol"),
    on="variant_id",
    how="left"
).join(
    df_pharmacogene.drop("gene_symbol"),
    on="variant_id",
    how="left"
)

print(f"Combined variant features: {df_variants.count():,} records")

# COMMAND ----------

print("Adding variant type identifier...")

df_variants = df_variants.withColumn("variant_type", F.lit("snv_indel"))
df_structural_typed = df_structural.withColumn("variant_type", F.lit("structural"))

# COMMAND ----------

print("Aligning structural variants schema with variant schema...")

df_structural_aligned = df_structural_typed.select(
    F.col("sv_id").alias("variant_id"),
    F.col("genes_affected_list").alias("gene_symbol"),
    F.lit(None).cast("string").alias("clinical_significance"),
    F.col("predicted_high_impact").alias("pathogenic_flag"),
    F.lit(False).alias("benign_flag"),
    F.lit(False).alias("vus_flag"),
    F.lit(False).alias("conflicting_flag"),
    F.lit(False).alias("risk_factor_flag"),
    F.lit(False).alias("drug_response_flag"),
    F.lit(None).cast("string").alias("review_status"),
    F.lit(0).alias("stars"),
    F.lit(False).alias("has_assertion_criteria"),
    F.lit(False).alias("no_assertion_provided"),
    F.lit(False).alias("expert_panel_review"),
    F.lit(False).alias("practice_guideline_review"),
    F.lit(0).alias("submission_count"),
    F.lit(0).alias("submitter_count"),
    F.lit(None).cast("string").alias("first_in_clinvar_date"),
    F.lit(None).cast("string").alias("last_updated_clinvar_date"),
    F.lit(0.0).alias("conservation_phylop"),
    F.lit(0.0).alias("conservation_cadd"),
    F.when(F.col("genes_affected_count") > 1, True).otherwise(False).alias("has_multiple_genes"),
    F.col("genes_affected_count").alias("gene_count"),
    F.lit(0).alias("clinical_confidence_score"),
    F.lit(0).alias("evidence_strength"),
    F.lit(0.0).alias("conservation_composite"),
    F.lit(0).alias("clinical_actionability"),
    F.lit(None).cast("string").alias("consequence_type"),
    F.col("predicted_high_impact").alias("is_high_impact"),
    F.col("predicted_moderate_impact").alias("is_moderate_impact"),
    F.lit(False).alias("is_low_impact"),
    F.lit(False).alias("is_lof"),
    F.col("overlaps_exon").alias("affects_protein_domain"),
    F.lit(False).alias("affects_dna_binding"),
    F.lit(False).alias("affects_kinase_domain"),
    F.lit(False).alias("affects_signal_peptide"),
    F.lit(0.0).alias("conservation_phastcons"),
    F.lit(0.0).alias("conservation_gerp"),
    F.lit(False).alias("in_conserved_region"),
    F.lit(False).alias("high_conservation_flag"),
    F.lit(0).alias("domain_impact_count"),
    F.col("sv_impact_score").alias("impact_severity_score"),
    F.col("sv_priority_score").alias("functional_impact_score"),
    F.col("is_high_confidence_pathogenic").alias("high_confidence_deleterious"),
    F.lit("not_conserved").alias("conservation_category"),
    F.lit(None).cast("string").alias("drug_name"),
    F.lit(None).cast("string").alias("drug_class"),
    F.lit(False).alias("is_cyp_gene"),
    F.lit(False).alias("is_drug_target"),
    F.lit(False).alias("is_kinase"),
    F.lit(False).alias("is_receptor"),
    F.lit(False).alias("is_transporter"),
    F.lit(False).alias("is_metabolic_enzyme"),
    F.lit(False).alias("affects_drug_metabolism"),
    F.lit(False).alias("affects_drug_response"),
    F.lit(False).alias("affects_drug_toxicity"),
    F.lit(False).alias("domain_affecting"),
    F.lit(None).cast("string").alias("domain_type"),
    F.lit(0.0).alias("interaction_score"),
    F.lit(None).cast("string").alias("evidence_level"),
    F.lit(False).alias("clinical_annotation_exists"),
    F.lit(False).alias("fda_label_exists"),
    F.lit(False).alias("guideline_exists"),
    F.lit(0).alias("protein_function_count"),
    F.lit(0).alias("drug_impact_count"),
    F.lit(0).alias("clinical_evidence_score"),
    F.lit("low").alias("pharmacogene_priority"),
    F.lit(0.0).alias("druggability_score"),
    F.lit(False).alias("high_impact_pharmacogene"),
    F.col("sv_type"),
    F.col("sv_length"),
    F.col("sv_size_category"),
    F.col("gene_disruption_potential"),
    F.col("is_high_confidence_pathogenic"),
    F.col("frequency_category"),
    F.col("dosage_sensitivity_flag"),
    "variant_type"
)

# COMMAND ----------

print("Unioning variant and structural variant datasets...")

df_combined = df_variants.unionByName(df_structural_aligned, allowMissingColumns=True)

print(f"Total combined records: {df_combined.count():,}")

# COMMAND ----------

print("Handling missing values in combined dataset...")

numeric_cols = [
    "stars", "submission_count", "submitter_count", "conservation_phylop", 
    "conservation_cadd", "gene_count", "clinical_confidence_score", 
    "evidence_strength", "conservation_composite", "clinical_actionability",
    "conservation_phastcons", "conservation_gerp", "domain_impact_count",
    "impact_severity_score", "functional_impact_score", "interaction_score",
    "protein_function_count", "drug_impact_count", "clinical_evidence_score",
    "druggability_score"
]

boolean_cols = [
    "pathogenic_flag", "benign_flag", "vus_flag", "conflicting_flag",
    "risk_factor_flag", "drug_response_flag", "has_assertion_criteria",
    "no_assertion_provided", "expert_panel_review", "practice_guideline_review",
    "has_multiple_genes", "is_high_impact", "is_moderate_impact", "is_low_impact",
    "is_lof", "affects_protein_domain", "affects_dna_binding", 
    "affects_kinase_domain", "affects_signal_peptide", "in_conserved_region",
    "high_conservation_flag", "high_confidence_deleterious", "is_cyp_gene",
    "is_drug_target", "is_kinase", "is_receptor", "is_transporter",
    "is_metabolic_enzyme", "affects_drug_metabolism", "affects_drug_response",
    "affects_drug_toxicity", "domain_affecting", "clinical_annotation_exists",
    "fda_label_exists", "guideline_exists", "high_impact_pharmacogene"
]

fill_dict = {}
for col in numeric_cols:
    fill_dict[col] = 0.0
for col in boolean_cols:
    fill_dict[col] = False

df_combined = df_combined.fillna(fill_dict)

# COMMAND ----------

print("Selecting ML-ready features...")

ml_features = [
    "variant_id",
    "gene_symbol",
    "variant_type",
    "pathogenic_flag",
    "benign_flag",
    "vus_flag",
    "clinical_confidence_score",
    "conservation_phylop",
    "conservation_cadd",
    "conservation_phastcons",
    "conservation_gerp",
    "conservation_composite",
    "is_high_impact",
    "is_moderate_impact",
    "is_lof",
    "affects_protein_domain",
    "domain_impact_count",
    "impact_severity_score",
    "functional_impact_score",
    "high_confidence_deleterious",
    "is_cyp_gene",
    "is_drug_target",
    "affects_drug_metabolism",
    "clinical_evidence_score",
    "druggability_score",
    "high_impact_pharmacogene"
]

df_ml_dataset = df_combined.select(*ml_features)

print(f"ML dataset records: {df_ml_dataset.count():,}")
print(f"Features selected: {len(ml_features)}")

# COMMAND ----------

print("Creating train/validation/test splits (70/15/15)...")

df_ml_dataset = df_ml_dataset.withColumn("random_id", F.rand(seed=42))

df_train = df_ml_dataset.filter(F.col("random_id") < 0.70).drop("random_id")
df_validation = df_ml_dataset.filter((F.col("random_id") >= 0.70) & (F.col("random_id") < 0.85)).drop("random_id")
df_test = df_ml_dataset.filter(F.col("random_id") >= 0.85).drop("random_id")

print(f"Train set: {df_train.count():,} records")
print(f"Validation set: {df_validation.count():,} records")
print(f"Test set: {df_test.count():,} records")

# COMMAND ----------

print("Verifying class distribution in splits...")

for df_split, split_name in [(df_train, "Train"), (df_validation, "Validation"), (df_test, "Test")]:
    print(f"\n{split_name} Set Distribution:")
    df_split.groupBy("pathogenic_flag").count().show()

# COMMAND ----------

print(f"Writing train set to {catalog_name}.gold.ml_dataset_train...")
df_train.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_train")

print(f"Writing validation set to {catalog_name}.gold.ml_dataset_validation...")
df_validation.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_validation")

print(f"Writing test set to {catalog_name}.gold.ml_dataset_test...")
df_test.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_dataset_test")

print("Combined ML dataset creation complete!")

# COMMAND ----------

print("Final verification:")
print(f"Train: {spark.table(f'{catalog_name}.gold.ml_dataset_train').count():,}")
print(f"Validation: {spark.table(f'{catalog_name}.gold.ml_dataset_validation').count():,}")
print(f"Test: {spark.table(f'{catalog_name}.gold.ml_dataset_test').count():,}")
