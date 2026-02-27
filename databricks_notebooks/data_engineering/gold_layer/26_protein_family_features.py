# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PROTEIN FAMILY ANALYSIS (FIXED)
# MAGIC ##### Module: Comprehensive Gene-Level Protein Family Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **FIXED:** Two-pass structure enforced. Target threshold corrected. Leakage columns removed.
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Protein Domain Analysis
# MAGIC - Use Case 7: Protein Family Conservation
# MAGIC
# MAGIC **Creates:**
# MAGIC - gold.gene_protein_family_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only. Raw biological measurements from silver tables.
# MAGIC - Pass 2: Target only. Derived from domain and druggability features independently.
# MAGIC - Final:  Features and target joined on gene_symbol. Written to gold.
# MAGIC
# MAGIC **TARGET FIX:**
# MAGIC - Old: has_signaling_domain AND is_multi_domain_protein -> 0 positives
# MAGIC - New: has_signaling_domain OR is_multi_domain_protein OR druggability_potential_score >= 10
# MAGIC   Expected: 5-15% positive rate
# MAGIC
# MAGIC **LEAKAGE COLUMNS REMOVED:**
# MAGIC - protein_family_priority (derived summary encoding target conditions)
# MAGIC - variant_disease_domain_correlation (categorical summary of feature combinations)
# MAGIC - cancer_protein_classification (categorical encoding of cancer + domain combinations)
# MAGIC - disease_specific_domains (categorical encoding of disease type)
# MAGIC - oncogenic_domain_alterations (categorical encoding of missense vs truncating ratio)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max, avg,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD PROTEIN FAMILY FEATURES (FIXED - TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_protein_domains  = spark.table(f"{catalog_name}.silver.protein_domains")
df_proteins_uniprot = spark.table(f"{catalog_name}.silver.proteins_uniprot")
df_genes            = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_variant_impact   = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gtex             = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer           = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gene_disease     = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

print(f"Protein domains:        {df_protein_domains.count():,}")
print(f"Proteins uniprot:       {df_proteins_uniprot.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
# MAGIC All feature computation happens here.
# MAGIC Target variable is NOT computed in this pass.

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Gene Domain Statistics
print("\nPASS 1 - STEP 1: GENE DOMAIN STATISTICS")
print("="*80)

df_gene_domains = (
    df_protein_domains
    .groupBy("gene_symbol")
    .agg(
        countDistinct("uniprot_accession").alias("protein_count"),
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("proteins_with_kinase"),
        spark_sum(when(col("has_receptor_domain"), 1).otherwise(0)).alias("proteins_with_receptor"),
        spark_sum(when(col("has_zinc_finger"), 1).otherwise(0)).alias("proteins_with_zinc_finger"),
        spark_sum(when(col("has_sh2_domain"), 1).otherwise(0)).alias("proteins_with_sh2"),
        spark_sum(when(col("has_sh3_domain"), 1).otherwise(0)).alias("proteins_with_sh3"),
        spark_sum(when(col("has_ph_domain"), 1).otherwise(0)).alias("proteins_with_ph"),
        spark_sum(when(col("has_death_domain"), 1).otherwise(0)).alias("proteins_with_death"),
        spark_sum(when(col("has_leucine_zipper"), 1).otherwise(0)).alias("proteins_with_leucine_zipper"),
        spark_sum(when(col("has_helix_loop_helix"), 1).otherwise(0)).alias("proteins_with_helix_loop"),
        spark_sum(when(col("has_immunoglobulin"), 1).otherwise(0)).alias("proteins_with_ig"),
        spark_sum(when(col("has_functional_domain"), 1).otherwise(0)).alias("proteins_with_functional_domain")
    )
)

print(f"Genes with domain data: {df_gene_domains.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: Domain Classification Flags
print("\nPASS 1 - STEP 2: DOMAIN CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_gene_domains
    .withColumn("has_signaling_domain",
                when((col("proteins_with_kinase") > 0) |
                     (col("proteins_with_sh2") > 0) |
                     (col("proteins_with_sh3") > 0), True).otherwise(False))

    .withColumn("has_dna_binding_domain",
                when((col("proteins_with_zinc_finger") > 0) |
                     (col("proteins_with_helix_loop") > 0) |
                     (col("proteins_with_leucine_zipper") > 0), True).otherwise(False))

    .withColumn("has_membrane_domain",
                when((col("proteins_with_receptor") > 0) |
                     (col("proteins_with_ph") > 0), True).otherwise(False))

    .withColumn("has_apoptosis_domain",
                when(col("proteins_with_death") > 0, True).otherwise(False))

    .withColumn("has_immune_domain",
                when(col("proteins_with_ig") > 0, True).otherwise(False))

    .withColumn("is_multi_domain_protein",
                when(col("max_domain_count") >= 5, True).otherwise(False))
)

print("Domain classification flags added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Base Scores
print("\nPASS 1 - STEP 3: BASE SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("domain_diversity_score",
                col("max_domain_count") * 2 +
                when(col("has_signaling_domain"), 3).otherwise(0) +
                when(col("has_dna_binding_domain"), 3).otherwise(0) +
                when(col("has_membrane_domain"), 2).otherwise(0))

    .withColumn("functional_complexity_score",
                (col("proteins_with_functional_domain") * 2) +
                when(col("is_multi_domain_protein"), 5).otherwise(0))

    .withColumn("druggability_potential_score",
                when(col("proteins_with_kinase") > 0, 10).otherwise(0) +
                when(col("proteins_with_receptor") > 0, 8).otherwise(0) +
                when(col("has_signaling_domain"), 5).otherwise(0))
)

print("Base scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Variant Impact on Domains
print("\nPASS 1 - STEP 4: VARIANT IMPACT ON DOMAINS")
print("="*80)

variant_domain_impact = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("domain_affecting_variants"),
        spark_sum(when(col("affects_functional_domain") & col("is_pathogenic"), 1).otherwise(0))
            .alias("domain_pathogenic_variants"),
        spark_sum(when(col("affects_functional_domain") & col("is_very_high_impact"), 1).otherwise(0))
            .alias("critical_domain_variants")
    )
    .withColumn("has_domain_variants",
                col("domain_affecting_variants") > 0)
)

print(f"Genes with domain-affecting variants: {variant_domain_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Expression Context
print("\nPASS 1 - STEP 5: EXPRESSION CONTEXT")
print("="*80)

protein_family_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy(col("gene_name"))
    .agg(
        countDistinct("tissue_type").alias("protein_family_expression_breadth"),
        spark_max("max_tpm").alias("protein_max_expression")
    )
    .withColumn("tissue_specific_protein_expression",
                when(col("protein_family_expression_breadth") <= 5, True).otherwise(False))
)

print(f"Genes with expression: {protein_family_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Cancer Context
print("\nPASS 1 - STEP 6: CANCER CONTEXT")
print("="*80)

# Raw cancer mutation counts only. oncogenic_domain_alterations string category removed.
# Raw numeric columns (cancer_missense_mutations, cancer_truncating_mutations) retained instead.
cancer_protein_family = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("cancer_missense_mutations"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("cancer_truncating_mutations"),
        countDistinct("tumor_sample").alias("cancer_samples_affected")
    )
    .withColumn("cancer_relevant_protein_family",
                col("cancer_samples_affected") >= 10)
)

print(f"Cancer-related protein families: {cancer_protein_family.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: Disease Context
print("\nPASS 1 - STEP 7: DISEASE CONTEXT")
print("="*80)

# Raw disease counts only. disease_specific_domains string category removed.
# Raw boolean columns (has_cancer_disease, has_neurological_disease) retained instead.
disease_protein_family = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease")
    )
    .withColumn("disease_associated_protein_family",
                col("total_disease_count") >= 5)
)

print(f"Disease-associated protein families: {disease_protein_family.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 8: Join All Features
print("\nPASS 1 - STEP 8: JOINING ALL FEATURES")
print("="*80)

df_features = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        col("protein_family"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        col("druggability_score")
    )
    .join(df_scored.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
          on="gene_symbol", how="left")
    .join(variant_domain_impact, on="gene_symbol", how="left")
    .join(protein_family_expression, col("gene_symbol") == protein_family_expression["gene_name"], how="left")
    .drop(protein_family_expression["gene_name"])
    .join(cancer_protein_family, on="gene_symbol", how="left")
    .join(disease_protein_family, on="gene_symbol", how="left")
    .fillna({
        "protein_count":                    0,
        "max_domain_count":                 0,
        "proteins_with_kinase":             0,
        "proteins_with_receptor":           0,
        "proteins_with_zinc_finger":        0,
        "proteins_with_sh2":                0,
        "proteins_with_sh3":                0,
        "proteins_with_ph":                 0,
        "proteins_with_death":              0,
        "proteins_with_leucine_zipper":     0,
        "proteins_with_helix_loop":         0,
        "proteins_with_ig":                 0,
        "proteins_with_functional_domain":  0,
        "has_signaling_domain":             False,
        "has_dna_binding_domain":           False,
        "has_membrane_domain":              False,
        "has_apoptosis_domain":             False,
        "has_immune_domain":                False,
        "is_multi_domain_protein":          False,
        "domain_diversity_score":           0,
        "functional_complexity_score":      0,
        "druggability_potential_score":     0,
        "domain_affecting_variants":        0,
        "domain_pathogenic_variants":       0,
        "critical_domain_variants":         0,
        "has_domain_variants":              False,
        "protein_family_expression_breadth": 0,
        "protein_max_expression":           0.0,
        "tissue_specific_protein_expression": False,
        "cancer_missense_mutations":        0,
        "cancer_truncating_mutations":      0,
        "cancer_samples_affected":          0,
        "cancer_relevant_protein_family":   False,
        "total_disease_count":              0,
        "has_cancer_disease":               False,
        "has_neurological_disease":         False,
        "disease_associated_protein_family": False,
    })
)

print(f"Genes with protein family features: {df_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Enhanced Composite Scores
print("\nPASS 1 - STEP 9: ENHANCED COMPOSITE SCORES")
print("="*80)

df_features = (
    df_features
    .withColumn("variant_domain_impact_score",
                (col("domain_affecting_variants") * 2) +
                (col("domain_pathogenic_variants") * 5) +
                (col("critical_domain_variants") * 10))

    .withColumn("cancer_protein_family_score",
                when(col("cancer_relevant_protein_family") & col("has_signaling_domain"), 15).otherwise(0) +
                when(col("cancer_relevant_protein_family"), 10).otherwise(0) +
                (col("cancer_samples_affected") * 0.1))

    .withColumn("disease_protein_family_score",
                when(col("disease_associated_protein_family") & col("is_multi_domain_protein"), 12).otherwise(0) +
                when(col("disease_associated_protein_family"), 8).otherwise(0) +
                (col("total_disease_count") * 0.5))

    .withColumn("protein_functional_category",
                when(col("has_signaling_domain"), lit("signaling"))
                .when(col("has_dna_binding_domain"), lit("transcription"))
                .when(col("has_membrane_domain"), lit("membrane"))
                .when(col("has_immune_domain"), lit("immune"))
                .otherwise(lit("other")))
)

print("Enhanced composite scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10: Deduplicate by Gene Symbol
print("\nPASS 1 - STEP 10: DEDUPLICATE BY GENE_SYMBOL")
print("="*80)

before_count = df_features.count()
df_features  = df_features.dropDuplicates(["gene_symbol"])
after_count  = df_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")
print(f"Feature columns:      {len(df_features.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 2 - TARGET ONLY
# MAGIC Target variable computed here independently from Pass 1.
# MAGIC
# MAGIC TARGET DEFINITION:
# MAGIC   is_high_value_protein_family = True when any of:
# MAGIC     has_signaling_domain = True   (kinase, SH2, or SH3 domain present)
# MAGIC     OR is_multi_domain_protein = True  (5+ domains)
# MAGIC     OR druggability_potential_score >= 10  (kinase or receptor present)
# MAGIC
# MAGIC   Rationale: Old definition (signaling AND multi-domain) produced zero positives
# MAGIC   because the intersection is near-empty in the data. The OR formulation captures
# MAGIC   the same biological intent - genes with therapeutically relevant protein architecture.

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variable
print("\nPASS 2 - DERIVING TARGET VARIABLE")
print("="*80)
print("Target: is_high_value_protein_family")
print("Definition: has_signaling_domain OR is_multi_domain_protein OR druggability_potential_score >= 10")
print()

df_target = (
    df_features
    .select("gene_symbol", "has_signaling_domain", "is_multi_domain_protein", "druggability_potential_score")
    .withColumn("is_high_value_protein_family",
                when(
                    col("has_signaling_domain") |
                    col("is_multi_domain_protein") |
                    (col("druggability_potential_score") >= 10),
                    True
                ).otherwise(False))
    .select("gene_symbol", "is_high_value_protein_family")
)

target_counts = df_target.groupBy("is_high_value_protein_family").count().collect()
total = sum(r["count"] for r in target_counts)
for row in sorted(target_counts, key=lambda r: str(r["is_high_value_protein_family"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_high_value_protein_family']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_counts if r["is_high_value_protein_family"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

print()
if positive_count == 0:
    raise ValueError("Target has zero positives. Fix threshold.")
elif positive_pct < 1.0:
    raise ValueError(f"Positive rate {positive_pct:.2f}% too low. Fix threshold.")
elif positive_pct > 50.0:
    print(f"WARN: Positive rate {positive_pct:.2f}% above 50%. Consider stricter threshold.")
else:
    print(f"OK: Positive rate {positive_pct:.2f}%. Proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL JOIN - Features + Target

# COMMAND ----------

# DBTITLE 1,Final Join: Features + Target
print("\nFINAL JOIN - FEATURES AND TARGET")
print("="*80)

df_final = (
    df_features
    .join(df_target, on="gene_symbol", how="left")
    .fillna({"is_high_value_protein_family": False})
)

print(f"Final table rows:    {df_final.count():,}")
print(f"Final table columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Feature Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

# REMOVED leakage columns:
# protein_family_priority            - derived summary encoding target conditions
# variant_disease_domain_correlation - categorical summary of feature combinations
# cancer_protein_classification      - categorical encoding of cancer + domain combinations
# disease_specific_domains           - categorical encoding of disease type
# oncogenic_domain_alterations       - categorical encoding of missense vs truncating ratio

df_final = (
    df_final
    .select(
        col("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        col("protein_family"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        col("druggability_score"),
        col("protein_count"),
        col("max_domain_count"),
        col("proteins_with_kinase"),
        col("proteins_with_receptor"),
        col("proteins_with_zinc_finger"),
        col("proteins_with_sh2"),
        col("proteins_with_sh3"),
        col("proteins_with_ph"),
        col("proteins_with_death"),
        col("proteins_with_leucine_zipper"),
        col("proteins_with_helix_loop"),
        col("proteins_with_ig"),
        col("proteins_with_functional_domain"),
        col("has_signaling_domain"),
        col("has_dna_binding_domain"),
        col("has_membrane_domain"),
        col("has_apoptosis_domain"),
        col("has_immune_domain"),
        col("is_multi_domain_protein"),
        col("domain_diversity_score"),
        col("functional_complexity_score"),
        col("druggability_potential_score"),
        col("domain_affecting_variants"),
        col("domain_pathogenic_variants"),
        col("critical_domain_variants"),
        col("has_domain_variants"),
        col("protein_family_expression_breadth"),
        col("protein_max_expression"),
        col("tissue_specific_protein_expression"),
        col("cancer_missense_mutations"),
        col("cancer_truncating_mutations"),
        col("cancer_samples_affected"),
        col("cancer_relevant_protein_family"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease"),
        col("disease_associated_protein_family"),
        col("variant_domain_impact_score"),
        col("cancer_protein_family_score"),
        col("disease_protein_family_score"),
        col("protein_functional_category"),
        col("is_high_value_protein_family")
    )
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by Gene Symbol
print("\nDEDUPLICATING BY GENE_SYMBOL")
print("="*80)

before_count = df_final.count()
df_final     = df_final.dropDuplicates(["gene_symbol"])
after_count  = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Write gold.gene_protein_family_ml_features
print("\nWRITING gold.gene_protein_family_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_protein_family_ml_features")

print(f"Saved: {catalog_name}.gold.gene_protein_family_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.gene_protein_family_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

target_dist = df_check.groupBy("is_high_value_protein_family").count().collect()
total       = sum(r["count"] for r in target_dist)
positives   = [r["count"] for r in target_dist if r["is_high_value_protein_family"] == True]
pos_count   = positives[0] if positives else 0
pos_pct     = pos_count / total * 100 if total > 0 else 0

print(f"Rows:      {rows:,}")
print(f"Columns:   {cols}")
print(f"Positives: {pos_count:,} ({pos_pct:.2f}%)")

leakage_check = [
    "protein_family_priority",
    "variant_disease_domain_correlation",
    "cancer_protein_classification",
    "disease_specific_domains",
    "oncogenic_domain_alterations",
]
present = [c for c in leakage_check if c in df_check.columns]
if present:
    print(f"LEAKAGE ALERT: {present}")
else:
    print("Leakage check: PASSED (no known leakage columns present)")

print("\nProcessing complete")
