# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PHARMACOGENE ANALYSIS (FIXED)
# MAGIC ##### Module: Comprehensive Gene-Level Pharmacogene Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **FIXED:** Two-pass structure enforced. Inverted target corrected. Leakage columns removed.
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 14: Drug Target Identification
# MAGIC
# MAGIC **Creates:** gold.gene_pharmacogene_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only. Raw biological measurements from silver tables.
# MAGIC - Pass 2: Target only. Derived from pharmacogene evidence independently.
# MAGIC - Final:  Features and target joined on gene_symbol. Written to gold.
# MAGIC
# MAGIC **TARGET FIX:**
# MAGIC - Old: is_high_priority_pharmacogene derived from pharmacogene_priority which used
# MAGIC   clinical_utility_score >= 15/20. clinical_utility_score included has_pharmgkb_annotation
# MAGIC   (a leakage flag) making 95.6% positive. Inverted target.
# MAGIC - New: is_high_priority_pharmacogene = drug_relationships >= 5 AND druggability_score >= 0.5
# MAGIC   Pure biological signal. Expected 10-25% positive rate.
# MAGIC
# MAGIC **LEAKAGE COLUMNS REMOVED:**
# MAGIC - pharmacogene_priority (derived summary encoding target conditions)
# MAGIC - pharmacogene_category_enhanced (categorical summary of feature combinations)
# MAGIC - clinical_actionability_tier (re-encoding of pharmacogene_priority)
# MAGIC - variant_impact_burden (categorical encoding of pathogenic_variants)
# MAGIC - drug_metabolism_tissue_expression (categorical encoding of liver/kidney flags)
# MAGIC - expression_breadth (categorical encoding of tissues_expressed_count)
# MAGIC - cancer_mutation_burden (categorical encoding of unique_tumor_samples)
# MAGIC - primary_indication_category (categorical encoding of disease type flags)
# MAGIC - has_pharmgkb_annotation (boolean derived from source_count - leakage into target via clinical_utility_score)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max,
    when, lit, trim, upper, lower, coalesce, array_contains, split, size
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD PHARMACOGENE FEATURES (FIXED - TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_pharmgkb_genes  = spark.table(f"{catalog_name}.silver.pharmgkb_genes")
df_relationships   = spark.table(f"{catalog_name}.silver.pharmgkb_relationships")
df_genes           = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_variant_impact  = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gtex            = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer          = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gene_disease    = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")

print(f"PharmGKB genes:         {df_pharmgkb_genes.count():,}")
print(f"PharmGKB relationships: {df_relationships.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")
print(f"Protein domains:        {df_protein_domains.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
# MAGIC All feature computation happens here.
# MAGIC Target variable is NOT computed in this pass.

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Gene Relationships from PharmGKB
print("\nPASS 1 - STEP 1: GENE RELATIONSHIPS")
print("="*80)

df_gene_relationships = (
    df_relationships
    .filter(col("entity1_type") == "Gene")
    .select(
        col("entity1_name").alias("gene_symbol"),
        col("entity2_type").alias("related_entity_type"),
        col("entity2_name").alias("related_entity_name"),
        col("evidence")
    )
)

df_relationship_counts = (
    df_gene_relationships
    .groupBy("gene_symbol")
    .agg(
        count("*").alias("total_relationships"),
        countDistinct("related_entity_type").alias("entity_type_count"),
        spark_sum(when(col("related_entity_type") == "Chemical", 1).otherwise(0)).alias("drug_relationships"),
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_relationships"),
        spark_sum(when(col("related_entity_type") == "Variant", 1).otherwise(0)).alias("variant_relationships"),
        spark_sum(when(col("evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count")
    )
)

print(f"Genes with relationships: {df_relationship_counts.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: Variant-Level Pharmacogene Impact
print("\nPASS 1 - STEP 2: VARIANT-LEVEL PHARMACOGENE IMPACT")
print("="*80)

gene_variant_stats = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("total_gene_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_variants"),
        spark_sum(when(col("is_missense_variant"), 1).otherwise(0)).alias("missense_variants"),
        spark_sum(when(col("is_loss_of_function"), 1).otherwise(0)).alias("lof_variants"),
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("domain_affecting_variants"),
        avg("pathogenicity_score").alias("avg_pathogenicity_score")
    )
    .withColumn("has_pharmacogene_variants",
                col("total_gene_variants") > 0)
)

print(f"Genes with variant statistics: {gene_variant_stats.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Expression Data
print("\nPASS 1 - STEP 3: EXPRESSION DATA")
print("="*80)

# Raw numeric expression features only.
# drug_metabolism_tissue_expression and expression_breadth string categories removed.
# Raw boolean flags (is_liver_expressed, is_kidney_expressed) and numeric counts retained instead.
gene_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy(col("gene_name"))
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("max_tpm").alias("max_expression_tpm"),
        avg("max_tpm").alias("avg_expression_tpm")
    )
    .join(
        df_gtex.filter((col("tissue_type") == "Liver") & (col("max_tpm") > 1.0))
               .select(col("gene_name").alias("liver_gene"), lit(True).alias("is_liver_expressed")),
        col("gene_name") == col("liver_gene"),
        "left"
    )
    .drop("liver_gene")
    .join(
        df_gtex.filter((col("tissue_type") == "Kidney") & (col("max_tpm") > 1.0))
               .select(col("gene_name").alias("kidney_gene"), lit(True).alias("is_kidney_expressed")),
        col("gene_name") == col("kidney_gene"),
        "left"
    )
    .drop("kidney_gene")
    .fillna({"is_liver_expressed": False, "is_kidney_expressed": False})
)

print(f"Genes with expression data: {gene_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Cancer Context
print("\nPASS 1 - STEP 4: CANCER CONTEXT")
print("="*80)

# Raw cancer counts only. cancer_mutation_burden string category removed.
# unique_tumor_samples retained as raw numeric feature instead.
cancer_genes = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("unique_tumor_samples")
    )
    .withColumn("is_oncology_drug_target",
                col("cancer_mutation_count") >= 50)
)

print(f"Genes with cancer data: {cancer_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Disease Associations
print("\nPASS 1 - STEP 5: DISEASE ASSOCIATIONS")
print("="*80)

# Raw disease boolean flags only. primary_indication_category string category removed.
# Raw boolean columns (has_cancer_disease, has_cardiovascular_disease etc.) retained instead.
disease_genes = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease")
    )
)

print(f"Genes with disease associations: {disease_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Protein Domain Complexity
print("\nPASS 1 - STEP 6: PROTEIN DOMAIN COMPLEXITY")
print("="*80)

protein_complexity = (
    df_protein_domains
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("has_kinase_domain_count")
    )
    .withColumn("is_complex_drug_target",
                col("max_domain_count") >= 5)
)

print(f"Genes with protein domain data: {protein_complexity.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: PharmGKB Gene Data
print("\nPASS 1 - STEP 7: PHARMGKB GENE DATA")
print("="*80)

df_pharmgkb_features = (
    df_pharmgkb_genes
    .select(
        upper(trim(col("gene_symbol"))).alias("gene_symbol"),
        col("gene_name").alias("pharmgkb_name"),
        col("source_count")
    )
    .join(df_relationship_counts, on="gene_symbol", how="left")
)

df_pharmgkb_with_data = df_pharmgkb_features.filter(
    (col("source_count").isNotNull()) &
    (col("total_relationships").isNotNull()) &
    (col("total_relationships") > 0)
)

print(f"PharmGKB genes with relationships: {df_pharmgkb_with_data.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 8: Join All Features
print("\nPASS 1 - STEP 8: JOINING ALL FEATURES")
print("="*80)

df_features = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name").alias("gene_full_name"),
        col("description"),
        col("chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transporter"),
        col("is_metabolic"),
        col("is_pharmacogene"),
        col("druggability_score"),
        col("pharmacogene_category"),
        col("drug_metabolism_role")
    )
    .join(df_pharmgkb_with_data, on="gene_symbol", how="inner")
    .join(gene_variant_stats, on="gene_symbol", how="left")
    .join(gene_expression, col("gene_symbol") == gene_expression["gene_name"], how="left")
    .join(cancer_genes, on="gene_symbol", how="left")
    .join(disease_genes, on="gene_symbol", how="left")
    .join(protein_complexity, on="gene_symbol", how="left")
    .drop(gene_expression["gene_name"])
    .drop(disease_genes["gene_symbol"])
    .fillna({
        "total_gene_variants":         0,
        "pathogenic_variants":         0,
        "missense_variants":           0,
        "lof_variants":                0,
        "domain_affecting_variants":   0,
        "avg_pathogenicity_score":     0.0,
        "has_pharmacogene_variants":   False,
        "tissues_expressed_count":     0,
        "max_expression_tpm":          0.0,
        "avg_expression_tpm":          0.0,
        "is_liver_expressed":          False,
        "is_kidney_expressed":         False,
        "cancer_mutation_count":       0,
        "unique_tumor_samples":        0,
        "is_oncology_drug_target":     False,
        "total_disease_count":         0,
        "has_cancer_disease":          False,
        "has_cardiovascular_disease":  False,
        "has_neurological_disease":    False,
        "has_metabolic_disease":       False,
        "max_domain_count":            0,
        "has_kinase_domain_count":     0,
        "is_complex_drug_target":      False,
    })
)

print(f"Gene pharmacogene joined: {df_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Classification Flags
print("\nPASS 1 - STEP 9: CLASSIFICATION FLAGS")
print("="*80)

df_features = (
    df_features
    .withColumn("is_drug_metabolizer",
                when(col("is_metabolic") & (col("drug_relationships") > 0), True).otherwise(False))

    .withColumn("is_drug_transporter_gene",
                when(col("is_transporter") & (col("drug_relationships") > 0), True).otherwise(False))

    .withColumn("is_drug_target_gene",
                when((col("is_kinase") | col("is_receptor") | col("is_enzyme")) &
                     (col("drug_relationships") > 0), True).otherwise(False))

    .withColumn("has_high_druggability",
                when(col("druggability_score") >= 0.7, True).otherwise(False))

    .withColumn("is_hepatic_metabolizer",
                col("is_liver_expressed") & col("is_drug_metabolizer"))

    .withColumn("is_renal_transporter",
                col("is_kidney_expressed") & col("is_drug_transporter_gene"))

    .withColumn("is_validated_cancer_target",
                col("is_oncology_drug_target") &
                (col("is_kinase") | col("is_receptor")))
)

print("Classification flags added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10: Comprehensive Scores
print("\nPASS 1 - STEP 10: COMPREHENSIVE SCORES")
print("="*80)

df_features = (
    df_features
    .withColumn("pharmacogene_evidence_score",
                coalesce(col("evidence_count"), lit(0)) +
                when(col("source_count").isNotNull(), 5).otherwise(0) +
                when(col("has_high_druggability"), 3).otherwise(0) +
                when(col("has_pharmacogene_variants"), 2).otherwise(0))

    .withColumn("drug_interaction_score",
                coalesce(col("drug_relationships"), lit(0)) * 2 +
                coalesce(col("evidence_count"), lit(0)) +
                when(col("is_liver_expressed") | col("is_kidney_expressed"), 3).otherwise(0) +
                when(col("pathogenic_variants") > 0, 2).otherwise(0))

    .withColumn("clinical_utility_score",
                when(col("source_count").isNotNull(), 10).otherwise(0) +
                when(col("has_high_druggability"), 5).otherwise(0) +
                (coalesce(col("drug_relationships"), lit(0)) * 0.5) +
                when(col("total_disease_count") >= 5, 5).otherwise(0) +
                when(col("is_oncology_drug_target"), 5).otherwise(0))

    .withColumn("pharmacogene_variant_impact_score",
                (coalesce(col("pathogenic_variants"), lit(0)) * 3) +
                (coalesce(col("domain_affecting_variants"), lit(0)) * 2) +
                coalesce(col("lof_variants"), lit(0)))

    .withColumn("metabolism_context_score",
                when(col("is_hepatic_metabolizer"), 10).otherwise(0) +
                when(col("is_renal_transporter"), 8).otherwise(0) +
                when(col("is_liver_expressed"), 3).otherwise(0))
)

print("Comprehensive scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 11: Deduplicate by Gene Symbol
print("\nPASS 1 - STEP 11: DEDUPLICATE BY GENE_SYMBOL")
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
# MAGIC   is_high_priority_pharmacogene = True when:
# MAGIC     drug_relationships >= 5 AND druggability_score >= 0.5
# MAGIC
# MAGIC   Rationale: Old definition derived target from clinical_utility_score which
# MAGIC   included has_pharmgkb_annotation. Since ALL rows in this table come from an
# MAGIC   inner join with PharmGKB data, has_pharmgkb_annotation was True for 95.6%
# MAGIC   of rows, making the target effectively inverted (near-constant).
# MAGIC   New definition uses raw biological signal only: sufficient drug relationships
# MAGIC   (evidence of pharmacogenomic activity) AND meaningful druggability score.

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variable
print("\nPASS 2 - DERIVING TARGET VARIABLE")
print("="*80)
print("Target: is_high_priority_pharmacogene")
print("Definition: drug_relationships >= 5 AND druggability_score >= 0.5")
print()

df_target = (
    df_features
    .select("gene_symbol", "drug_relationships", "druggability_score")
    .withColumn("is_high_priority_pharmacogene",
                when(
                    (coalesce(col("drug_relationships"), lit(0)) >= 5) &
                    (coalesce(col("druggability_score"), lit(0.0)) >= 0.5),
                    True
                ).otherwise(False))
    .select("gene_symbol", "is_high_priority_pharmacogene")
)

target_counts = df_target.groupBy("is_high_priority_pharmacogene").count().collect()
total = sum(r["count"] for r in target_counts)
for row in sorted(target_counts, key=lambda r: str(r["is_high_priority_pharmacogene"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_high_priority_pharmacogene']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_counts if r["is_high_priority_pharmacogene"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

print()
if positive_count == 0:
    raise ValueError("Target has zero positives. Fix threshold.")
elif positive_pct < 1.0:
    raise ValueError(f"Positive rate {positive_pct:.2f}% too low. Fix threshold.")
elif positive_pct > 90.0:
    raise ValueError(f"Positive rate {positive_pct:.2f}% too high. Target still inverted. Fix threshold.")
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
    .fillna({"is_high_priority_pharmacogene": False})
)

print(f"Final table rows:    {df_final.count():,}")
print(f"Final table columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Feature Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

# REMOVED leakage columns:
# pharmacogene_priority          - derived summary encoding target conditions
# pharmacogene_category_enhanced - categorical summary of feature combinations
# clinical_actionability_tier    - re-encoding of pharmacogene_priority
# variant_impact_burden          - categorical encoding of pathogenic_variants
# drug_metabolism_tissue_expression - categorical encoding of liver/kidney flags
# expression_breadth             - categorical encoding of tissues_expressed_count
# cancer_mutation_burden         - categorical encoding of unique_tumor_samples
# primary_indication_category    - categorical encoding of disease type flags
# has_pharmgkb_annotation        - boolean derived from source_count, leakage into old target

df_final = (
    df_final
    .select(
        col("gene_symbol"),
        col("gene_full_name"),
        col("pharmgkb_name"),
        col("description"),
        col("chromosome"),
        col("source_count"),
        col("is_drug_metabolizer"),
        col("is_drug_transporter_gene"),
        col("is_drug_target_gene"),
        col("has_high_druggability"),
        col("is_pharmacogene"),
        col("is_hepatic_metabolizer"),
        col("is_renal_transporter"),
        col("is_validated_cancer_target"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transporter"),
        col("is_metabolic"),
        col("druggability_score"),
        col("total_relationships"),
        col("entity_type_count"),
        col("drug_relationships"),
        col("disease_relationships"),
        col("variant_relationships"),
        col("evidence_count"),
        col("total_gene_variants"),
        col("pathogenic_variants"),
        col("missense_variants"),
        col("lof_variants"),
        col("domain_affecting_variants"),
        col("avg_pathogenicity_score"),
        col("has_pharmacogene_variants"),
        col("tissues_expressed_count"),
        col("max_expression_tpm"),
        col("avg_expression_tpm"),
        col("is_liver_expressed"),
        col("is_kidney_expressed"),
        col("cancer_mutation_count"),
        col("unique_tumor_samples"),
        col("is_oncology_drug_target"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease"),
        col("max_domain_count"),
        col("has_kinase_domain_count"),
        col("is_complex_drug_target"),
        col("pharmacogene_evidence_score"),
        col("drug_interaction_score"),
        col("clinical_utility_score"),
        col("pharmacogene_variant_impact_score"),
        col("metabolism_context_score"),
        col("pharmacogene_category"),
        col("drug_metabolism_role"),
        col("is_high_priority_pharmacogene")
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

# DBTITLE 1,Write gold.gene_pharmacogene_ml_features
print("\nWRITING gold.gene_pharmacogene_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_pharmacogene_ml_features")

print(f"Saved: {catalog_name}.gold.gene_pharmacogene_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.gene_pharmacogene_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

target_dist = df_check.groupBy("is_high_priority_pharmacogene").count().collect()
total       = sum(r["count"] for r in target_dist)
positives   = [r["count"] for r in target_dist if r["is_high_priority_pharmacogene"] == True]
pos_count   = positives[0] if positives else 0
pos_pct     = pos_count / total * 100 if total > 0 else 0

print(f"Rows:      {rows:,}")
print(f"Columns:   {cols}")
print(f"Positives: {pos_count:,} ({pos_pct:.2f}%)")

leakage_check = [
    "pharmacogene_priority",
    "pharmacogene_category_enhanced",
    "clinical_actionability_tier",
    "variant_impact_burden",
    "drug_metabolism_tissue_expression",
    "expression_breadth",
    "cancer_mutation_burden",
    "primary_indication_category",
    "has_pharmgkb_annotation",
]
present = [c for c in leakage_check if c in df_check.columns]
if present:
    print(f"LEAKAGE ALERT: {present}")
else:
    print("Leakage check: PASSED (no known leakage columns present)")

print("\nProcessing complete")
