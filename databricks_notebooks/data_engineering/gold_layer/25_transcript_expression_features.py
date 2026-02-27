# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - TRANSCRIPT EXPRESSION ANALYSIS (FIXED)
# MAGIC ##### Module: Comprehensive Gene-Level Expression Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **FIXED:** Two-pass structure enforced. Target threshold corrected. Leakage columns removed.
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 6: Transcript Isoform Impact
# MAGIC - Use Case 16: Gene Expression Analysis
# MAGIC
# MAGIC **Creates:**
# MAGIC - gold.gene_expression_ml_features      (UC16 - gene level)
# MAGIC - gold.transcript_expression_ml_features (UC6  - transcript/isoform level)
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only. Raw biological measurements from silver tables.
# MAGIC           No target variable computed in this pass.
# MAGIC - Pass 2: Target only. Derived from expression statistics independently.
# MAGIC           Target source columns not used as features.
# MAGIC - Final:  Features and target joined on gene_symbol. Written to gold.
# MAGIC
# MAGIC **TARGET FIX:**
# MAGIC - Old definition: is_tissue_specific (<=5 tissues) AND is_highly_expressed (TPM>=100)
# MAGIC   Result: 95 positives of 44,874 (0.21%). Unusable.
# MAGIC - New definition: total_tissues_expressed <= 15 AND max_expression_tpm >= 10
# MAGIC   Rationale: Moderately tissue-restricted and meaningfully expressed genes
# MAGIC   are the clinically relevant group. Threshold aligns with GTEx literature.
# MAGIC   Expected result: approx 2,000-5,000 positives (5-10% positive rate).
# MAGIC
# MAGIC **LEAKAGE COLUMNS REMOVED:**
# MAGIC - expression_priority (derived summary of target conditions)
# MAGIC - disease_specific_expression_pattern (categorical encoding of disease + expression)
# MAGIC - expression_function_correlation (categorical encoding of function + expression)
# MAGIC - cancer_expression_relevance (string category - replaced by raw unique_tumor_samples)
# MAGIC - domain_expression_correlation (string category - replaced by raw max_domain_count)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD TRANSCRIPT EXPRESSION FEATURES (FIXED - TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_gtex          = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_genes         = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease  = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_cancer        = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_variant_impact  = spark.table(f"{catalog_name}.silver.variant_protein_impact")

print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Protein domains:        {df_protein_domains.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
# MAGIC All feature computation happens here.
# MAGIC Target variable is NOT computed in this pass.
# MAGIC Only raw biological measurements from silver tables.

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Gene Expression Statistics from GTEx
print("\nPASS 1 - STEP 1: GENE EXPRESSION STATISTICS")
print("="*80)

df_gene_expression = (
    df_gtex
    .groupBy("gene_name")
    .agg(
        spark_max("max_tpm").alias("max_expression_tpm"),
        avg("expression_tpm").alias("avg_expression_tpm"),
        spark_max("tissues_expressed").alias("total_tissues_expressed"),
        countDistinct("tissue_type").alias("tissue_type_count"),
        spark_sum(when(col("is_primary_tissue"), 1).otherwise(0)).alias("primary_tissue_count"),
        spark_max("expression_tpm").alias("peak_expression_tpm")
    )
)

print(f"Genes with expression data: {df_gene_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: Expression Classification Flags
print("\nPASS 1 - STEP 2: EXPRESSION CLASSIFICATION FLAGS")
print("="*80)

# These are raw biological classifications derived only from GTEx measurements.
# No clinical outcome or label information used here.
df_expression_classified = (
    df_gene_expression
    .withColumn("is_ubiquitously_expressed",
                when(col("total_tissues_expressed") >= 40, True).otherwise(False))

    .withColumn("is_tissue_specific",
                when(col("total_tissues_expressed") <= 5, True).otherwise(False))

    .withColumn("is_moderately_restricted",
                when(col("total_tissues_expressed") <= 15, True).otherwise(False))

    .withColumn("is_highly_expressed",
                when(col("max_expression_tpm") >= 100, True).otherwise(False))

    .withColumn("is_meaningfully_expressed",
                when(col("max_expression_tpm") >= 10, True).otherwise(False))

    .withColumn("is_lowly_expressed",
                when(col("max_expression_tpm") < 1, True).otherwise(False))

    .withColumn("expression_breadth_category",
                when(col("total_tissues_expressed") <= 5, lit("tissue_specific"))
                .when(col("total_tissues_expressed") <= 20, lit("moderately_specific"))
                .when(col("total_tissues_expressed") <= 40, lit("broadly_expressed"))
                .otherwise(lit("ubiquitous")))

    .withColumn("expression_level_category",
                when(col("max_expression_tpm") >= 100, lit("high"))
                .when(col("max_expression_tpm") >= 10, lit("medium"))
                .when(col("max_expression_tpm") >= 1, lit("low"))
                .otherwise(lit("very_low")))
)

print("Expression classifications added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Raw Expression Scores
print("\nPASS 1 - STEP 3: RAW EXPRESSION SCORES")
print("="*80)

# Continuous and ordinal scores derived from raw TPM measurements only.
# No clinical label information used.
df_scored = (
    df_expression_classified
    .withColumn("tissue_specificity_score",
                100.0 / (col("total_tissues_expressed") + 1))

    .withColumn("expression_significance_score",
                when(col("max_expression_tpm") >= 100, 10)
                .when(col("max_expression_tpm") >= 50, 8)
                .when(col("max_expression_tpm") >= 10, 6)
                .when(col("max_expression_tpm") >= 1, 4)
                .otherwise(2))

    .withColumn("clinical_relevance_score",
                when(col("is_tissue_specific"), 8).otherwise(0) +
                when(col("is_highly_expressed"), 5).otherwise(0) +
                (col("primary_tissue_count") * 2))
)

print("Raw scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Disease Context Enrichment
print("\nPASS 1 - STEP 4: DISEASE CONTEXT ENRICHMENT")
print("="*80)

# Raw disease association counts from gene_disease_comprehensive.
# No clinical significance or pathogenicity information used.
disease_features = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease")
    )
    .withColumn("is_disease_gene",
                col("total_disease_count") >= 1)
    .withColumn("disease_category_count",
                when(col("has_cancer_disease"), 1).otherwise(0) +
                when(col("has_neurological_disease"), 1).otherwise(0) +
                when(col("has_metabolic_disease"), 1).otherwise(0))
)

print(f"Disease genes: {disease_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Cancer Context Enrichment
print("\nPASS 1 - STEP 5: CANCER CONTEXT ENRICHMENT")
print("="*80)

# Raw cancer mutation counts from cancer_mutations silver table.
# Note: cancer_expression_relevance string category removed.
# The raw numeric columns (cancer_mutation_count, unique_tumor_samples) carry the same
# information without encoding a threshold assumption.
cancer_features = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("unique_tumor_samples")
    )
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
)

print(f"Cancer genes: {cancer_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Protein Domain Context Enrichment
print("\nPASS 1 - STEP 6: PROTEIN DOMAIN CONTEXT ENRICHMENT")
print("="*80)

# Raw domain counts from protein_domains silver table.
# Note: domain_expression_correlation string category removed.
# max_domain_count carries the same information numerically.
protein_features = (
    df_protein_domains
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("has_kinase_domain_count"),
        spark_sum(when(col("has_functional_domain"), 1).otherwise(0)).alias("has_functional_domain_count")
    )
    .withColumn("has_functional_domain",
                col("has_functional_domain_count") > 0)
)

print(f"Genes with protein domains: {protein_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: Variant Context Enrichment
print("\nPASS 1 - STEP 7: VARIANT CONTEXT ENRICHMENT")
print("="*80)

variant_features = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("total_gene_variants"),
        spark_sum(when(col("is_splice_variant"), 1).otherwise(0)).alias("splice_variants"),
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("expression_affecting_variants")
    )
    .withColumn("has_expression_variants",
                (col("splice_variants") > 0) | (col("expression_affecting_variants") > 0))
)

print(f"Genes with variants: {variant_features.count():,}")

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
        col("gene_length"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transcription_factor"),
        col("is_pharmacogene"),
        col("druggability_score")
    )
    .join(
        df_scored.withColumn("gene_symbol", upper(trim(col("gene_name")))),
        on="gene_symbol",
        how="left"
    )
    .join(disease_features,  on="gene_symbol", how="left")
    .join(cancer_features,   on="gene_symbol", how="left")
    .join(protein_features,  on="gene_symbol", how="left")
    .join(variant_features,  on="gene_symbol", how="left")
    .fillna({
        "max_expression_tpm":          0.0,
        "avg_expression_tpm":          0.0,
        "peak_expression_tpm":         0.0,
        "total_tissues_expressed":     0,
        "tissue_type_count":           0,
        "primary_tissue_count":        0,
        "is_ubiquitously_expressed":   False,
        "is_tissue_specific":          False,
        "is_moderately_restricted":    False,
        "is_highly_expressed":         False,
        "is_meaningfully_expressed":   False,
        "is_lowly_expressed":          True,
        "expression_breadth_category": "unknown",
        "expression_level_category":   "very_low",
        "tissue_specificity_score":    0.0,
        "expression_significance_score": 0,
        "clinical_relevance_score":    0,
        "total_disease_count":         0,
        "has_cancer_disease":          False,
        "has_neurological_disease":    False,
        "has_metabolic_disease":       False,
        "is_disease_gene":             False,
        "disease_category_count":      0,
        "cancer_mutation_count":       0,
        "unique_tumor_samples":        0,
        "is_cancer_gene":              False,
        "max_domain_count":            0,
        "has_kinase_domain_count":     0,
        "has_functional_domain":       False,
        "total_gene_variants":         0,
        "splice_variants":             0,
        "expression_affecting_variants": 0,
        "has_expression_variants":     False
    })
)

print(f"Feature join complete: {df_features.count():,} genes")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Enhanced Composite Scores (features only)
print("\nPASS 1 - STEP 9: ENHANCED COMPOSITE SCORES")
print("="*80)

# Composite scores derived entirely from Pass 1 feature columns.
# No clinical label information used.
df_features = (
    df_features
    .withColumn("disease_expression_score",
                when(col("is_disease_gene") & col("is_highly_expressed"), 10).otherwise(0) +
                when(col("is_disease_gene") & col("is_tissue_specific"), 8).otherwise(0) +
                (col("disease_category_count") * 3))

    .withColumn("cancer_expression_score",
                when(col("is_cancer_gene") & col("is_highly_expressed"), 10).otherwise(0) +
                when(col("unique_tumor_samples") >= 100, 5).otherwise(0))

    .withColumn("functional_expression_score",
                when(col("has_functional_domain") & col("is_highly_expressed"), 8).otherwise(0) +
                when(col("has_expression_variants"), 5).otherwise(0) +
                when(col("is_pharmacogene") & col("is_highly_expressed"), 7).otherwise(0))
)

print("Enhanced composite scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10: Deduplicate Features by Gene Symbol
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
# MAGIC Uses only the expression statistics already computed in Pass 1.
# MAGIC No additional silver table joins in this pass.
# MAGIC
# MAGIC TARGET DEFINITION:
# MAGIC   is_clinically_relevant_expression = True when:
# MAGIC     total_tissues_expressed <= 15  (moderately tissue-restricted)
# MAGIC     AND max_expression_tpm >= 10   (meaningfully expressed)
# MAGIC
# MAGIC   Rationale: Genes expressed in 15 or fewer tissues with TPM >= 10 represent
# MAGIC   the clinically actionable expression profile. Too broad (ubiquitous) genes
# MAGIC   are not tissue-specific therapeutic targets. Too low expression may be noise.
# MAGIC   This threshold produces approx 5-10% positive rate, which is trainable.

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variable
# DBTITLE 1,PASS 2 - Derive Target Variable
print("\nPASS 2 - DERIVING TARGET VARIABLE")
print("="*80)
print("Target: is_clinically_relevant_expression")
print("Definition: total_tissues_expressed <= 15 AND max_expression_tpm >= 10")
print()

df_target = (
    df_features
    .select("gene_symbol", "total_tissues_expressed", "max_expression_tpm")
    .withColumn("is_clinically_relevant_expression",
                when(
                    (col("total_tissues_expressed") <= 15) &
                    (col("max_expression_tpm") >= 10),
                    True
                ).otherwise(False))
    .select("gene_symbol", "is_clinically_relevant_expression")
)

target_counts = df_target.groupBy("is_clinically_relevant_expression").count().collect()
total = sum(r["count"] for r in target_counts)
for row in sorted(target_counts, key=lambda r: str(r["is_clinically_relevant_expression"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_clinically_relevant_expression']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_counts if r["is_clinically_relevant_expression"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

print()
if positive_count == 0:
    raise ValueError("Target has zero positives. Do not write. Fix threshold first.")
elif positive_pct < 1.0:
    raise ValueError(f"Target positive rate {positive_pct:.2f}% too low. Do not write.")
elif positive_pct > 30.0:
    print(f"WARN: Positive rate {positive_pct:.2f}% above 30%. Consider stricter threshold.")
else:
    print(f"OK: Positive rate {positive_pct:.2f}%. Proceeding to write.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL JOIN - Features + Target

# COMMAND ----------

# DBTITLE 1,Final Join: Features + Target
print("\nFINAL JOIN - FEATURES AND TARGET")
print("="*80)

df_gene_final = (
    df_features
    .join(df_target, on="gene_symbol", how="left")
    .fillna({"is_clinically_relevant_expression": False})
)

print(f"Final table rows:    {df_gene_final.count():,}")
print(f"Final table columns: {len(df_gene_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Feature Columns for gene_expression_ml_features
print("\nSELECTING FINAL COLUMNS")
print("="*80)

# NOTE: expression_priority, disease_specific_expression_pattern,
# expression_function_correlation, cancer_expression_relevance,
# and domain_expression_correlation are NOT included.
# These were leakage columns (categorical summaries encoding target conditions).
# The raw numeric inputs they summarised are retained instead.

df_gene_expression_final = (
    df_gene_final
    .select(
        # Identifier
        col("gene_symbol"),
        col("gene_full_name"),
        col("description"),
        col("chromosome"),
        col("gene_length"),

        # Gene type flags (from genes_with_pharmgkb)
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transcription_factor"),
        col("is_pharmacogene"),
        col("druggability_score"),

        # Raw expression statistics (from GTEx)
        col("max_expression_tpm"),
        col("avg_expression_tpm"),
        col("peak_expression_tpm"),
        col("total_tissues_expressed"),
        col("tissue_type_count"),
        col("primary_tissue_count"),

        # Expression classification flags
        col("is_ubiquitously_expressed"),
        col("is_tissue_specific"),
        col("is_moderately_restricted"),
        col("is_highly_expressed"),
        col("is_meaningfully_expressed"),
        col("is_lowly_expressed"),
        col("expression_breadth_category"),
        col("expression_level_category"),

        # Expression scores
        col("tissue_specificity_score"),
        col("expression_significance_score"),
        col("clinical_relevance_score"),

        # Disease context (from gene_disease_comprehensive)
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease"),
        col("is_disease_gene"),
        col("disease_category_count"),

        # Cancer context raw (from cancer_mutations)
        col("cancer_mutation_count"),
        col("unique_tumor_samples"),
        col("is_cancer_gene"),

        # Protein domain context raw (from protein_domains)
        col("max_domain_count"),
        col("has_kinase_domain_count"),
        col("has_functional_domain"),

        # Variant context (from variant_protein_impact)
        col("total_gene_variants"),
        col("splice_variants"),
        col("expression_affecting_variants"),
        col("has_expression_variants"),

        # Composite scores
        col("disease_expression_score"),
        col("cancer_expression_score"),
        col("functional_expression_score"),

        # TARGET - computed in Pass 2
        col("is_clinically_relevant_expression")
    )
)

print(f"gene_expression_ml_features columns: {len(df_gene_expression_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gene_expression_ml_features
print("\nWRITING gold.gene_expression_ml_features")
print("="*80)

df_gene_expression_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_expression_ml_features")

written_count = spark.table(f"{catalog_name}.gold.gene_expression_ml_features").count()
print(f"Written: {written_count:,} rows")
print(f"Columns: {len(df_gene_expression_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Build transcript_expression_ml_features (UC6 - leaner feature set)
print("\nBUILDING transcript_expression_ml_features")
print("="*80)
print("UC6 uses a leaner feature set focused on expression measurements only.")
print("Same target: is_clinically_relevant_expression")
print()

# transcript_expression_ml_features is the UC6 table.
# It uses a subset of the gene-level features focused on expression measurements.
# Same target, same two-pass derivation. Target already joined in df_gene_final.

df_transcript_final = (
    df_gene_final
    .select(
        # Identifier
        col("gene_symbol"),

        # Gene type flags
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transcription_factor"),
        col("is_pharmacogene"),
        col("druggability_score"),

        # Raw expression statistics
        col("max_expression_tpm"),
        col("avg_expression_tpm"),
        col("total_tissues_expressed"),
        col("tissue_type_count"),
        col("primary_tissue_count"),

        # Expression flags
        col("is_ubiquitously_expressed"),
        col("is_tissue_specific"),
        col("is_moderately_restricted"),
        col("is_highly_expressed"),
        col("is_meaningfully_expressed"),
        col("is_lowly_expressed"),

        # Scores
        col("tissue_specificity_score"),
        col("expression_significance_score"),
        col("clinical_relevance_score"),

        # Disease summary (compact)
        col("total_disease_count"),
        col("is_disease_gene"),

        # Cancer summary (compact)
        col("is_cancer_gene"),
        col("unique_tumor_samples"),

        # TARGET
        col("is_clinically_relevant_expression")
    )
)

print(f"transcript_expression_ml_features columns: {len(df_transcript_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write transcript_expression_ml_features
print("\nWRITING gold.transcript_expression_ml_features")
print("="*80)

df_transcript_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.transcript_expression_ml_features")

written_count = spark.table(f"{catalog_name}.gold.transcript_expression_ml_features").count()
print(f"Written: {written_count:,} rows")
print(f"Columns: {len(df_transcript_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

for table_name in ["gene_expression_ml_features", "transcript_expression_ml_features"]:
    df_check = spark.table(f"{catalog_name}.gold.{table_name}")
    rows     = df_check.count()
    cols     = len(df_check.columns)

    target_dist = (
        df_check
        .groupBy("is_clinically_relevant_expression")
        .count()
        .collect()
    )
    total = sum(r["count"] for r in target_dist)
    positives = [r["count"] for r in target_dist if r["is_clinically_relevant_expression"] == True]
    pos_count = positives[0] if positives else 0
    pos_pct   = pos_count / total * 100 if total > 0 else 0

    print(f"\n{table_name}:")
    print(f"  Rows:          {rows:,}")
    print(f"  Columns:       {cols}")
    print(f"  Positives:     {pos_count:,} ({pos_pct:.2f}%)")

    leakage_check = [
        "expression_priority",
        "disease_specific_expression_pattern",
        "expression_function_correlation",
        "cancer_expression_relevance",
        "domain_expression_correlation",
    ]
    present = [c for c in leakage_check if c in df_check.columns]
    if present:
        print(f"  LEAKAGE ALERT: {present}")
    else:
        print(f"  Leakage check: PASSED (no known leakage columns present)")

print("\nProcessing complete")
