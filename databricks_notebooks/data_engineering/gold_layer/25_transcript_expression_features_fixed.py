# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - TRANSCRIPT EXPRESSION ANALYSIS (FIXED)
# MAGIC ##### Module: Comprehensive Gene-Level Expression Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 6: Transcript Isoform Impact
# MAGIC - Use Case 16: Gene Expression Analysis
# MAGIC
# MAGIC **Creates:**
# MAGIC - gold.gene_expression_ml_features
# MAGIC - gold.transcript_expression_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: All features including original leakage candidates restored.
# MAGIC           No target variable computed here.
# MAGIC - Pass 2: Target derived independently from expression statistics.
# MAGIC - Scan:   Dynamic Pearson correlation scan auto-drops leaky columns.
# MAGIC - Final:  Clean features joined with target and written to gold.
# MAGIC
# MAGIC **TARGET:**
# MAGIC - is_clinically_relevant_expression = total_tissues_expressed <= 15
# MAGIC   AND max_expression_tpm >= 10

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    max as spark_max, min as spark_min,
    when, lit, trim, upper, coalesce,
    dense_rank
)
from pyspark.sql.functions import corr as spark_corr
from pyspark.sql.window import Window

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

df_gtex            = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_genes           = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease    = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_cancer          = spark.table(f"{catalog_name}.silver.cancer_mutations")
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
# MAGIC All features including original leakage candidates are computed here.
# MAGIC The dynamic scan will detect and drop any leaky columns automatically.
# MAGIC Target variable is NOT computed in this pass.

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Gene Expression Statistics
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

df_expression_classified = (
    df_gene_expression

    .withColumn("is_ubiquitously_expressed",
                col("total_tissues_expressed") >= 40)

    .withColumn("is_tissue_specific",
                col("total_tissues_expressed") <= 5)

    .withColumn("is_moderately_restricted",
                col("total_tissues_expressed") <= 15)

    .withColumn("is_highly_expressed",
                col("max_expression_tpm") >= 100)

    .withColumn("is_meaningfully_expressed",
                col("max_expression_tpm") >= 10)

    .withColumn("is_lowly_expressed",
                col("max_expression_tpm") < 1)

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

    .withColumn("expression_priority",
                when(col("is_tissue_specific") & col("is_highly_expressed"), lit("critical"))
                .when(col("is_tissue_specific") | col("is_highly_expressed"), lit("high"))
                .when(col("is_moderately_restricted") & col("is_meaningfully_expressed"), lit("medium"))
                .otherwise(lit("low")))
)

print("Scores and expression_priority calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Disease Context Enrichment
print("\nPASS 1 - STEP 4: DISEASE CONTEXT ENRICHMENT")
print("="*80)

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

cancer_features = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("unique_tumor_samples")
    )
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)

    .withColumn("cancer_expression_relevance",
                when(col("cancer_mutation_count") >= 100, lit("high_cancer_burden"))
                .when(col("cancer_mutation_count") >= 10, lit("moderate_cancer_burden"))
                .otherwise(lit("low_cancer_burden")))
)

print(f"Cancer genes: {cancer_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Protein Domain Context Enrichment
print("\nPASS 1 - STEP 6: PROTEIN DOMAIN CONTEXT ENRICHMENT")
print("="*80)

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

    .withColumn("domain_expression_correlation",
                when(col("max_domain_count") >= 5, lit("multi_domain"))
                .when(col("max_domain_count") >= 2, lit("few_domains"))
                .otherwise(lit("single_or_none")))
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
        "max_expression_tpm":            0.0,
        "avg_expression_tpm":            0.0,
        "peak_expression_tpm":           0.0,
        "total_tissues_expressed":       0,
        "tissue_type_count":             0,
        "primary_tissue_count":          0,
        "is_ubiquitously_expressed":     False,
        "is_tissue_specific":            False,
        "is_moderately_restricted":      False,
        "is_highly_expressed":           False,
        "is_meaningfully_expressed":     False,
        "is_lowly_expressed":            True,
        "expression_breadth_category":   "unknown",
        "expression_level_category":     "very_low",
        "expression_priority":           "low",
        "tissue_specificity_score":      0.0,
        "expression_significance_score": 0,
        "clinical_relevance_score":      0,
        "total_disease_count":           0,
        "has_cancer_disease":            False,
        "has_neurological_disease":      False,
        "has_metabolic_disease":         False,
        "is_disease_gene":               False,
        "disease_category_count":        0,
        "cancer_mutation_count":         0,
        "unique_tumor_samples":          0,
        "is_cancer_gene":                False,
        "cancer_expression_relevance":   "low_cancer_burden",
        "max_domain_count":              0,
        "has_kinase_domain_count":       0,
        "has_functional_domain":         False,
        "domain_expression_correlation": "single_or_none",
        "total_gene_variants":           0,
        "splice_variants":               0,
        "expression_affecting_variants": 0,
        "has_expression_variants":       False
    })
)

print(f"Feature join complete: {df_features.count():,} genes")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Composite Scores and Remaining Leakage Candidates
print("\nPASS 1 - STEP 9: COMPOSITE SCORES")
print("="*80)

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

    .withColumn("disease_specific_expression_pattern",
                when(col("has_cancer_disease") & col("is_tissue_specific"), lit("cancer_tissue_specific"))
                .when(col("has_neurological_disease") & col("is_tissue_specific"), lit("neuro_tissue_specific"))
                .when(col("is_disease_gene") & col("is_highly_expressed"), lit("disease_high_expression"))
                .otherwise(lit("other")))

    .withColumn("expression_function_correlation",
                when(col("is_kinase") & col("is_highly_expressed"), lit("kinase_high"))
                .when(col("is_transcription_factor") & col("is_tissue_specific"), lit("tf_specific"))
                .when(col("is_enzyme") & col("is_meaningfully_expressed"), lit("enzyme_expressed"))
                .otherwise(lit("other")))
)

print("Composite scores calculated")

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

# COMMAND ----------

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

target_counts  = df_target.groupBy("is_clinically_relevant_expression").count().collect()
total          = sum(r["count"] for r in target_counts)
positives      = [r["count"] for r in target_counts if r["is_clinically_relevant_expression"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

for row in sorted(target_counts, key=lambda r: str(r["is_clinically_relevant_expression"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_clinically_relevant_expression']}: {row['count']:,} ({pct:.2f}%)")

print()
if positive_count == 0:
    raise ValueError("Target has zero positives. Fix threshold.")
elif positive_pct < 1.0:
    raise ValueError(f"Target positive rate {positive_pct:.2f}% too low. Fix threshold.")
elif positive_pct > 30.0:
    print(f"WARN: Positive rate {positive_pct:.2f}% above 30%.")
else:
    print(f"OK: Positive rate {positive_pct:.2f}%. Proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAKAGE SCAN

# COMMAND ----------

# DBTITLE 1,Correlation Scan Utility
def run_leakage_scan(df_feat, df_tgt, target_cols, primary_key,
                     sample_threshold=100_000, sample_fraction=0.40,
                     drop_threshold=0.98, warn_threshold=0.85):

    print("\nLEAKAGE CORRELATION SCAN")
    print("="*80)

    row_count = df_feat.count()
    print(f"Feature rows: {row_count:,}")

    df_scan_base = df_feat.join(df_tgt, on=primary_key, how="inner")

    if row_count >= sample_threshold:
        df_scan = df_scan_base.sample(withReplacement=False,
                                      fraction=sample_fraction, seed=42)
        print(f"Sampling: 40% -> {df_scan.count():,} rows for scan")
    else:
        df_scan = df_scan_base
        print(f"Sampling: 100% -> {row_count:,} rows for scan")

    exclude_cols = set(
        [primary_key] + target_cols +
        ["gene_name", "variant_id", "gene_symbol", "variant_key",
         "chromosome", "position", "reference_allele", "alternate_allele",
         "variant_name", "gene_full_name", "pharmgkb_name", "description",
         "variant_location", "drug_metabolism_role", "pharmacogene_category",
         "gene_chromosome", "official_gene_symbol", "review_status", "variant_type"]
    )

    numeric_types = {"IntegerType", "LongType", "DoubleType",
                     "FloatType", "BooleanType", "ShortType"}
    string_types  = {"StringType"}

    numeric_cols = [
        f.name for f in df_scan.schema.fields
        if f.name not in exclude_cols
        and type(f.dataType).__name__ in numeric_types
    ]
    string_cols = [
        f.name for f in df_scan.schema.fields
        if f.name not in exclude_cols
        and type(f.dataType).__name__ in string_types
    ]

    print(f"Scanning {len(numeric_cols)} numeric/boolean + "
          f"{len(string_cols)} string columns against {len(target_cols)} target(s)...")
    print()

    dropped_cols = {}
    warned_cols  = {}

    for feat_col in numeric_cols:
        max_r_abs   = 0.0
        col_results = {}
        for tgt_col in target_cols:
            try:
                r_val = df_scan.select(
                    spark_corr(feat_col, tgt_col).alias("r")
                ).collect()[0]["r"]
                if r_val is None:
                    continue
                r_abs = abs(r_val)
                col_results[tgt_col] = round(r_val, 4)
                if r_abs > max_r_abs:
                    max_r_abs = r_abs
            except Exception:
                continue
        if max_r_abs >= drop_threshold:
            dropped_cols[feat_col] = col_results
            print(f"  DROP  {feat_col}: {col_results}")
        elif max_r_abs >= warn_threshold:
            warned_cols[feat_col] = col_results
            print(f"  WARN  {feat_col}: {col_results}")

    for feat_col in string_cols:
        encoded_col = f"__enc_{feat_col}"
        w       = Window.orderBy(feat_col)
        df_enc  = df_scan.withColumn(encoded_col,
                                     dense_rank().over(w).cast("double"))
        max_r_abs   = 0.0
        col_results = {}
        for tgt_col in target_cols:
            try:
                r_val = df_enc.select(
                    spark_corr(encoded_col, tgt_col).alias("r")
                ).collect()[0]["r"]
                if r_val is None:
                    continue
                r_abs = abs(r_val)
                col_results[tgt_col] = round(r_val, 4)
                if r_abs > max_r_abs:
                    max_r_abs = r_abs
            except Exception:
                continue
        if max_r_abs >= drop_threshold:
            dropped_cols[feat_col] = col_results
            print(f"  DROP  {feat_col} (string): {col_results}")
        elif max_r_abs >= warn_threshold:
            warned_cols[feat_col] = col_results
            print(f"  WARN  {feat_col} (string): {col_results}")

    print()
    print(f"Auto-dropped: {len(dropped_cols)} columns")
    print(f"Warned:       {len(warned_cols)} columns (kept)")

    cols_to_drop  = list(dropped_cols.keys())
    df_feat_clean = df_feat.drop(*cols_to_drop) if cols_to_drop else df_feat

    return df_feat_clean, dropped_cols, warned_cols

# COMMAND ----------

# DBTITLE 1,Run Leakage Scan
df_features_clean, dropped, warned = run_leakage_scan(
    df_feat     = df_features,
    df_tgt      = df_target,
    target_cols = ["is_clinically_relevant_expression"],
    primary_key = "gene_symbol"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL JOIN - Clean Features + Target

# COMMAND ----------

# DBTITLE 1,Final Join: Clean Features + Target
print("\nFINAL JOIN - CLEAN FEATURES AND TARGET")
print("="*80)

df_gene_final = (
    df_features_clean
    .join(df_target, on="gene_symbol", how="left")
    .fillna({"is_clinically_relevant_expression": False})
    .dropDuplicates(["gene_symbol"])
    .drop("gene_name")
)

print(f"Final rows:    {df_gene_final.count():,}")
print(f"Final columns: {len(df_gene_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gene_expression_ml_features
print("\nWRITING gold.gene_expression_ml_features")
print("="*80)

df_gene_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_expression_ml_features")

written = spark.table(f"{catalog_name}.gold.gene_expression_ml_features").count()
print(f"Written: {written:,} rows | Columns: {len(df_gene_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Build transcript_expression_ml_features
print("\nBUILDING gold.transcript_expression_ml_features")
print("="*80)

# Leaner subset from the already-clean df_gene_final
# All leaky columns already removed by scan
keep_cols_transcript = [
    "gene_symbol",
    "gene_full_name",
    "description",
    "chromosome",
    "gene_length",
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_transcription_factor",
    "max_expression_tpm",
    "avg_expression_tpm",
    "peak_expression_tpm",
    "total_tissues_expressed",
    "tissue_type_count",
    "primary_tissue_count",
    "is_ubiquitously_expressed",
    "is_tissue_specific",
    "is_highly_expressed",
    "is_lowly_expressed",
    "expression_breadth_category",
    "expression_level_category",
    "tissue_specificity_score",
    "expression_significance_score",
    "clinical_relevance_score",
    "is_clinically_relevant_expression"
]

available              = set(df_gene_final.columns)
keep_cols_transcript   = [c for c in keep_cols_transcript if c in available]
df_transcript_final    = df_gene_final.select(*keep_cols_transcript)

print(f"transcript_expression_ml_features columns: {len(df_transcript_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write transcript_expression_ml_features
print("\nWRITING gold.transcript_expression_ml_features")
print("="*80)

df_transcript_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.transcript_expression_ml_features")

written = spark.table(f"{catalog_name}.gold.transcript_expression_ml_features").count()
print(f"Written: {written:,} rows | Columns: {len(df_transcript_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

for table_name in ["gene_expression_ml_features", "transcript_expression_ml_features"]:
    df_check = spark.table(f"{catalog_name}.gold.{table_name}")
    rows     = df_check.count()
    cols     = len(df_check.columns)

    target_dist = df_check.groupBy("is_clinically_relevant_expression").count().collect()
    total       = sum(r["count"] for r in target_dist)
    positives   = [r["count"] for r in target_dist
                   if r["is_clinically_relevant_expression"] == True]
    pos_count   = positives[0] if positives else 0
    pos_pct     = pos_count / total * 100 if total > 0 else 0

    print(f"\n{table_name}:")
    print(f"  Rows:          {rows:,}")
    print(f"  Columns:       {cols}")
    print(f"  Positives:     {pos_count:,} ({pos_pct:.2f}%)")
    print(f"  Auto-dropped:  {list(dropped.keys())}")
    print(f"  Warned (kept): {list(warned.keys())}")

print("\nProcessing complete")
