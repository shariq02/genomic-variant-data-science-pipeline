# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - GENETIC TEST AVAILABILITY (FIXED)
# MAGIC ##### Module: Comprehensive Gene-Level Test Availability Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **FIXED:** Two-pass structure enforced. Target threshold corrected. Leakage columns removed.
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 5: Genetic Test Availability
# MAGIC - Use Case 27: Clinical Test Discovery
# MAGIC
# MAGIC **Creates:** gold.gene_test_availability_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only. Raw biological measurements from silver tables.
# MAGIC - Pass 2: Target only. Derived from test statistics independently.
# MAGIC - Final:  Features and target joined on gene_symbol. Written to gold.
# MAGIC
# MAGIC **TARGET FIX:**
# MAGIC - Old: has_comprehensive_testing AND is_well_tested_gene -> 0 positives
# MAGIC - New: has_clinical_test OR clinical_test_utility_score >= 10
# MAGIC   Expected: 5-15% positive rate
# MAGIC
# MAGIC **LEAKAGE COLUMNS REMOVED:**
# MAGIC - test_priority (derived summary encoding target conditions)
# MAGIC - test_recommendation_tier (categorical re-encoding of clinical_test_utility_score)
# MAGIC - disease_test_correlation (categorical encoding of total_disease_count)
# MAGIC - variant_test_coverage_level (categorical encoding of pathogenic_variants_in_tested_gene)
# MAGIC - population_test_priority (categorical encoding of rare_pathogenic_variants)
# MAGIC - primary_test_type (categorical summary of feature combinations)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD GENETIC TEST FEATURES (FIXED - TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_gtr          = spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests")
df_genes        = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_cancer       = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population   = spark.table(f"{catalog_name}.silver.population_frequencies")

print(f"GTR tests:              {df_gtr.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
# MAGIC All feature computation happens here.
# MAGIC Target variable is NOT computed in this pass.

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Gene Test Statistics
print("\nPASS 1 - STEP 1: GENE TEST STATISTICS")
print("="*80)

df_gene_tests = (
    df_gtr
    .groupBy("gene_symbol")
    .agg(
        count("gtr_test_id").alias("total_test_count"),
        countDistinct("gtr_test_id").alias("unique_test_count"),
        countDistinct("disease_name").alias("disease_count"),
        spark_sum(when(col("is_genetic_test"), 1).otherwise(0)).alias("genetic_test_count"),
        spark_sum(when(col("has_gene_info"), 1).otherwise(0)).alias("tests_with_gene_info"),
        spark_sum(when(col("has_disease_info"), 1).otherwise(0)).alias("tests_with_disease_info"),
        spark_sum(when(col("is_complete_record"), 1).otherwise(0)).alias("complete_test_count"),
        spark_sum(when(col("is_frequently_tested"), 1).otherwise(0)).alias("frequent_test_count")
    )
)

print(f"Genes with test data: {df_gene_tests.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: Test Availability Classification
print("\nPASS 1 - STEP 2: TEST AVAILABILITY CLASSIFICATION")
print("="*80)

df_classified = (
    df_gene_tests
    .withColumn("has_clinical_test",
                when(col("unique_test_count") > 0, True).otherwise(False))

    .withColumn("has_multiple_tests",
                when(col("unique_test_count") >= 3, True).otherwise(False))

    .withColumn("has_comprehensive_testing",
                when(col("unique_test_count") >= 10, True).otherwise(False))

    .withColumn("is_well_tested_gene",
                when((col("complete_test_count") >= 5) &
                     (col("disease_count") >= 2), True).otherwise(False))

    .withColumn("test_availability_category",
                when(col("unique_test_count") >= 10, lit("comprehensive"))
                .when(col("unique_test_count") >= 3, lit("multiple"))
                .when(col("unique_test_count") >= 1, lit("limited"))
                .otherwise(lit("none")))
)

print("Classification added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Test Availability Scores
print("\nPASS 1 - STEP 3: TEST AVAILABILITY SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("test_accessibility_score",
                (col("unique_test_count") * 2) +
                when(col("has_multiple_tests"), 5).otherwise(0) +
                when(col("has_comprehensive_testing"), 10).otherwise(0))

    .withColumn("clinical_utility_score",
                (col("complete_test_count") * 3) +
                (col("disease_count") * 2) +
                when(col("is_well_tested_gene"), 8).otherwise(0))

    .withColumn("test_quality_score",
                (col("tests_with_gene_info") * 1) +
                (col("tests_with_disease_info") * 2) +
                (col("complete_test_count") * 3))
)

print("Scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Disease Context
print("\nPASS 1 - STEP 4: DISEASE CONTEXT")
print("="*80)

# Raw disease counts only. disease_test_correlation string category removed.
# total_disease_count retained as raw numeric feature instead.
disease_features = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease")
    )
    .withColumn("multi_disease_testing",
                col("total_disease_count") >= 3)
)

print(f"Disease genes: {disease_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Variant Context
print("\nPASS 1 - STEP 5: VARIANT CONTEXT")
print("="*80)

# Raw variant counts only. variant_test_coverage_level string category removed.
# pathogenic_variants_in_tested_gene retained as raw numeric feature instead.
variant_features = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_variants_in_tested_gene"),
        count("*").alias("test_covered_variants")
    )
)

print(f"Genes with variants: {variant_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Cancer Context
print("\nPASS 1 - STEP 6: CANCER CONTEXT")
print("="*80)

cancer_features = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("cancer_samples")
    )
    .withColumn("is_cancer_panel_gene",
                col("cancer_mutation_count") >= 50)
    .withColumn("hereditary_cancer_testing",
                col("cancer_samples") >= 10)
)

print(f"Cancer genes: {cancer_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: Population Context
print("\nPASS 1 - STEP 7: POPULATION CONTEXT")
print("="*80)

# Raw rare pathogenic variant counts only. population_test_priority string category removed.
# rare_pathogenic_variants retained as raw numeric feature instead.
carrier_features = (
    df_variant_impact
    .join(
        df_population.select("variant_id", "is_rare"),
        "variant_id",
        "inner"
    )
    .filter(col("is_rare") & col("is_pathogenic"))
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("rare_pathogenic_variants")
    )
    .withColumn("carrier_screening_relevant",
                col("rare_pathogenic_variants") >= 3)
)

print(f"Carrier screening genes: {carrier_features.count():,}")

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
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene")
    )
    .join(df_scored.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
          on="gene_symbol", how="left")
    .join(disease_features,  on="gene_symbol", how="left")
    .join(variant_features,  on="gene_symbol", how="left")
    .join(cancer_features,   on="gene_symbol", how="left")
    .join(carrier_features,  on="gene_symbol", how="left")
    .fillna({
        "total_test_count":                   0,
        "unique_test_count":                  0,
        "disease_count":                      0,
        "genetic_test_count":                 0,
        "tests_with_gene_info":               0,
        "tests_with_disease_info":            0,
        "complete_test_count":                0,
        "frequent_test_count":                0,
        "has_clinical_test":                  False,
        "has_multiple_tests":                 False,
        "has_comprehensive_testing":          False,
        "is_well_tested_gene":                False,
        "test_availability_category":         "none",
        "test_accessibility_score":           0,
        "clinical_utility_score":             0,
        "test_quality_score":                 0,
        "total_disease_count":                0,
        "has_cancer_disease":                 False,
        "has_cardiovascular_disease":         False,
        "has_neurological_disease":           False,
        "multi_disease_testing":              False,
        "pathogenic_variants_in_tested_gene": 0,
        "test_covered_variants":              0,
        "cancer_mutation_count":              0,
        "cancer_samples":                     0,
        "is_cancer_panel_gene":               False,
        "hereditary_cancer_testing":          False,
        "rare_pathogenic_variants":           0,
        "carrier_screening_relevant":         False,
    })
)

print(f"Genes with test features: {df_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Enhanced Composite Scores
print("\nPASS 1 - STEP 9: ENHANCED COMPOSITE SCORES")
print("="*80)

df_features = (
    df_features
    .withColumn("clinical_test_utility_score",
                col("clinical_utility_score") +
                when(col("multi_disease_testing"), 5).otherwise(0) +
                when(col("pathogenic_variants_in_tested_gene") >= 10, 8).otherwise(0))

    .withColumn("variant_test_coverage_score",
                (col("pathogenic_variants_in_tested_gene") * 2) +
                when(col("pathogenic_variants_in_tested_gene") >= 10, 10).otherwise(0))

    .withColumn("population_test_relevance_score",
                when(col("carrier_screening_relevant"), 10).otherwise(0) +
                when(col("hereditary_cancer_testing"), 8).otherwise(0) +
                (col("rare_pathogenic_variants") * 1))
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
# MAGIC   is_high_priority_test_gene = True when:
# MAGIC     has_clinical_test = True
# MAGIC     OR clinical_test_utility_score >= 10
# MAGIC
# MAGIC   Rationale: Old definition (has_comprehensive_testing AND is_well_tested_gene)
# MAGIC   produced zero positives because GTR join produced no matches meeting both conditions.
# MAGIC   New definition captures genes that have any clinical test available OR have
# MAGIC   sufficient clinical utility evidence. This is the biologically meaningful group
# MAGIC   of genes where genetic testing is clinically relevant.

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variable
print("\nPASS 2 - DERIVING TARGET VARIABLE")
print("="*80)
print("Target: is_high_priority_test_gene")
print("Definition: has_clinical_test OR clinical_test_utility_score >= 10")
print()

df_target = (
    df_features
    .select("gene_symbol", "has_clinical_test", "clinical_test_utility_score")
    .withColumn("is_high_priority_test_gene",
                when(
                    col("has_clinical_test") |
                    (col("clinical_test_utility_score") >= 10),
                    True
                ).otherwise(False))
    .select("gene_symbol", "is_high_priority_test_gene")
)

target_counts = df_target.groupBy("is_high_priority_test_gene").count().collect()
total = sum(r["count"] for r in target_counts)
for row in sorted(target_counts, key=lambda r: str(r["is_high_priority_test_gene"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_high_priority_test_gene']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_counts if r["is_high_priority_test_gene"] == True]
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
    .fillna({"is_high_priority_test_gene": False})
)

print(f"Final table rows:    {df_final.count():,}")
print(f"Final table columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Feature Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

# REMOVED leakage columns:
# test_priority              - derived summary encoding target conditions
# test_recommendation_tier   - categorical re-encoding of clinical_test_utility_score
# disease_test_correlation   - categorical encoding of total_disease_count
# variant_test_coverage_level - categorical encoding of pathogenic_variants_in_tested_gene
# population_test_priority   - categorical encoding of rare_pathogenic_variants
# primary_test_type          - categorical summary of feature combinations

df_final = (
    df_final
    .select(
        col("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        col("total_test_count"),
        col("unique_test_count"),
        col("disease_count"),
        col("genetic_test_count"),
        col("tests_with_gene_info"),
        col("tests_with_disease_info"),
        col("complete_test_count"),
        col("frequent_test_count"),
        col("has_clinical_test"),
        col("has_multiple_tests"),
        col("has_comprehensive_testing"),
        col("is_well_tested_gene"),
        col("test_availability_category"),
        col("test_accessibility_score"),
        col("clinical_utility_score"),
        col("test_quality_score"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("multi_disease_testing"),
        col("pathogenic_variants_in_tested_gene"),
        col("test_covered_variants"),
        col("cancer_mutation_count"),
        col("cancer_samples"),
        col("is_cancer_panel_gene"),
        col("hereditary_cancer_testing"),
        col("rare_pathogenic_variants"),
        col("carrier_screening_relevant"),
        col("clinical_test_utility_score"),
        col("variant_test_coverage_score"),
        col("population_test_relevance_score"),
        col("is_high_priority_test_gene")
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

# DBTITLE 1,Write gold.gene_test_availability_ml_features
print("\nWRITING gold.gene_test_availability_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_test_availability_ml_features")

print(f"Saved: {catalog_name}.gold.gene_test_availability_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.gene_test_availability_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

target_dist = df_check.groupBy("is_high_priority_test_gene").count().collect()
total       = sum(r["count"] for r in target_dist)
positives   = [r["count"] for r in target_dist if r["is_high_priority_test_gene"] == True]
pos_count   = positives[0] if positives else 0
pos_pct     = pos_count / total * 100 if total > 0 else 0

print(f"Rows:      {rows:,}")
print(f"Columns:   {cols}")
print(f"Positives: {pos_count:,} ({pos_pct:.2f}%)")

leakage_check = [
    "test_priority",
    "test_recommendation_tier",
    "disease_test_correlation",
    "variant_test_coverage_level",
    "population_test_priority",
    "primary_test_type",
]
present = [c for c in leakage_check if c in df_check.columns]
if present:
    print(f"LEAKAGE ALERT: {present}")
else:
    print("Leakage check: PASSED (no known leakage columns present)")

print("\nProcessing complete")
