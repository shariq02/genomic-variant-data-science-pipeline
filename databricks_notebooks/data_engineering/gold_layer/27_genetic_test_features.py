# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - GENETIC TEST AVAILABILITY
# MAGIC ##### Module: Comprehensive Gene-Level Test Availability Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 5: Genetic Test Availability
# MAGIC - Use Case 27: Clinical Test Discovery
# MAGIC
# MAGIC **Creates:** gold.gene_test_availability_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only. No target computed here.
# MAGIC - Pass 2: Target derived independently from test statistics.
# MAGIC - Final:  Features and target joined on gene_symbol. Written to gold.
# MAGIC
# MAGIC **TARGET FIX:**
# MAGIC - Old: has_comprehensive_testing AND is_well_tested_gene -> 0 positives
# MAGIC - New: has_clinical_test OR clinical_test_utility_score >= 10
# MAGIC   Expected: 5-15% positive rate

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

print("GOLD GENETIC TEST FEATURES (TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_gtr            = spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests")
df_genes          = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease   = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_cancer         = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population     = spark.table(f"{catalog_name}.silver.population_frequencies")

print(f"GTR tests:              {df_gtr.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
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

    .withColumn("disease_test_correlation",
                when(col("total_disease_count") >= 10, lit("high_disease_burden"))
                .when(col("total_disease_count") >= 3, lit("moderate_disease_burden"))
                .otherwise(lit("low_disease_burden")))
)

print(f"Disease genes: {disease_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Variant Context
print("\nPASS 1 - STEP 5: VARIANT CONTEXT")
print("="*80)

variant_features = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_variants_in_tested_gene"),
        count("*").alias("test_covered_variants")
    )
    .withColumn("variant_test_coverage_level",
                when(col("pathogenic_variants_in_tested_gene") >= 20, lit("high"))
                .when(col("pathogenic_variants_in_tested_gene") >= 5, lit("moderate"))
                .otherwise(lit("low")))
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

    .withColumn("population_test_priority",
                when(col("rare_pathogenic_variants") >= 10, lit("high_carrier_risk"))
                .when(col("rare_pathogenic_variants") >= 3, lit("moderate_carrier_risk"))
                .otherwise(lit("low_carrier_risk")))
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
        "disease_test_correlation":           "low_disease_burden",
        "multi_disease_testing":              False,
        "pathogenic_variants_in_tested_gene": 0,
        "test_covered_variants":              0,
        "variant_test_coverage_level":        "low",
        "cancer_mutation_count":              0,
        "cancer_samples":                     0,
        "is_cancer_panel_gene":               False,
        "hereditary_cancer_testing":          False,
        "rare_pathogenic_variants":           0,
        "carrier_screening_relevant":         False,
        "population_test_priority":           "low_carrier_risk"
    })
)

print(f"Genes with test features: {df_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Composite Scores
print("\nPASS 1 - STEP 9: COMPOSITE SCORES")
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

    .withColumn("test_priority",
                when(col("has_comprehensive_testing") & col("is_well_tested_gene"), lit("critical"))
                .when(col("has_clinical_test"), lit("high"))
                .when(col("clinical_utility_score") >= 10, lit("medium"))
                .otherwise(lit("low")))

    .withColumn("primary_test_type",
                when(col("is_cancer_panel_gene"), lit("cancer_panel"))
                .when(col("carrier_screening_relevant"), lit("carrier_screening"))
                .when(col("has_clinical_test"), lit("clinical_diagnostic"))
                .otherwise(lit("none")))

    .withColumn("test_recommendation_tier",
                when(col("clinical_utility_score") >= 20, lit("tier_1"))
                .when(col("clinical_utility_score") >= 10, lit("tier_2"))
                .when(col("clinical_utility_score") >= 5, lit("tier_3"))
                .otherwise(lit("unclassified")))
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

target_counts  = df_target.groupBy("is_high_priority_test_gene").count().collect()
total          = sum(r["count"] for r in target_counts)
positives      = [r["count"] for r in target_counts if r["is_high_priority_test_gene"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

for row in sorted(target_counts, key=lambda r: str(r["is_high_priority_test_gene"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_high_priority_test_gene']}: {row['count']:,} ({pct:.2f}%)")

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
    .dropDuplicates(["gene_symbol"])
)

print(f"Final rows:    {df_final.count():,}")
print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_final.select(
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
    col("disease_test_correlation"),
    col("multi_disease_testing"),
    col("pathogenic_variants_in_tested_gene"),
    col("test_covered_variants"),
    col("variant_test_coverage_level"),
    col("cancer_mutation_count"),
    col("cancer_samples"),
    col("is_cancer_panel_gene"),
    col("hereditary_cancer_testing"),
    col("rare_pathogenic_variants"),
    col("carrier_screening_relevant"),
    col("population_test_priority"),
    col("clinical_test_utility_score"),
    col("variant_test_coverage_score"),
    col("population_test_relevance_score"),
    col("test_priority"),
    col("is_high_priority_test_gene"),
    col("primary_test_type"),
    col("test_recommendation_tier")
)

print(f"Final columns: {len(df_final.columns)}")

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
print("\nProcessing complete")
