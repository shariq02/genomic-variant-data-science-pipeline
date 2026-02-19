# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - GENETIC TEST AVAILABILITY
# MAGIC ##### Module: Gene-Level Test Availability Features
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 5: Genetic Test Availability
# MAGIC - Use Case 27: Clinical Test Discovery
# MAGIC 
# MAGIC **Input:**
# MAGIC - silver.gtr_gene_disease_tests (created in notebook 22)
# MAGIC - silver.genes_ultra_enriched
# MAGIC 
# MAGIC **Output:** gold.genetic_test_ml_features

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

print("GOLD GENETIC TEST FEATURES")
print("Gene-level test availability feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_gtr = spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"GTR tests: {df_gtr.count():,}")
print(f"Genes: {df_genes.count():,}")

print("\nGTR schema:")
df_gtr.printSchema()

print("\nSample GTR data:")
df_gtr.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Calculate Gene Test Statistics
print("\nCALCULATING GENE TEST STATISTICS")
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
print("\nTest statistics:")
df_gene_tests.describe().show()

print("\nTop 10 genes by test count:")
df_gene_tests.orderBy(col("total_test_count").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Add Test Availability Classification
print("\nADDING TEST AVAILABILITY CLASSIFICATION")
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
                when(col("unique_test_count") >= 10, "comprehensive")
                .when(col("unique_test_count") >= 3, "multiple")
                .when(col("unique_test_count") >= 1, "limited")
                .otherwise("none"))
)

print("Added classification")
print("\nTest availability distribution:")
df_classified.groupBy("test_availability_category").count().orderBy("test_availability_category").show()

print("\nClinical test availability:")
df_classified.groupBy("has_clinical_test").count().show()

# COMMAND ----------

# DBTITLE 1,Calculate Test Availability Scores
print("\nCALCULATING TEST AVAILABILITY SCORES")
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

print("Added scores")
print("\nScore distribution:")
df_scored.select("test_accessibility_score", "clinical_utility_score", "test_quality_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data
print("\nJOINING WITH GENE MASTER DATA")
print("="*80)

df_with_genes = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme")
    )
    .join(
        df_scored.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
        on="gene_symbol",
        how="left"
    )
)

print(f"Genes with test features: {df_with_genes.count():,}")
print("\nSample with genes:")
df_with_genes.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Test Priority Classification
print("\nADDING TEST PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_with_genes
    .withColumn("test_priority",
                when(col("clinical_utility_score") >= 20, "high")
                .when(col("clinical_utility_score") >= 10, "medium")
                .otherwise("low"))
    
    .withColumn("is_high_priority_test_gene",
                when((col("has_comprehensive_testing")) & 
                     (col("is_well_tested_gene")), True).otherwise(False))
)

print("Added priority classification")
print("\nPriority distribution:")
df_priority.groupBy("test_priority").count().orderBy("test_priority").show()

print("\nHigh priority test genes:")
high_priority = df_priority.filter(col("is_high_priority_test_gene")).count()
print(f"  Count: {high_priority:,}")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        col("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        
        coalesce(col("total_test_count"), lit(0)).alias("total_test_count"),
        coalesce(col("unique_test_count"), lit(0)).alias("unique_test_count"),
        coalesce(col("disease_count"), lit(0)).alias("disease_count"),
        coalesce(col("genetic_test_count"), lit(0)).alias("genetic_test_count"),
        coalesce(col("tests_with_gene_info"), lit(0)).alias("tests_with_gene_info"),
        coalesce(col("tests_with_disease_info"), lit(0)).alias("tests_with_disease_info"),
        coalesce(col("complete_test_count"), lit(0)).alias("complete_test_count"),
        coalesce(col("frequent_test_count"), lit(0)).alias("frequent_test_count"),
        
        coalesce(col("has_clinical_test"), lit(False)).alias("has_clinical_test"),
        coalesce(col("has_multiple_tests"), lit(False)).alias("has_multiple_tests"),
        coalesce(col("has_comprehensive_testing"), lit(False)).alias("has_comprehensive_testing"),
        coalesce(col("is_well_tested_gene"), lit(False)).alias("is_well_tested_gene"),
        
        coalesce(col("test_availability_category"), lit("none")).alias("test_availability_category"),
        
        coalesce(col("test_accessibility_score"), lit(0)).alias("test_accessibility_score"),
        coalesce(col("clinical_utility_score"), lit(0)).alias("clinical_utility_score"),
        coalesce(col("test_quality_score"), lit(0)).alias("test_quality_score"),
        
        coalesce(col("test_priority"), lit("low")).alias("test_priority"),
        coalesce(col("is_high_priority_test_gene"), lit(False)).alias("is_high_priority_test_gene")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} genes")

print("\nFeature columns:")
for col_name in df_final.columns:
    print(f"  - {col_name}")

print("\nSample final features:")
df_final.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Deduplicate by Gene Symbol
print("\nDEDUPLICATING BY GENE_SYMBOL")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["gene_symbol"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Genetic Test Features
print("\nSAVING GOLD GENETIC TEST FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.genetic_test_ml_features")

print(f"Saved: {catalog_name}.gold.genetic_test_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nGENETIC TEST FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.genetic_test_ml_features").count()
print(f"\nTable created:")
print(f"  gold.genetic_test_ml_features: {result_count:,} genes")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.genetic_test_ml_features") \
    .groupBy("test_priority") \
    .count() \
    .orderBy("test_priority") \
    .show()

print("\nTest availability breakdown:")
spark.table(f"{catalog_name}.gold.genetic_test_ml_features") \
    .groupBy("test_availability_category") \
    .count() \
    .orderBy("test_availability_category") \
    .show()

print("\nProcessing complete")
