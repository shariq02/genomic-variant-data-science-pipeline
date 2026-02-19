# Databricks notebook source
# MAGIC %md
# MAGIC #### DATA PROCESSING - GENETIC TEST REGISTRY
# MAGIC ##### Module: GTR Gene Disease Tests Processing
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 5: Genetic Test Availability
# MAGIC - Use Case 27: Clinical Test Discovery
# MAGIC
# MAGIC **Input:** default.gtr_gene_disease_tests  
# MAGIC **Output:** silver.gtr_gene_disease_tests

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, count as spark_count
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR GTR GENE DISEASE TESTS PROCESSING")

# COMMAND ----------

# DBTITLE 1,Load GTR Data
print("\nLOADING GTR DATA")
print("="*80)

df_gtr = spark.table(f"{catalog_name}.default.gtr_gene_disease_tests")

gtr_count = df_gtr.count()
print(f"GTR test records: {gtr_count:,}")

print("\nSchema:")
df_gtr.printSchema()

print("\nSample data:")
df_gtr.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Check Data Quality
print("\nDATA QUALITY CHECKS")
print("="*80)

print("\nUnique tests:")
unique_tests = df_gtr.select("gtr_test_id").distinct().count()
print(f"  Tests: {unique_tests:,}")

print("\nUnique genes:")
unique_genes = df_gtr.select("gene_symbol").distinct().count()
print(f"  Genes: {unique_genes:,}")

print("\nUnique diseases:")
unique_diseases = df_gtr.select("disease_name").distinct().count()
print(f"  Diseases: {unique_diseases:,}")

print("\nTop 10 genes by test count:")
df_gtr.groupBy("gene_symbol").count().orderBy(col("count").desc()).show(10, truncate=False)

print("\nNull counts:")
df_gtr.select([
    spark_count(when(col(c).isNull(), c)).alias(c) 
    for c in df_gtr.columns
]).show(vertical=True)

# COMMAND ----------

# DBTITLE 1,Clean GTR Data
print("\nCLEANING GTR DATA")
print("="*80)

df_gtr_clean = (
    df_gtr
    .withColumn("gtr_test_id", trim(col("gtr_test_id")))
    .withColumn("test_name", trim(col("test_name")))
    .withColumn("gene_symbol", upper(trim(col("gene_symbol"))))
    .withColumn("disease_name", trim(col("disease_name")))
    
    .filter(col("gtr_test_id").isNotNull())
    .filter(col("gtr_test_id") != "")
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    
    .select(
        "gtr_test_id",
        "test_name",
        "gene_symbol",
        "gene_id",
        "disease_name",
        "disease_id",
        "data_source",
        "download_date"
    )
)

gtr_clean_count = df_gtr_clean.count()
print(f"Clean GTR records: {gtr_clean_count:,}")
print(f"Removed: {gtr_count - gtr_clean_count:,} records")

df_gtr_clean.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Test Classification Flags
print("\nADDING TEST CLASSIFICATION FLAGS")
print("="*80)

df_gtr_enriched = (
    df_gtr_clean
    .withColumn("is_genetic_test",
                when(col("test_name").isNotNull(), True).otherwise(False))
    
    .withColumn("has_gene_info",
                when(col("gene_id").isNotNull(), True).otherwise(False))
    
    .withColumn("has_disease_info",
                when(col("disease_id").isNotNull(), True).otherwise(False))
    
    .withColumn("is_complete_record",
                when((col("gene_id").isNotNull()) & 
                     (col("disease_id").isNotNull()), True).otherwise(False))
)

print("Added test classification flags")
df_gtr_enriched.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Calculate Gene Test Statistics
print("\nCALCULATING GENE TEST STATISTICS")
print("="*80)

from pyspark.sql.window import Window

gene_window = Window.partitionBy("gene_symbol")

df_gtr_enriched = (
    df_gtr_enriched
    .withColumn("gene_test_count", 
                spark_count("gtr_test_id").over(gene_window))
    
    .withColumn("is_frequently_tested",
                when(col("gene_test_count") >= 10, True).otherwise(False))
)

print("\nFrequently tested genes:")
df_gtr_enriched.filter(col("is_frequently_tested")) \
    .select("gene_symbol", "gene_test_count") \
    .distinct() \
    .orderBy(col("gene_test_count").desc()) \
    .show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validate Enriched Data
print("\nVALIDATING ENRICHED DATA")
print("="*80)

print("\nTest classification:")
print(f"  Genetic tests: {df_gtr_enriched.filter(col('is_genetic_test')).count():,}")
print(f"  With gene info: {df_gtr_enriched.filter(col('has_gene_info')).count():,}")
print(f"  With disease info: {df_gtr_enriched.filter(col('has_disease_info')).count():,}")
print(f"  Complete records: {df_gtr_enriched.filter(col('is_complete_record')).count():,}")

print("\nFrequently tested genes:")
frequently_tested = df_gtr_enriched.filter(col("is_frequently_tested")).select("gene_symbol").distinct().count()
print(f"  Genes with 10+ tests: {frequently_tested:,}")

print("\nSample enriched records:")
df_gtr_enriched.filter(col("is_complete_record")).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver GTR
print("\nSAVING SILVER GTR")
print("="*80)

df_gtr_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.gtr_gene_disease_tests")

print(f"Saved: {catalog_name}.silver.gtr_gene_disease_tests")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("GTR PROCESSING COMPLETE")
print("="*80)

final_count = spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests").count()
print(f"\nTable created:")
print(f"  silver.gtr_gene_disease_tests: {final_count:,} records")

print("\nTop genes by test count:")
spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests") \
    .groupBy("gene_symbol") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10, truncate=False)

print("\nSample records:")
spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests") \
    .select("gtr_test_id", "gene_symbol", "disease_name", "gene_test_count") \
    .show(5, truncate=60)

print("\nProcessing complete")
