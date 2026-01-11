# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Gene Data Processing
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:**  9 January 2026  
# MAGIC **Purpose:** Clean and process gene data with PySpark  
# MAGIC **Input:** workspace.default.gene_metadata  
# MAGIC **Output:** workspace.bronze.genes_raw → workspace.silver.genes_clean

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, concat_ws
)
from pyspark.sql.types import StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration & Catalog Setup
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("="*70)
print("GENE DATA PROCESSING - DATABRICKS UNITY CATALOG")
print("="*70)
print(f"Catalog: {catalog_name}")
print(f"Processing: default.gene_metadata -> bronze.genes_raw -> silver.genes_clean")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Uploaded Gene Data
print("\nReading gene data from workspace.default.gene_metadata...")

df_genes_raw = spark.table(f"{catalog_name}.default.gene_metadata")

print(f"Loaded {df_genes_raw.count():,} raw genes")
print(f"Columns: {len(df_genes_raw.columns)}")

# COMMAND ----------

# DBTITLE 1,Inspect Raw Schema & Sample
print("\nRaw Schema:")
df_genes_raw.printSchema()

print("\nSample raw data:")
display(df_genes_raw.limit(5), truncate=False)

# COMMAND ----------

# DBTITLE 1,Save to Bronze Layer
print("\n" + "="*40)
print("SAVING TO BRONZE LAYER")
print("="*40)

df_genes_raw.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.genes_raw")

print(f"Saved to: {catalog_name}.bronze.genes_raw")

# COMMAND ----------

# DBTITLE 1,Profiling: Gene Type Distribution
print("\n" + "="*40)
print("DATA PROFILING - UNKNOWN VALUE ANALYSIS")
print("="*40)

print("\n1. Gene Type Analysis:")
df_genes_raw.groupBy("gene_type") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(20)

unknown_gene_types = df_genes_raw.filter(
    (col("gene_type") == "Unknown") | 
    (col("gene_type").isNull())
).count()
print(f"\nGenes with Unknown gene_type: {unknown_gene_types}")

# COMMAND ----------

# DBTITLE 1,Profiling: Null & “Unknown” Analysis
print("\n2. Null Value Analysis:")

string_columns = [
    "gene_name", "official_symbol", "description", "chromosome", "map_location", "gene_type", "summary", "other_aliases", "other_designations", "strand"
]

null_counts = df_genes_raw.select([
    count(when(col(c).isNull(), 1)).alias(f"{c}_null") for c in df_genes_raw.columns
] + [
    count(when(col(c) == "Unknown", 1)).alias(f"{c}_unknown") for c in string_columns
])

null_counts.show(vertical=True)

# COMMAND ----------

# DBTITLE 1,Enhanced Cleaning Transformations
df_genes_clean = (
    df_genes_raw
        .withColumn("gene_name", upper(trim(col("gene_name"))))
        .withColumn("official_symbol", 
                when(col("official_symbol").isNotNull(), 
                     upper(trim(col("official_symbol"))))
                .otherwise(col("gene_name")))
        .withColumn("chromosome", 
                regexp_replace(upper(trim(col("chromosome"))), "^CHR", ""))
        .withColumn("chromosome",
                when(col("chromosome").isin(
                    '1','2','3','4','5','6','7','8','9','10',
                    '11','12','13','14','15','16','17','18','19','20',
                    '21','22','X','Y','MT'
                ), col("chromosome"))
                .otherwise(None))
        .withColumn("gene_type_clean",
                when((col("gene_type") == "Unknown") | col("gene_type").isNull(),
                     when(lower(col("description")).contains("protein"), "protein-coding")
                     .when(lower(col("description")).contains("rna"), "RNA")
                     .when(lower(col("description")).contains("pseudogene"), "pseudogene")
                     .when(lower(col("description")).contains("mirna"), "miRNA")
                     .when(lower(col("description")).contains("lncrna"), "lncRNA")
                     .otherwise("protein-coding")
                )
                .otherwise(trim(col("gene_type"))))
        .withColumn("start_position",
                when((col("start_position").isNotNull()) & 
                     (col("start_position") > 0), 
                     col("start_position"))
                .otherwise(None))
    .withColumn("end_position",
                when((col("end_position").isNotNull()) & 
                     (col("end_position") > 0), 
                     col("end_position"))
                .otherwise(None))
    .withColumn("gene_length",
                when((col("start_position").isNotNull()) & 
                     (col("end_position").isNotNull()),
                     col("end_position") - col("start_position"))
                .otherwise(col("gene_length")))
    .withColumn("description",
                when((col("description").isNull()) | 
                     (col("description") == "Unknown") |
                     (col("description") == ""),
                     concat_ws(" ", 
                               lit("Gene:"), 
                               col("gene_name"),
                               lit("on chromosome"),
                               col("chromosome")))
                .otherwise(trim(col("description"))))
    .filter(col("gene_name").isNotNull())
    .filter(col("gene_id").isNotNull())
    .filter(col("chromosome").isNotNull())
    .dropDuplicates(["gene_id"])
    .withColumn("gene_type", col("gene_type_clean"))
    .drop("gene_type_clean")
)

# COMMAND ----------

# DBTITLE 1,Cleaning Summary
print(f"Cleaning complete!")
print(f"   Before: {df_genes_raw.count():,} genes")
print(f"   After:  {df_genes_clean.count():,} genes")
print(f"   Removed: {df_genes_raw.count() - df_genes_clean.count():,} invalid rows")

# COMMAND ----------

# DBTITLE 1,Verify Cleaning Results
print("\n" + "="*40)
print("VERIFY ENHANCED CLEANING")
print("="*40)

print("\n1. Gene Type Distribution AFTER Cleaning:")
df_genes_clean.groupBy("gene_type") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(20)

unknown_after = df_genes_clean.filter(
    (col("gene_type") == "Unknown") | 
    (col("gene_type").isNull())
).count()
print(f"\nGenes with Unknown gene_type after cleaning: {unknown_after}")

# COMMAND ----------

# DBTITLE 1,Chromosome Distribution & Sample Data
print("\n2. Chromosome Distribution:")
df_genes_clean.groupBy("chromosome") \
    .count() \
    .orderBy("chromosome") \
    .show(30)

print("\n3. Sample Cleaned Data:")
df_genes_clean.select(
    "gene_id", "gene_name", "chromosome", "gene_type", 
    "start_position", "end_position", "gene_length", "description"
).show(10, truncate=True)

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
print("\n" + "="*40)
print("SAVING TO SILVER LAYER")
print("="*40)

df_genes_clean.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_clean")

print(f"Saved to: {catalog_name}.silver.genes_clean")

saved_count = spark.table(f"{catalog_name}.silver.genes_clean").count()
print(f"Verified: {saved_count:,} genes in Silver layer")


# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("\n" + "="*40)
print("SUMMARY STATISTICS")
print("="*40)

summary_stats = {
    "total_genes": df_genes_clean.count(),
    "unique_chromosomes": df_genes_clean.select(countDistinct("chromosome")).collect()[0][0],
    "unique_gene_types": df_genes_clean.select(countDistinct("gene_type")).collect()[0][0],
    "genes_with_positions": df_genes_clean.filter(col("start_position").isNotNull()).count(),
    "avg_gene_length": df_genes_clean.select(avg("gene_length")).collect()[0][0]
}

print("\nProcessing Summary:")
for key, value in summary_stats.items():
    if isinstance(value, float):
        print(f"  {key}: {value:,.2f}")
    else:
        print(f"  {key}: {value:,}")

