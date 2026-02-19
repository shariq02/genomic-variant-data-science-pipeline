# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, expr
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
print("Spark Initialized")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print(f"Catalog: {catalog_name}")
print("Processing RefSeq protein-gene mappings")

# COMMAND ----------

# DBTITLE 1,Read Raw RefSeq Proteins
df_refseq_proteins_raw = spark.table(f"{catalog_name}.default.protein_gene_mapping_human")

print(f"Loaded RefSeq proteins: {df_refseq_proteins_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Inspect Schema
print("RefSeq Proteins Schema:")
df_refseq_proteins_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Sample Raw Data
df_refseq_proteins_raw.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 1: Clean RefSeq Proteins
df_proteins_clean = (
    df_refseq_proteins_raw
    .withColumn("protein_accession", trim(col("protein_accession")))
    .withColumn("gene_id", expr("try_cast(trim(gene_id) as int)"))
    .withColumn("gene_symbol", upper(trim(col("symbol"))))
    .withColumn("rna_accession", trim(col("rna_accession")))
    
    # Filter valid proteins
    .filter(col("protein_accession").isNotNull())
    .filter(col("protein_accession") != "")
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    .filter(col("gene_symbol") != "N/A")
    
    # Keep only rows with valid gene_id (filters out non-numeric)
    .filter(col("gene_id").isNotNull())
)

print(f"Clean RefSeq proteins: {df_proteins_clean.count():,}")
df_proteins_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: Create Proteins RefSeq Table
df_proteins_refseq = (
    df_proteins_clean
    .select(
        "protein_accession",
        "gene_id",
        "gene_symbol",
        "rna_accession"
    )
    .dropDuplicates(["protein_accession"])
)

print(f"Unique RefSeq proteins: {df_proteins_refseq.count():,}")
df_proteins_refseq.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save RefSeq Proteins to Silver
df_proteins_refseq.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.proteins_refseq")

print(f"Saved table: {catalog_name}.silver.proteins_refseq")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("VALIDATION SUMMARY")
print("=" * 70)

proteins_refseq_count = spark.table(f"{catalog_name}.silver.proteins_refseq").count()

print(f"\nsilver.proteins_refseq: {proteins_refseq_count:,} rows")

# Gene ID distribution
print("\nGenes with most protein isoforms:")
spark.table(f"{catalog_name}.silver.proteins_refseq") \
    .groupBy("gene_symbol") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

# Sample proteins
print("\nSample Proteins:")
spark.table(f"{catalog_name}.silver.proteins_refseq").show(10, truncate=60)

print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
