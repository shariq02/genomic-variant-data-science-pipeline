# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace
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
print("Processing UniProt Swiss-Prot proteins")

# COMMAND ----------

# DBTITLE 1,Read Raw UniProt Proteins
df_uniprot_raw = spark.table(f"{catalog_name}.default.uniprot_swissprot_human")

print(f"Loaded UniProt proteins: {df_uniprot_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Inspect Schema
print("UniProt Proteins Schema:")
df_uniprot_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Sample Raw Data
df_uniprot_raw.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 1: Clean UniProt Proteins
df_uniprot_clean = (
    df_uniprot_raw
    .withColumn("uniprot_accession", trim(col("uniprot_accession")))
    .withColumn("gene_symbol", upper(trim(col("gene_symbol"))))
    .withColumn("protein_name", trim(col("protein_name")))
    .withColumn("taxid", trim(col("taxid")))
    
    # Remove evidence codes in curly braces from gene_symbol
    .withColumn("gene_symbol", regexp_replace(col("gene_symbol"), "\\{.*?\\}", ""))
    .withColumn("gene_symbol", trim(col("gene_symbol")))
    
    # Remove evidence codes from protein_name
    .withColumn("protein_name", regexp_replace(col("protein_name"), "\\{.*?\\}", ""))
    .withColumn("protein_name", trim(col("protein_name")))
    
    # Filter valid proteins
    .filter(col("uniprot_accession").isNotNull())
    .filter(col("uniprot_accession") != "")
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    
    # Human proteins only (taxid = 9606)
    .filter(col("taxid") == "9606")
)

print(f"Clean UniProt proteins: {df_uniprot_clean.count():,}")
df_uniprot_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: Create UniProt Proteins Table
df_proteins_uniprot = (
    df_uniprot_clean
    .select(
        "uniprot_accession",
        "gene_symbol",
        "protein_name",
        "taxid"
    )
    .dropDuplicates(["uniprot_accession"])
)

print(f"Unique UniProt proteins: {df_proteins_uniprot.count():,}")
df_proteins_uniprot.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save UniProt Proteins to Silver
df_proteins_uniprot.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.proteins_uniprot")

print(f"Saved table: {catalog_name}.silver.proteins_uniprot")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("VALIDATION SUMMARY")
print("=" * 70)

proteins_uniprot_count = spark.table(f"{catalog_name}.silver.proteins_uniprot").count()

print(f"\nsilver.proteins_uniprot: {proteins_uniprot_count:,} rows")

# Genes with most UniProt entries
print("\nGenes with most UniProt entries:")
spark.table(f"{catalog_name}.silver.proteins_uniprot") \
    .groupBy("gene_symbol") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

# Sample proteins
print("\nSample Proteins:")
spark.table(f"{catalog_name}.silver.proteins_uniprot").show(10, truncate=60)

print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
