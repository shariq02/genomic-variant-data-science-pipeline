# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lit, when, coalesce
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
print("Processing gene-disease links and OMIM catalog")

# COMMAND ----------

# DBTITLE 1,Read Raw Tables
df_gene_disease_raw = spark.table(f"{catalog_name}.default.gene_disease_ncbi")
df_omim_raw = spark.table(f"{catalog_name}.default.omim_mim_2_gene_medgen_raw")

print(f"Loaded gene-disease associations: {df_gene_disease_raw.count():,}")
print(f"Loaded OMIM entries: {df_omim_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Inspect Schemas
print("Gene-Disease Schema:")
df_gene_disease_raw.printSchema()

print("\nOMIM Schema:")
df_omim_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Sample Raw Data
print("Gene-Disease Sample:")
df_gene_disease_raw.show(3, truncate=60)

print("\nOMIM Sample:")
df_omim_raw.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 1: Clean Gene-Disease Links
df_gene_disease_clean = (
    df_gene_disease_raw
    .withColumn("gene_id", trim(col("gene_id")).cast("int"))
    .withColumn("gene_symbol", upper(trim(col("gene_symbol"))))
    .withColumn("medgen_id", trim(col("medgen_id")))
    .withColumn("omim_id", trim(col("omim_id")))
    .withColumn("disease_name", trim(col("disease_name")))
    .withColumn("association_type", trim(col("association_type")))
    
    # Filter valid rows
    .filter(col("gene_id").isNotNull())
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    .filter(col("medgen_id").isNotNull())
    .filter(col("medgen_id") != "")
)

print(f"Clean gene-disease links: {df_gene_disease_clean.count():,}")
df_gene_disease_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: Create Gene-Disease Links Table
df_gene_disease_links = (
    df_gene_disease_clean
    .select(
        "gene_id",
        "gene_symbol",
        "medgen_id",
        "omim_id",
        "disease_name",
        "association_type"
    )
    .dropDuplicates(["gene_id", "medgen_id"])
)

print(f"Unique gene-disease links: {df_gene_disease_links.count():,}")
df_gene_disease_links.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Gene-Disease Links to Silver
df_gene_disease_links.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.gene_disease_links")

print(f"Saved table: {catalog_name}.silver.gene_disease_links")

# COMMAND ----------

# DBTITLE 1,STEP 3: Clean OMIM Catalog
df_omim_clean = (
    df_omim_raw
    .withColumn("mim_number", trim(col("mim_number")))
    .withColumn("gene_id", trim(col("gene_id")).cast("int"))
    .withColumn("entry_type", trim(col("entry_type")))
    .withColumn("medgen_cui", trim(col("medgen_cui")))
    
    # Filter valid entries
    .filter(col("mim_number").isNotNull())
    .filter(col("mim_number") != "")
)

print(f"Clean OMIM entries: {df_omim_clean.count():,}")
df_omim_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 4: Create OMIM Catalog Table
df_omim_catalog = (
    df_omim_clean
    .select(
        "mim_number",
        "gene_id",
        "entry_type",
        "medgen_cui"
    )
    .dropDuplicates(["mim_number"])
)

print(f"Unique OMIM entries: {df_omim_catalog.count():,}")
df_omim_catalog.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save OMIM Catalog to Silver
df_omim_catalog.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.omim_catalog")

print(f"Saved table: {catalog_name}.silver.omim_catalog")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("VALIDATION SUMMARY")
print("=" * 70)

gene_disease_count = spark.table(f"{catalog_name}.silver.gene_disease_links").count()
omim_count = spark.table(f"{catalog_name}.silver.omim_catalog").count()

print(f"\nsilver.gene_disease_links: {gene_disease_count:,} rows")
spark.table(f"{catalog_name}.silver.gene_disease_links").select(
    "gene_id", "gene_symbol", "medgen_id", "disease_name"
).show(5, truncate=60)

print(f"\nsilver.omim_catalog: {omim_count:,} rows")
spark.table(f"{catalog_name}.silver.omim_catalog").show(5, truncate=60)

print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
