# Databricks notebook source
# MAGIC %md
# MAGIC #### EXTRACT PROTEIN DOMAINS FROM UNIPROT
# MAGIC ##### Add Pfam domains and functional features
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 27, 2026
# MAGIC
# MAGIC **Purpose:** Extract protein domain information for variant impact analysis
# MAGIC
# MAGIC **Data Source:** UniProt Swiss-Prot (already loaded)  
# MAGIC **Creates:** silver.protein_domains

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, explode, split, regexp_extract, trim, upper
)
import requests
import time
from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("INITIALIZED SPARK FOR PROTEIN DOMAINS EXTRACTION")

# COMMAND ----------

# DBTITLE 1,Load UniProt Proteins
df_uniprot = spark.table(f"{catalog_name}.silver.proteins_uniprot")
uniprot_count = df_uniprot.count()
print(f"Loaded UniProt proteins: {uniprot_count:,}")

# COMMAND ----------

# DBTITLE 1,Parse Existing Protein Names for Domains
print("EXTRACTING DOMAINS FROM PROTEIN NAMES")
print("="*80)
print("Note: This extracts basic domain info from protein_name field")

# Many UniProt names contain domain information
# Example: "Zinc finger protein 1 (C2H2-type domain)"
df_domains_from_names = (
    df_uniprot
    .withColumn("has_zinc_finger",
                col("protein_name").rlike("(?i)zinc finger"))
    .withColumn("has_kinase_domain",
                col("protein_name").rlike("(?i)kinase"))
    .withColumn("has_receptor_domain",
                col("protein_name").rlike("(?i)receptor"))
    .withColumn("has_sh2_domain",
                col("protein_name").rlike("(?i)SH2"))
    .withColumn("has_sh3_domain",
                col("protein_name").rlike("(?i)SH3"))
    .withColumn("has_ph_domain",
                col("protein_name").rlike("(?i)PH domain|pleckstrin"))
    .withColumn("has_death_domain",
                col("protein_name").rlike("(?i)death domain"))
    .withColumn("has_leucine_zipper",
                col("protein_name").rlike("(?i)leucine zipper"))
    .withColumn("has_helix_loop_helix",
                col("protein_name").rlike("(?i)helix-loop-helix"))
    .withColumn("has_immunoglobulin",
                col("protein_name").rlike("(?i)immunoglobulin"))
)

# Count domain occurrences (USE spark_sum instead of sum)
domain_stats = df_domains_from_names.select(
    spark_sum(when(col("has_zinc_finger"), 1).otherwise(0)).alias("zinc_finger"),
    spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("kinase"),
    spark_sum(when(col("has_receptor_domain"), 1).otherwise(0)).alias("receptor"),
    spark_sum(when(col("has_sh2_domain"), 1).otherwise(0)).alias("sh2"),
    spark_sum(when(col("has_sh3_domain"), 1).otherwise(0)).alias("sh3")
).collect()[0]

print(f"\nDomain counts from protein names:")
print(f"  Zinc fingers: {domain_stats['zinc_finger']:,}")
print(f"  Kinases: {domain_stats['kinase']:,}")
print(f"  Receptors: {domain_stats['receptor']:,}")
print(f"  SH2 domains: {domain_stats['sh2']:,}")
print(f"  SH3 domains: {domain_stats['sh3']:,}")

# COMMAND ----------

# DBTITLE 1,Create Basic Protein Domains Table
print("\nCREATING BASIC PROTEIN DOMAINS TABLE")
print("="*80)

# Create a simple domain table from name parsing
df_protein_domains_basic = (
    df_domains_from_names
    .select(
        "uniprot_accession",
        "gene_symbol",
        "protein_name",
        "has_zinc_finger",
        "has_kinase_domain",
        "has_receptor_domain",
        "has_sh2_domain",
        "has_sh3_domain",
        "has_ph_domain",
        "has_death_domain",
        "has_leucine_zipper",
        "has_helix_loop_helix",
        "has_immunoglobulin"
    )
    .withColumn("domain_count",
                when(col("has_zinc_finger"), 1).otherwise(0) +
                when(col("has_kinase_domain"), 1).otherwise(0) +
                when(col("has_receptor_domain"), 1).otherwise(0) +
                when(col("has_sh2_domain"), 1).otherwise(0) +
                when(col("has_sh3_domain"), 1).otherwise(0) +
                when(col("has_ph_domain"), 1).otherwise(0) +
                when(col("has_death_domain"), 1).otherwise(0) +
                when(col("has_leucine_zipper"), 1).otherwise(0) +
                when(col("has_helix_loop_helix"), 1).otherwise(0) +
                when(col("has_immunoglobulin"), 1).otherwise(0))
    .withColumn("has_functional_domain",
                col("domain_count") > 0)
)

domain_table_count = df_protein_domains_basic.count()
print(f"Protein domain records: {domain_table_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Basic Protein Domains
df_protein_domains_basic.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.protein_domains")

print(f"Saved: {catalog_name}.silver.protein_domains")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*80)
print("PROTEIN DOMAINS EXTRACTION COMPLETE")
print("="*80)

print(f"\nTotal proteins: {uniprot_count:,}")
print(f"Proteins with identified domains: {df_protein_domains_basic.filter(col('has_functional_domain')).count():,}")

print("\nDomain types identified:")
print("  - Zinc fingers")
print("  - Kinase domains")
print("  - Receptor domains")
print("  - SH2/SH3 domains")
print("  - PH domains")
print("  - Death domains")
print("  - Leucine zippers")
print("  - Helix-loop-helix")
print("  - Immunoglobulin domains")

print("\nTable created:")
print(f"  {catalog_name}.silver.protein_domains")

print("\nLIMITATIONS:")
print("  - Basic domain detection from protein names only")
print("  - No exact domain boundaries (start/end positions)")
