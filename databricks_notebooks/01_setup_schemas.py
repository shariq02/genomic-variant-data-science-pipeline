# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Setup Bronze / Silver / Gold Schemas  
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC **Purpose:** Create schema layers in Databricks Unity Catalog  
# MAGIC **Run this notebook FIRST**
# MAGIC
# MAGIC **Updated for ALL data:**
# MAGIC - gene_metadata_all (190K genes)
# MAGIC - clinvar_all_variants (4M+ variants)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("Spark Session initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Display Current Catalog & Database
print("Current Catalog:", spark.catalog.currentCatalog())
print("Current Database:", spark.catalog.currentDatabase())

# COMMAND ----------

# DBTITLE 1,Use Workspace Catalog
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
print(f"Created schema: {catalog_name}.bronze")

# COMMAND ----------

# DBTITLE 1,Create Silver Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")
print(f"Created schema: {catalog_name}.silver")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
print(f"Created schema: {catalog_name}.gold")

# COMMAND ----------

# DBTITLE 1,Verify Schemas Created
print("\n" + "="*70)
print("SCHEMAS CREATED")
print("="*70)

schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
for schema in schemas:
    print(f"  - {schema.databaseName}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Verify Uploaded Data Header
print("\n" + "="*70)
print("VERIFYING YOUR UPLOADED DATA")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Verify gene_metadata_all Table
try:
    df_genes = spark.table("workspace.default.gene_metadata_all")
    gene_count = df_genes.count()
    print(f"Found gene_metadata_all: {gene_count:,} rows")
    
    gene_types = df_genes.groupBy("gene_type").count().orderBy(col("count").desc())
    print("\nGene types:")
    display(gene_types.limit(10))
    
    print("\nSample genes:")
    display(df_genes.limit(5))
except Exception as e:
    print(f"Error reading gene_metadata_all: {e}")
    print("Please upload: data/raw/genes/gene_metadata_all.csv")

# COMMAND ----------

# DBTITLE 1,Verify clinvar_all_variants Table
try:
    df_variants = spark.table("workspace.default.clinvar_all_variants")
    variant_count = df_variants.count()
    print(f"\nFound clinvar_all_variants: {variant_count:,} rows")
    
    clinical_sig = df_variants.groupBy("clinical_significance").count().orderBy(col("count").desc())
    print("\nClinical significance distribution:")
    display(clinical_sig.limit(10))
    
    print("\nSample variants:")
    display(df_variants.limit(5))
except Exception as e:
    print(f"Error reading clinvar_all_variants: {e}")
    print("Please upload: data/raw/variants/clinvar_all_variants.csv")

# COMMAND ----------

# DBTITLE 1,Final Completion Message
print("\n" + "="*70)
print("SETUP COMPLETE!")
print("="*70)
print("Next: Run 02_gene_data_processing.py")
print("="*70)
