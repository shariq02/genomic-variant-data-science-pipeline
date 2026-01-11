# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Setup Bronze / Silver / Gold Schemas  
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:**  3 January 2026  
# MAGIC **Purpose:**  Create schema layers in Databricks Unity Catalog  
# MAGIC **Run this notebook FIRST**

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("Spark Session initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Display Current Catalog & Database
# Display current catalog
print("Current Catalog:", spark.catalog.currentCatalog())
print("Current Database:", spark.catalog.currentDatabase())

# COMMAND ----------

# DBTITLE 1,Use Workspace Catalog
# STEP 1: USE WORKSPACE CATALOG

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
# STEP 2: CREATE SCHEMAS (DATABASE LAYERS)
# Bronze schema (raw data)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
print(f"Created schema: {catalog_name}.bronze")

# COMMAND ----------

# DBTITLE 1,Create Silver Schema
# Silver schema (cleaned data)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")
print(f"Created schema: {catalog_name}.silver")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
# Gold schema (aggregated data)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
print(f"Created schema: {catalog_name}.gold")

# COMMAND ----------

# DBTITLE 1,Verify Schemas Created
print("\n" + "="*30)
print("SCHEMAS CREATED")
print("="*30)

schemas = spark.sql(
    f"SHOW SCHEMAS IN {catalog_name}"
).collect()
for schema in schemas:
    print(f"  - {schema.databaseName}")
print("="*30)

# COMMAND ----------

# DBTITLE 1,Verify Uploaded Data Header
print("\n" + "="*30)
print("VERIFYING YOUR UPLOADED DATA")
print("="*30)

# COMMAND ----------

# DBTITLE 1,Verify gene_metadata Table
# Check gene_metadata
try:
    df_genes = spark.table("workspace.default.gene_metadata")
    gene_count = df_genes.count()
    print(f"Found gene_metadata: {gene_count:,} rows")
    print("\nSample genes:")
    display(df_genes.limit(5))
except Exception as e:
    print(f"Error reading gene_metadata: {e}")

# COMMAND ----------

# DBTITLE 1,Verify clinvar_pathogenic Table
# Check clinvar_pathogenic
try:
    df_variants = spark.table("workspace.default.clinvar_pathogenic")
    variant_count = df_variants.count()
    print(f"\nFound clinvar_pathogenic: {variant_count:,} rows")
    print("\nSample variants:")
    display(df_variants.limit(5))
except Exception as e:
    print(f"Error reading clinvar_pathogenic: {e}")

# COMMAND ----------

# DBTITLE 1,Final Completion Message
print("SETUP COMPLETE!")
