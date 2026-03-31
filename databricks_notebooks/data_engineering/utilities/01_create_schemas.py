# Databricks notebook source
# MAGIC %md
# MAGIC ## CREATE DATABRICKS SCHEMAS
# MAGIC ### Setup all required schemas before processing
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC
# MAGIC **Purpose:** Create all required schemas in Databricks
# MAGIC
# MAGIC **Run Order:** STEP 0 - Run this FIRST before any processing
# MAGIC ```
# MAGIC 0. 00_create_schemas.py (THIS SCRIPT - RUN FIRST)
# MAGIC 1. 02_gene_data_processing_ENHANCED.py
# MAGIC 2. 03_variant_data_processing_ENHANCED.py
# MAGIC 3. 04_create_gene_alias_mapper.py
# MAGIC 4. 05_feature_engineering.py
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("CREATE DATABRICKS SCHEMAS")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"

print("Catalog: {}".format(catalog_name))
print("Creating schemas: silver, gold, reference")

# COMMAND ----------

# DBTITLE 1,Create Silver Schema
print("Creating silver schema...")
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.silver".format(catalog_name))
print("SUCCESS: silver schema created")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
print("Creating gold schema...")
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.gold".format(catalog_name))
print("SUCCESS: gold schema created")

# COMMAND ----------

# DBTITLE 1,Create Reference Schema
print("Creating reference schema...")
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.reference".format(catalog_name))
print("SUCCESS: reference schema created")

# COMMAND ----------

# DBTITLE 1,Verify All Schemas
print("VERIFYING ALL SCHEMAS")
print("="*50)

schemas = spark.sql("SHOW SCHEMAS IN {}".format(catalog_name)).collect()

print("")
print("Available schemas in {}:".format(catalog_name))
for schema in schemas:
    schema_name = schema.databaseName
    print("  - {}".format(schema_name))

# COMMAND ----------

# DBTITLE 1,Check Each Schema
print("SCHEMA DETAILS")
print("="*50)

for schema_name in ["silver", "gold", "reference"]:
    full_schema = "{}.{}".format(catalog_name, schema_name)
    
    try:
        tables = spark.sql("SHOW TABLES IN {}".format(full_schema)).collect()
        table_count = len(tables)
        
        print("Schema: {}".format(full_schema))
        print("  Tables: {}".format(table_count))
        
        if table_count > 0:
            print("  Existing tables:")
            for table in tables:
                print("    - {}".format(table.tableName))
        else:
            print("  (No tables yet - ready for processing)")
            
    except Exception as e:
        print("ERROR checking {}: {}".format(full_schema, str(e)))

# COMMAND ----------

# DBTITLE 1,Final Summary
print("SUCCESS: ALL SCHEMAS READY")
print("="*50)

print("Created/verified schemas:")
print("  1. {}.silver   - Cleaned and enriched data".format(catalog_name))
print("  2. {}.gold     - Analytical features".format(catalog_name))
print("  3. {}.reference - Lookup tables".format(catalog_name))
