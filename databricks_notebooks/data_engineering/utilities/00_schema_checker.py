# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Checker - All Schemas
# MAGIC Dynamically shows all tables in default, silver, reference, and gold schemas

# COMMAND ----------

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("DYNAMIC SCHEMA CHECKER")
print("="*80)
print("Checking all tables in: default, silver, reference, gold")
print("="*80)

# COMMAND ----------

def check_schema(schema_name):
    """Check all tables in a schema."""
    print(f"\n{'='*80}")
    print(f"SCHEMA: {schema_name.upper()}")
    print(f"{'='*80}")
    
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
        
        if len(tables) == 0:
            print(f"No tables found in {schema_name}")
            return
        
        print(f"Total tables: {len(tables)}")
        print("\nTables:")
        for table in tables:
            print(f"  - {table.tableName}")
        
        print(f"\n{'-'*80}")
        print(f"TABLE DETAILS")
        print(f"{'-'*80}\n")
        
        for table in tables:
            table_name = table.tableName
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            print(f"\nTABLE: {schema_name}.{table_name}")
            print(f"{'='*80}")
            
            try:
                df = spark.table(full_table_name)
                row_count = df.count()
                
                print(f"Rows: {row_count:,}")
                print(f"\nSchema:")
                df.printSchema()
                
                print(f"\nSample data:")
                df.show(3, truncate=60)
                
                print(f"\n{'-'*80}\n")
                
            except Exception as e:
                print(f"Error reading table: {e}")
                print(f"\n{'-'*80}\n")
    
    except Exception as e:
        print(f"Error accessing schema {schema_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Default Schema

# COMMAND ----------

check_schema("default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Schema

# COMMAND ----------

check_schema("silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Schema

# COMMAND ----------

check_schema("reference")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Schema

# COMMAND ----------

check_schema("gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("SCHEMA CHECK COMPLETE")
print("="*80)

schemas = ["default", "silver", "reference", "gold"]

print("\nTable counts by schema:")
for schema in schemas:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}").collect()
        print(f"  {schema}: {len(tables)} tables")
    except:
        print(f"  {schema}: Not accessible or empty")

print("\nAll schemas checked")
print("Use this output as reference for feature engineering")

