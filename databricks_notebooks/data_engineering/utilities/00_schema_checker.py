# Databricks notebook source
# MAGIC %md
# MAGIC ## Schema Exporter - All Schemas to CSV
# MAGIC Exports schema and sample data for all tables to CSV files

# COMMAND ----------

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SCHEMA EXPORTER TO CSV")
print("="*80)

# COMMAND ----------

def export_schema_to_csv(schema_name):
    """Export schema information to CSV."""
    print(f"\nExporting {schema_name} schema...")
    
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
        
        if len(tables) == 0:
            print(f"No tables in {schema_name}")
            return
        
        print(f"Found {len(tables)} tables")
        
        volume_name = f"{schema_name}_exports"
        
        try:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
            print(f"Volume ready: {catalog_name}.{schema_name}.{volume_name}")
        except Exception as e:
            print(f"Volume check: {e}")
        
        schema_rows = []
        
        for table in tables:
            table_name = table.tableName
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            try:
                df = spark.table(full_table_name)
                
                for field in df.schema.fields:
                    dtype = str(field.dataType)
                    
                    if 'BooleanType' in dtype:
                        pg_type = 'BOOLEAN'
                    elif 'LongType' in dtype or 'IntegerType' in dtype:
                        pg_type = 'INT'
                    elif 'DoubleType' in dtype or 'FloatType' in dtype:
                        pg_type = 'DOUBLE'
                    elif 'DateType' in dtype:
                        pg_type = 'DATE'
                    else:
                        pg_type = 'STRING'
                    
                    schema_rows.append((table_name, field.name, pg_type))
            
            except Exception as e:
                print(f"Error processing {table_name}: {e}")
        
        if schema_rows:
            schema_df = spark.createDataFrame(schema_rows, ["table_name", "column_name", "data_type"])
            
            output_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{schema_name}_schema"
            
            schema_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            
            print(f"Exported schema: {len(schema_rows)} columns")
            print(f"Location: {output_path}")
    
    except Exception as e:
        print(f"Error exporting {schema_name}: {e}")

# COMMAND ----------

def export_sample_data_to_csv(schema_name, sample_rows=5):
    """Export sample data for all tables to CSV."""
    print(f"\nExporting {schema_name} sample data...")
    
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
        
        if len(tables) == 0:
            print(f"No tables in {schema_name}")
            return
        
        volume_name = f"{schema_name}_exports"
        
        try:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
        except:
            pass
        
        for table in tables:
            table_name = table.tableName
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            try:
                df = spark.table(full_table_name)
                sample_df = df.limit(sample_rows)
                
                for col_name in sample_df.columns:
                    col_type = str([f.dataType for f in sample_df.schema.fields if f.name == col_name][0])
                    if 'ArrayType' in col_type or 'MapType' in col_type or 'StructType' in col_type:
                        sample_df = sample_df.withColumn(col_name, sample_df[col_name].cast("string"))
                
                output_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{table_name}_sample"
                
                sample_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
                
                print(f"  {table_name}: {sample_rows} sample rows")
            
            except Exception as e:
                print(f"  Error with {table_name}: {e}")
    
    except Exception as e:
        print(f"Error exporting samples for {schema_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Default Schema

# COMMAND ----------

export_schema_to_csv("default")
export_sample_data_to_csv("default", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Silver Schema

# COMMAND ----------

export_schema_to_csv("silver")
export_sample_data_to_csv("silver", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Reference Schema

# COMMAND ----------

export_schema_to_csv("reference")
export_sample_data_to_csv("reference", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Gold Schema

# COMMAND ----------

export_schema_to_csv("gold")
export_sample_data_to_csv("gold", 5)

# COMMAND ----------

print("\n" + "="*80)
print("EXPORT COMPLETE")
print("="*80)

schemas = ["default", "silver", "reference", "gold"]

print("\nExported files:")
for schema in schemas:
    print(f"\n{schema.upper()} SCHEMA:")
    print(f"  Schema: /Volumes/{catalog_name}/{schema}/{schema}_exports/{schema}_schema/")
    print(f"  Samples: /Volumes/{catalog_name}/{schema}/{schema}_exports/[table_name]_sample/")



