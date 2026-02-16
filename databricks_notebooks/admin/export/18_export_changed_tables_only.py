# Databricks notebook source
# MAGIC %md
# MAGIC #### SMART EXPORT - CHANGED TABLES ONLY
# MAGIC ##### Compares Databricks vs exported versions, exports only changed tables
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 2026

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SMART EXPORT - CHANGED TABLES ONLY")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Configuration
TABLES_TO_MONITOR = {
    "clinical_ml_features": "gold",
    "disease_ml_features": "gold",
    "pharmacogene_ml_features": "gold",
    "structural_variant_ml_features": "gold",
    "variant_impact_ml_features": "gold",
    "ml_dataset_variants_train": "gold",
    "ml_dataset_variants_validation": "gold",
    "ml_dataset_variants_test": "gold",
    "ml_dataset_structural_variants_train": "gold",
    "ml_dataset_structural_variants_validation": "gold",
    "ml_dataset_structural_variants_test": "gold"
}

VOLUME_NAME = "gold_exports"
CHECKPOINT_FILE = f"/Volumes/{catalog_name}/gold/{VOLUME_NAME}/.export_metadata.json"

# COMMAND ----------

# DBTITLE 1,Create Export Volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.gold.{VOLUME_NAME}")
volume_path = f"/Volumes/{catalog_name}/gold/{VOLUME_NAME}/"
print(f"Export volume: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Load Previous Export Metadata
print("\nLOADING PREVIOUS EXPORT METADATA")
print("="*80)

try:
    metadata_content = dbutils.fs.head(CHECKPOINT_FILE)
    previous_exports = json.loads(metadata_content)
    print(f"Found metadata for {len(previous_exports)} tables")
except:
    previous_exports = {}
    print("No previous metadata found - will export all tables")

# COMMAND ----------

# DBTITLE 1,Check Current Table States
print("\nCHECKING CURRENT TABLE STATES")
print("="*80)

current_states = {}

for table_name, schema in TABLES_TO_MONITOR.items():
    try:
        df = spark.table(f"{catalog_name}.{schema}.{table_name}")
        row_count = df.count()
        col_count = len(df.columns)
        
        current_states[table_name] = {
            "rows": row_count,
            "columns": col_count,
            "schema": schema
        }
        
        print(f"{table_name}: {row_count:,} rows, {col_count} cols")
        
    except Exception as e:
        print(f"{table_name}: ERROR - {str(e)[:100]}")
        current_states[table_name] = {"error": str(e)}

# COMMAND ----------

# DBTITLE 1,Identify Changed Tables
print("\nIDENTIFYING CHANGED TABLES")
print("="*80)

tables_to_export = []

for table_name, current_state in current_states.items():
    if "error" in current_state:
        print(f"{table_name}: SKIP (error loading)")
        continue
    
    if table_name not in previous_exports:
        print(f"{table_name}: EXPORT (new table)")
        tables_to_export.append(table_name)
        continue
    
    prev_state = previous_exports[table_name]
    
    if current_state["rows"] != prev_state.get("rows"):
        print(f"{table_name}: EXPORT (row count changed: {prev_state.get('rows'):,} -> {current_state['rows']:,})")
        tables_to_export.append(table_name)
        continue
    
    if current_state["columns"] != prev_state.get("columns"):
        print(f"{table_name}: EXPORT (column count changed: {prev_state.get('columns')} -> {current_state['columns']})")
        tables_to_export.append(table_name)
        continue
    
    print(f"{table_name}: SKIP (unchanged)")

print(f"\nTables to export: {len(tables_to_export)}/{len(TABLES_TO_MONITOR)}")

# COMMAND ----------

# DBTITLE 1,Export Changed Tables
if len(tables_to_export) == 0:
    print("\nNo tables need export - all up to date")
    dbutils.notebook.exit("SUCCESS: No exports needed")

print("\nEXPORTING CHANGED TABLES")
print("="*80)

export_results = {}

for table_name in tables_to_export:
    try:
        schema = current_states[table_name]["schema"]
        df = spark.table(f"{catalog_name}.{schema}.{table_name}")
        output_path = f"{volume_path}{table_name}"
        
        print(f"\n{table_name}:")
        print(f"  Rows: {current_states[table_name]['rows']:,}")
        print(f"  Columns: {current_states[table_name]['columns']}")
        print(f"  Exporting to: {output_path}")
        
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        files = dbutils.fs.ls(output_path)
        csv_files = [f for f in files if f.path.endswith('.csv')]
        
        if csv_files:
            csv_file = csv_files[0]
            size_mb = csv_file.size / (1024 * 1024)
            print(f"  Exported: {csv_file.name} ({size_mb:.2f} MB)")
            
            export_results[table_name] = {
                "success": True,
                "size_mb": size_mb,
                "file": csv_file.name
            }
        else:
            print(f"  ERROR: No CSV file created")
            export_results[table_name] = {"success": False, "error": "No CSV file"}
            
    except Exception as e:
        print(f"  ERROR: {str(e)[:100]}")
        export_results[table_name] = {"success": False, "error": str(e)[:100]}

# COMMAND ----------

# DBTITLE 1,Update Metadata
print("\nUPDATING EXPORT METADATA")
print("="*80)

new_metadata = previous_exports.copy()

for table_name in tables_to_export:
    if export_results[table_name]["success"]:
        new_metadata[table_name] = current_states[table_name].copy()
        new_metadata[table_name]["export_file"] = export_results[table_name]["file"]
        new_metadata[table_name]["size_mb"] = export_results[table_name]["size_mb"]
        print(f"{table_name}: Metadata updated")

metadata_json = json.dumps(new_metadata, indent=2)
dbutils.fs.put(CHECKPOINT_FILE, metadata_json, overwrite=True)

print(f"\nMetadata saved to: {CHECKPOINT_FILE}")

# COMMAND ----------

# DBTITLE 1,Export Summary
print("\n" + "="*80)
print("EXPORT SUMMARY")
print("="*80)

successful = [t for t in tables_to_export if export_results[t]["success"]]
failed = [t for t in tables_to_export if not export_results[t]["success"]]

print(f"\nSuccessful: {len(successful)}/{len(tables_to_export)}")
for table in successful:
    print(f"  - {table}")

if failed:
    print(f"\nFailed: {len(failed)}")
    for table in failed:
        print(f"  - {table}: {export_results[table]['error']}")

print("\n" + "="*80)
print("NEXT STEP: Run download_from_databricks.py")
print("="*80)
