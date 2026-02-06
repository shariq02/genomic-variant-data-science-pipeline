# Databricks notebook source
# MAGIC %md
# MAGIC #### EXPORT GOLD LAYER TABLES & CLEANUP
# MAGIC ##### Export 11 ML Tables + Delete Obsolete Gold Tables
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 2026
# MAGIC
# MAGIC **Tables to Export (11 total):**
# MAGIC - 5 Gold Feature Tables (from 17a-17e)
# MAGIC - 6 ML Dataset Tables (from prepare_combined_ml_dataset)
# MAGIC
# MAGIC **Cleanup:** Delete all other gold tables to free space

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("EXPORT GOLD LAYER TABLES & CLEANUP")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Read Gold Layer Tables (11 tables)
print("\nReading Gold layer tables...")
print("="*80)

# 5 Gold Feature Tables (from 17a-17e)
df_clinical = spark.table(f"{catalog_name}.gold.clinical_ml_features")
df_disease = spark.table(f"{catalog_name}.gold.disease_ml_features")
df_pharmacogene = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features")
df_structural = spark.table(f"{catalog_name}.gold.structural_variant_ml_features")
df_variant_impact = spark.table(f"{catalog_name}.gold.variant_impact_ml_features")

# 6 ML Dataset Tables (from prepare_combined_ml_dataset)
df_variants_train = spark.table(f"{catalog_name}.gold.ml_dataset_variants_train")
df_variants_val = spark.table(f"{catalog_name}.gold.ml_dataset_variants_validation")
df_variants_test = spark.table(f"{catalog_name}.gold.ml_dataset_variants_test")
df_sv_train = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_train")
df_sv_val = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_validation")
df_sv_test = spark.table(f"{catalog_name}.gold.ml_dataset_structural_variants_test")

print("\n5 GOLD FEATURE TABLES:")
print(f"  clinical_ml_features:            {df_clinical.count():,} rows, {len(df_clinical.columns)} cols")
print(f"  disease_ml_features:             {df_disease.count():,} rows, {len(df_disease.columns)} cols")
print(f"  pharmacogene_ml_features:        {df_pharmacogene.count():,} rows, {len(df_pharmacogene.columns)} cols")
print(f"  structural_variant_ml_features:  {df_structural.count():,} rows, {len(df_structural.columns)} cols")
print(f"  variant_impact_ml_features:      {df_variant_impact.count():,} rows, {len(df_variant_impact.columns)} cols")

print("\n6 ML DATASET TABLES (Train/Val/Test):")
print(f"  ml_dataset_variants_train:              {df_variants_train.count():,} rows, {len(df_variants_train.columns)} cols")
print(f"  ml_dataset_variants_validation:         {df_variants_val.count():,} rows, {len(df_variants_val.columns)} cols")
print(f"  ml_dataset_variants_test:               {df_variants_test.count():,} rows, {len(df_variants_test.columns)} cols")
print(f"  ml_dataset_structural_variants_train:   {df_sv_train.count():,} rows, {len(df_sv_train.columns)} cols")
print(f"  ml_dataset_structural_variants_val:     {df_sv_val.count():,} rows, {len(df_sv_val.columns)} cols")
print(f"  ml_dataset_structural_variants_test:    {df_sv_test.count():,} rows, {len(df_sv_test.columns)} cols")

# COMMAND ----------

# DBTITLE 1,Create Export Volume
volume_name = "gold_exports"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.gold.{volume_name}")

volume_path = f"/Volumes/{catalog_name}/gold/{volume_name}/"
print(f"\nExport volume: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Export Tables to Volume (11 tables as single CSV each)
print("\nEXPORTING 11 TABLES TO VOLUME")
print("="*80)

tables_to_export = {
    # 5 Gold Feature Tables
    "clinical_ml_features": df_clinical,
    "disease_ml_features": df_disease,
    "pharmacogene_ml_features": df_pharmacogene,
    "structural_variant_ml_features": df_structural,
    "variant_impact_ml_features": df_variant_impact,
    
    # 6 ML Dataset Tables
    "ml_dataset_variants_train": df_variants_train,
    "ml_dataset_variants_validation": df_variants_val,
    "ml_dataset_variants_test": df_variants_test,
    "ml_dataset_structural_variants_train": df_sv_train,
    "ml_dataset_structural_variants_validation": df_sv_val,
    "ml_dataset_structural_variants_test": df_sv_test
}

for table_name, df in tables_to_export.items():
    output_path = f"{volume_path}{table_name}"
    
    print(f"\nExporting {table_name}...")
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(output_path)
    
    print(f"  Exported to: {output_path}")

print("\n" + "="*80)
print("EXPORT COMPLETE - 11 TABLES")
print("="*80)

# COMMAND ----------

# DBTITLE 1,List All Gold Tables (for cleanup)
print("\nLISTING ALL GOLD TABLES")
print("="*80)

all_gold_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.gold").collect()

print(f"\nTotal gold tables: {len(all_gold_tables)}")
for table in all_gold_tables:
    table_name = table['tableName']
    print(f"  - {table_name}")

# COMMAND ----------

# DBTITLE 1,Identify Tables to Keep vs Delete
print("\nTABLE CLEANUP ANALYSIS")
print("="*80)

# Tables to KEEP (11 essential tables)
tables_to_keep = {
    "clinical_ml_features",
    "disease_ml_features",
    "pharmacogene_ml_features",
    "structural_variant_ml_features",
    "variant_impact_ml_features",
    "ml_dataset_variants_train",
    "ml_dataset_variants_validation",
    "ml_dataset_variants_test",
    "ml_dataset_structural_variants_train",
    "ml_dataset_structural_variants_validation",
    "ml_dataset_structural_variants_test"
}

# Identify tables to delete
tables_to_delete = []
for table in all_gold_tables:
    table_name = table['tableName']
    if table_name not in tables_to_keep:
        tables_to_delete.append(table_name)

print(f"\nTables to KEEP: {len(tables_to_keep)}")
for t in sorted(tables_to_keep):
    print(f"  KEEP: {t}")

print(f"\nTables to DELETE: {len(tables_to_delete)}")
for t in sorted(tables_to_delete):
    print(f"  DELETE: {t}")

# COMMAND ----------

# DBTITLE 1,Delete Obsolete Gold Tables (FREE SPACE)
print("\nDELETING OBSOLETE GOLD TABLES")
print("="*80)

if len(tables_to_delete) == 0:
    print("No tables to delete - gold layer already clean")
else:
    print(f"Deleting {len(tables_to_delete)} obsolete tables...\n")
    
    deleted_count = 0
    failed_deletes = []
    
    for table_name in tables_to_delete:
        try:
            print(f"  Deleting: {table_name}...")
            spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.gold.{table_name}")
            deleted_count += 1
            print(f"    DELETED")
        except Exception as e:
            print(f"    FAILED: {e}")
            failed_deletes.append(table_name)
    
    print("\n" + "="*80)
    print("CLEANUP SUMMARY")
    print("="*80)
    print(f"Successfully deleted: {deleted_count}/{len(tables_to_delete)} tables")
    
    if failed_deletes:
        print(f"\nFailed to delete ({len(failed_deletes)}):")
        for t in failed_deletes:
            print(f"  - {t}")
    
    print("\n" + "="*80)
    print("GOLD LAYER CLEANUP COMPLETE")
    print("="*80)

# COMMAND ----------

# DBTITLE 1,Verify Export & Final Gold State
print("\nEXPORT VERIFICATION")
print("="*80)

try:
    for table_name, df in tables_to_export.items():
        folder_path = f"{volume_path}{table_name}/"
        
        print(f"\n{table_name}:")
        print("-"*70)
        
        df_count = df.count()
        print(f"  DataFrame rows: {df_count:,}")
        print(f"  DataFrame columns: {len(df.columns)}")
        
        try:
            files = dbutils.fs.ls(folder_path)
            csv_files = [f for f in files if f.path.endswith('.csv') and 'part-' in f.path]
            
            if csv_files:
                csv_file = csv_files[0]
                size_mb = csv_file.size / (1024 * 1024)
                print(f"  Exported file: {csv_file.name}")
                print(f"  File size: {size_mb:.2f} MB")
                
                df_verify = spark.read.csv(folder_path, header=True)
                verify_count = df_verify.count()
                verify_cols = len(df_verify.columns)
                print(f"  Verified rows: {verify_count:,}")
                print(f"  Verified columns: {verify_cols}")
                
                if df_count == verify_count:
                    print(f"  Status: OK")
                else:
                    print(f"  Status: WARNING - Row mismatch!")
            else:
                print(f"  Status: ERROR - No CSV file found")
        
        except Exception as e:
            print(f"  Status: ERROR - {str(e)[:100]}")

except Exception as e:
    print(f"\nVerification error: {e}")

# COMMAND ----------

# DBTITLE 1,Final Gold Layer State
print("\n" + "="*80)
print("FINAL GOLD LAYER STATE")
print("="*80)

final_gold_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.gold").collect()
print(f"\nRemaining gold tables: {len(final_gold_tables)}")
for table in final_gold_tables:
    print(f"  - {table['tableName']}")

print("\n" + "="*80)
print("EXPORT & CLEANUP COMPLETE")
print("="*80)
print(f"\n11 tables exported to: {volume_path}")
print(f"Obsolete tables deleted: {len(tables_to_delete)}")
print("\nNEXT STEP: Download using download_from_databricks.py")
print("="*80)
