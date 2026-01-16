# Databricks notebook source
# MAGIC %md
# MAGIC #### EXPORT GOLD LAYER TABLES - UPDATED FOR 65-COLUMN SCHEMA
# MAGIC ##### Export After Running COMPLETE Pipeline
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 17, 2026 (Updated)
# MAGIC
# MAGIC **IMPORTANT:** Run this AFTER:
# MAGIC - 02_gene_data_processing_COMPLETE.py
# MAGIC - 04_create_gene_alias_mapper.py
# MAGIC - 05_feature_engineering_COMPLETE.py

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("EXPORT GOLD LAYER TABLES - UPDATED SCHEMA")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Verify Schema Before Export
print("\nVERIFYING UPDATED SCHEMA")
print("="*70)

# Check gene_features schema
df_gene_features_check = spark.table(f"{catalog_name}.gold.gene_features")
gene_cols = len(df_gene_features_check.columns)

print(f"\ngene_features columns: {gene_cols}")

if gene_cols == 65:
    print("  Status: CORRECT - New 65-column schema")
elif gene_cols == 51:
    print("  Status: ERROR - Still old 51-column schema!")
    print("  Action: Run 05_feature_engineering_COMPLETE.py first")
    raise Exception("Schema not updated - please run COMPLETE scripts first")
else:
    print(f"  Status: UNKNOWN - Expected 65, got {gene_cols}")

# Check for new columns
expected_new_cols = [
    'is_kinase', 'is_phosphatase', 'is_receptor', 'is_enzyme', 'is_transporter',
    'has_glycoprotein', 'has_receptor_keyword', 'has_enzyme_keyword',
    'has_kinase_keyword', 'has_binding_keyword',
    'primary_function', 'biological_process', 'cellular_location', 'druggability_score'
]

present_cols = [c for c in expected_new_cols if c in df_gene_features_check.columns]
missing_cols = [c for c in expected_new_cols if c not in df_gene_features_check.columns]

print(f"\nNew columns check:")
print(f"  Present: {len(present_cols)}/14")

if missing_cols:
    print(f"  MISSING: {', '.join(missing_cols)}")
    print(f"\n  STOP: Run 05_feature_engineering_COMPLETE.py first!")
    raise Exception("Schema not updated - export aborted")
else:
    print(f"  All new columns present")
    print(f"  Ready to export!")

# COMMAND ----------

# DBTITLE 1,Read Gold Layer Tables
print("\nReading Gold layer tables...")

df_gene_features = spark.table(f"{catalog_name}.gold.gene_features")
df_chromosome_features = spark.table(f"{catalog_name}.gold.chromosome_features")
df_gene_disease = spark.table(f"{catalog_name}.gold.gene_disease_association")
df_ml_features = spark.table(f"{catalog_name}.gold.ml_features")

print(f"  gene_features: {df_gene_features.count():,} rows, {len(df_gene_features.columns)} columns")
print(f"  chromosome_features: {df_chromosome_features.count():,} rows, {len(df_chromosome_features.columns)} columns")
print(f"  gene_disease_association: {df_gene_disease.count():,} rows, {len(df_gene_disease.columns)} columns")
print(f"  ml_features: {df_ml_features.count():,} rows, {len(df_ml_features.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Create Export Volume
volume_name = "gold_exports"
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.gold.{volume_name}
""")

volume_path = f"/Volumes/{catalog_name}/gold/{volume_name}/"
print(f"\nExport volume: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Export Tables to Volume (Single CSV per table)
print("\nEXPORTING TO VOLUME (SINGLE CSV FILES)")
print("="*70)

tables_to_export = {
    "gene_features": df_gene_features,
    "chromosome_features": df_chromosome_features,
    "gene_disease_association": df_gene_disease,
    "ml_features": df_ml_features
}

for table_name, df in tables_to_export.items():
    output_path = f"{volume_path}{table_name}"
    
    print(f"\nExporting {table_name}...")
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    
    # Export as SINGLE CSV file (coalesce to 1 partition)
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(output_path)
    
    print(f"  Exported to: {output_path}")

print("\n" + "="*70)
print("EXPORT COMPLETE!")

# COMMAND ----------

# DBTITLE 1,EXPORT VERIFICATION - ROW COUNTS AND FILE SIZES
print("\n" + "="*70)
print("EXPORT VERIFICATION")
print("="*70)

try:
    for table_name, df in tables_to_export.items():
        folder_path = f"{volume_path}{table_name}/"
        
        print(f"\n{table_name}:")
        print("-"*70)
        
        # Row count from DataFrame
        df_count = df.count()
        print(f"  DataFrame rows: {df_count:,}")
        print(f"  DataFrame columns: {len(df.columns)}")
        
        # Check exported files
        try:
            files = dbutils.fs.ls(folder_path)
            csv_files = [f for f in files if f.path.endswith('.csv') and 'part-' in f.path]
            
            if csv_files:
                csv_file = csv_files[0]
                size_mb = csv_file.size / (1024 * 1024)
                print(f"  Exported file: {csv_file.name}")
                print(f"  File size: {size_mb:.2f} MB")
                
                # Verify by reading back
                df_verify = spark.read.csv(folder_path, header=True)
                verify_count = df_verify.count()
                verify_cols = len(df_verify.columns)
                print(f"  Verified rows: {verify_count:,}")
                print(f"  Verified columns: {verify_cols}")
                
                if df_count == verify_count:
                    print(f"  Status: OK - Export verified")
                else:
                    print(f"  Status: WARNING - Row count mismatch!")
                    print(f"    Expected: {df_count:,}")
                    print(f"    Actual: {verify_count:,}")
            else:
                print(f"  Status: ERROR - No CSV file found")
                
        except Exception as e:
            print(f"  Status: ERROR - {str(e)[:100]}")
            
except Exception as e:
    print(f"\nVerification error: {e}")

# COMMAND ----------

# DBTITLE 1,List Exported Files
print("\nExported files in volume:")
print("-"*70)

for table_name in tables_to_export.keys():
    table_path = f"{volume_path}{table_name}/"
    try:
        files = dbutils.fs.ls(table_path)
        csv_file = [f for f in files if f.path.endswith('.csv')][0]
        size_mb = csv_file.size / (1024 * 1024)
        print(f"\n{table_name}:")
        print(f"  Path: {csv_file.path}")
        print(f"  Size: {size_mb:.2f} MB")
    except Exception as e:
        print(f"\n{table_name}: Error - {e}")

# COMMAND ----------

# DBTITLE 1,Download Instructions - Databricks CLI
print("\n" + "="*70)
print("DOWNLOAD INSTRUCTIONS")
print("="*70)

print("\n1. Install Databricks CLI (if not installed):")
print("   pip install databricks-cli")

print("\n2. Configure authentication:")
print("   databricks configure --token")
print("   Host: https://your-workspace.cloud.databricks.com")
print("   Token: (generate from User Settings > Access Tokens)")

print("\n3. Download files:")
print(f"   databricks fs cp -r {volume_path}gene_features/ ./data/processed/gene_features/")
print(f"   databricks fs cp -r {volume_path}chromosome_features/ ./data/processed/chromosome_features/")
print(f"   databricks fs cp -r {volume_path}gene_disease_association/ ./data/processed/gene_disease_association/")
print(f"   databricks fs cp -r {volume_path}ml_features/ ./data/processed/ml_features/")

print("\n4. Each folder contains a .csv file - extract it:")
print("   cd data/processed/gene_features")
print("   mv *.csv ../gene_features.csv")
print("   cd ..")
print("   rm -rf gene_features/")
print("   (Repeat for other tables)")

print("\n" + "="*70)
print("AFTER DOWNLOAD")
print("="*70)

print("\n1. Verify CSV columns:")
print("   head -1 data/processed/gene_features.csv | grep 'is_kinase'")
print("   head -1 data/processed/gene_features.csv | grep 'druggability_score'")

print("\n2. Load to PostgreSQL:")
print("   python scripts/transformation/load_gold_to_postgres_COMPLETE.py")

print("\n3. Expected output:")
print("   Present: 14/14")
print("   SUCCESS: All new columns present!")

# COMMAND ----------

# DBTITLE 1,FINAL SUMMARY
print("\n" + "="*70)
print("EXPORT SUMMARY")
print("="*70)

expected_counts = {
    "gene_features": "~20,000",
    "chromosome_features": "~25",
    "gene_disease_association": "~400,000",
    "ml_features": "~20,000"
}

print("\nRow count comparison:")
for table, expected in expected_counts.items():
    actual = tables_to_export[table].count()
    actual_cols = len(tables_to_export[table].columns)
    print(f"  {table:30s} Rows: {actual:,} | Columns: {actual_cols}")

print("\n" + "="*70)
print("SCHEMA VERIFICATION")
print("="*70)

# Check gene_features has new columns
if 'is_kinase' in df_gene_features.columns:
    print("  PASS - is_kinase present")
else:
    print("  FAIL - is_kinase missing")

if 'primary_function' in df_gene_features.columns:
    print("  PASS - primary_function present")
else:
    print("  FAIL - primary_function missing")

if 'druggability_score' in df_gene_features.columns:
    print("  PASS - druggability_score present")
else:
    print("  FAIL - druggability_score missing")

print(f"\nTotal new columns present: {len(present_cols)}/14")

print("\n" + "="*70)
print("READY FOR DOWNLOAD")
print("="*70)
print("\nUse Databricks CLI commands shown above to download")
print("Or download directly from:")
print(f"  Catalog > {catalog_name} > gold > {volume_name}")
print("="*70)
