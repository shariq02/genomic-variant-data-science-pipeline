# Databricks notebook source
# MAGIC %md
# MAGIC #### EXPORT GOLD LAYER TABLES
# MAGIC ##### Export All ML Feature Tables from Gold Layer
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** January 2026
# MAGIC
# MAGIC **Gold Layer Tables (5):**
# MAGIC - clinical_ml_features (69 columns)
# MAGIC - disease_ml_features (83 columns)
# MAGIC - pharmacogene_ml_features (64 columns)
# MAGIC - structural_variant_ml_features (46 columns)
# MAGIC - variant_impact_ml_features (75 columns)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("EXPORT GOLD LAYER TABLES")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Gold Layer Tables
print("\nReading Gold layer tables...")

df_clinical = spark.table(f"{catalog_name}.gold.clinical_ml_features")
df_disease = spark.table(f"{catalog_name}.gold.disease_ml_features")
df_pharmacogene = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features")
df_structural = spark.table(f"{catalog_name}.gold.structural_variant_ml_features")
df_variant_impact = spark.table(f"{catalog_name}.gold.variant_impact_ml_features")

print(f"  clinical_ml_features:            {df_clinical.count():,} rows, {len(df_clinical.columns)} columns")
print(f"  disease_ml_features:             {df_disease.count():,} rows, {len(df_disease.columns)} columns")
print(f"  pharmacogene_ml_features:        {df_pharmacogene.count():,} rows, {len(df_pharmacogene.columns)} columns")
print(f"  structural_variant_ml_features:  {df_structural.count():,} rows, {len(df_structural.columns)} columns")
print(f"  variant_impact_ml_features:      {df_variant_impact.count():,} rows, {len(df_variant_impact.columns)} columns")

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
    "clinical_ml_features": df_clinical,
    "disease_ml_features": df_disease,
    "pharmacogene_ml_features": df_pharmacogene,
    "structural_variant_ml_features": df_structural,
    "variant_impact_ml_features": df_variant_impact
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

print("\n" + "="*70)
print("EXPORT COMPLETE!")

# COMMAND ----------

# DBTITLE 1,EXPORT VERIFICATION
print("\n" + "="*70)
print("EXPORT VERIFICATION")
print("="*70)

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

# DBTITLE 1,Download Instructions
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
print(f"   databricks fs cp -r {volume_path}clinical_ml_features/ ./data/processed/clinical_ml_features/")
print(f"   databricks fs cp -r {volume_path}disease_ml_features/ ./data/processed/disease_ml_features/")
print(f"   databricks fs cp -r {volume_path}pharmacogene_ml_features/ ./data/processed/pharmacogene_ml_features/")
print(f"   databricks fs cp -r {volume_path}structural_variant_ml_features/ ./data/processed/structural_variant_ml_features/")
print(f"   databricks fs cp -r {volume_path}variant_impact_ml_features/ ./data/processed/variant_impact_ml_features/")

print("\n4. Each folder contains a .csv file - extract it:")
print("   cd data/processed/clinical_ml_features")
print("   mv *.csv ../clinical_ml_features.csv")
print("   cd ..")
print("   rm -rf clinical_ml_features/")
print("   (Repeat for other tables)")

print("\n5. Load to PostgreSQL:")
print("   python scripts/transformation/load_gold_to_postgres.py")

# COMMAND ----------

# DBTITLE 1,FINAL SUMMARY
print("\n" + "="*70)
print("EXPORT SUMMARY")
print("="*70)

print("\nRow count comparison:")
for table_name, df in tables_to_export.items():
    actual = df.count()
    actual_cols = len(df.columns)
    print(f"  {table_name:40s} Rows: {actual:,} | Columns: {actual_cols}")

print("\n" + "="*70)
print("READY FOR DOWNLOAD")
print("="*70)
print("\nUse Databricks CLI commands shown above to download")
print("Or download directly from:")
print(f"  Catalog > {catalog_name} > gold > {volume_name}")
print("="*70)
