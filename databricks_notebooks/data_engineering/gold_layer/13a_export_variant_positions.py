# Databricks notebook source
# MAGIC %md
# MAGIC #### EXPORT VARIANT POSITIONS FOR UCSC DOWNLOAD
# MAGIC ##### Step 0: Prepare data for local UCSC conservation score extraction
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 26, 2026
# MAGIC
# MAGIC **Purpose:** Export variant positions to CSV for local processing
# MAGIC **Output:** Unity Catalog Volume (~150 MB CSV)

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("EXPORTING VARIANT POSITIONS FOR UCSC DOWNLOAD")

# COMMAND ----------

# Create volume for exports (if not exists)
volume_name = "conservation_exports"
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.silver.{volume_name}
""")

volume_path = f"/Volumes/{catalog_name}/silver/{volume_name}/"
print(f"\nExport volume: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Export Variant Positions
print("\nLOADING VARIANT POSITIONS")
print("="*80)

# Load variants
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
total_count = df_variants.count()

print(f"Total variants: {total_count:,}")

# Select only needed columns
df_positions = df_variants.select(
    "variant_id",
    "chromosome", 
    "position"
)

# Export as SINGLE CSV file
output_path = f"{volume_path}variant_positions"

print(f"\nExporting to: {output_path}")
print("This may take a few minutes...")

# Export as single CSV file (coalesce to 1 partition)
df_positions.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f" Export complete!")

# COMMAND ----------

# DBTITLE 1,Verify Export
df_verify = spark.read.csv(f"{output_path}", header=True)
verify_count = df_verify.count()
verify_cols = len(df_verify.columns)

print(f"Rows exported: {verify_count:,}")
print(f"Columns: {verify_cols}")
print(f"Expected rows: {total_count:,}")

if verify_count == total_count:
    print(f" Verification PASSED - All rows exported")
else:
    print(f" Verification FAILED - Row count mismatch!")

print(f"\nSample data:")
df_verify.show(10)

# Find the actual CSV file
files = dbutils.fs.ls(output_path)
csv_file = [f for f in files if f.path.endswith('.csv') and 'part-' in f.path][0]
csv_size_mb = csv_file.size / (1024 * 1024)

print(f"\nExported file:")
print(f"  Name: {csv_file.name}")
print(f"  Size: {csv_size_mb:.1f} MB")
print(f"  Full path: {csv_file.path}")
