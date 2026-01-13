# Databricks notebook source
# MAGIC %md
# MAGIC #### EXPORT LARGE GOLD LAYER TABLES
# MAGIC ##### Handle 5GB+ Tables with Direct Export
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC
# MAGIC **Problem:** gene_features and gene_disease_association are 5GB+ (only 10K rows downloadable via UI)
# MAGIC
# MAGIC **Solution:** Export to Volume as single CSV, then download via Databricks CLI or direct link

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("="*70)
print("EXPORT LARGE GOLD LAYER TABLES")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Gold Layer Tables
print("\nReading Gold layer tables...")

df_gene_features = spark.table(f"{catalog_name}.gold.gene_features")
df_chromosome_features = spark.table(f"{catalog_name}.gold.chromosome_features")
df_gene_disease = spark.table(f"{catalog_name}.gold.gene_disease_association")
df_ml_features = spark.table(f"{catalog_name}.gold.ml_features")

print(f" gene_features: {df_gene_features.count():,} rows")
print(f" chromosome_features: {df_chromosome_features.count():,} rows")
print(f" gene_disease_association: {df_gene_disease.count():,} rows")
print(f" ml_features: {df_ml_features.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create Export Volume
volume_name = "gold_exports"
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.gold.{volume_name}
""")

volume_path = f"/Volumes/{catalog_name}/gold/{volume_name}/"
print(f"Export volume: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Export Tables to Volume (Single CSV per table)
print("\n" + "="*70)
print("EXPORTING TO VOLUME (SINGLE CSV FILES)")
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
    
    # Export as SINGLE CSV file (coalesce to 1 partition)
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(output_path)
    
    print(f"  Exported to: {output_path}")

print("\n" + "="*70)
print("EXPORT COMPLETE!")
print("="*70)

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

# DBTITLE 1,METHOD 1: Download via Databricks CLI (RECOMMENDED FOR LARGE FILES)
print("\n" + "="*70)
print("METHOD 1: DATABRICKS CLI (RECOMMENDED)")
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

# COMMAND ----------

# DBTITLE 1,METHOD 2: Export to External Storage (S3/Azure/GCS)
print("\n" + "="*70)
print("METHOD 2: EXPORT TO CLOUD STORAGE")
print("="*70)

print("\nIf you have S3/Azure Blob/GCS configured:")
print("\n# For S3:")
print("s3_path = 's3://your-bucket/gold-exports/'")
print("for table_name, df in tables_to_export.items():")
print("    df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{s3_path}{table_name}.csv')")

print("\n# For Azure Blob:")
print("azure_path = 'wasbs://container@storage.blob.core.windows.net/gold-exports/'")
print("for table_name, df in tables_to_export.items():")
print("    df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{azure_path}{table_name}.csv')")

print("\nThen download from cloud storage to local machine")

# COMMAND ----------

# DBTITLE 1,METHOD 3: Split Large Tables into Chunks
print("\n" + "="*70)
print("METHOD 3: SPLIT LARGE TABLES (ALTERNATIVE)")
print("="*70)

print("\nFor very large tables, split into manageable chunks:")

def export_in_chunks(df, table_name, chunk_size=50000):
    """Export large dataframe in chunks"""
    total_rows = df.count()
    num_chunks = (total_rows // chunk_size) + 1
    
    print(f"\nExporting {table_name} in {num_chunks} chunks...")
    
    for i in range(num_chunks):
        chunk_path = f"{volume_path}{table_name}_chunk_{i+1:03d}"
        
        df_chunk = df.limit(chunk_size).offset(i * chunk_size)
        
        df_chunk.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(chunk_path)
        
        print(f"  Chunk {i+1}/{num_chunks} exported")

# Example usage (uncomment to use):
# export_in_chunks(df_gene_features, "gene_features_chunked", chunk_size=100000)
# export_in_chunks(df_gene_disease, "gene_disease_chunked", chunk_size=100000)

print("\nAfter downloading chunks, combine locally:")
print("  cd data/processed")
print("  cat gene_features_chunk_*.csv > gene_features.csv")

# COMMAND ----------

# DBTITLE 1,METHOD 4: Use Databricks SQL Warehouse (If Available)
print("\n" + "="*70)
print("METHOD 4: SQL WAREHOUSE QUERY & DOWNLOAD")
print("="*70)

print("\n1. Go to: SQL Warehouses in Databricks")
print("2. Create a query:")
print("   SELECT * FROM workspace.gold.gene_features")
print("3. Run query")
print("4. Click 'Download Results' (supports larger downloads)")
print("5. Repeat for other tables")

# COMMAND ----------

# DBTITLE 1,RECOMMENDED APPROACH SUMMARY
print("\n" + "="*70)
print("RECOMMENDED APPROACH FOR YOUR DATA")
print("="*70)

print("\nBased on your data sizes:")
print("  • gene_features: 5GB+ → Use Databricks CLI")
print("  • gene_disease_association: 5GB+ → Use Databricks CLI")
print("  • chromosome_features: Small → UI download OK")
print("  • ml_features: Medium → UI download OK")

print("\nStep-by-step:")
print("\n1. Small tables (chromosome_features, ml_features):")
print("   • Go to Catalog → workspace → gold → gold_exports")
print("   • Download directly from UI")

print("\n2. Large tables (gene_features, gene_disease_association):")
print("   • Install Databricks CLI: pip install databricks-cli")
print("   • Configure: databricks configure --token")
print("   • Download via CLI commands shown above")

print("\n3. Verify downloads:")
print("   • Check file sizes match")
print("   • Open in text editor to verify CSV format")

print("\n4. Load to PostgreSQL:")
print("   • python scripts/transformation/load_gold_to_postgres.py")

# COMMAND ----------

# DBTITLE 1,Generate Direct Download Links (if possible)
print("\n" + "="*70)
print("DIRECT DOWNLOAD PATHS")
print("="*70)

print("\nDirect volume paths for CLI download:")
for table_name in tables_to_export.keys():
    print(f"\n{table_name}:")
    print(f"  {volume_path}{table_name}/")

print("\n" + "="*70)
print("Use these paths with 'databricks fs cp' command")
print("="*70)

# COMMAND ----------

# DBTITLE 1,EXPORT VERIFICATION - ROW COUNTS AND FILE SIZES
print("\n" + "="*70)
print("EXPORT VERIFICATION")
print("="*70)

try:
    for table_name, df in tables_to_export.items():
        folder_path = f"{volume_path}{table_name}/"  # REMOVED .csv extension
        
        print(f"\n{table_name}:")
        print("-"*70)
        
        # Row count from DataFrame
        df_count = df.count()
        print(f"  DataFrame rows: {df_count:,}")
        
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
                print(f"  Verified rows: {verify_count:,}")
                
                if df_count == verify_count:
                    print(f"  Status: [OK] Export verified")
                else:
                    print(f"  Status: [WARNING] Row count mismatch!")
                    print(f"    Expected: {df_count:,}")
                    print(f"    Actual: {verify_count:,}")
            else:
                print(f"  Status: [ERROR] No CSV file found")
                
        except Exception as e:
            print(f"  Status: [ERROR] {str(e)[:100]}")
            
except Exception as e:
    print(f"\nVerification error: {e}")

print("\n" + "="*70)
print("EXPECTED VS ACTUAL ROW COUNTS")
print("="*70)

expected_counts = {
    "gene_features": "~190,000",
    "chromosome_features": "~25",
    "gene_disease_association": "~500,000+",
    "ml_features": "~190,000"
}

print("\nComparison:")
for table, expected in expected_counts.items():
    actual = tables_to_export[table].count()
    print(f"  {table:30s} Expected: {expected:15s} | Actual: {actual:,}")

print("\n" + "="*70)
print("READY FOR DOWNLOAD")
print("="*70)
print("\nRun on local machine:")
print("  python scripts/databricks/export_csv_from_databricks.py")
print("="*70)

