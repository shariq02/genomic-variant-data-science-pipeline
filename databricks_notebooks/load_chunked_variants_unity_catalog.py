# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD CHUNKED CSV FILES TO UNITY CATALOG
# MAGIC ##### Upload Multiple Variant Chunks to Volume, Then Combine
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC **Purpose:** Load multiple variant CSV chunks via Unity Catalog Volume
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Create Unity Catalog Volume
# MAGIC 2. Upload chunks via UI to Volume
# MAGIC 3. Run this notebook to combine into one table

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("="*70)
print("LOAD CHUNKED VARIANT FILES - UNITY CATALOG")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Create Volume (Run Once)
catalog_name = "workspace"
schema_name = "default"
volume_name = "variant_chunks"

# Create volume if not exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
""")

print(f"Volume created: {catalog_name}.{schema_name}.{volume_name}")
print(f"Upload path: /Volumes/{catalog_name}/{schema_name}/{volume_name}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MANUAL STEP: Upload Chunks to Volume
# MAGIC 
# MAGIC 1. Go to: **Catalog** → **workspace** → **default** → **variant_chunks** (Volume)
# MAGIC 2. Click **Upload** button
# MAGIC 3. Upload all `clinvar_variants_part_*.csv` files
# MAGIC 4. Come back and run the cells below

# COMMAND ----------

# DBTITLE 1,List Uploaded Files
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

uploaded_files = dbutils.fs.ls(volume_path)
csv_files = [f.path for f in uploaded_files if f.path.endswith('.csv')]

print(f"\nFound {len(csv_files)} CSV files in volume:")
for f in sorted(csv_files):
    print(f"  - {f}")

# COMMAND ----------

# DBTITLE 1,Load First Chunk (Create Table)
print("\n" + "="*70)
print("LOADING DATA")
print("="*70)

if len(csv_files) == 0:
    print("ERROR: No CSV files found in volume!")
    print("Please upload the chunk files first.")
    dbutils.notebook.exit("No files found")

# Sort files to load in order
csv_files_sorted = sorted(csv_files)

print(f"\n1. Loading first chunk (creating table)...")
first_file = csv_files_sorted[0]
print(f"   File: {first_file}")

df_first = spark.read.csv(
    first_file,
    header=True,
    inferSchema=True
)

row_count = df_first.count()
print(f"   Rows: {row_count:,}")

# Create table
TABLE_NAME = f"{catalog_name}.default.clinvar_all_variants"

df_first.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"   Created table: {TABLE_NAME}")
total_rows = row_count

# COMMAND ----------

# DBTITLE 1,Load Remaining Chunks (Append)
print("\n2. Loading remaining chunks (appending)...")

for i, chunk_file in enumerate(csv_files_sorted[1:], start=2):
    try:
        print(f"\n   Loading chunk {i}/{len(csv_files)}...")
        print(f"   File: {chunk_file}")
        
        df_chunk = spark.read.csv(
            chunk_file,
            header=True,
            inferSchema=True
        )
        
        row_count = df_chunk.count()
        print(f"   Rows: {row_count:,}")
        
        # Append to table
        df_chunk.write \
            .mode("append") \
            .saveAsTable(TABLE_NAME)
        
        total_rows += row_count
        print(f"   Appended successfully")
        print(f"   Total rows so far: {total_rows:,}")
        
    except Exception as e:
        print(f"   ERROR loading chunk {i}: {e}")
        continue

# COMMAND ----------

# DBTITLE 1,Verify Final Table
print("\n" + "="*70)
print("VERIFICATION")
print("="*70)

df_final = spark.table(TABLE_NAME)
final_count = df_final.count()

print(f"\nFinal table: {TABLE_NAME}")
print(f"Total rows: {final_count:,}")
print(f"Columns: {len(df_final.columns)}")

print("\nSchema:")
df_final.printSchema()

print("\nSample data:")
display(df_final.limit(5))

# COMMAND ----------

# DBTITLE 1,Data Quality Check
print("\nData Quality Checks:")

print("\n1. Variants by clinical significance:")
display(
    df_final.groupBy("clinical_significance")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
)

print("\n2. Variants by chromosome:")
display(
    df_final.groupBy("chromosome")
            .count()
            .orderBy("chromosome")
)

print("\n3. Top genes by variant count:")
display(
    df_final.groupBy("gene_name")
            .count()
            .orderBy(col("count").desc())
            .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Cleanup Volume (Optional)
# Uncomment to delete uploaded chunks after successful load
# dbutils.fs.rm(volume_path, recurse=True)
# print("Volume cleaned up")

print("\n" + "="*70)
print("SUCCESS: All chunks loaded into single table!")
print("="*70)
print(f"Table: {TABLE_NAME}")
print(f"Total rows: {final_count:,}")
print("="*70)
