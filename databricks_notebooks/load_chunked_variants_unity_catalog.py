# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD CHUNKED CSV FILES - ROBUST VERSION
# MAGIC ##### Handles All Schema Variations Across Chunks
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC
# MAGIC **Features:**
# MAGIC - Enforces consistent schema across ALL chunks
# MAGIC - Handles type conflicts automatically
# MAGIC - Continues on errors and reports at end
# MAGIC - Validates data after load

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("="*70)
print("ROBUST CHUNKED VARIANT LOADER")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Define Strict Schema (ALL STRING for safety)
# Using STRING for all columns that might have type conflicts
# We'll convert to proper types later in processing

schema = StructType([
    StructField("variant_id", StringType(), True),
    StructField("allele_id", StringType(), True),
    StructField("accession", StringType(), True),
    StructField("gene_id", StringType(), True),
    StructField("gene_name", StringType(), True),
    StructField("clinical_significance", StringType(), True),
    StructField("clinical_significance_simple", StringType(), True),
    StructField("disease", StringType(), True),
    StructField("phenotype_ids", StringType(), True),
    StructField("chromosome", StringType(), True),  # STRING (handles X, Y, MT)
    StructField("position", StringType(), True),  # STRING initially (convert later)
    StructField("stop_position", StringType(), True),  # STRING initially
    StructField("cytogenetic", StringType(), True),
    StructField("variant_type", StringType(), True),
    StructField("variant_name", StringType(), True),
    StructField("reference_allele", StringType(), True),
    StructField("alternate_allele", StringType(), True),
    StructField("review_status", StringType(), True),
    StructField("number_submitters", StringType(), True),  # STRING (some chunks have NULL)
    StructField("origin", StringType(), True),
    StructField("origin_simple", StringType(), True),
    StructField("last_evaluated", StringType(), True),
    StructField("assembly", StringType(), True)
])

print("✓ Schema defined: ALL STRING types for compatibility")
print("  (Will convert to proper types in processing step)")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
schema_name = "default"
volume_name = "variant_chunks"
TABLE_NAME = f"{catalog_name}.{schema_name}.clinvar_all_variants"

# Create volume
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
""")

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"
print(f"Volume: {volume_path}")

# COMMAND ----------

# DBTITLE 1,List All CSV Files
uploaded_files = dbutils.fs.ls(volume_path)
csv_files = [f.path for f in uploaded_files if f.path.endswith('.csv')]
csv_files_sorted = sorted(csv_files)

print(f"\nFound {len(csv_files_sorted)} CSV files")
print(f"First file: {csv_files_sorted[0].split('/')[-1]}")
print(f"Last file: {csv_files_sorted[-1].split('/')[-1]}")

if len(csv_files_sorted) == 0:
    print("\nERROR: No CSV files found!")
    print("Please upload chunk files to the volume first.")
    dbutils.notebook.exit("No files")

# COMMAND ----------

# DBTITLE 1,Drop Existing Table (Fresh Start)
print("\nDropping existing table (if exists)...")
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
print("✓ Ready for fresh load")

# COMMAND ----------

# DBTITLE 1,Load First Chunk (Create Table)
print("\n" + "="*70)
print("STEP 1: CREATE TABLE FROM FIRST CHUNK")
print("="*70)

first_file = csv_files_sorted[0]
print(f"\nLoading: {first_file.split('/')[-1]}")

df_first = spark.read.csv(
    first_file,
    header=True,
    schema=schema,
    mode="PERMISSIVE",  # Continue on malformed rows
    columnNameOfCorruptRecord="_corrupt_record"
)

row_count = df_first.count()
print(f"Rows: {row_count:,}")

# Create table
df_first.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"✓ Table created: {TABLE_NAME}")
total_rows = row_count
successful_chunks = 1

# COMMAND ----------

# DBTITLE 1,Load Remaining Chunks (with Error Tracking)
print("\n" + "="*70)
print("STEP 2: APPEND REMAINING CHUNKS")
print("="*70)

errors = []
successful_loads = []

for i, chunk_file in enumerate(csv_files_sorted[1:], start=2):
    chunk_name = chunk_file.split('/')[-1]
    
    try:
        print(f"\n[{i}/{len(csv_files_sorted)}] Loading: {chunk_name}")
        
        df_chunk = spark.read.csv(
            chunk_file,
            header=True,
            schema=schema,
            mode="PERMISSIVE",
            columnNameOfCorruptRecord="_corrupt_record"
        )
        
        row_count = df_chunk.count()
        
        # Append
        df_chunk.write \
            .mode("append") \
            .saveAsTable(TABLE_NAME)
        
        total_rows += row_count
        successful_chunks += 1
        successful_loads.append(chunk_name)
        
        print(f"  ✓ {row_count:,} rows | Total: {total_rows:,}")
        
    except Exception as e:
        error_msg = str(e)[:200]
        errors.append({
            "chunk": chunk_name,
            "number": i,
            "error": error_msg
        })
        print(f"  ✗ FAILED: {error_msg}")
        continue

print(f"\n" + "="*70)
print(f"LOADING COMPLETE")
print(f"  Successful: {successful_chunks}/{len(csv_files_sorted)}")
print(f"  Failed: {len(errors)}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Error Report
if errors:
    print("\n" + "="*70)
    print("ERROR REPORT")
    print("="*70)
    
    print(f"\n{len(errors)} chunks failed to load:")
    for err in errors:
        print(f"\n  Chunk #{err['number']}: {err['chunk']}")
        print(f"    Error: {err['error']}")
    
    print("\n" + "="*70)
else:
    print("\n✓ ALL CHUNKS LOADED SUCCESSFULLY!")

# COMMAND ----------

# DBTITLE 1,Verify Final Table
print("\n" + "="*70)
print("VERIFICATION")
print("="*70)

df_final = spark.table(TABLE_NAME)
final_count = df_final.count()
final_columns = len(df_final.columns)

print(f"\nTable: {TABLE_NAME}")
print(f"  Total rows: {final_count:,}")
print(f"  Columns: {final_columns}")
print(f"  Chunks loaded: {successful_chunks}/{len(csv_files_sorted)}")
print(f"  Success rate: {(successful_chunks/len(csv_files_sorted)*100):.1f}%")

# COMMAND ----------

# DBTITLE 1,Schema Verification
print("\nSchema (first 10 columns):")
df_final.printSchema()

print("\nSample data:")
display(df_final.limit(10))

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
print("\n" + "="*70)
print("DATA QUALITY CHECKS")
print("="*70)

# Check for NULL values in key columns
print("\n1. NULL Values Check:")
null_checks = df_final.select([
    (col(c).isNull().cast("int")).alias(c) 
    for c in ["gene_name", "chromosome", "position", "clinical_significance"]
])
null_counts = null_checks.agg(*[spark_sum(c).alias(c) for c in null_checks.columns])
display(null_counts)

# Check chromosome distribution
print("\n2. Chromosome Distribution:")
display(
    df_final.groupBy("chromosome")
            .count()
            .orderBy("chromosome")
)

# Check clinical significance
print("\n3. Clinical Significance:")
display(
    df_final.groupBy("clinical_significance")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
)

# Top genes
print("\n4. Top 20 Genes by Variant Count:")
display(
    df_final.groupBy("gene_name")
            .count()
            .orderBy(col("count").desc())
            .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\n" + "="*70)
print("FINAL SUMMARY")
print("="*70)

summary = {
    "Total Chunks": len(csv_files_sorted),
    "Successful Loads": successful_chunks,
    "Failed Loads": len(errors),
    "Total Rows Loaded": f"{final_count:,}",
    "Expected Rows": "~4,000,000+",
    "Table Name": TABLE_NAME,
    "Data Complete": "Yes" if len(errors) == 0 else f"Missing {len(errors)} chunks"
}

for key, value in summary.items():
    print(f"  {key:.<30} {value}")

print("="*70)

if len(errors) == 0:
    print("\n✓ SUCCESS: All data loaded successfully!")
    print("\nNext: Run 03_variant_data_processing.py")
else:
    print(f"\n⚠ WARNING: {len(errors)} chunks failed")
    print("  Review error report above and retry failed chunks")

print("="*70)

# COMMAND ----------

# DBTITLE 1,Save Load Report (Optional)
# Create a summary DataFrame
if errors or successful_loads:
    report_data = []
    
    for i, chunk in enumerate(csv_files_sorted, 1):
        chunk_name = chunk.split('/')[-1]
        status = "SUCCESS" if chunk_name in successful_loads or i == 1 else "FAILED"
        error = next((e["error"] for e in errors if e["chunk"] == chunk_name), None)
        
        report_data.append({
            "chunk_number": i,
            "chunk_name": chunk_name,
            "status": status,
            "error": error if error else "None"
        })
    
    df_report = spark.createDataFrame(report_data)
    
    print("\nLoad Report:")
    display(df_report)
    
    # Optionally save report
    # df_report.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.variant_load_report")
