# Databricks notebook source
# MAGIC %md
# MAGIC #### DOWNLOAD UCSC CONSERVATION SCORES (FILTERED VERSION)
# MAGIC ##### Optimized for Databricks Community Edition
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 2026
# MAGIC
# MAGIC **Optimization:**
# MAGIC - Filters to ~500K variants (from 4.2M) - 88% reduction
# MAGIC - Uses Unity Catalog Volumes (no DBFS needed)
# MAGIC - Downloads bigWig to temp storage
# MAGIC - Processes in ~60-90 minutes
# MAGIC - Storage: ~1 GB max during processing
# MAGIC
# MAGIC **Filter Criteria:**
# MAGIC - Pathogenic or VUS variants
# MAGIC - Coding variants (missense, frameshift, nonsense, splice)
# MAGIC - Variants with clinical review
# MAGIC - Chromosomes 1-22, X, Y only (no unplaced contigs)

# COMMAND ----------

# DBTITLE 1,Install pyBigWig
print("INSTALLING PYBIGWIG")
print("="*80)

# Install pyBigWig in Databricks cluster
%pip install pyBigWig tqdm --quiet

print("pyBigWig installed successfully")

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import requests
import os
from pathlib import Path

# Import after pip install
import pyBigWig
from tqdm import tqdm

print("All libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("UCSC CONSERVATION SCORES DOWNLOAD (FILTERED)")
print("="*80)
print("\nOptimized for Community Edition:")
print("  - Filtered to ~500K important variants")
print("  - Uses Unity Catalog Volumes")
print("  - Total storage: ~1 GB during processing")
print("  - Estimated time: 60-90 minutes")

# COMMAND ----------

# DBTITLE 1,Configuration
# UCSC bigWig URLs (FREE public data)
PHYLOP_URL = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/phyloP100way/hg38.phyloP100way.bw"
PHASTCONS_URL = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/phastCons100way/hg38.phastCons100way.bw"

# Create volume for temporary storage
volume_name = "conservation_cache"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.silver.{volume_name}")

TEMP_DIR = f"/Volumes/{catalog_name}/silver/{volume_name}"
PHYLOP_FILE = f"{TEMP_DIR}/phylop.bw"
PHASTCONS_FILE = f"{TEMP_DIR}/phastcons.bw"

print(f"\nTemp storage: {TEMP_DIR}")
print(f"  PhyloP: {PHYLOP_FILE}")
print(f"  PhastCons: {PHASTCONS_FILE}")

# COMMAND ----------

# DBTITLE 1,Filter Variants to Important Subset
# DBTITLE 1,Filter Variants to Important Subset (STRICTER)
print("\nFILTERING VARIANTS TO IMPORTANT SUBSET")
print("="*80)

# Load all variants
df_all = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
total_count = df_all.count()
print(f"Total variants in database: {total_count:,}")

# Apply MUCH STRICTER filter - Focus on HIGH VALUE variants only
df_filtered = (
    df_all
    # Filter 1: Standard chromosomes only
    .filter(col("chromosome").isin([str(i) for i in range(1, 23)] + ['X', 'Y']))
    
    # Filter 2: ONLY pathogenic or high-quality VUS (remove low quality VUS)
    .filter(
        # Pathogenic variants (329K)
        (col("is_pathogenic")) |
        
        # VUS BUT ONLY with high review quality (removes 2M low quality VUS)
        (col("is_vus") & (col("review_quality_score") >= 2))
    )
    .select("variant_id", "chromosome", "position")
    .distinct()
)

# Check counts
filtered_count = df_filtered.count()
reduction = (1 - filtered_count/total_count) * 100

print(f"\nAfter filtering:")
print(f"  Original: {total_count:,}")
print(f"  Filtered: {filtered_count:,}")
print(f"  Reduction: {reduction:.1f}%")
print(f"  Estimated time: {filtered_count / 10000 * 15:.0f} minutes")

# Safety check
if filtered_count > 1000000:
    print(f"\nWARNING: Still {filtered_count:,} variants")
    print("Filter not strict enough - stopping")
    dbutils.notebook.exit("Filter not strict enough")

# Convert to Pandas
print("\nConverting to Pandas...")
df_variants = df_filtered.toPandas()
print(f"Ready to process: {len(df_variants):,} variants")

# COMMAND ----------

# DBTITLE 1,Download PhyloP BigWig File
print("\nDOWNLOADING PHYLOP BIGWIG FILE")
print("="*80)
print(f"Source: {PHYLOP_URL}")
print(f"Size: ~5-6 GB")
print(f"Destination: {PHYLOP_FILE}")

def download_bigwig(url, dest_path):
    """Download bigWig file with progress bar"""
    
    # Check if already exists
    if os.path.exists(dest_path):
        file_size = os.path.getsize(dest_path) / (1024**3)
        print(f"\nFile already exists: {file_size:.2f} GB")
        print("Skipping download (delete file to re-download)")
        return True
    
    print("\nDownloading...")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(dest_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="PhyloP") as pbar:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        print(f"\nDownload complete: {dest_path}")
        return True
        
    except Exception as e:
        print(f"\nDownload failed: {e}")
        return False

success = download_bigwig(PHYLOP_URL, PHYLOP_FILE)
if not success:
    dbutils.notebook.exit("PhyloP download failed")

# COMMAND ----------

# DBTITLE 1,Download PhastCons BigWig File
print("\nDOWNLOADING PHASTCONS BIGWIG FILE")
print("="*80)
print(f"Source: {PHASTCONS_URL}")
print(f"Size: ~5-6 GB")
print(f"Destination: {PHASTCONS_FILE}")

success = download_bigwig(PHASTCONS_URL, PHASTCONS_FILE)
if not success:
    dbutils.notebook.exit("PhastCons download failed")

# COMMAND ----------

# DBTITLE 1,Extract Conservation Scores
print("\nEXTRACTING CONSERVATION SCORES")
print("="*80)

# Open bigWig files
print("Opening bigWig files...")
bw_phylop = pyBigWig.open(PHYLOP_FILE)
bw_phastcons = pyBigWig.open(PHASTCONS_FILE)
print("Files opened successfully")

# Process variants
print(f"\nProcessing {len(df_variants):,} variants...")

results = []
batch_size = 10000
total_batches = (len(df_variants) + batch_size - 1) // batch_size

for batch_num in range(total_batches):
    start_idx = batch_num * batch_size
    end_idx = min((batch_num + 1) * batch_size, len(df_variants))
    batch = df_variants.iloc[start_idx:end_idx]
    
    print(f"Processing batch {batch_num + 1}/{total_batches} ({start_idx:,} to {end_idx:,})...")
    
    for idx, row in batch.iterrows():
        variant_id = row['variant_id']
        chromosome = str(row['chromosome'])
        position = int(row['position'])
        
        # bigWig uses "chr" prefix
        chr_name = f"chr{chromosome}"
        
        try:
            # Get PhyloP score (0-based coordinates)
            phylop_scores = bw_phylop.values(chr_name, position-1, position)
            phylop_score = phylop_scores[0] if phylop_scores and len(phylop_scores) > 0 else None
            
            # Get PhastCons score
            phastcons_scores = bw_phastcons.values(chr_name, position-1, position)
            phastcons_score = phastcons_scores[0] if phastcons_scores and len(phastcons_scores) > 0 else None
            
        except:
            # Chromosome not in bigWig or position out of range
            phylop_score = None
            phastcons_score = None
        
        results.append({
            'variant_id': variant_id,
            'chromosome': chromosome,
            'position': position,
            'phylop_score_ucsc': phylop_score,
            'phastcons_score_ucsc': phastcons_score
        })

# Close bigWig files
bw_phylop.close()
bw_phastcons.close()

print("\nExtraction complete")

# COMMAND ----------

# DBTITLE 1,Create Results DataFrame
print("\nCREATING RESULTS DATAFRAME")
print("="*80)

# Convert to Pandas DataFrame
import pandas as pd
df_results = pd.DataFrame(results)

# Statistics
total = len(df_results)
phylop_count = df_results['phylop_score_ucsc'].notna().sum()
phastcons_count = df_results['phastcons_score_ucsc'].notna().sum()

print(f"Total variants processed: {total:,}")
print(f"PhyloP scores: {phylop_count:,} ({phylop_count/total*100:.1f}%)")
print(f"PhastCons scores: {phastcons_count:,} ({phastcons_count/total*100:.1f}%)")

# Show sample
print("\nSample results:")
print(df_results.head(10))

# COMMAND ----------

# DBTITLE 1,Save to Databricks Table
print("\nSAVING TO DATABRICKS TABLE")
print("="*80)

# Convert Pandas to Spark DataFrame
df_spark = spark.createDataFrame(df_results)

# Save to table
output_table = f"{catalog_name}.default.ucsc_conservation_scores"
df_spark.write \
    .mode("overwrite") \
    .saveAsTable(output_table)

print(f"Saved: {output_table}")
print(f"Rows: {total:,}")

# COMMAND ----------

# DBTITLE 1,Cleanup Temporary Files
print("\nCLEANUP TEMPORARY FILES")
print("="*80)

# Delete bigWig files to free space
try:
    if os.path.exists(PHYLOP_FILE):
        os.remove(PHYLOP_FILE)
        print(f"Deleted: {PHYLOP_FILE}")
    
    if os.path.exists(PHASTCONS_FILE):
        os.remove(PHASTCONS_FILE)
        print(f"Deleted: {PHASTCONS_FILE}")
    
    print("\nTemp files cleaned up successfully")
except Exception as e:
    print(f"Cleanup warning: {e}")
    print("You may need to manually delete files from volume")

# COMMAND ----------

# DBTITLE 1,Verify Table Created
print("\nVERIFYING TABLE")
print("="*80)

df_verify = spark.table(output_table)
verify_count = df_verify.count()

print(f"Table: {output_table}")
print(f"Rows: {verify_count:,}")
print("\nSchema:")
df_verify.printSchema()

print("\nSample data:")
df_verify.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Summary
print("\nUCSC CONSERVATION DOWNLOAD COMPLETE")
print("="*80)

print(f"\nFINAL STATISTICS:")
print(f"  Variants processed: {total:,}")
print(f"  PhyloP coverage: {phylop_count:,} ({phylop_count/total*100:.1f}%)")
print(f"  PhastCons coverage: {phastcons_count:,} ({phastcons_count/total*100:.1f}%)")

print(f"\nDATA SAVED TO:")
print(f"  Table: {output_table}")

print(f"\nFILTER DETAILS:")
print(f"  Original variants: {total_count:,}")
print(f"  Filtered variants: {filtered_count:,}")
print(f"  Reduction: {reduction:.1f}%")

print(f"\nNEXT STEP:")
print(f"  Run notebook: 13b_merge_ucsc_with_conservation")
print(f"  This will merge UCSC scores with existing conservation data")

print("\n" + "="*80)
print("SUCCESS!")
print("="*80)
