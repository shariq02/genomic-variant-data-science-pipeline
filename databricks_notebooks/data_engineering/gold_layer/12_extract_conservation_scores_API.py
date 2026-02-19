# Databricks notebook source
# MAGIC %md
# MAGIC #### ENRICH VARIANTS WITH CONSERVATION SCORES VIA API
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 23, 2026
# MAGIC
# MAGIC **Data Source:** MyVariant.info API (aggregates dbNSFP, gnomAD, ClinVar)

# COMMAND ----------

# DBTITLE 1,Install Required Library
# Install requests library for API calls
%pip install requests --quiet

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct, regexp_extract, concat, lit, when, current_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType, TimestampType
import requests
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Row

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("API-BASED VARIANT ENRICHMENT")

# COMMAND ----------

# DBTITLE 1,Configuration
# MyVariant.info API (FREE, aggregates dbNSFP + gnomAD)
MYVARIANT_API = "https://myvariant.info/v1/variant"

# Fields to retrieve
FIELDS = [
    "dbnsfp.phylop100way_vertebrate",
    "dbnsfp.phastcons100way_vertebrate",
    "dbnsfp.gerp.rs",
    "dbnsfp.sift.score",
    "dbnsfp.polyphen2.hdiv.score",
    "cadd.phred",
    "gnomad_genome.af.af"
]

# FASTER BATCH PROCESSING - 10 PARALLEL BATCHES!
BATCH_SIZE = 1000  # Keep at 1000 (API limit)
REQUESTS_PER_SECOND = 10  # Increased from 3
PARALLEL_BATCHES = 10  # Process 10 batches simultaneously!

print(f"Batch size: {BATCH_SIZE}")
print(f"Rate limit: {REQUESTS_PER_SECOND} requests/second")
print(f"Parallel batches: {PARALLEL_BATCHES}")
print(f"Expected time: ~{3850 / (REQUESTS_PER_SECOND * PARALLEL_BATCHES) / 60:.0f} minutes (~{3850 / (REQUESTS_PER_SECOND * PARALLEL_BATCHES) / 60 / 60:.1f} hours)")

# COMMAND ----------

# DBTITLE 1,Check if Already Processed
print("\nCHECKING FOR EXISTING CONSERVATION DATA")
print("="*80)

try:
    df_existing = spark.table(f"{catalog_name}.silver.conservation_scores")
    existing_count = df_existing.count()
    
    print(f" Found existing conservation_scores table with {existing_count:,} variants")
    print("\nTo re-process: Manually drop the table first, then re-run this notebook")
    print(f"  Command: DROP TABLE {catalog_name}.silver.conservation_scores")
    
    # Exit - don't re-process
    dbutils.notebook.exit("Conservation scores already exist. Skipping API queries.")
    
except:
    print("No existing conservation_scores table found. Processing all variants...")

# COMMAND ----------

# DBTITLE 1,Load Variants with FIXED Alleles
print("\nLOADING VARIANTS WITH FIXED ALLELES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
total_variants = df_variants.count()

print(f"Total variants: {total_variants:,}")

# Extract alleles from variant_name and create proper HGVS IDs
df_variants_subset = (
    df_variants
    # Extract alleles from variant_name (e.g., "c.407C>A" -> C and A)
    .withColumn("ref_from_name", 
                regexp_extract(col("variant_name"), r'([ACGT])>([ACGT])', 1))
    .withColumn("alt_from_name",
                regexp_extract(col("variant_name"), r'([ACGT])>([ACGT])', 2))
    
    # Use extracted alleles (since reference_allele and alternate_allele are "na")
    .withColumn("ref_allele_fixed",
                when(col("reference_allele") == "na", col("ref_from_name"))
                .otherwise(col("reference_allele")))
    .withColumn("alt_allele_fixed",
                when(col("alternate_allele") == "na", col("alt_from_name"))
                .otherwise(col("alternate_allele")))
    
    # Create proper HGVS ID for API
    .withColumn("variant_hgvs",
                concat(
                    lit("chr"),
                    col("chromosome"),
                    lit(":g."),
                    col("position"),
                    col("ref_allele_fixed"),
                    lit(">"),
                    col("alt_allele_fixed")
                ))
    
    # Select needed columns
    .select(
        "variant_id",
        "chromosome",
        "position",
        "ref_allele_fixed",
        "alt_allele_fixed",
        "gene_name",
        "variant_hgvs"
    )
)

# Filter out any that still don't have valid alleles
df_variants_valid = df_variants_subset.filter(
    (col("ref_allele_fixed").isNotNull()) &
    (col("alt_allele_fixed").isNotNull()) &
    (col("ref_allele_fixed") != "") &
    (col("alt_allele_fixed") != "")
)

valid_count = df_variants_valid.count()
print(f"Variants with valid alleles: {valid_count:,}")

print("\nSample HGVS IDs (first 10):")
df_variants_valid.select("variant_hgvs").show(10, truncate=False)

print(" Variants ready for API queries")

# COMMAND ----------

# DBTITLE 1,Define API Query Function
def query_myvariant_batch(variant_ids):
    """
    Query MyVariant.info API for a batch of variants
    Returns list of dictionaries with scores
    Handles cases where API returns lists instead of single values
    """
    try:
        response = requests.post(
            MYVARIANT_API,
            data={
                'ids': ','.join(variant_ids),
                'fields': ','.join(FIELDS)
            },
            timeout=60
        )
        
        if response.status_code == 200:
            results = response.json()
            
            # Extract scores for each variant
            enriched = []
            for variant_data in results:
                if 'notfound' in variant_data:
                    enriched.append(None)
                    continue
                
                # Extract nested scores
                dbnsfp = variant_data.get('dbnsfp', {})
                
                # Helper function to get first value if list
                def get_first_value(value):
                    if isinstance(value, list) and len(value) > 0:
                        return value[0]
                    return value
                
                # Get phylop (could be list)
                phylop_raw = dbnsfp.get('phylop100way_vertebrate')
                phylop = get_first_value(phylop_raw)
                
                # Get phastcons (could be list)
                phastcons_raw = dbnsfp.get('phastcons100way_vertebrate')
                phastcons = get_first_value(phastcons_raw)
                
                # Get GERP (could be nested dict or list)
                gerp_raw = dbnsfp.get('gerp', {})
                if isinstance(gerp_raw, dict):
                    gerp = get_first_value(gerp_raw.get('rs'))
                else:
                    gerp = None
                
                # Get SIFT (could be nested dict or list)
                sift_raw = dbnsfp.get('sift', {})
                if isinstance(sift_raw, dict):
                    sift = get_first_value(sift_raw.get('score'))
                else:
                    sift = None
                
                # Get PolyPhen (could be nested dict or list)
                polyphen_raw = dbnsfp.get('polyphen2', {})
                if isinstance(polyphen_raw, dict):
                    hdiv_raw = polyphen_raw.get('hdiv', {})
                    if isinstance(hdiv_raw, dict):
                        polyphen = get_first_value(hdiv_raw.get('score'))
                    else:
                        polyphen = None
                else:
                    polyphen = None
                
                # Get CADD (could be nested dict or list)
                cadd_raw = variant_data.get('cadd', {})
                if isinstance(cadd_raw, dict):
                    cadd = get_first_value(cadd_raw.get('phred'))
                else:
                    cadd = None
                
                # Get gnomAD (could be nested dict or list)
                gnomad_raw = variant_data.get('gnomad_genome', {})
                if isinstance(gnomad_raw, dict):
                    af_raw = gnomad_raw.get('af', {})
                    if isinstance(af_raw, dict):
                        gnomad = get_first_value(af_raw.get('af'))
                    else:
                        gnomad = get_first_value(af_raw)
                else:
                    gnomad = None
                
                scores = {
                    'variant_id': variant_data.get('query', ''),
                    'phylop_score': phylop,
                    'phastcons_score': phastcons,
                    'gerp_score': gerp,
                    'sift_score': sift,
                    'polyphen_score': polyphen,
                    'cadd_phred': cadd,
                    'gnomad_af': gnomad
                }
                
                enriched.append(scores)
            
            return enriched
        else:
            return None
            
    except Exception as e:
        return None

print("API query function defined (with list handling)")

# COMMAND ----------

# DBTITLE 1,Process Variants in Batches with IMMEDIATE Checkpoint
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading

print("\nProcessing variants in PARALLEL batches...")
print(f"Total batches: {valid_count // BATCH_SIZE + 1:,}")
print(f"Processing {PARALLEL_BATCHES} batches SIMULTANEOUSLY")

# Collect variant IDs
variant_ids = [row.variant_hgvs for row in df_variants_valid.collect()]
print(f"Collected {len(variant_ids):,} variant IDs")

# CHECKPOINT: Save EVERY batch to table
CHECKPOINT_TABLE = f"{catalog_name}.silver.conservation_checkpoint"
RESULTS_TABLE = f"{catalog_name}.silver.conservation_scores_temp"

# Create schemas
checkpoint_schema = StructType([
    StructField("batch_number", IntegerType(), False)
])

results_schema = StructType([
    StructField("variant_id", StringType(), True),
    StructField("phylop_score", DoubleType(), True),
    StructField("phastcons_score", DoubleType(), True),
    StructField("gerp_score", DoubleType(), True),
    StructField("sift_score", DoubleType(), True),
    StructField("polyphen_score", DoubleType(), True),
    StructField("cadd_phred", DoubleType(), True),
    StructField("gnomad_af", DoubleType(), True)
])

# Load existing progress
processed_batches = set()
try:
    df_checkpoint = spark.table(CHECKPOINT_TABLE)
    processed_batches = set([row.batch_number for row in df_checkpoint.collect()])
    print(f" Resuming from checkpoint: {len(processed_batches)} batches already processed")
except:
    print("No checkpoint found. Starting fresh...")
    df_empty_checkpoint = spark.createDataFrame([], checkpoint_schema)
    df_empty_checkpoint.write.mode("overwrite").saveAsTable(CHECKPOINT_TABLE)
    
    df_empty_results = spark.createDataFrame([], results_schema)
    df_empty_results.write.mode("overwrite").saveAsTable(RESULTS_TABLE)

# Create batches to process
all_batches = []
batch_count = 0
for i in range(0, len(variant_ids), BATCH_SIZE):
    batch_count += 1
    if batch_count not in processed_batches:
        batch_ids = variant_ids[i:i+BATCH_SIZE]
        all_batches.append((batch_count, batch_ids))

print(f"Need to process: {len(all_batches):,} new batches")

# Track progress
successful_batches = 0
total_variants_saved = 0
lock = threading.Lock()

# Function to process and save a single batch (with rate limiting inside)
def process_and_save_batch(batch_info):
    """Process a single batch - truly parallel"""
    global successful_batches, total_variants_saved
    
    batch_num, batch_ids = batch_info
    
    try:
        # Query API (this is where parallelism happens!)
        results = query_myvariant_batch(batch_ids)
        
        if results:
            batch_results = [r for r in results if r is not None]
            
            if batch_results:
                # Save results
                try:
                    df_batch = spark.createDataFrame(batch_results, results_schema)
                    df_batch.write.mode("append").saveAsTable(RESULTS_TABLE)
                    
                    with lock:
                        successful_batches += 1
                        total_variants_saved += len(batch_results)
                        
                except Exception as e:
                    pass
        
        # Save checkpoint
        try:
            checkpoint_data = [(batch_num,)]
            df_checkpoint_batch = spark.createDataFrame(checkpoint_data, checkpoint_schema)
            df_checkpoint_batch.write.mode("append").saveAsTable(CHECKPOINT_TABLE)
        except:
            pass
        
        return True
        
    except Exception as e:
        return False

# Process ALL batches in parallel (submit all at once!)
import time
start_time = time.time()

print(f"\nSubmitting {len(all_batches):,} batches to {PARALLEL_BATCHES} parallel workers...")

with ThreadPoolExecutor(max_workers=PARALLEL_BATCHES) as executor:
    # Submit ALL batches
    futures = [executor.submit(process_and_save_batch, batch) for batch in all_batches]
    
    # Monitor progress
    completed = 0
    for future in concurrent.futures.as_completed(futures):
        completed += 1
        
        # Progress update every 100 batches
        if completed % 100 == 0:
            elapsed_min = (time.time() - start_time) / 60
            rate = completed / elapsed_min if elapsed_min > 0 else 0
            remaining = len(all_batches) - completed
            eta_min = remaining / rate if rate > 0 else 0
            
            print(f"  Progress: {completed:,}/{len(all_batches):,} batches ({completed/len(all_batches)*100:.1f}%) | {successful_batches:,} successful | {total_variants_saved:,} variants | {elapsed_min:.1f} min elapsed | ~{eta_min:.0f} min remaining")

elapsed_total = (time.time() - start_time) / 60
print(f"\n" + "="*80)
print("PARALLEL PROCESSING COMPLETE")
print("="*80)
print(f"Total time: {elapsed_total:.1f} minutes")
print(f"Total batches processed: {len(all_batches):,}")
print(f"Successful batches: {successful_batches:,}")
print(f"Total variants saved: {total_variants_saved:,}")
print(f"Average rate: {len(all_batches) / elapsed_total:.1f} batches/minute")

# COMMAND ----------

# DBTITLE 1,Finalize Results
print("\nFINALIZING RESULTS")
print("="*80)

# Read all accumulated results from temp table
df_all_results = spark.table(RESULTS_TABLE)
total_enriched = df_all_results.count()

print(f"Total enriched variants in table: {total_enriched:,}")
print(f"Success rate: {total_enriched / len(variant_ids) * 100:.1f}%")

# Join with original variant data
df_conservation = (
    df_variants_valid
    .join(df_all_results,
          df_variants_valid.variant_hgvs == df_all_results.variant_id,
          "left")
    .select(
        df_variants_valid.variant_id,
        df_variants_valid.chromosome,
        df_variants_valid.position,
        df_variants_valid.ref_allele_fixed.alias("reference_allele"),
        df_variants_valid.alt_allele_fixed.alias("alternate_allele"),
        df_variants_valid.gene_name,
        df_all_results.phylop_score,
        df_all_results.phastcons_score,
        df_all_results.gerp_score,
        df_all_results.sift_score,
        df_all_results.polyphen_score,
        df_all_results.cadd_phred,
        df_all_results.gnomad_af
    )
)

# COMMAND ----------

# DBTITLE 1,Add Conservation Categories
print("\nADDING CONSERVATION CATEGORIES")
print("="*80)

df_conservation_enriched = (
    df_conservation
    # PhyloP interpretation (>2.7 = highly conserved)
    .withColumn("is_highly_conserved",
                when(col("phylop_score") > 2.7, True).otherwise(False))
    
    # GERP interpretation (>4 = constrained)
    .withColumn("is_constrained",
                when(col("gerp_score") > 4.0, True).otherwise(False))
    
    # CADD interpretation (>20 = likely deleterious)
    .withColumn("is_likely_deleterious",
                when(col("cadd_phred") > 20.0, True).otherwise(False))
    
    # Combined conservation level (0-3)
    .withColumn("conservation_level",
                when(col("phylop_score") > 2.7, 1).otherwise(0) +
                when(col("gerp_score") > 4.0, 1).otherwise(0) +
                when(col("cadd_phred") > 20.0, 1).otherwise(0))
    
    # Population frequency category
    .withColumn("is_common_variant",
                when(col("gnomad_af") > 0.01, True).otherwise(False))
    .withColumn("is_rare_variant",
                when(col("gnomad_af") < 0.001, True).otherwise(False))
)
print("Conservation categories added")

# COMMAND ----------

# DBTITLE 1,Verify Results
print("\nCONSERVATION SCORE STATISTICS")
print("="*80)

df_conservation_enriched.select(
    "phylop_score",
    "gerp_score",
    "cadd_phred",
    "gnomad_af"
).summary().show()

print("\nConservation level distribution:")
df_conservation_enriched.groupBy("conservation_level").count().orderBy("conservation_level").show()

print("\nPopulation frequency distribution:")
df_conservation_enriched.groupBy("is_common_variant", "is_rare_variant").count().show()

print("\nSample enriched variants:")
df_conservation_enriched.filter(
    col("conservation_level") >= 2
).select(
    "gene_name",
    "chromosome",
    "position",
    "phylop_score",
    "gerp_score",
    "cadd_phred",
    "gnomad_af"
).show(5)

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
print("\nSAVING TO SILVER LAYER")
print("="*80)

df_conservation_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.conservation_scores")

print(f" Saved: {catalog_name}.silver.conservation_scores")

# COMMAND ----------

# DBTITLE 1,Cleanup Temp Tables
print("\nCLEANUP")
print("="*80)

try:
    spark.sql(f"DROP TABLE IF EXISTS {CHECKPOINT_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {RESULTS_TABLE}")
    print(" Temp tables removed")
except Exception as e:
    print(f"Cleanup warning: {e}")

# COMMAND ----------

# DBTITLE 1,Coverage Statistics
print("\nCOVERAGE STATISTICS")
print("="*80)

total = df_conservation_enriched.count()

stats = {
    'PhyloP': df_conservation_enriched.filter(col("phylop_score").isNotNull()).count(),
    'PhastCons': df_conservation_enriched.filter(col("phastcons_score").isNotNull()).count(),
    'GERP': df_conservation_enriched.filter(col("gerp_score").isNotNull()).count(),
    'SIFT': df_conservation_enriched.filter(col("sift_score").isNotNull()).count(),
    'PolyPhen': df_conservation_enriched.filter(col("polyphen_score").isNotNull()).count(),
    'CADD': df_conservation_enriched.filter(col("cadd_phred").isNotNull()).count(),
    'gnomAD': df_conservation_enriched.filter(col("gnomad_af").isNotNull()).count()
}

print(f"{'Score Type':<15} {'Count':>12} {'Coverage':>10}")
print("-" * 40)
for score_type, count in stats.items():
    coverage = count / total * 100
    print(f"{score_type:<15} {count:>12,} {coverage:>9.1f}%")

# COMMAND ----------

# DBTITLE 1,Summary
print("API-BASED ENRICHMENT COMPLETE")
print("="*80)

print(f"\nTotal variants enriched: {total:,}")
print(f"Highly conserved: {df_conservation_enriched.filter(col('is_highly_conserved')).count():,}")
print(f"Constrained (GERP>4): {df_conservation_enriched.filter(col('is_constrained')).count():,}")
print(f"Likely deleterious (CADD>20): {df_conservation_enriched.filter(col('is_likely_deleterious')).count():,}")
print(f"Rare variants (AF<0.1%): {df_conservation_enriched.filter(col('is_rare_variant')).count():,}")

print("\nData sources (ALL FREE via MyVariant.info API):")
print("  - PhyloP (from dbNSFP)")
print("  - PhastCons (from dbNSFP)")
print("  - GERP (from dbNSFP)")
print("  - SIFT (from dbNSFP)")
print("  - PolyPhen (from dbNSFP)")
print("  - CADD (from CADD database)")
print("  - gnomAD (population frequencies)")

print("\nTable created:")
print(f"  {catalog_name}.silver.conservation_scores")

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Diagnose Missing Data
print("DIAGNOSING MISSING DATA")
print("="*80)

df_conservation = spark.table(f"{catalog_name}.silver.conservation_scores")

print("\nColumn coverage:")
print(f"Total variants: {df_conservation.count():,}")
print(f"phylop_score NOT NULL: {df_conservation.filter(col('phylop_score').isNotNull()).count():,}")
print(f"phastcons_score NOT NULL: {df_conservation.filter(col('phastcons_score').isNotNull()).count():,}")
print(f"gerp_score NOT NULL: {df_conservation.filter(col('gerp_score').isNotNull()).count():,}")
print(f"sift_score NOT NULL: {df_conservation.filter(col('sift_score').isNotNull()).count():,}")
print(f"polyphen_score NOT NULL: {df_conservation.filter(col('polyphen_score').isNotNull()).count():,}")
print(f"cadd_phred NOT NULL: {df_conservation.filter(col('cadd_phred').isNotNull()).count():,}")
print(f"gnomad_af NOT NULL: {df_conservation.filter(col('gnomad_af').isNotNull()).count():,}")

print("\nSample variants WITH data:")
df_conservation.filter(
    col("cadd_phred").isNotNull()
).select(
    "gene_name", "chromosome", "position",
    "phylop_score", "gerp_score", "cadd_phred", "gnomad_af"
).show(10)

print("\nChecking temp results table:")
try:
    df_temp = spark.table(f"{catalog_name}.silver.conservation_scores_temp")
    print(f"Temp table variants: {df_temp.count():,}")
    
    print("\nTemp table column coverage:")
    print(f"phylop_score NOT NULL: {df_temp.filter(col('phylop_score').isNotNull()).count():,}")
    print(f"gerp_score NOT NULL: {df_temp.filter(col('gerp_score').isNotNull()).count():,}")
    print(f"cadd_phred NOT NULL: {df_temp.filter(col('cadd_phred').isNotNull()).count():,}")
    
except Exception as e:
    print(f"Temp table not found: {e}")
