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
from pyspark.sql.functions import col, udf, struct, concat, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType
import requests
import time
import json
import os
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

# Batch processing
BATCH_SIZE = 1000  # API allows up to 1000 IDs per request
REQUESTS_PER_SECOND = 3  # Be nice to free API

print(f"Batch size: {BATCH_SIZE}")
print(f"Rate limit: {REQUESTS_PER_SECOND} requests/second")

# COMMAND ----------

# DBTITLE 1,Load Variants
print("\nLoading variants from silver.variants_ultra_enriched...")

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
total_variants = df_variants.count()

print(f"Total variants: {total_variants:,}")

# Select only needed columns
df_variants_subset = df_variants.select(
    "variant_id",
    "chromosome",
    "position",
    "reference_allele",
    "alternate_allele",
    "gene_name"
)

print("Variant columns selected")

# COMMAND ----------

# DBTITLE 1,Create Variant ID for API
# Format: chr17:g.43124027C>T
df_variants_with_id = (
    df_variants_subset
    .withColumn("variant_hgvs",
                concat(
                    lit("chr"),
                    col("chromosome"),
                    lit(":g."),
                    col("position"),
                    col("reference_allele"),
                    lit(">"),
                    col("alternate_allele")
                ))
)

print("Created HGVS variant IDs")
df_variants_with_id.select("variant_hgvs").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Define API Query Function
def query_myvariant_batch(variant_ids):
    """
    Query MyVariant.info API for a batch of variants
    Returns list of dictionaries with scores
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
                
                scores = {
                    'variant_id': variant_data.get('query', ''),
                    'phylop_score': dbnsfp.get('phylop100way_vertebrate'),
                    'phastcons_score': dbnsfp.get('phastcons100way_vertebrate'),
                    'gerp_score': dbnsfp.get('gerp', {}).get('rs') if isinstance(dbnsfp.get('gerp'), dict) else None,
                    'sift_score': dbnsfp.get('sift', {}).get('score') if isinstance(dbnsfp.get('sift'), dict) else None,
                    'polyphen_score': dbnsfp.get('polyphen2', {}).get('hdiv', {}).get('score') if isinstance(dbnsfp.get('polyphen2'), dict) else None,
                    'cadd_phred': variant_data.get('cadd', {}).get('phred') if isinstance(variant_data.get('cadd'), dict) else None,
                    'gnomad_af': variant_data.get('gnomad_genome', {}).get('af', {}).get('af') if isinstance(variant_data.get('gnomad_genome', {}).get('af'), dict) else variant_data.get('gnomad_genome', {}).get('af')
                }
                
                enriched.append(scores)
            
            return enriched
        else:
            print(f"API error: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Request failed: {e}")
        return None

print("API query function defined")

# COMMAND ----------

# DBTITLE 1,Check if Already Processed
print("\nCHECKING FOR EXISTING CONSERVATION DATA")
print("="*80)

try:
    df_existing = spark.table(f"{catalog_name}.silver.conservation_scores")
    existing_count = df_existing.count()
    
    print(f"Found existing conservation_scores table with {existing_count:,} variants")
    
    choice = input("\nTable exists. What do you want to do?\n  [1] Skip (use existing)\n  [2] Re-process all\nChoice: ")
    
    if choice == "1":
        print("Using existing conservation scores. Skipping API queries.")
        dbutils.notebook.exit("Using existing data")
    else:
        print("Re-processing all variants...")
        
except:
    print("No existing conservation_scores table found. Processing all variants...")


# COMMAND ----------

# DBTITLE 1,Process Variants in Batches
print("\nProcessing variants in batches...")
print(f"Total batches: {total_variants // BATCH_SIZE + 1:,}")
print("This will take 30-60 minutes for all variants")

# Collect variant IDs (we need to batch them for API)
variant_ids = [row.variant_hgvs for row in df_variants_with_id.collect()]

print(f"Collected {len(variant_ids):,} variant IDs")

# CHECKPOINT: Save progress every N batches
CHECKPOINT_INTERVAL = 100  # Save every 100 batches
checkpoint_file = "/dbfs/tmp/conservation_checkpoint.json"

# Load existing progress if available
processed_batches = set()
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, 'r') as f:
        checkpoint_data = json.load(f)
        processed_batches = set(checkpoint_data.get('processed_batches', []))
    print(f"Resuming from checkpoint: {len(processed_batches)} batches already processed")

# Process in batches
all_results = []
batch_count = 0

for i in range(0, len(variant_ids), BATCH_SIZE):
    batch_count += 1
    
    # Skip if already processed
    if batch_count in processed_batches:
        if batch_count % 10 == 0:
            print(f"Skipping batch {batch_count} (already processed)")
        continue
    
    batch_ids = variant_ids[i:i+BATCH_SIZE]
    
    if batch_count % 10 == 0:
        print(f"Processing batch {batch_count}/{len(variant_ids)//BATCH_SIZE + 1} ({i:,}/{len(variant_ids):,} variants)")
    
    # Query API
    results = query_myvariant_batch(batch_ids)
    
    if results:
        all_results.extend([r for r in results if r is not None])
    
    # Mark as processed
    processed_batches.add(batch_count)
    
    # Save checkpoint periodically
    if batch_count % CHECKPOINT_INTERVAL == 0:
        with open(checkpoint_file, 'w') as f:
            json.dump({'processed_batches': list(processed_batches)}, f)
        print(f"  Checkpoint saved: {batch_count} batches completed")
    
    # Rate limiting
    time.sleep(1.0 / REQUESTS_PER_SECOND)

# Final checkpoint save
with open(checkpoint_file, 'w') as f:
    json.dump({'processed_batches': list(processed_batches)}, f)

print(f"\nEnriched {len(all_results):,} variants")
print(f"Success rate: {len(all_results) / len(variant_ids) * 100:.1f}%")

# Clean up checkpoint file after successful completion
if os.path.exists(checkpoint_file):
    os.remove(checkpoint_file)
    print("Checkpoint file removed (processing complete)")

# COMMAND ----------

# DBTITLE 1,Create DataFrame from Results
print("\nCreating DataFrame from API results...")

# Define schema
conservation_schema = StructType([
    StructField("variant_id", StringType(), True),
    StructField("phylop_score", DoubleType(), True),
    StructField("phastcons_score", DoubleType(), True),
    StructField("gerp_score", DoubleType(), True),
    StructField("sift_score", DoubleType(), True),
    StructField("polyphen_score", DoubleType(), True),
    StructField("cadd_phred", DoubleType(), True),
    StructField("gnomad_af", DoubleType(), True)
])

# Convert results to Rows
rows = [Row(**r) for r in all_results]

# Create DataFrame
df_conservation_raw = spark.createDataFrame(rows, schema=conservation_schema)

print(f"Created DataFrame with {df_conservation_raw.count():,} records")

# COMMAND ----------

# DBTITLE 1,Join with Original Variants
print("\nJoining conservation scores with original variants...")

df_conservation = (
    df_variants_with_id
    .join(df_conservation_raw,
          df_variants_with_id.variant_hgvs == df_conservation_raw.variant_id,
          "left")
    .select(
        df_variants_with_id.variant_id.alias("variant_id"),
        df_variants_with_id.chromosome,
        df_variants_with_id.position,
        df_variants_with_id.reference_allele,  
        df_variants_with_id.alternate_allele,
        df_variants_with_id.gene_name,
        df_conservation_raw.phylop_score,
        df_conservation_raw.phastcons_score,
        df_conservation_raw.gerp_score,
        df_conservation_raw.sift_score,
        df_conservation_raw.polyphen_score,
        df_conservation_raw.cadd_phred,
        df_conservation_raw.gnomad_af
    )
)

conservation_count = df_conservation.count()
print(f"Conservation table: {conservation_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Add Conservation Categories
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

print(f"Saved: {catalog_name}.silver.conservation_scores")

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
