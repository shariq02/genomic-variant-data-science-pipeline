# Databricks notebook source
# MAGIC %md
# MAGIC #### GENE COORDINATE ENRICHMENT
# MAGIC ##### Populate Missing Gene Positions from Multiple Data Sources
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 28, 2026
# MAGIC
# MAGIC **Purpose:** Fill missing start_position and end_position in genes_ultra_enriched
# MAGIC
# MAGIC **Data Sources (in priority order):**
# MAGIC 1. transcripts table (most detailed)
# MAGIC 2. genes_refseq table
# MAGIC 3. gene_universal_search reference table
# MAGIC 4. variants_ultra_enriched (infer from variant positions)
# MAGIC
# MAGIC **Updates:** genes_ultra_enriched.start_position, end_position

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, min as spark_min, max as spark_max, count, lit, length
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, IntegerType
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR GENE COORDINATE ENRICHMENT")

# COMMAND ----------

# DBTITLE 1,Analyze Current State
print("\nANALYZING CURRENT STATE")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

total_genes = df_genes.count()
with_coords = df_genes.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()
missing_coords = total_genes - with_coords

# Check ID coverage
with_ensembl = df_genes.filter(
    col("ensembl_id").isNotNull() & 
    (col("ensembl_id") != "") &
    (length(col("ensembl_id")) > 10)
).count()

with_entrez = df_genes.filter(
    col("entrez_id").isNotNull()
).count()

print(f"Total genes: {total_genes:,}")
print(f"With coordinates: {with_coords:,} ({with_coords/total_genes*100:.1f}%)")
print(f"Missing coordinates: {missing_coords:,} ({missing_coords/total_genes*100:.1f}%)")
print(f"\nID Coverage:")
print(f"  Valid Ensembl IDs: {with_ensembl:,} ({with_ensembl/total_genes*100:.1f}%)")
print(f"  Valid Entrez IDs: {with_entrez:,} ({with_entrez/total_genes*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Identify Genes for Ensembl API
print("\nIDENTIFYING GENES FOR ENSEMBL API")
print("="*80)

# Get genes that:
# 1. Need coordinates (start/end is null)
# 2. Have valid Ensembl IDs
# 3. Are real genes (not LOC)
genes_for_ensembl_query = (
    df_genes
    .filter(col("start_position").isNull() | col("end_position").isNull())
    .filter(col("ensembl_id").isNotNull())
    .filter(col("ensembl_id") != "")
    .filter(length(col("ensembl_id")) > 10)
    .filter(~col("gene_name").startswith("LOC"))  # Skip LOC genes
    .select("gene_name", "ensembl_id", "chromosome")
    .collect()
)

print(f"Genes for Ensembl API: {len(genes_for_ensembl_query):,}")
print(f"Estimated time: {len(genes_for_ensembl_query) * 0.07 / 60:.1f} minutes")

if len(genes_for_ensembl_query) == 0:
    print("\nNo genes need Ensembl API lookup!")
    dbutils.notebook.exit("COMPLETE: No API lookups needed")

print("\nSample genes to query:")
for i, gene in enumerate(genes_for_ensembl_query[:10]):
    print(f"  {i+1}. {gene.gene_name} - {gene.ensembl_id}")

# COMMAND ----------

# DBTITLE 1,Define Ensembl API Function
def query_ensembl_for_coordinates(gene_info):
    """Query Ensembl REST API for gene coordinates"""
    ensembl_id = gene_info.ensembl_id
    gene_name = gene_info.gene_name
    
    try:
        # Ensembl REST API endpoint
        url = f"https://rest.ensembl.org/lookup/id/{ensembl_id}?content-type=application/json"
        
        response = requests.get(url, timeout=10)
        time.sleep(0.07)  # Rate limiting: 15 req/sec
        
        if response.status_code == 200:
            data = response.json()
            
            start = data.get('start')
            end = data.get('end')
            chromosome = data.get('seq_region_name')
            
            if start and end:
                return {
                    'gene_name': gene_name,
                    'ensembl_id': ensembl_id,
                    'start_position': int(start),
                    'end_position': int(end),
                    'chromosome': str(chromosome) if chromosome else gene_info.chromosome,
                    'api_source': 'ensembl',
                    'success': True,
                    'error_msg': None
                }
        
        # API returned non-200 or no coordinates
        return {
            'gene_name': gene_name,
            'ensembl_id': ensembl_id,
            'start_position': None,
            'end_position': None,
            'chromosome': None,
            'api_source': 'ensembl',
            'success': False,
            'error_msg': f"Status {response.status_code}"
        }
        
    except Exception as e:
        return {
            'gene_name': gene_name,
            'ensembl_id': ensembl_id,
            'start_position': None,
            'end_position': None,
            'chromosome': None,
            'api_source': 'ensembl',
            'success': False,
            'error_msg': str(e)
        }

print("Ensembl API function ready")

# COMMAND ----------

# DBTITLE 1,Query Ensembl API with Progress
print("\nQUERYING ENSEMBL API")
print("="*80)

BATCH_SIZE = 100
MAX_WORKERS = 10
CHECKPOINT_FREQ = 10

# Create checkpoint table if not exists
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.silver.ensembl_coord_results (
            gene_name STRING,
            ensembl_id STRING,
            start_position LONG,
            end_position LONG,
            chromosome STRING,
            api_source STRING,
            success BOOLEAN,
            error_msg STRING,
            batch_id INT,
            query_timestamp TIMESTAMP
        )
    """)
except:
    pass

# Check for already processed genes
df_processed = spark.table(f"{catalog_name}.silver.ensembl_coord_results")
processed_genes = set([row.gene_name for row in df_processed.select("gene_name").collect()])

genes_to_query = [g for g in genes_for_ensembl_query if g.gene_name not in processed_genes]

print(f"Total to query: {len(genes_to_query):,}")
print(f"Already processed: {len(processed_genes):,}")

if len(genes_to_query) == 0:
    print("All genes already processed!")
else:
    total_batches = (len(genes_to_query) + BATCH_SIZE - 1) // BATCH_SIZE
    all_results = []
    success_count = 0
    
    result_schema = StructType([
        StructField("gene_name", StringType(), True),
        StructField("ensembl_id", StringType(), True),
        StructField("start_position", LongType(), True),
        StructField("end_position", LongType(), True),
        StructField("chromosome", StringType(), True),
        StructField("api_source", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("error_msg", StringType(), True),
        StructField("batch_id", IntegerType(), True)
    ])
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(genes_to_query))
        batch = genes_to_query[start_idx:end_idx]
        
        # Query in parallel
        batch_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(query_ensembl_for_coordinates, gene) for gene in batch]
            
            for future in as_completed(futures):
                result = future.result()
                batch_results.append(result)
                if result['success']:
                    success_count += 1
        
        all_results.extend(batch_results)
        
        # Progress
        processed = end_idx
        success_rate = (success_count / processed * 100) if processed > 0 else 0
        print(f"Batch {batch_num+1}/{total_batches} | "
              f"Processed: {processed:,}/{len(genes_to_query):,} ({processed/len(genes_to_query)*100:.1f}%) | "
              f"Success: {success_count:,} ({success_rate:.1f}%)")
        
        # Checkpoint
        if (batch_num + 1) % CHECKPOINT_FREQ == 0 or (batch_num + 1) == total_batches:
            print(f"  Saving checkpoint...")
            
            checkpoint_data = [{
                'gene_name': r['gene_name'],
                'ensembl_id': r['ensembl_id'],
                'start_position': r['start_position'],
                'end_position': r['end_position'],
                'chromosome': r['chromosome'],
                'api_source': r['api_source'],
                'success': r['success'],
                'error_msg': r['error_msg'],
                'batch_id': batch_num + 1
            } for r in all_results]
            
            df_batch = spark.createDataFrame(checkpoint_data, schema=result_schema)
            df_batch = df_batch.withColumn("query_timestamp", lit(datetime.now()))
            df_batch.write.mode("append").saveAsTable(f"{catalog_name}.silver.ensembl_coord_results")
            
            all_results = []
            print(f"  Checkpoint saved!")
    
    print(f"\nEnsembl API complete: {success_count:,} successful")

# COMMAND ----------

# DBTITLE 1,Merge Ensembl Results
print("\nMERGING ENSEMBL RESULTS")
print("="*80)

# Load all successful results
df_ensembl_coords = (
    spark.table(f"{catalog_name}.silver.ensembl_coord_results")
    .filter(col("success") == True)
    .filter(col("start_position").isNotNull())
    .filter(col("end_position").isNotNull())
    .select(
        col("gene_name").alias("gene_name_ensembl"),
        col("start_position").alias("ensembl_start"),
        col("end_position").alias("ensembl_end"),
        col("chromosome").alias("ensembl_chr")
    )
    .dropDuplicates(["gene_name_ensembl"])
)

ensembl_count = df_ensembl_coords.count()
print(f"Successful Ensembl lookups: {ensembl_count:,}")

# Merge with genes table
df_genes_updated = (
    df_genes
    .join(df_ensembl_coords, col("gene_name") == col("gene_name_ensembl"), "left")
    .drop("gene_name_ensembl", "ensembl_chr")
    
    # Update coordinates
    .withColumn("start_position",
                coalesce(col("start_position"), col("ensembl_start")))
    
    .withColumn("end_position",
                coalesce(col("end_position"), col("ensembl_end")))
    
    # Update gene_length
    .withColumn("gene_length",
                when(col("gene_length").isNull() & 
                     col("start_position").isNotNull() & 
                     col("end_position").isNotNull(),
                     col("end_position") - col("start_position"))
                .otherwise(col("gene_length")))
    
    # Update coordinate_source
    .withColumn("coordinate_source",
                when(col("coordinate_source") == "missing",
                     when(col("ensembl_start").isNotNull(), lit("ensembl"))
                     .otherwise(lit("missing")))
                .otherwise(col("coordinate_source")))
    
    .drop("ensembl_start", "ensembl_end")
)

# Save
df_genes_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")

print("Ensembl coordinates merged")

# COMMAND ----------

# DBTITLE 1,Final Statistics
print("\nFINAL STATISTICS")
print("="*80)

df_genes_final = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

total = df_genes_final.count()
with_coords_final = df_genes_final.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()
missing_final = total - with_coords_final

print(f"Total genes: {total:,}")
print(f"\nBEFORE Ensembl API:")
print(f"  With coordinates: {with_coords:,} ({with_coords/total*100:.1f}%)")

print(f"\nAFTER Ensembl API:")
print(f"  With coordinates: {with_coords_final:,} ({with_coords_final/total*100:.1f}%)")
print(f"  Still missing: {missing_final:,} ({missing_final/total*100:.1f}%)")

print(f"\nIMPROVEMENT:")
print(f"  Added: {with_coords_final - with_coords:,} genes")
print(f"  Improvement: {(with_coords_final - with_coords)/total*100:.1f} percentage points")

print("\nCoordinate sources:")
df_genes_final.groupBy("coordinate_source").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("COORDINATE ENRICHMENT COMPLETE")
print("="*80)

print(f"\nFinal coverage: {with_coords_final/total*100:.1f}%")
print(f"Expected final: ~35-40% (with Ensembl API for real genes)")
print(f"Expected SV-gene mapping: ~{with_coords_final/total*100:.0f}% coverage")

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,What ARE the 134K Missing Genes?
print("ANALYZING THE 134K MISSING GENES")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

genes_missing = df_genes.filter(
    col("start_position").isNull() | col("end_position").isNull()
)

print(f"Total missing coordinates: {genes_missing.count():,}")

# Categorize by gene name pattern
print("\nBreakdown by gene type:")
print(f"  LOC genes: {genes_missing.filter(col('gene_name').startswith('LOC')).count():,}")
print(f"  LINC genes: {genes_missing.filter(col('gene_name').startswith('LINC')).count():,}")
print(f"  Pseudogenes (P suffix): {genes_missing.filter(col('gene_name').rlike('P[0-9]+$')).count():,}")
print(f"  MIR genes: {genes_missing.filter(col('gene_name').startswith('MIR')).count():,}")
print(f"  SNORD genes: {genes_missing.filter(col('gene_name').startswith('SNORD')).count():,}")

# Check if they have ANY identifiers
print("\nID coverage for missing genes:")
print(f"  Have Ensembl ID: {genes_missing.filter(col('ensembl_id').isNotNull() & (col('ensembl_id') != '') & (length(col('ensembl_id')) > 10)).count():,}")
print(f"  Have HGNC ID: {genes_missing.filter(col('hgnc_id').isNotNull() & (col('hgnc_id') != '')).count():,}")
print(f"  Have MIM ID: {genes_missing.filter(col('mim_id').isNotNull() & (col('mim_id') != '')).count():,}")

# Sample of missing genes
print("\nSample of missing genes:")
genes_missing.select('gene_name', 'official_symbol', 'ensembl_id', 'hgnc_id', 'chromosome').show(30, truncate=False)

# Check variants - do these genes have variants?
print("\nDo missing genes have variants?")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

missing_gene_names = [row.gene_name for row in genes_missing.select('gene_name').limit(1000).collect()]
variants_for_missing = df_variants.filter(col('gene_name').isin(missing_gene_names[:100])).count()

print(f"Variants for sample of 100 missing genes: {variants_for_missing:,}")
