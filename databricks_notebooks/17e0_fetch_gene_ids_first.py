# Databricks notebook source
# MAGIC %md
# MAGIC #### GENE ID ENRICHMENT - Fetch Missing Ensembl/Entrez IDs
# MAGIC ##### Use Gene Symbols from gene_universal_search
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 28, 2026
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, length, lit, first, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR MULTI-SOURCE ID ENRICHMENT")

# COMMAND ----------

# DBTITLE 1,Source 1 - Copy gene_id from variants_ultra_enriched
print("\nSOURCE 1: GENE_ID FROM VARIANTS")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

# Get gene_id from variants (use first non-null value per gene)
from pyspark.sql.functions import first

variant_gene_ids = (
    df_variants
    .filter(col('gene_id').isNotNull() & (col('gene_id') != ''))
    .groupBy('gene_name')
    .agg(
        first('gene_id', ignorenulls=True).alias('variant_gene_id')
    )
)

print(f"Genes with gene_id from variants: {variant_gene_ids.count():,}")
variant_gene_ids.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Source 2 - Copy gene_id from gene_disease_links
print("\nSOURCE 2: GENE_ID FROM GENE_DISEASE_LINKS")
print("="*80)

df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_links")

disease_gene_ids = (
    df_gene_disease
    .filter(expr("try_cast(gene_id as int) is not null"))
    .groupBy(col('gene_symbol').alias('gene_name'))
    .agg(
        first('gene_id', ignorenulls=True).alias('disease_gene_id')
    )
)

print(f"Genes with gene_id from disease links: {disease_gene_ids.count():,}")
disease_gene_ids.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Source 3 - Query HGNC API for Ensembl IDs
print("\nSOURCE 3: HGNC API FOR ENSEMBL IDs")
print("="*80)

# Get genes with HGNC IDs but no Ensembl IDs
genes_with_hgnc = (
    df_genes
    .filter(
        ((col('ensembl_id').isNull() | (col('ensembl_id') == '') | (length(col('ensembl_id')) <= 10))) &
        ((col('hgnc_id') != '') & col('hgnc_id').isNotNull())
    )
    .select('gene_name', 'hgnc_id')
    .collect()
)

print(f"Genes to query via HGNC API: {len(genes_with_hgnc):,}")
print(f"Estimated time: {len(genes_with_hgnc) * 0.1 / 60:.1f} minutes")

# HGNC API query function
def query_hgnc_api(gene_info):
    """Query HGNC REST API for Ensembl and Entrez IDs"""
    hgnc_id = gene_info.hgnc_id
    gene_name = gene_info.gene_name
    
    try:
        # HGNC REST API endpoint
        url = f"https://rest.genenames.org/fetch/hgnc_id/{hgnc_id}"
        headers = {'Accept': 'application/json'}
        
        response = requests.get(url, headers=headers, timeout=10)
        time.sleep(0.1)  # Rate limiting
        
        if response.status_code == 200:
            data = response.json()
            
            if 'response' in data and 'docs' in data['response']:
                docs = data['response']['docs']
                if len(docs) > 0:
                    doc = docs[0]
                    
                    ensembl_id = doc.get('ensembl_gene_id', '')
                    entrez_id = doc.get('entrez_id', '')
                    
                    return {
                        'gene_name': gene_name,
                        'hgnc_id': hgnc_id,
                        'hgnc_ensembl_id': ensembl_id if ensembl_id else '',
                        'hgnc_entrez_id': str(entrez_id) if entrez_id else '',
                        'success': True
                    }
    except:
        pass
    
    return {
        'gene_name': gene_name,
        'hgnc_id': hgnc_id,
        'hgnc_ensembl_id': '',
        'hgnc_entrez_id': '',
        'success': False
    }

# Query HGNC API in parallel
print("\nQuerying HGNC API...")
BATCH_SIZE = 100
MAX_WORKERS = 10

hgnc_results = []
success_count = 0

total_batches = (len(genes_with_hgnc) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(total_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(genes_with_hgnc))
    batch = genes_with_hgnc[start_idx:end_idx]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(query_hgnc_api, gene) for gene in batch]
        
        for future in as_completed(futures):
            result = future.result()
            hgnc_results.append(result)
            if result['success']:
                success_count += 1
    
    if (batch_num + 1) % 10 == 0:
        print(f"  Batch {batch_num+1}/{total_batches} | Success: {success_count:,}")

print(f"\nHGNC API complete: {success_count:,} successful lookups")

# Convert to DataFrame
hgnc_schema = StructType([
    StructField("gene_name", StringType(), True),
    StructField("hgnc_id", StringType(), True),
    StructField("hgnc_ensembl_id", StringType(), True),
    StructField("hgnc_entrez_id", StringType(), True)
])

hgnc_data = [{
    'gene_name': r['gene_name'],
    'hgnc_id': r['hgnc_id'],
    'hgnc_ensembl_id': r['hgnc_ensembl_id'],
    'hgnc_entrez_id': r['hgnc_entrez_id']
} for r in hgnc_results if r['success']]

df_hgnc_ids = spark.createDataFrame(hgnc_data, schema=hgnc_schema)

# COMMAND ----------

# DBTITLE 1,Merge All ID Sources
print("\nMERGING ALL ID SOURCES")
print("="*80)

# First, let's check what type entrez_id actually is in the table
print("Checking entrez_id column type:")
df_genes.select('entrez_id').printSchema()

df_genes_updated = (
    df_genes
    
    # Join variant gene_ids
    .join(variant_gene_ids, 'gene_name', 'left')
    
    # Join disease gene_ids  
    .join(disease_gene_ids, 'gene_name', 'left')
    
    # Join HGNC IDs
    .join(
        df_hgnc_ids.drop('hgnc_id'),
        'gene_name', 
        'left'
    )
    
    # Convert all string IDs to BIGINT using try_cast (returns NULL for empty strings)
    .withColumn('variant_gene_id_bigint', expr("try_cast(variant_gene_id as bigint)"))
    .withColumn('disease_gene_id_bigint', expr("try_cast(disease_gene_id as bigint)"))
    .withColumn('hgnc_entrez_id_bigint', expr("try_cast(hgnc_entrez_id as bigint)"))
    
    # Update Entrez ID - now all are BIGINT or NULL
    .withColumn('entrez_id',
                coalesce(
                    col('entrez_id'),  # Keep existing if not null
                    col('variant_gene_id_bigint'),
                    col('disease_gene_id_bigint'),
                    col('hgnc_entrez_id_bigint')
                ))
    
    # Update Ensembl ID (string column)
    .withColumn('ensembl_id',
                when((col('ensembl_id').isNull()) | (col('ensembl_id') == '') | (length(col('ensembl_id')) <= 10),
                     coalesce(
                         when((col('hgnc_ensembl_id').isNotNull()) & (col('hgnc_ensembl_id') != '') & (length(col('hgnc_ensembl_id')) > 10), 
                              col('hgnc_ensembl_id')),
                         col('ensembl_id')
                     ))
                .otherwise(col('ensembl_id')))
    
    # Drop all temporary columns
    .drop('variant_gene_id', 'disease_gene_id', 'hgnc_ensembl_id', 'hgnc_entrez_id',
          'variant_gene_id_bigint', 'disease_gene_id_bigint', 'hgnc_entrez_id_bigint')
)

# Save with overwrite to handle schema changes
df_genes_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")

print("IDs merged successfully")

# COMMAND ----------

# DBTITLE 1,Final Statistics
print("\nFINAL STATISTICS")
print("="*80)

df_genes_final = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
total = df_genes_final.count()

final_ensembl = df_genes_final.filter(
    (col('ensembl_id') != '') & col('ensembl_id').isNotNull() & (length(col('ensembl_id')) > 10)
).count()

# Minimal fix: entrez_id is BIGINT, so filter for non-null and positive values
final_entrez = df_genes_final.filter(
    col('entrez_id').isNotNull() & (col('entrez_id') > 0)
).count()

print(f"Total genes: {total:,}")

print(f"\nFinal ID Coverage:")
print(f"  Ensembl IDs: {final_ensembl:,} ({final_ensembl/total*100:.1f}%)")
print(f"  Entrez IDs: {final_entrez:,} ({final_entrez/total*100:.1f}%)")
print(f"  Either ID: {max(final_ensembl, final_entrez):,} ({max(final_ensembl, final_entrez)/total*100:.1f}%)")

print(f"\nID Sources (estimated):")
print(f"  From variants: {variant_gene_ids.count():,}")
print(f"  From disease links: {disease_gene_ids.count():,}")
print(f"  From HGNC API: {success_count:,}")


# COMMAND ----------

# DBTITLE 1,Summary
print("MULTI-SOURCE ID ENRICHMENT COMPLETE")
print("="*80)

print(f"\nAchieved:")
print(f"  Ensembl coverage: {final_ensembl/total*100:.1f}%")
print(f"  Entrez coverage: {final_entrez/total*100:.1f}%")

print(f"\nData sources used:")
print(f"  1. variants_ultra_enriched.gene_id")
print(f"  2. gene_disease_links.gene_id")
print(f"  3. HGNC REST API (via hgnc_id)")
