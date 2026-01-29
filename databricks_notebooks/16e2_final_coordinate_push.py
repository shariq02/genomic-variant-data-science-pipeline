# Databricks notebook source
# MAGIC %md
# MAGIC #### FINAL COORDINATE PUSH - Last 1,500 Genes
# MAGIC ##### Extract remaining genes that have IDs
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 28, 2026
# MAGIC
# MAGIC **Current:** 59,413 genes (30.7%)  
# MAGIC **Target:** 60,900 genes (31.4%)  
# MAGIC **Gap:** 474 Ensembl + 1,123 HGNC = ~1,500 genes

# COMMAND ----------

# DBTITLE 1,Import
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, length, lit
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

print("SPARK INITIALIZED FOR FINAL COORDINATE PUSH - LAST 1,500 GENES")

# COMMAND ----------

# DBTITLE 1,Extract Remaining Genes with IDs
print("\nEXTRACTING REMAINING GENES WITH IDs")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

# Get missing genes that have Ensembl IDs
missing_with_ensembl = (
    df_genes
    .filter(col("start_position").isNull() | col("end_position").isNull())
    .filter(col("ensembl_id").isNotNull())
    .filter(col("ensembl_id") != "")
    .filter(length(col("ensembl_id")) > 10)
    .select("gene_name", "ensembl_id", "chromosome")
    .collect()
)

# Get missing genes that have HGNC IDs (but no Ensembl)
missing_with_hgnc = (
    df_genes
    .filter(col("start_position").isNull() | col("end_position").isNull())
    .filter((col("ensembl_id").isNull()) | (col("ensembl_id") == "") | (length(col("ensembl_id")) <= 10))
    .filter(col("hgnc_id").isNotNull())
    .filter(col("hgnc_id") != "")
    .select("gene_name", "hgnc_id", "chromosome")
    .collect()
)

print(f"Missing with Ensembl IDs: {len(missing_with_ensembl):,}")
print(f"Missing with HGNC IDs (no Ensembl): {len(missing_with_hgnc):,}")
print(f"Total to query: {len(missing_with_ensembl) + len(missing_with_hgnc):,}")

# COMMAND ----------

# DBTITLE 1,Query Ensembl for Remaining Genes
print("\nQUERYING ENSEMBL FOR REMAINING GENES")
print("="*80)

def query_ensembl(gene_info):
    try:
        url = f"https://rest.ensembl.org/lookup/id/{gene_info.ensembl_id}?content-type=application/json"
        response = requests.get(url, timeout=10)
        time.sleep(0.07)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('start') and data.get('end'):
                return {
                    'gene_name': gene_info.gene_name,
                    'start_position': int(data['start']),
                    'end_position': int(data['end']),
                    'chromosome': str(data.get('seq_region_name', gene_info.chromosome)),
                    'source': 'ensembl_final',
                    'success': True
                }
    except:
        pass
    
    return {'gene_name': gene_info.gene_name, 'success': False}

ensembl_results = []
if len(missing_with_ensembl) > 0:
    print(f"Querying {len(missing_with_ensembl):,} genes...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(query_ensembl, gene) for gene in missing_with_ensembl]
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result['success']:
                ensembl_results.append(result)
            
            if (i + 1) % 50 == 0:
                print(f"  Processed: {i+1}/{len(missing_with_ensembl)} | Success: {len(ensembl_results)}")
    
    print(f"Ensembl results: {len(ensembl_results):,} successful")

# COMMAND ----------

# DBTITLE 1,Query HGNC for Remaining Genes
print("\nQUERYING HGNC FOR REMAINING GENES")
print("="*80)

def query_hgnc(gene_info):
    try:
        url = f"https://rest.genenames.org/fetch/hgnc_id/{gene_info.hgnc_id}"
        headers = {'Accept': 'application/json'}
        response = requests.get(url, headers=headers, timeout=10)
        time.sleep(0.1)
        
        if response.status_code == 200:
            data = response.json()
            if 'response' in data and 'docs' in data['response']:
                docs = data['response']['docs']
                if len(docs) > 0:
                    ensembl_id = docs[0].get('ensembl_gene_id')
                    
                    if ensembl_id:
                        # Now query Ensembl with this ID
                        time.sleep(0.07)
                        url2 = f"https://rest.ensembl.org/lookup/id/{ensembl_id}?content-type=application/json"
                        response2 = requests.get(url2, timeout=10)
                        
                        if response2.status_code == 200:
                            data2 = response2.json()
                            if data2.get('start') and data2.get('end'):
                                return {
                                    'gene_name': gene_info.gene_name,
                                    'start_position': int(data2['start']),
                                    'end_position': int(data2['end']),
                                    'chromosome': str(data2.get('seq_region_name', gene_info.chromosome)),
                                    'source': 'hgnc_to_ensembl',
                                    'success': True
                                }
    except:
        pass
    
    return {'gene_name': gene_info.gene_name, 'success': False}

hgnc_results = []
if len(missing_with_hgnc) > 0:
    print(f"Querying {len(missing_with_hgnc):,} genes...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:  # Slower for HGNC
        futures = [executor.submit(query_hgnc, gene) for gene in missing_with_hgnc]
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result['success']:
                hgnc_results.append(result)
            
            if (i + 1) % 50 == 0:
                print(f"  Processed: {i+1}/{len(missing_with_hgnc)} | Success: {len(hgnc_results)}")
    
    print(f"HGNC results: {len(hgnc_results):,} successful")

# COMMAND ----------

# DBTITLE 1,Merge Final Results
print("\nMERGING FINAL RESULTS")
print("="*80)

all_results = ensembl_results + hgnc_results
print(f"Total successful lookups: {len(all_results):,}")

if len(all_results) > 0:
    # Create DataFrame
    result_schema = StructType([
        StructField("gene_name", StringType(), True),
        StructField("start_position", LongType(), True),
        StructField("end_position", LongType(), True),
        StructField("chromosome", StringType(), True),
        StructField("source", StringType(), True)
    ])
    
    df_final_coords = spark.createDataFrame(all_results, schema=result_schema)
    
    # Merge with genes table
    df_genes_updated = (
        df_genes
        .join(
            df_final_coords.select(
                col("gene_name").alias("gene_name_final"),
                col("start_position").alias("final_start"),
                col("end_position").alias("final_end"),
                "source"
            ),
            col("gene_name") == col("gene_name_final"),
            "left"
        )
        .drop("gene_name_final")
        
        .withColumn("start_position",
                    coalesce(col("start_position"), col("final_start")))
        
        .withColumn("end_position",
                    coalesce(col("end_position"), col("final_end")))
        
        .withColumn("gene_length",
                    when(col("gene_length").isNull() & 
                         col("start_position").isNotNull() & 
                         col("end_position").isNotNull(),
                         col("end_position") - col("start_position"))
                    .otherwise(col("gene_length")))
        
        .withColumn("coordinate_source",
                    when(col("coordinate_source") == "missing",
                         when(col("source").isNotNull(), col("source"))
                         .otherwise(lit("missing")))
                    .otherwise(col("coordinate_source")))
        
        .drop("final_start", "final_end", "source")
    )
    
    # Save
    df_genes_updated.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")
    
    print("Final coordinates merged")

# COMMAND ----------

# DBTITLE 1,Final Statistics
print("\nFINAL STATISTICS")
print("="*80)

df_genes_final = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

total = df_genes_final.count()
with_coords = df_genes_final.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()

print(f"Total genes: {total:,}")
print(f"With coordinates: {with_coords:,} ({with_coords/total*100:.1f}%)")
print(f"Still missing: {total - with_coords:,} ({(total - with_coords)/total*100:.1f}%)")

print(f"\nImprovement from final push:")
print(f"  Before: 59,413 (30.7%)")
print(f"  After: {with_coords:,} ({with_coords/total*100:.1f}%)")
print(f"  Added: {with_coords - 59413:,} genes")

print("\nCoordinate sources:")
df_genes_final.groupBy("coordinate_source").count().orderBy("count", ascending=False).show()

print("\nBreakdown of missing genes:")
missing = df_genes_final.filter(col("start_position").isNull())
print(f"  Total missing: {missing.count():,}")
print(f"  LOC genes: {missing.filter(col('gene_name').startswith('LOC')).count():,}")
print(f"  Real genes: {missing.filter(~col('gene_name').startswith('LOC')).count():,}")

# Calculate real gene coverage
real_genes_total = df_genes_final.filter(~col('gene_name').startswith('LOC')).count()
real_genes_with_coords = df_genes_final.filter(
    (~col('gene_name').startswith('LOC')) & 
    col("start_position").isNotNull()
).count()

print(f"\nReal genes (non-LOC) coverage:")
print(f"  Total real genes: {real_genes_total:,}")
print(f"  With coordinates: {real_genes_with_coords:,} ({real_genes_with_coords/real_genes_total*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Summary
print("FINAL COORDINATE ENRICHMENT COMPLETE")
print("="*80)

print(f"\nFinal coverage: {with_coords/total*100:.1f}%")

print(f"\nReal genes coverage:")
real_genes = df_genes_final.filter(~col('gene_name').startswith('LOC')).count()
real_with_coords = df_genes_final.filter(
    (~col('gene_name').startswith('LOC')) &
    col("start_position").isNotNull()
).count()
print(f"  Total real genes: {real_genes:,}")
print(f"  Real genes with coords: {real_with_coords:,} ({real_with_coords/real_genes*100:.1f}%)")
