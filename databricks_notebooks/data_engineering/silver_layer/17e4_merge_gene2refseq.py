# Databricks notebook source
# MAGIC %md
# MAGIC #### MERGE NCBI gene2refseq COORDINATES
# MAGIC ##### Add coordinates for 25K missing genes
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 2026

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when, lit

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("NCBI gene2refseq COORDINATE MERGE")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load gene2refseq Coordinates
print("\nLOADING NCBI gene2refseq COORDINATES")
print("="*80)

try:
    gene2refseq_coords = spark.table(f"{catalog_name}.default.ncbi_gene_2_refseq_coordinates")
    
    gene2refseq_count = gene2refseq_coords.count()
    print(f"gene2refseq genes loaded: {gene2refseq_count:,}")
    
    print("\nSample gene2refseq data:")
    gene2refseq_coords.show(10, truncate=False)
    
    has_gene2refseq = True
    
except Exception as e:
    print(f"ERROR: Could not load ncbi_gene2refseq_coordinates table")
    print(f"Error: {e}")
    print("\nPlease:")
    print("  1. Upload ncbi_gene2refseq_coordinates.csv to Databricks")
    print("  2. Create table in workspace.default schema")
    has_gene2refseq = False

if not has_gene2refseq:
    dbutils.notebook.exit("FAILED: gene2refseq table not found")

# COMMAND ----------

# DBTITLE 1,Check Current State
print("\nCHECKING CURRENT GENE TABLE STATE")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

total_genes = df_genes.count()
with_coords = df_genes.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()
missing_coords = total_genes - with_coords

print(f"Total genes: {total_genes:,}")
print(f"With coordinates: {with_coords:,} ({with_coords/total_genes*100:.1f}%)")
print(f"Missing coordinates: {missing_coords:,} ({missing_coords/total_genes*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Match gene2refseq to genes by gene_id
print("\nMATCHING gene2refseq TO genes_ultra_enriched")
print("="*80)

# Convert gene_id to string for join
gene2refseq_for_join = gene2refseq_coords.select(
    col("gene_id").cast("string").alias("gene_id_ref"),
    col("start_position").alias("ref_start"),
    col("end_position").alias("ref_end"),
    col("gene_length").alias("ref_length"),
    col("source").alias("ref_source")
)

# Join on gene_id
matched = df_genes.join(
    gene2refseq_for_join,
    col("gene_id") == col("gene_id_ref"),
    "left"
)

matched_count = matched.filter(col("ref_start").isNotNull()).count()
print(f"Genes matched to gene2refseq: {matched_count:,} / {total_genes:,}") 

# COMMAND ----------

# DBTITLE 1,Merge Coordinates
print("\nMERGING COORDINATES")
print("="*80)

df_genes_updated = (
    matched
    .drop("gene_id_ref")
    
    # Update start_position (keep existing if not null, otherwise use gene2refseq)
    .withColumn("start_position",
                coalesce(col("start_position"), col("ref_start")))
    
    # Update end_position
    .withColumn("end_position",
                coalesce(col("end_position"), col("ref_end")))
    
    # Update gene_length
    .withColumn("gene_length",
                coalesce(
                    col("gene_length"),
                    col("ref_length"),
                    when(col("start_position").isNotNull() & col("end_position").isNotNull(),
                         col("end_position") - col("start_position"))
                ))
    
    .drop("ref_start", "ref_end", "ref_length", "ref_source")
)

# Save
df_genes_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")

print("genes_ultra_enriched updated")

# COMMAND ----------

# DBTITLE 1,Final Statistics
print("\nFINAL STATISTICS")
print("="*80)

df_genes_final = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

total = df_genes_final.count()
with_coords_final = df_genes_final.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()

improvement = with_coords_final - with_coords
improvement_pct = improvement / total * 100

print(f"Total genes: {total:,}")
print(f"\nBEFORE: {with_coords:,} ({with_coords/total*100:.1f}%)")
print(f"AFTER:  {with_coords_final:,} ({with_coords_final/total*100:.1f}%)")
print(f"\nIMPROVEMENT: +{improvement:,} genes (+{improvement_pct:.1f} percentage points)")

# COMMAND ----------

# DBTITLE 1,Real Genes Coverage
print("\nREAL GENES COVERAGE (excluding biological-region LOC)")
print("="*80)

real_genes = df_genes_final.filter(
    ~(col("gene_name").startswith("LOC") & (col("gene_type") == "biological-region"))
)

real_total = real_genes.count()
real_with_coords = real_genes.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()

print(f"Real genes: {real_total:,}")
print(f"With coordinates: {real_with_coords:,}")
print(f"Coverage: {real_with_coords/real_total*100:.1f}%")

print(f"\nEXPECTED SV OVERLAP:")
print(f"  Conservative: {real_with_coords * 0.4:,.0f} SVs (40% of genes with coords)")
print(f"  Realistic: {real_with_coords * 0.5:,.0f} SVs (50% of genes with coords)")
print(f"  Optimistic: {real_with_coords * 0.6:,.0f} SVs (60% of genes with coords)")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*80)
print("GENE2REFSEQ MERGE COMPLETE")
print("="*80)

print(f"\nOverall coverage: {with_coords_final/total*100:.1f}%")
print(f"Real genes coverage: {real_with_coords/real_total*100:.1f}%")
print(f"Added coordinates for: {improvement:,} genes")
