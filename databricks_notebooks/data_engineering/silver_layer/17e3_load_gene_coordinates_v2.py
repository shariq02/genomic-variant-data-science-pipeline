# Databricks notebook source
# MAGIC %md
# MAGIC #### GENE COORDINATE ENRICHMENT V2 - GENCODE CSV + VARIANTS
# MAGIC ##### Load gene coordinates from uploaded CSV + variants
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 2026
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Upload gencode_gene_coordinates.csv to Unity Catalog
# MAGIC - Table: workspace.silver.gencode_gene_coordinates

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, length,
    min as spark_min, max as spark_max, count
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GENE COORDINATE ENRICHMENT V2 - GENCODE CSV + VARIANTS")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Analyze Current State
print("\nCURRENT STATE")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

total_genes = df_genes.count()
with_coords = df_genes.filter(
    col("start_position").isNotNull() & col("end_position").isNotNull()
).count()

print(f"Total genes: {total_genes:,}")
print(f"With coordinates: {with_coords:,} ({with_coords/total_genes*100:.1f}%)")
print(f"Missing: {total_genes - with_coords:,}")

# COMMAND ----------

# DBTITLE 1,Load GENCODE Coordinates
print("\nSOURCE 1: LOADING GENCODE FROM CSV")
print("="*80)

try:
    # Load the GENCODE table (you created this from CSV upload)
    gencode_coords = spark.table(f"{catalog_name}.default.gencode_gene_coordinates")
    
    gencode_count = gencode_coords.count()
    print(f"GENCODE genes loaded: {gencode_count:,}")
    
    print("\nSample GENCODE data:")
    gencode_coords.show(10, truncate=False)
    
    print("\nGENCODE chromosomes:")
    gencode_coords.groupBy("chromosome").count().orderBy("count", ascending=False).show(25)
    
    has_gencode = True
    
except Exception as e:
    print(f"ERROR: Could not load gencode_gene_coordinates table")
    print(f"  Error: {e}")
    print("\n  Please ensure:")
    print("    1. Run extract_gencode_coordinates.py locally")
    print("    2. Upload gencode_gene_coordinates.csv to Databricks")
    print("    3. Create table: workspace.silver.gencode_gene_coordinates")
    print("\n  Proceeding with variants-only approach...")
    has_gencode = False
    gencode_coords = None

# COMMAND ----------

# DBTITLE 1,Extract Coordinates from Variants
print("\nSOURCE 2: EXTRACTING FROM VARIANTS")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

variant_coords = (
    df_variants
    .filter(col("position").isNotNull())
    .filter(col("gene_name").isNotNull())
    .filter(col("chromosome").isNotNull())
    .groupBy("gene_name", "chromosome")
    .agg(
        spark_min(col("position").cast("long")).alias("start_position"),
        spark_max(col("position").cast("long")).alias("end_position"),
        count("*").alias("variant_count")
    )
    .withColumn("gene_length", col("end_position") - col("start_position"))
    .withColumn("source", lit("variants"))
    .filter(col("variant_count") > 1)  # At least 2 variants for reliable boundary
    .drop("variant_count")
)

variant_count = variant_coords.count()
print(f" Coordinates from variants: {variant_count:,}")

print("\nSample variant-derived coordinates:")
variant_coords.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Merge Coordinate Sources
print("\nMERGING COORDINATE SOURCES")
print("="*80)

if has_gencode:
    # Priority: GENCODE (canonical) > Variants (observed)
    print("Strategy: GENCODE (primary) + Variants (fallback)")
    
    # Prepare GENCODE (already has correct schema)
    gencode_prepared = gencode_coords.select(
        "gene_name", "chromosome", "start_position", "end_position", "gene_length", "source"
    )
    
    # Union both sources
    all_coords = gencode_prepared.unionByName(variant_coords)
    
    # Remove duplicates (prefer GENCODE over variants)
    merged_coords = (
        all_coords
        .withColumn("priority",
                    when(col("source") == "gencode_v44", 1)
                    .when(col("source") == "variants", 2)
                    .otherwise(3))
        .orderBy("gene_name", "chromosome", "priority")
        .dropDuplicates(["gene_name", "chromosome"])
        .drop("priority")
    )
    
else:
    # Fallback: Variants only
    print("Strategy: Variants only (GENCODE not available)")
    merged_coords = variant_coords

merged_count = merged_coords.count()
print(f"\n Total unique coordinates: {merged_count:,}")

print("\nSource breakdown:")
merged_coords.groupBy("source").count().orderBy("count", ascending=False).show()

print("\nChromosome distribution:")
merged_coords.groupBy("chromosome").count().orderBy("count", ascending=False).show(25)

# COMMAND ----------

# DBTITLE 1,Update genes_ultra_enriched
print("\nUPDATING GENES TABLE")
print("="*80)

# Prepare coordinates for join
coords_for_join = merged_coords.select(
    col("gene_name").alias("coord_gene_name"),
    col("chromosome").alias("coord_chromosome"),
    col("start_position").alias("new_start"),
    col("end_position").alias("new_end"),
    col("gene_length").alias("new_length"),
    col("source").alias("new_source")
)

df_genes_updated = (
    df_genes
    .join(
        coords_for_join,
        (col("gene_name") == col("coord_gene_name")) &
        (col("chromosome") == col("coord_chromosome")),
        "left"
    )
    .drop("coord_gene_name", "coord_chromosome")
    
    # Update start_position (keep existing if not null, otherwise use new)
    .withColumn("start_position",
                coalesce(col("start_position"), col("new_start")))
    
    # Update end_position
    .withColumn("end_position",
                coalesce(col("end_position"), col("new_end")))
    
    # Update gene_length
    .withColumn("gene_length",
                coalesce(
                    col("gene_length"),
                    col("new_length"),
                    when(col("start_position").isNotNull() & col("end_position").isNotNull(),
                         col("end_position") - col("start_position"))
                ))
    
    # Drop temporary columns
    .drop("new_start", "new_end", "new_length", "new_source")
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
print(f"\n IMPROVEMENT: +{improvement:,} genes (+{improvement_pct:.1f} percentage points)")

print("\nChromosome coverage:")
df_genes_final.filter(col("start_position").isNotNull()).groupBy("chromosome").count().orderBy("count", ascending=False).show(25)

# COMMAND ----------

# DBTITLE 1,Validation Checks
print("\nVALIDATION CHECKS")
print("="*80)

# Valid chromosomes
valid_chroms = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
valid_chr_count = df_genes_final.filter(
    col("start_position").isNotNull() &
    col("chromosome").isin(valid_chroms)
).count()

print(f"Genes with valid chromosomes: {valid_chr_count:,} / {with_coords_final:,} ({valid_chr_count/with_coords_final*100:.1f}%)")

# Coordinate ranges
print("\nCoordinate statistics:")
df_genes_final.filter(col("start_position").isNotNull()).select(
    spark_min("start_position").alias("min_start"),
    spark_max("end_position").alias("max_end"),
    spark_min("gene_length").alias("min_length"),
    spark_max("gene_length").alias("max_length")
).show()

# Invalid lengths
invalid_lengths = df_genes_final.filter(
    col("gene_length").isNotNull() & (col("gene_length") <= 0)
).count()

if invalid_lengths > 0:
    print(f" WARNING: {invalid_lengths:,} genes have invalid lengths (<=0)")
else:
    print(f" PASS: All gene lengths valid (>0)")

# COMMAND ----------

# DBTITLE 1,Expected SV Coverage Estimate
print("\nEXPECTED SV-GENE MAPPING COVERAGE")
print("="*80)

coverage_rate = with_coords_final / total
estimated_sv_overlap_low = coverage_rate * 0.40  # Conservative: 40% of genes with coords
estimated_sv_overlap_high = coverage_rate * 0.60  # Optimistic: 60% of genes with coords

print(f"Gene coordinate coverage: {coverage_rate*100:.1f}%")
print(f"\nExpected SV-gene overlap rate:")
print(f"  Conservative: {estimated_sv_overlap_low*100:.0f}% of SVs will overlap genes")
print(f"  Optimistic:   {estimated_sv_overlap_high*100:.0f}% of SVs will overlap genes")
print(f"\nFor 216,951 SVs:")
print(f"  Expected overlaps: {216951*estimated_sv_overlap_low:,.0f} - {216951*estimated_sv_overlap_high:,.0f} SVs")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*80)
print("GENE COORDINATE ENRICHMENT COMPLETE")
print("="*80)

print(f"\nFinal coverage: {with_coords_final/total*100:.1f}%")
print(f"Improvement: +{improvement_pct:.1f} percentage points")

if coverage_rate >= 0.50:
    print("\n EXCELLENT: >50% coverage - SV mapping will work very well")
    success_level = "excellent"
elif coverage_rate >= 0.30:
    print("\n GOOD: 30-50% coverage - SV mapping should work well")
    success_level = "good"
elif coverage_rate >= 0.15:
    print("\n MODERATE: 15-30% coverage - SV mapping will have limited coverage")
    success_level = "moderate"
else:
    print("\n POOR: <15% coverage - SV mapping may not work")
    success_level = "poor"

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)
if success_level in ["excellent", "good"]:
    print(" Ready to run: 17e_feature_engineering_structural.py")
    print("  Expected: 40-60% of SVs will have gene overlaps")
elif success_level == "moderate":
    print(" Can proceed but expect lower SV coverage")
    print("  Run: 17e_feature_engineering_structural.py")
else:
    print(" Need to investigate gene coordinate enrichment")
    print("  Check: Why so few genes have coordinates?")

print("="*80)
