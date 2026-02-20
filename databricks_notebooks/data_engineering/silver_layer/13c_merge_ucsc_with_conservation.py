# Databricks notebook source
# MAGIC %md
# MAGIC #### MERGE UCSC CONSERVATION SCORES WITH EXISTING DATA
# MAGIC ##### Updates conservation_scores table with PhyloP and PhastCons from UCSC
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 27, 2026
# MAGIC
# MAGIC **Input:** ucsc_conservation_scores.csv (from local download script)   
# MAGIC **Output:** Updated silver.conservation_scores with PhyloP/PhastCons scores
# MAGIC
# MAGIC **MERGES WITH EXISTING DATA - Does NOT overwrite CADD/gnomAD scores!**

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when, lit

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("UCSC CONSERVATION SCORES MERGE")

# COMMAND ----------

# DBTITLE 1,Check for UCSC Data
print("\nCHECKING FOR UCSC DATA")
print("="*80)

# Try to find the uploaded table
UCSC_TABLE = f"{catalog_name}.default.ucsc_conservation_scores"

try:
    df_ucsc = spark.table(UCSC_TABLE)
    ucsc_count = df_ucsc.count()
    print(f" Found UCSC data: {ucsc_count:,} variants")
except Exception as e:
    print(f"\n ERROR: UCSC table not found!")
    print(f"  Looking for: {UCSC_TABLE}")
    print(f"\nPlease upload ucsc_conservation_scores.csv first:")
    print("  1. Go to Databricks UI")
    print("  2. Data > Create Table > Upload File")
    print("  3. Upload: ucsc_conservation_scores.csv")
    print("  4. Re-run this notebook")
    dbutils.notebook.exit("UCSC data not found")

# Show sample
print("\nSample UCSC data:")
df_ucsc.show(3)

# COMMAND ----------

# DBTITLE 1,Load Existing Conservation Scores
print("\nLOADING EXISTING CONSERVATION DATA")
print("="*80)

df_existing = spark.table(f"{catalog_name}.silver.conservation_scores")
existing_count = df_existing.count()

print(f"Existing conservation_scores: {existing_count:,} variants")

# Check current coverage
print("\nCurrent coverage (before UCSC merge):")
print(f"  PhyloP: {df_existing.filter(col('phylop_score').isNotNull()).count():,}")
print(f"  PhastCons: {df_existing.filter(col('phastcons_score').isNotNull()).count():,}")
print(f"  GERP: {df_existing.filter(col('gerp_score').isNotNull()).count():,}")
print(f"  CADD: {df_existing.filter(col('cadd_phred').isNotNull()).count():,}")
print(f"  gnomAD: {df_existing.filter(col('gnomad_af').isNotNull()).count():,}")

# COMMAND ----------

# DBTITLE 1,Merge UCSC with Existing Data
print("\nMERGING UCSC DATA WITH EXISTING TABLE")
print("="*80)

# Join on variant_id
df_merged = (
    df_existing
    .join(df_ucsc,
          df_existing.variant_id == df_ucsc.variant_id,
          "left")
    .select(
        df_existing.variant_id,
        df_existing.chromosome,
        df_existing.position,
        df_existing.reference_allele,
        df_existing.alternate_allele,
        df_existing.gene_name,
        
        # MERGE PhyloP: Use UCSC if available, else keep existing (likely NULL)
        coalesce(df_ucsc.phylop_score_ucsc, df_existing.phylop_score).alias("phylop_score"),
        
        # MERGE PhastCons: Use UCSC if available, else keep existing (likely NULL)
        coalesce(df_ucsc.phastcons_score_ucsc, df_existing.phastcons_score).alias("phastcons_score"),
        
        # KEEP existing scores (don't overwrite!)
        df_existing.gerp_score,
        df_existing.sift_score,
        df_existing.polyphen_score,
        df_existing.cadd_phred,
        df_existing.gnomad_af,
        
        # KEEP existing flags
        df_existing.is_highly_conserved,
        df_existing.is_constrained,
        df_existing.is_likely_deleterious,
        df_existing.conservation_level,
        df_existing.is_common_variant,
        df_existing.is_rare_variant
    )
)

merged_count = df_merged.count()
print(f"Merged table: {merged_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Update Conservation Flags
print("\nUPDATING CONSERVATION FLAGS")
print("="*80)

df_merged_updated = (
    df_merged
    # Update is_highly_conserved flag (PhyloP > 2.7)
    .withColumn("is_highly_conserved",
                when(col("phylop_score") > 2.7, True).otherwise(False))
    
    # Update conservation_level (0-3)
    # Note: GERP still NULL, so max level is 2 (PhyloP + CADD)
    .withColumn("conservation_level",
                when(col("phylop_score") > 2.7, 1).otherwise(0) +
                when(col("gerp_score") > 4.0, 1).otherwise(0) +
                when(col("cadd_phred") > 20.0, 1).otherwise(0))
)

print("Conservation flags updated based on new PhyloP scores")

# COMMAND ----------

# DBTITLE 1,Verify Merge Results
print("\nVERIFYING MERGE RESULTS")
print("="*80)

print("\nNew coverage (after UCSC merge):")
phylop_new = df_merged_updated.filter(col('phylop_score').isNotNull()).count()
phastcons_new = df_merged_updated.filter(col('phastcons_score').isNotNull()).count()
gerp_new = df_merged_updated.filter(col('gerp_score').isNotNull()).count()
cadd_new = df_merged_updated.filter(col('cadd_phred').isNotNull()).count()
gnomad_new = df_merged_updated.filter(col('gnomad_af').isNotNull()).count()

print(f"  PhyloP: {phylop_new:,} ({phylop_new/merged_count*100:.1f}%)")
print(f"  PhastCons: {phastcons_new:,} ({phastcons_new/merged_count*100:.1f}%)")
print(f"  GERP: {gerp_new:,} ({gerp_new/merged_count*100:.1f}%)")
print(f"  CADD: {cadd_new:,} ({cadd_new/merged_count*100:.1f}%)")
print(f"  gnomAD: {gnomad_new:,} ({gnomad_new/merged_count*100:.1f}%)")

print("\nConservation level distribution:")
df_merged_updated.groupBy("conservation_level").count().orderBy("conservation_level").show()

print("\nSample enriched variants:")
df_merged_updated.filter(
    col("conservation_level") >= 1
).select(
    "gene_name",
    "chromosome",
    "position",
    "phylop_score",
    "phastcons_score",
    "cadd_phred",
    "gnomad_af"
).show(3)

# COMMAND ----------

# DBTITLE 1,Save Updated Conservation Scores
print("\nSAVING UPDATED CONSERVATION SCORES")
print("="*80)

df_merged_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.conservation_scores")

print(f" Saved: {catalog_name}.silver.conservation_scores")
print("  MERGED UCSC PhyloP/PhastCons with existing CADD/gnomAD scores")

# COMMAND ----------

# DBTITLE 1,Cleanup UCSC Temp Table
print("\nCLEANUP")
print("="*80)

try:
    spark.sql(f"DROP TABLE IF EXISTS {UCSC_TABLE}")
    print(f" Removed temp table: {UCSC_TABLE}")
except:
    print("Temp table cleanup skipped")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("UCSC CONSERVATION MERGE COMPLETE")
print("="*80)

print(f"\nFINAL STATISTICS:")
print(f"  Total variants: {merged_count:,}")
print(f"  PhyloP coverage: {phylop_new:,} ({phylop_new/merged_count*100:.1f}%) ← NEW!")
print(f"  PhastCons coverage: {phastcons_new:,} ({phastcons_new/merged_count*100:.1f}%) ← NEW!")
print(f"  CADD coverage: {cadd_new:,} ({cadd_new/merged_count*100:.1f}%) ← KEPT!")
print(f"  gnomAD coverage: {gnomad_new:,} ({gnomad_new/merged_count*100:.1f}%) ← KEPT!")

print(f"\nIMPROVEMENT:")
print(f"  Before: 0 variants with PhyloP")
print(f"  After: {phylop_new:,} variants with PhyloP")
print(f"  Gain: {phylop_new:,} variants ({phylop_new/merged_count*100:.1f}% coverage)")

print(f"\nCONSERVATION LEVEL DISTRIBUTION:")
for level in range(4):
    count = df_merged_updated.filter(col("conservation_level") == level).count()
    print(f"  Level {level}: {count:,} variants ({count/merged_count*100:.1f}%)")

print(f"\nDATA SOURCES COMBINED:")
print(f" MyVariant.info API (CADD, gnomAD, SIFT, PolyPhen)")
print(f" UCSC Genome Browser (PhyloP, PhastCons)")

print(f"\nTABLE UPDATED:")
print(f"  {catalog_name}.silver.conservation_scores")
