# Databricks notebook source
# MAGIC %md
# MAGIC #### MERGE UCSC CONSERVATION WITH BASE CONSERVATION DATA
# MAGIC ##### Creates NEW table: conservation_with_phylop
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 2026
# MAGIC
# MAGIC **Input:**
# MAGIC - silver.conservation_base (CADD + gnomAD)
# MAGIC - default.ucsc_conservation_scores (PhyloP + PhastCons from 13a)
# MAGIC
# MAGIC **Output:**
# MAGIC - silver.conservation_with_phylop (NEW TABLE - combines all scores)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("MERGE UCSC CONSERVATION WITH BASE DATA")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Conservation Base
print("\nLOADING CONSERVATION BASE")
print("="*80)

df_base = spark.table(f"{catalog_name}.silver.conservation_scores")
base_count = df_base.count()

print(f"Conservation base: {base_count:,} variants")

print("\nBase coverage:")
print(f"  CADD: {df_base.filter(col('cadd_phred').isNotNull()).count():,}")
print(f"  gnomAD: {df_base.filter(col('gnomad_af').isNotNull()).count():,}")
print(f"  PhyloP: {df_base.filter(col('phylop_score').isNotNull()).count():,}")

# COMMAND ----------

# DBTITLE 1,Load UCSC Scores
print("\nLOADING UCSC SCORES")
print("="*80)

try:
    df_ucsc = spark.table(f"{catalog_name}.default.ucsc_conservation_scores")
    ucsc_count = df_ucsc.count()
    print(f"UCSC scores: {ucsc_count:,} variants")
    
    print("\nSample UCSC data:")
    df_ucsc.show(3, truncate=False)
    
except Exception as e:
    print(f"ERROR: UCSC table not found!")
    print(f"Please run 13a_download_ucsc_filtered first")
    dbutils.notebook.exit("UCSC data not available")

# COMMAND ----------

# DBTITLE 1,Merge Base with UCSC
print("\nMERGING BASE WITH UCSC DATA")
print("="*80)

df_merged = (
    df_base
    .join(df_ucsc, "variant_id", "left")
    .select(
        df_base.variant_id,
        df_base.chromosome,
        df_base.position,
        df_base.reference_allele,
        df_base.alternate_allele,
        df_base.gene_name,
        
        # MERGE PhyloP: Use UCSC if available, else keep base (likely NULL)
        coalesce(df_ucsc.phylop_score_ucsc, df_base.phylop_score).alias("phylop_score"),
        
        # MERGE PhastCons: Use UCSC if available, else keep base (likely NULL)
        coalesce(df_ucsc.phastcons_score_ucsc, df_base.phastcons_score).alias("phastcons_score"),
        
        # KEEP existing scores from base
        df_base.gerp_score,
        df_base.sift_score,
        df_base.polyphen_score,
        df_base.cadd_phred,
        df_base.gnomad_af,
        df_base.is_highly_conserved,
        df_base.is_constrained,
        df_base.is_likely_deleterious,
        df_base.conservation_level,
        df_base.is_common_variant,
        df_base.is_rare_variant
    )
)

merged_count = df_merged.count()
print(f"Merged variants: {merged_count:,}")

# COMMAND ----------

# DBTITLE 1,Update Conservation Flags
print("\nUPDATING CONSERVATION FLAGS")
print("="*80)

df_enriched = (
    df_merged
    # Update is_highly_conserved: PhyloP > 2.7 OR existing flag
    .withColumn("is_highly_conserved",
                when(col("phylop_score") > 2.7, True)
                .otherwise(col("is_highly_conserved")))
    
    # Update conservation_level: Count conservation evidence
    .withColumn("conservation_level",
                when(col("phylop_score") > 2.7, 1).otherwise(0) +
                when(col("gerp_score") > 4.0, 1).otherwise(0) +
                when(col("cadd_phred") > 20.0, 1).otherwise(0))
)

print("Conservation flags updated")

# COMMAND ----------

# DBTITLE 1,Verify Enrichment
print("\nVERIFYING ENRICHMENT")
print("="*80)

phylop_new = df_enriched.filter(col('phylop_score').isNotNull()).count()
phastcons_new = df_enriched.filter(col('phastcons_score').isNotNull()).count()
cadd_count = df_enriched.filter(col('cadd_phred').isNotNull()).count()
gnomad_count = df_enriched.filter(col('gnomad_af').isNotNull()).count()

print(f"\nFinal coverage:")
print(f"  PhyloP: {phylop_new:,} ({phylop_new/merged_count*100:.1f}%) NEW!")
print(f"  PhastCons: {phastcons_new:,} ({phastcons_new/merged_count*100:.1f}%) NEW!")
print(f"  CADD: {cadd_count:,} ({cadd_count/merged_count*100:.1f}%) KEPT")
print(f"  gnomAD: {gnomad_count:,} ({gnomad_count/merged_count*100:.1f}%) KEPT")

print("\nConservation level distribution:")
df_enriched.groupBy("conservation_level").count().orderBy("conservation_level").show()

# COMMAND ----------

# DBTITLE 1,Save to NEW Table
print("\nSAVING TO NEW TABLE")
print("="*80)

output_table = f"{catalog_name}.silver.conservation_with_phylop"

df_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

print(f"Saved: {output_table}")
print(f"  Total variants: {merged_count:,}")
print(f"  PhyloP enriched: {phylop_new:,}")

# COMMAND ----------

# DBTITLE 1,Summary
print("\nUCSC CONSERVATION MERGE COMPLETE")
print("="*80)

print(f"\nTABLES:")
print(f"  Base (preserved): {catalog_name}.silver.conservation_base")
print(f"  Enriched (NEW): {catalog_name}.silver.conservation_with_phylop")

print(f"\nENRICHMENT STATS:")
print(f"  PhyloP added: {phylop_new:,} variants")
print(f"  PhastCons added: {phastcons_new:,} variants")
print(f"  Coverage: {phylop_new/merged_count*100:.1f}%")

print(f"\nNEXT STEP:")
print(f"  Gold notebooks should use: conservation_with_phylop")

print("\n" + "="*80)
print("SUCCESS!")
print("="*80)
