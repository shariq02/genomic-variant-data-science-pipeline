# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD PHARMGKB DATA AND UPDATE PHARMACOGENE FLAGS
# MAGIC ##### Merges ~6,000-8,000 pharmacogenes from all sources
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 24, 2026
# MAGIC
# MAGIC **Input:** all_pharmacogenes_combined.csv (uploaded to DBFS)
# MAGIC **Output:** Updated silver.genes_ultra_enriched with comprehensive pharmacogene flags
# MAGIC
# MAGIC **Sources:**
# MAGIC - PharmGKB (~700 genes)
# MAGIC - CPIC (~20 genes)
# MAGIC - FDA (~90 genes)
# MAGIC - DrugBank Inferred (~5,000+ genes)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, upper, trim, coalesce, split, size, array_contains, concat
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("PHARMGKB DATA INTEGRATION")

# COMMAND ----------

# DBTITLE 1,Configuration
PHARMGKB_TABLE = f"{catalog_name}.default.all_pharmacogenes_combined"
print(f"Reading from table: {PHARMGKB_TABLE}")

# COMMAND ----------

# DBTITLE 1,Load PharmGKB Data
print("\nLOADING PHARMGKB DATA FROM TABLE")
print("="*80)

df_pharmgkb = spark.table(PHARMGKB_TABLE)

pharmgkb_count = df_pharmgkb.count()
print(f"Loaded pharmacogenes: {pharmgkb_count:,}")

# Show schema
print("\nSchema:")
df_pharmgkb.printSchema()

# Show sample
print("\nSample data:")
df_pharmgkb.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Source Breakdown
print("\nSOURCE BREAKDOWN")
print("="*80)

# Count by source
df_pharmgkb.groupBy("source").count().orderBy("count", ascending=False).show(20, truncate=False)

print("\nSource count distribution:")
df_pharmgkb.groupBy("source_count").count().orderBy("source_count").show()

# COMMAND ----------

# DBTITLE 1,Load Existing Genes
print("\nLOADING EXISTING GENES")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
genes_count = df_genes.count()

print(f"Total genes: {genes_count:,}")

# COMMAND ----------

# DBTITLE 1,Merge PharmGKB with Genes
print("\nMERGING PHARMGKB DATA WITH GENES")
print("="*80)

# Prepare PharmGKB data for join
df_pharmgkb_clean = (
    df_pharmgkb
    .withColumn("gene_symbol_upper", upper(trim(col("gene_symbol"))))
    .select(
        col("gene_symbol_upper").alias("pharmgkb_symbol"),
        col("source").alias("pharmgkb_sources"),
        col("evidence").alias("pharmgkb_evidence"),
        col("source_count").alias("pharmgkb_source_count")
    )
)

# Join with genes
df_genes_merged = (
    df_genes
    .join(df_pharmgkb_clean,
          upper(trim(col("official_symbol"))) == col("pharmgkb_symbol"),
          "left")
    .drop("pharmgkb_symbol")
)

# Check merge
matched_count = df_genes_merged.filter(col("pharmgkb_sources").isNotNull()).count()
print(f"Genes matched with PharmGKB: {matched_count:,} ({matched_count/genes_count*100:.2f}%)")

# COMMAND ----------

# DBTITLE 1,Update Pharmacogene Flags
print("\nUPDATING PHARMACOGENE FLAGS")
print("="*80)

df_genes_updated = (
    df_genes_merged
    # Flag as pharmacogene if in PharmGKB OR already flagged
    .withColumn("is_pharmacogene_new",
                col("pharmgkb_sources").isNotNull() |
                coalesce(col("is_pharmacogene"), lit(False)))
    
    # Determine category with priority to PharmGKB evidence
    .withColumn("pharmacogene_category_new",
                when(col("pharmgkb_evidence").contains("clinical_guideline"), 
                     lit("Clinical_Guideline"))
                .when(col("pharmgkb_evidence").contains("drug_label"),
                     lit("FDA_Label"))
                .when(col("pharmgkb_evidence").contains("pharmacogene_database"),
                     lit("PharmGKB_Validated"))
                .when(col("pharmgkb_evidence").contains("drug_target_family"),
                     lit("Drug_Target_Family"))
                # Fall back to existing category
                .otherwise(col("pharmacogene_category")))
    
    # Evidence level based on source count
    .withColumn("pharmacogene_evidence_level",
                when(col("pharmgkb_source_count") >= 3, lit("High"))
                .when(col("pharmgkb_source_count") == 2, lit("Medium"))
                .when(col("pharmgkb_source_count") == 1, lit("Low"))
                .when(col("is_pharmacogene"), lit("Internal_Only"))
                .otherwise(lit(None)))
    
    # Update role description
    .withColumn("drug_metabolism_role_new",
                when(col("pharmgkb_sources").isNotNull(),
                     concat(
                         lit("Pharmacogene ("),
                         col("pharmgkb_sources"),
                         lit("): "),
                         coalesce(col("drug_metabolism_role"), lit("Drug-related gene"))
                     ))
                .otherwise(col("drug_metabolism_role")))
    
    # Replace old columns with new
    .drop("is_pharmacogene", "pharmacogene_category", "drug_metabolism_role")
    .withColumnRenamed("is_pharmacogene_new", "is_pharmacogene")
    .withColumnRenamed("pharmacogene_category_new", "pharmacogene_category")
    .withColumnRenamed("drug_metabolism_role_new", "drug_metabolism_role")
)

# Count pharmacogenes
pharmacogene_count = df_genes_updated.filter(col("is_pharmacogene")).count()
print(f"\nTotal pharmacogenes after merge: {pharmacogene_count:,}")
print(f"Percentage of all genes: {pharmacogene_count/genes_count*100:.2f}%")

# COMMAND ----------

# DBTITLE 1,Breakdown by Category
print("\nPHARMACOGENE CATEGORY BREAKDOWN")
print("="*80)

df_genes_updated.filter(col("is_pharmacogene")) \
    .groupBy("pharmacogene_category") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Evidence Level Breakdown
print("\nEVIDENCE LEVEL BREAKDOWN")
print("="*80)

df_genes_updated.filter(col("is_pharmacogene")) \
    .groupBy("pharmacogene_evidence_level") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------

# DBTITLE 1,Sample High-Confidence Pharmacogenes
print("\nSAMPLE HIGH-CONFIDENCE PHARMACOGENES")
print("="*80)

df_genes_updated.filter(
    (col("is_pharmacogene")) &
    (col("pharmacogene_evidence_level") == "High")
).select(
    "gene_name",
    "official_symbol",
    "pharmacogene_category",
    "pharmgkb_sources",
    "pharmgkb_source_count"
).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Updated Genes
print("\nSAVING UPDATED GENES")
print("="*80)

df_genes_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"Saved: {catalog_name}.silver.genes_ultra_enriched")

# COMMAND ----------

# DBTITLE 1,Save PharmGKB Reference Table
print("\nSAVING PHARMGKB REFERENCE TABLE")
print("="*80)

df_pharmgkb.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.pharmgkb_genes")

print(f" Saved: {catalog_name}.silver.pharmgkb_genes")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("PHARMGKB INTEGRATION COMPLETE")
print("="*80)

print(f"  Total genes: {genes_count:,}")
print(f"  Pharmacogenes: {pharmacogene_count:,}")
print(f"  Coverage: {pharmacogene_count/genes_count*100:.2f}%")
print(f"  Improvement: {pharmacogene_count - 328:,} additional pharmacogenes")

print(f"\nSOURCE INTEGRATION:")
print(f"  PharmGKB genes loaded: {pharmgkb_count:,}")
print(f"  Matched to our genes: {matched_count:,}")
print(f"  Match rate: {matched_count/pharmgkb_count*100:.1f}%")

print(f"\nEVIDENCE LEVELS:")
evidence_stats = df_genes_updated.filter(col("is_pharmacogene")) \
    .groupBy("pharmacogene_evidence_level") \
    .count() \
    .collect()

for row in evidence_stats:
    level = row["pharmacogene_evidence_level"]
    count = row["count"]
    print(f"  {level}: {count:,}")

print(f"\nTABLES UPDATED:")
print(f"  1. {catalog_name}.silver.genes_ultra_enriched (updated with pharmacogene flags)")
print(f"  2. {catalog_name}.silver.pharmgkb_genes (new reference table)")
