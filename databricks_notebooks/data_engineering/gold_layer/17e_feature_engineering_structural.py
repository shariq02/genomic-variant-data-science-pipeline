# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - STRUCTURAL VARIANT USE CASE (ENHANCED WITH GENE MAPPING)
# MAGIC ##### Module 5: Structural Variant Impact Analysis with Gene Overlap
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 28, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Structural Variant Impact (CNVs, SVs)
# MAGIC
# MAGIC **Input:** 
# MAGIC - structural_variants: 216,951 SVs
# MAGIC - genes_ultra_enriched: ~60K genes with coordinates (31%)
# MAGIC
# MAGIC **Output:** SV features with gene mappings    
# MAGIC **Creates:** gold.structural_variant_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct, abs as spark_abs,
    length, concat_ws, collect_list, size
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR STRUCTURAL VARIANT FEATURE ENGINEERING - MODULE 5")

# COMMAND ----------

# DBTITLE 1,Check Table Availability
print("\nCHECKING TABLE AVAILABILITY")
print("="*80)

try:
    df_structural = spark.table(f"{catalog_name}.silver.structural_variants")
    has_structural = True
    structural_count = df_structural.count()
    print(f"Structural variants: {structural_count:,}")
except Exception as e:
    has_structural = False
    print("Structural variants: Not available")
    print(f"Error: {str(e)}")
    print("\nThis module requires structural_variants table")
    print("Exiting with informational message")
    dbutils.notebook.exit("SKIPPED: structural_variants table not found")

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"Structural variants: {structural_count:,}")
print(f"Genes: {df_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Basic SV Features
print("\nCREATING BASIC SV FEATURES")
print("="*80)

df_sv = (
    df_structural
    .select(
        col("variant_id").alias("sv_id"),
        "study_id",
        "variant_name",
        "variant_type",
        "chromosome",
        col("start_position").alias("start_pos"),
        col("end_position").alias("end_pos"),
        "assembly"
    )
    
    # Calculate SV size
    .withColumn("sv_size",
                spark_abs(col("end_pos") - col("start_pos")))
    
    # SV type classification
    .withColumn("sv_type_class",
                when(col("variant_type").rlike("(?i)deletion|loss|del"), lit("Deletion"))
                .when(col("variant_type").rlike("(?i)duplication|gain|dup"), lit("Duplication"))
                .when(col("variant_type").rlike("(?i)inversion|inv"), lit("Inversion"))
                .when(col("variant_type").rlike("(?i)translocation|trans"), lit("Translocation"))
                .when(col("variant_type").rlike("(?i)insertion|ins"), lit("Insertion"))
                .when(col("variant_type").rlike("(?i)copy|cnv"), lit("Copy_Number_Variant"))
                .otherwise(lit("Other_SV")))
    
    # Size categories
    .withColumn("sv_size_category",
                when(col("sv_size") < 1000, lit("Small"))
                .when(col("sv_size") < 10000, lit("Medium"))
                .when(col("sv_size") < 100000, lit("Large"))
                .when(col("sv_size") < 1000000, lit("Very_Large"))
                .otherwise(lit("Mega")))
)

print("Basic SV features created")

# COMMAND ----------

# DBTITLE 1,Map SVs to Genes Using Coordinate Overlap
print("\nMAPPING SVs TO GENES (COORDINATE OVERLAP)")
print("="*80)
print("This may take a few minutes for large datasets...")

# Prepare genes with coordinates
df_genes_coord = (
    df_genes
    .select(
        "gene_name",
        "official_symbol",
        "chromosome",
        col("start_position").alias("gene_start"),
        col("end_position").alias("gene_end"),
        "gene_length",
        "is_transporter",
        
        # Gene flags from schema
        #"is_pharmacogene",
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "is_gpcr",
        "mim_id"
    )
    .filter(col("start_position").isNotNull() & col("end_position").isNotNull())
    .withColumn("is_pharmacogene",
            col("is_kinase") | col("is_receptor") | col("is_enzyme") | 
            col("is_gpcr") | col("is_transporter"))
    # Derive gene flags
    .withColumn("is_omim_gene",
                col("mim_id").isNotNull())
)

genes_with_coords_count = df_genes_coord.count()
print(f"Genes with coordinates: {genes_with_coords_count:,}")

# Join SVs with genes on chromosome (reduces search space)
# Then filter for actual overlap
df_sv_gene_overlap = (
    df_sv
    .join(
        df_genes_coord,
        df_sv.chromosome == df_genes_coord.chromosome,
        "inner"
    )
    
    # Filter for actual coordinate overlap
    # SV overlaps gene if: (SV_start <= Gene_end) AND (SV_end >= Gene_start)
    .filter(
        (col("start_pos") <= col("gene_end")) &
        (col("end_pos") >= col("gene_start"))
    )
    
    # Calculate overlap details
    .withColumn("overlap_start",
                when(col("start_pos") > col("gene_start"), col("start_pos"))
                .otherwise(col("gene_start")))
    
    .withColumn("overlap_end",
                when(col("end_pos") < col("gene_end"), col("end_pos"))
                .otherwise(col("gene_end")))
    
    .withColumn("overlap_size",
                col("overlap_end") - col("overlap_start"))
    
    .withColumn("gene_overlap_percentage",
                (col("overlap_size") / col("gene_length")) * 100)
    
    .withColumn("sv_overlap_percentage",
                (col("overlap_size") / col("sv_size")) * 100)
    
    # Overlap classification
    .withColumn("overlap_type",
                when(col("gene_overlap_percentage") >= 90, lit("Complete_Gene_Overlap"))
                .when(col("gene_overlap_percentage") >= 50, lit("Major_Gene_Overlap"))
                .when(col("gene_overlap_percentage") >= 10, lit("Partial_Gene_Overlap"))
                .otherwise(lit("Minor_Gene_Overlap")))
    
    # Functional impact based on gene and SV type
    .withColumn("gene_disruption_type",
                when((col("sv_type_class") == "Deletion") & 
                     (col("gene_overlap_percentage") > 50), lit("Gene_Loss"))
                .when((col("sv_type_class") == "Duplication") & 
                      (col("gene_overlap_percentage") > 50), lit("Gene_Gain"))
                .when(col("sv_type_class") == "Inversion", lit("Gene_Disruption"))
                .otherwise(lit("Uncertain_Effect")))
)

overlap_count = df_sv_gene_overlap.count()
print(f"Found {overlap_count:,} SV-gene overlaps")

# COMMAND ----------

# DBTITLE 1,Aggregate Gene Information per SV
print("\nAGGREGATING GENE INFORMATION PER SV")
print("="*80)

sv_gene_summary = (
    df_sv_gene_overlap
    .groupBy("sv_id")
    .agg(
        count("*").alias("affected_gene_count"),
        collect_list("gene_name").alias("affected_genes_list"),
        
        # Count genes by overlap severity
        spark_sum(when(col("overlap_type") == "Complete_Gene_Overlap", 1).otherwise(0))
            .alias("complete_overlap_genes"),
        spark_sum(when(col("overlap_type") == "Major_Gene_Overlap", 1).otherwise(0))
            .alias("major_overlap_genes"),
        
        # Count functional gene types affected
        spark_sum(when(col("is_pharmacogene"), 1).otherwise(0)).alias("pharmacogenes_affected"),
        spark_sum(when(col("is_kinase"), 1).otherwise(0)).alias("kinases_affected"),
        spark_sum(when(col("is_receptor"), 1).otherwise(0)).alias("receptors_affected"),
        spark_sum(when(col("is_omim_gene"), 1).otherwise(0)).alias("omim_genes_affected"),
        
        # Count by disruption type
        spark_sum(when(col("gene_disruption_type") == "Gene_Loss", 1).otherwise(0))
            .alias("genes_lost"),
        spark_sum(when(col("gene_disruption_type") == "Gene_Gain", 1).otherwise(0))
            .alias("genes_gained"),
        
        # Overlap statistics
        avg("gene_overlap_percentage").alias("avg_gene_overlap_pct"),
        spark_max("gene_overlap_percentage").alias("max_gene_overlap_pct")
    )
    .withColumn("affected_genes",
                concat_ws(",", col("affected_genes_list")))
    .drop("affected_genes_list")
)

print("Gene aggregations complete")

# COMMAND ----------

# DBTITLE 1,Merge Gene Summary with SV Data
print("\nMERGING GENE SUMMARY WITH SV DATA")
print("="*80)

df_sv = (
    df_sv
    .join(sv_gene_summary, "sv_id", "left")
    
    # Fill nulls for SVs with no gene overlap
    .fillna({
        "affected_gene_count": 0,
        "complete_overlap_genes": 0,
        "major_overlap_genes": 0,
        "pharmacogenes_affected": 0,
        "kinases_affected": 0,
        "receptors_affected": 0,
        "omim_genes_affected": 0,
        "genes_lost": 0,
        "genes_gained": 0,
        "avg_gene_overlap_pct": 0,
        "max_gene_overlap_pct": 0
    })
    
    # Gene overlap flags
    .withColumn("has_gene_overlap",
                col("affected_gene_count") > 0)
    
    .withColumn("is_multi_gene_sv",
                col("affected_gene_count") > 1)
    
    .withColumn("affects_pharmacogenes",
                col("pharmacogenes_affected") > 0)
    
    .withColumn("affects_omim_genes",
                col("omim_genes_affected") > 0)
)

print("Merge complete")

# COMMAND ----------

# DBTITLE 1,Calculate SV Impact Scores
print("\nCALCULATING IMPACT SCORES")
print("="*80)

df_sv = (
    df_sv
    
    # Gene impact severity classification
    .withColumn("gene_impact_severity",
                when((col("complete_overlap_genes") >= 3) | 
                     (col("pharmacogenes_affected") >= 2), lit("Critical_Multi_Gene"))
                .when((col("complete_overlap_genes") >= 1) & 
                      (col("pharmacogenes_affected") >= 1), lit("Critical_Single_Gene"))
                .when(col("affected_gene_count") >= 5, lit("High_Multi_Gene"))
                .when((col("affected_gene_count") >= 1) & 
                      (col("omim_genes_affected") >= 1), lit("High_Single_Gene"))
                .when(col("affected_gene_count") >= 1, lit("Moderate"))
                .otherwise(lit("Low")))
    
    # Size impact score (0-5)
    .withColumn("size_impact_score",
                when(col("sv_size") >= 1000000, 5)
                .when(col("sv_size") >= 100000, 4)
                .when(col("sv_size") >= 10000, 3)
                .when(col("sv_size") >= 1000, 2)
                .otherwise(1))
    
    # Type impact score (0-4)
    .withColumn("type_impact_score",
                when(col("sv_type_class").isin(["Deletion", "Translocation"]), 4)
                .when(col("sv_type_class") == "Duplication", 3)
                .when(col("sv_type_class") == "Inversion", 2)
                .when(col("sv_type_class") == "Insertion", 1)
                .otherwise(0))
    
    # Gene impact score (0-6)
    .withColumn("gene_impact_score",
                when(col("gene_impact_severity") == "Critical_Multi_Gene", 6)
                .when(col("gene_impact_severity") == "Critical_Single_Gene", 5)
                .when(col("gene_impact_severity") == "High_Multi_Gene", 4)
                .when(col("gene_impact_severity") == "High_Single_Gene", 3)
                .when(col("gene_impact_severity") == "Moderate", 2)
                .when(col("gene_impact_severity") == "Low", 1)
                .otherwise(0))
    
    # Combined SV pathogenicity score (0-15)
    .withColumn("sv_pathogenicity_score",
                col("size_impact_score") +
                col("type_impact_score") +
                col("gene_impact_score"))
    
    # Predicted pathogenicity
    .withColumn("predicted_sv_pathogenicity",
                when(col("sv_pathogenicity_score") >= 12, lit("Likely_Pathogenic"))
                .when(col("sv_pathogenicity_score") >= 8, lit("Possibly_Pathogenic"))
                .when(col("sv_pathogenicity_score") >= 4, lit("Uncertain"))
                .otherwise(lit("Likely_Benign")))
    
    # High-risk SV flag
    .withColumn("is_high_risk_sv",
                (col("sv_pathogenicity_score") >= 10) |
                (col("gene_impact_severity").isin(["Critical_Multi_Gene", "Critical_Single_Gene"])) |
                (col("affects_pharmacogenes") & (col("pharmacogenes_affected") >= 2)))
    
    # Chromosome features
    .withColumn("is_autosomal",
                ~col("chromosome").isin("X", "Y", "MT"))
    
    .withColumn("chromosome_impact_modifier",
                when(col("chromosome") == "X", lit("X_Linked"))
                .when(col("chromosome") == "Y", lit("Y_Linked"))
                .when(col("chromosome") == "MT", lit("Mitochondrial"))
                .otherwise(lit("Autosomal")))
)

print("SV impact scores calculated")

# COMMAND ----------

# DBTITLE 1,Calculate Chromosome and Study Statistics
print("\nCALCULATING CHROMOSOME AND STUDY STATISTICS")
print("="*80)

# Chromosome-level stats
chromosome_stats = (
    df_sv
    .groupBy("chromosome")
    .agg(
        count("*").alias("chr_total_svs"),
        spark_sum(when(col("has_gene_overlap"), 1).otherwise(0)).alias("chr_gene_affecting_svs"),
        spark_sum(when(col("is_high_risk_sv"), 1).otherwise(0)).alias("chr_high_risk_svs"),
        avg("sv_size").alias("chr_avg_sv_size"),
        avg("affected_gene_count").alias("chr_avg_genes_per_sv")
    )
    .withColumn("chr_gene_disruption_rate",
                col("chr_gene_affecting_svs") / col("chr_total_svs"))
)

# Study-level stats
study_stats = (
    df_sv
    .groupBy("study_id")
    .agg(
        count("*").alias("study_total_svs"),
        countDistinct("chromosome").alias("study_chr_count"),
        spark_sum(when(col("has_gene_overlap"), 1).otherwise(0)).alias("study_gene_affecting_svs")
    )
    .withColumn("study_quality",
                when((col("study_total_svs") >= 100) & (col("study_chr_count") >= 10),
                     lit("High_Quality"))
                .when(col("study_total_svs") >= 50, lit("Moderate_Quality"))
                .otherwise(lit("Limited_Quality")))
)

# Join back
df_sv = (
    df_sv
    .join(chromosome_stats, "chromosome", "left")
    .join(study_stats, "study_id", "left")
)

print("Statistics calculated")

# COMMAND ----------

# DBTITLE 1,Create Final Features Table
print("\nCREATING FINAL STRUCTURAL VARIANT ML FEATURES")
print("="*80)

structural_features = df_sv.select(
    # IDs
    "sv_id", "study_id", "variant_name", "chromosome",
    "start_pos", "end_pos", "assembly",
    
    # SV characteristics
    "variant_type",
    "sv_type_class",
    "sv_size",
    "sv_size_category",
    
    # Gene overlap information
    "has_gene_overlap",
    "affected_gene_count",
    "affected_genes",
    "complete_overlap_genes",
    "major_overlap_genes",
    "is_multi_gene_sv",
    
    # Functional genes affected
    "pharmacogenes_affected",
    "kinases_affected",
    "receptors_affected",
    "omim_genes_affected",
    "affects_pharmacogenes",
    "affects_omim_genes",
    
    # Gene disruption
    "genes_lost",
    "genes_gained",
    "avg_gene_overlap_pct",
    "max_gene_overlap_pct",
    
    # Impact classification
    "gene_impact_severity",
    
    # Impact scores
    "size_impact_score",
    "type_impact_score",
    "gene_impact_score",
    "sv_pathogenicity_score",
    "predicted_sv_pathogenicity",
    "is_high_risk_sv",
    
    # Chromosome features
    "is_autosomal",
    "chromosome_impact_modifier",
    
    # Chromosome-level statistics
    "chr_total_svs",
    "chr_gene_affecting_svs",
    "chr_high_risk_svs",
    "chr_avg_sv_size",
    "chr_avg_genes_per_sv",
    "chr_gene_disruption_rate",
    
    # Study-level statistics
    "study_total_svs",
    "study_chr_count",
    "study_gene_affecting_svs",
    "study_quality"
)

feature_count = structural_features.count()
print(f"Structural variant ML features: {feature_count:,} SVs")

# COMMAND ----------

# DBTITLE 1,Deduplicate by sv_id
print("\nDEDUPLICATING BY SV_ID")
print("="*80)

before_count = structural_features.count()
structural_features = structural_features.dropDuplicates(["sv_id"])
after_count = structural_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
structural_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.structural_variant_ml_features")

print(f"Saved: {catalog_name}.gold.structural_variant_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nGene overlap statistics:")
structural_features.select(
    spark_sum(when(col("has_gene_overlap"), 1).otherwise(0)).alias("svs_with_genes"),
    spark_sum(when(col("is_multi_gene_sv"), 1).otherwise(0)).alias("multi_gene_svs"),
    avg("affected_gene_count").alias("avg_genes_per_sv")
).show()

print("\nGene impact severity:")
structural_features.groupBy("gene_impact_severity").count().orderBy("count", ascending=False).show()

print("\nPredicted pathogenicity:")
structural_features.groupBy("predicted_sv_pathogenicity").count().orderBy("count", ascending=False).show()

print("\nHigh-risk SVs:")
structural_features.select(
    spark_sum(when(col("is_high_risk_sv"), 1).otherwise(0)).alias("high_risk_count")
).show()

print("\nFunctional genes affected:")
structural_features.select(
    spark_sum(when(col("affects_pharmacogenes"), 1).otherwise(0)).alias("affects_pharma"),
    spark_sum(when(col("affects_omim_genes"), 1).otherwise(0)).alias("affects_omim")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("STRUCTURAL VARIANT FEATURE ENGINEERING COMPLETE (ENHANCED)")
print("="*80)

print(f"\nTotal features created: {feature_count:,} SVs")

print("\nENHANCEMENTS in this version:")
print("  - Gene mapping via coordinate overlap")
print("  - Affected gene counting and classification")
print("  - Gene disruption type (loss/gain/disruption)")
print("  - Functional gene impact (pharmacogenes, OMIM genes)")
print("  - Multi-gene SV identification")
print("  - Comprehensive impact scoring (0-15 scale)")

print("\nKey Features:")
print("  - 50+ features total")
print("  - Gene-aware impact scoring")
print("  - Pharmacogene disruption flags")
print("  - OMIM gene disruption flags")
print("  - Chromosome-level disruption rates")

print("\nTable created:")
print(f"  {catalog_name}.gold.structural_variant_ml_features")
