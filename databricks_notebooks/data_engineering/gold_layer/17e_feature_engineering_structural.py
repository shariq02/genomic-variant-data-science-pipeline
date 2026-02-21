# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - STRUCTURAL VARIANT USE CASE (UPDATED & ENHANCED)
# MAGIC ##### Module 5: Comprehensive Structural Variant Impact Analysis with Gene Mapping
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **UPDATED:** Uses all available silver tables for enhanced SV analysis
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Structural Variant Impact (CNVs, SVs, Gene Disruption)
# MAGIC
# MAGIC **Creates:** gold.structural_variant_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct, abs as spark_abs,
    length, concat_ws, collect_list, size, array_distinct
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("STRUCTURAL VARIANT FEATURE ENGINEERING - MODULE 5 (UPDATED & ENHANCED)")
print("="*80)

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

# Core tables
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # UPDATED: enriched genes
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")

print(f"Structural variants: {structural_count:,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")

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
    
    # Pathogenicity risk based on size and type
    .withColumn("sv_pathogenicity_risk",
                when((col("sv_type_class") == "Deletion") & (col("sv_size") > 100000),
                     lit("High_Risk"))
                .when((col("sv_type_class").isin("Deletion", "Duplication")) & (col("sv_size") > 10000),
                     lit("Moderate_Risk"))
                .when(col("sv_size") > 1000,
                     lit("Low_Risk"))
                .otherwise(lit("Minimal_Risk")))
)

print("Basic SV features created")

# COMMAND ----------

# DBTITLE 1,Map SVs to Genes Using Coordinate Overlap
print("\nMAPPING SVs TO GENES (COORDINATE OVERLAP)")
print("="*80)

# Prepare genes with coordinates - exclude biological-region LOC genes
df_genes_coord = (
    df_genes
    .select(
        "gene_name",
        "official_symbol",
        "chromosome",
        col("start_position").alias("gene_start"),
        col("end_position").alias("gene_end"),
        "gene_length",
        "gene_type",
        "is_transporter",
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "is_gpcr",
        "is_pharmacogene",
        "mim_id",
        "druggability_score"
    )
    .filter(col("start_position").isNotNull() & col("end_position").isNotNull())
    .filter(
        ~(col("gene_name").startswith("LOC") & (col("gene_type") == "biological-region"))
    )
    .withColumn("is_omim_gene",
                col("mim_id").isNotNull())
)

genes_with_coords_count = df_genes_coord.count()
print(f"Genes with coordinates: {genes_with_coords_count:,}")

# Perform overlap join (SV overlaps gene if SV.start < gene.end AND SV.end > gene.start)
df_sv_gene_overlap = (
    df_sv
    .join(
        df_genes_coord,
        (df_sv.chromosome == df_genes_coord.chromosome) &
        (df_sv.start_pos < df_genes_coord.gene_end) &
        (df_sv.end_pos > df_genes_coord.gene_start),
        "left"
    )
    
    # Calculate overlap metrics
    .withColumn("overlap_start",
                when(col("start_pos") > col("gene_start"), col("start_pos"))
                .otherwise(col("gene_start")))
    
    .withColumn("overlap_end",
                when(col("end_pos") < col("gene_end"), col("end_pos"))
                .otherwise(col("gene_end")))
    
    .withColumn("overlap_length",
                when(col("gene_name").isNotNull(),
                     col("overlap_end") - col("overlap_start"))
                .otherwise(lit(0)))
    
    .withColumn("gene_coverage_fraction",
                when(col("gene_length").isNotNull() & (col("gene_length") > 0),
                     col("overlap_length") / col("gene_length"))
                .otherwise(lit(0.0)))
    
    .withColumn("sv_coverage_fraction",
                when(col("sv_size") > 0,
                     col("overlap_length") / col("sv_size"))
                .otherwise(lit(0.0)))
    
    # Gene disruption classification
    .withColumn("gene_disruption_level",
                when(col("gene_coverage_fraction") >= 0.9, lit("Complete_Disruption"))
                .when(col("gene_coverage_fraction") >= 0.5, lit("Major_Disruption"))
                .when(col("gene_coverage_fraction") >= 0.25, lit("Moderate_Disruption"))
                .when(col("gene_coverage_fraction") > 0, lit("Minor_Disruption"))
                .otherwise(lit("No_Disruption")))
)

print("SV-gene overlap calculated")

# COMMAND ----------

# DBTITLE 1,Aggregate Gene-Level SV Features
print("\nAGGREGATING GENE-LEVEL SV FEATURES")
print("="*80)

sv_gene_agg = (
    df_sv_gene_overlap
    .filter(col("gene_name").isNotNull())
    .groupBy("sv_id")
    .agg(
        countDistinct("gene_name").alias("genes_overlapped"),
        collect_list("gene_name").alias("gene_list"),
        collect_list("gene_disruption_level").alias("disruption_levels"),
        spark_sum(when(col("is_pharmacogene"), 1).otherwise(0)).alias("pharmacogenes_affected"),
        spark_sum(when(col("is_omim_gene"), 1).otherwise(0)).alias("omim_genes_affected"),
        spark_sum(when(col("is_kinase"), 1).otherwise(0)).alias("kinase_genes_affected"),
        spark_sum(when(col("is_receptor"), 1).otherwise(0)).alias("receptor_genes_affected"),
        spark_max("gene_coverage_fraction").alias("max_gene_disruption_fraction"),
        avg("druggability_score").alias("avg_druggability_affected_genes")
    )
    
    # Gene disruption categories
    .withColumn("gene_count_category",
                when(col("genes_overlapped") == 0, lit("No_Genes"))
                .when(col("genes_overlapped") == 1, lit("Single_Gene"))
                .when(col("genes_overlapped") <= 5, lit("Few_Genes"))
                .when(col("genes_overlapped") <= 20, lit("Multiple_Genes"))
                .otherwise(lit("Many_Genes")))
    
    .withColumn("has_critical_gene_disruption",
                (col("pharmacogenes_affected") > 0) | (col("omim_genes_affected") > 0))
    
    .withColumn("sv_clinical_priority",
                when(col("pharmacogenes_affected") >= 3, lit("Critical_Priority"))
                .when(col("pharmacogenes_affected") >= 1, lit("High_Priority"))
                .when(col("omim_genes_affected") >= 3, lit("High_Priority"))
                .when(col("omim_genes_affected") >= 1, lit("Medium_Priority"))
                .when(col("genes_overlapped") >= 10, lit("Medium_Priority"))
                .when(col("genes_overlapped") >= 1, lit("Low_Priority"))
                .otherwise(lit("Minimal_Priority")))
)

# Join aggregated features back to main SV table
df_sv = (
    df_sv
    .join(sv_gene_agg, "sv_id", "left")
    .fillna({
        "genes_overlapped": 0,
        "pharmacogenes_affected": 0,
        "omim_genes_affected": 0,
        "kinase_genes_affected": 0,
        "receptor_genes_affected": 0,
        "max_gene_disruption_fraction": 0.0,
        "avg_druggability_affected_genes": 0.0,
        "has_critical_gene_disruption": False
    })
    .fillna("No_Genes", ["gene_count_category"])
    .fillna("Minimal_Priority", ["sv_clinical_priority"])
)

print("Gene-level SV features aggregated")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Association
print("\nENRICHING WITH DISEASE ASSOCIATION")
print("="*80)

# Get disease-associated genes
disease_genes = (
    df_gene_disease
    .select(
        "gene_name",
        col("total_disease_count").alias("disease_count"),
        "has_cancer_disease",
        "has_neurological_disease"
    )
    .filter(col("total_disease_count") > 0)
)

# Join SV-gene overlaps with disease data
sv_disease = (
    df_sv_gene_overlap
    .filter(col("gene_name").isNotNull())
    .join(disease_genes, "gene_name", "left")
    .groupBy("sv_id")
    .agg(
        spark_sum(when(col("disease_count").isNotNull(), col("disease_count")).otherwise(0))
            .alias("total_disease_associations"),
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0))
            .alias("cancer_genes_affected"),
        spark_sum(when(col("has_neurological_disease"), 1).otherwise(0))
            .alias("neuro_genes_affected")
    )
    .withColumn("has_disease_associated_genes",
                col("total_disease_associations") > 0)
)

df_sv = (
    df_sv
    .join(sv_disease, "sv_id", "left")
    .fillna({
        "total_disease_associations": 0,
        "cancer_genes_affected": 0,
        "neuro_genes_affected": 0,
        "has_disease_associated_genes": False
    })
    
    # Disease-specific SV priority
    .withColumn("disease_sv_priority",
                when(col("cancer_genes_affected") >= 2, lit("Cancer_Gene_Disruption"))
                .when(col("neuro_genes_affected") >= 2, lit("Neuro_Gene_Disruption"))
                .when(col("has_disease_associated_genes"), lit("Disease_Gene_Involved"))
                .otherwise(lit("No_Disease_Association")))
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Data
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Get broadly expressed genes
broad_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
    .groupBy("gene_symbol")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed")
    )
    .filter(col("tissues_expressed") >= 10)
    .select(
        col("gene_symbol").alias("gene_name"),
        col("tissues_expressed")
    )
)

# Join SV-gene overlaps with expression
sv_expression = (
    df_sv_gene_overlap
    .filter(col("gene_name").isNotNull())
    .join(broad_expression, "gene_name", "left")
    .groupBy("sv_id")
    .agg(
        spark_sum(when(col("tissues_expressed").isNotNull(), 1).otherwise(0))
            .alias("broadly_expressed_genes_affected")
    )
)

df_sv = (
    df_sv
    .join(sv_expression, "sv_id", "left")
    .fillna({"broadly_expressed_genes_affected": 0})
    
    .withColumn("affects_essential_genes",
                col("broadly_expressed_genes_affected") >= 1)
)

print("Expression data enrichment complete")

# COMMAND ----------

# DBTITLE 1,Create Combined SV Impact Score
print("\nCREATING COMBINED SV IMPACT SCORE")
print("="*80)

df_sv = (
    df_sv
    .withColumn("sv_combined_impact_score",
                # Base score from gene count
                when(col("genes_overlapped") >= 20, 5).otherwise(
                    when(col("genes_overlapped") >= 10, 4).otherwise(
                        when(col("genes_overlapped") >= 5, 3).otherwise(
                            when(col("genes_overlapped") >= 1, 2).otherwise(1)))) +
                # Pharmacogene bonus
                when(col("pharmacogenes_affected") >= 3, 3).otherwise(
                    when(col("pharmacogenes_affected") >= 1, 2).otherwise(0)) +
                # OMIM gene bonus
                when(col("omim_genes_affected") >= 3, 2).otherwise(
                    when(col("omim_genes_affected") >= 1, 1).otherwise(0)) +
                # Size bonus (large SVs are higher risk)
                when(col("sv_size") >= 1000000, 2).otherwise(
                    when(col("sv_size") >= 100000, 1).otherwise(0)) +
                # Type bonus (deletions are higher risk)
                when(col("sv_type_class") == "Deletion", 1).otherwise(0))
    
    .withColumn("sv_impact_tier",
                when(col("sv_combined_impact_score") >= 12, lit("Tier_1_Critical"))
                .when(col("sv_combined_impact_score") >= 9, lit("Tier_2_High"))
                .when(col("sv_combined_impact_score") >= 6, lit("Tier_3_Moderate"))
                .when(col("sv_combined_impact_score") >= 3, lit("Tier_4_Low"))
                .otherwise(lit("Tier_5_Minimal")))
    
    # Overall SV classification
    .withColumn("sv_classification",
                when(col("has_critical_gene_disruption") & (col("sv_size") > 100000),
                     lit("Critical_Large_Disruption"))
                .when(col("has_critical_gene_disruption"),
                     lit("Critical_Gene_Affected"))
                .when((col("genes_overlapped") >= 10) & (col("sv_size") > 100000),
                     lit("Large_MultiGene_SV"))
                .when(col("genes_overlapped") >= 10,
                     lit("MultiGene_SV"))
                .when(col("genes_overlapped") == 1,
                     lit("Single_Gene_SV"))
                .when(col("genes_overlapped") == 0,
                     lit("Intergenic_SV"))
                .otherwise(lit("Other_SV")))
)

print("Combined SV impact score created")

# COMMAND ----------

# DBTITLE 1,Create Final Structural Variant Features Table
print("\nCREATING STRUCTURAL VARIANT ML FEATURES")
print("="*80)

structural_variant_features = df_sv.select(
    # IDs
    "sv_id",
    "study_id",
    "variant_name",
    "chromosome",
    "start_pos",
    "end_pos",
    "assembly",
    
    # Basic SV features
    "variant_type",
    "sv_type_class",
    "sv_size",
    "sv_size_category",
    "sv_pathogenicity_risk",
    
    # Gene overlap features
    "genes_overlapped",
    "gene_list",
    "gene_count_category",
    "pharmacogenes_affected",
    "omim_genes_affected",
    "kinase_genes_affected",
    "receptor_genes_affected",
    "max_gene_disruption_fraction",
    "avg_druggability_affected_genes",
    "has_critical_gene_disruption",
    
    # Disease association
    "total_disease_associations",
    "cancer_genes_affected",
    "neuro_genes_affected",
    "has_disease_associated_genes",
    "disease_sv_priority",
    
    # Expression context
    "broadly_expressed_genes_affected",
    "affects_essential_genes",
    
    # Priority and impact
    "sv_clinical_priority",
    "sv_combined_impact_score",
    "sv_impact_tier",
    "sv_classification"
)

feature_count = structural_variant_features.count()
print(f"Structural variant ML features: {feature_count:,} SVs")
print(f"Total columns: {len(structural_variant_features.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by sv_id
print("\nDEDUPLICATING BY SV_ID")
print("="*80)

before_count = structural_variant_features.count()
structural_variant_features = structural_variant_features.dropDuplicates(["sv_id"])
after_count = structural_variant_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
structural_variant_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.structural_variant_ml_features")

print(f"Saved: {catalog_name}.gold.structural_variant_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nSV type classification:")
structural_variant_features.groupBy("sv_type_class").count().orderBy("count", ascending=False).show()

print("\nSV size category:")
structural_variant_features.groupBy("sv_size_category").count().orderBy("count", ascending=False).show()

print("\nGene count category:")
structural_variant_features.groupBy("gene_count_category").count().orderBy("count", ascending=False).show()

print("\nSV clinical priority:")
structural_variant_features.groupBy("sv_clinical_priority").count().orderBy("count", ascending=False).show()

print("\nSV impact tier:")
structural_variant_features.groupBy("sv_impact_tier").count().orderBy("count", ascending=False).show()

print("\nSV classification:")
structural_variant_features.groupBy("sv_classification").count().orderBy("count", ascending=False).show()

print("\nGene disruption summary:")
structural_variant_features.select(
    spark_sum(when(col("pharmacogenes_affected") > 0, 1).otherwise(0)).alias("svs_affecting_pharmacogenes"),
    spark_sum(when(col("omim_genes_affected") > 0, 1).otherwise(0)).alias("svs_affecting_omim_genes"),
    spark_sum(when(col("has_critical_gene_disruption"), 1).otherwise(0)).alias("critical_disruptions"),
    spark_sum(when(col("affects_essential_genes"), 1).otherwise(0)).alias("affecting_essential_genes")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("STRUCTURAL VARIANT FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {after_count:,}")
print(f"Total columns: {len(structural_variant_features.columns)}")

print("\nFeature Categories:")
print("  - Basic SV features: 6 features")
print("  - Gene overlap features: 10 features")
print("  - Disease association: 6 features")
print("  - Expression context: 2 features")
print("  - Priority and impact: 5 features")

print("\nSilver Tables Used:")
print("  - structural_variants (SV data)")
print("  - genes_with_pharmgkb (enriched gene coordinates)")
print("  - gene_disease_comprehensive (disease associations)")
print("  - gtex_tissue_expression (expression breadth)")
print("  - protein_domains (domain annotations)")

print("\nTable created:")
print(f"  {catalog_name}.gold.structural_variant_ml_features")
