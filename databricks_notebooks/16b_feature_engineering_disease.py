# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - DISEASE USE CASES
# MAGIC ##### Module 2: Disease Association, Polygenic Risk, Gene Prioritization
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 27, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Disease Association Discovery (Gene-Disease links)
# MAGIC - Use Case 5: Multi-Gene Disease Risk (Polygenic scores)
# MAGIC - Use Case 6: Gene Prioritization (Clinical utility ranking)
# MAGIC
# MAGIC **Creates:** gold.disease_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct, collect_list,
    concat_ws, size, array_distinct, length
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR DISEASE FEATURE ENGINEERING - MODULE 2")

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_links")

# Load reference tables for disease enrichment
df_omim_lookup = spark.table(f"{catalog_name}.reference.omim_disease_lookup")
df_mondo_lookup = spark.table(f"{catalog_name}.reference.mondo_disease_lookup")
df_orphanet_lookup = spark.table(f"{catalog_name}.reference.orphanet_disease_lookup")

print(f"Variants: {df_variants.count():,}")
print(f"Genes: {df_genes.count():,}")
print(f"Gene-disease links: {df_gene_disease.count():,}")
print(f"OMIM lookup: {df_omim_lookup.count():,}")
print(f"MONDO lookup: {df_mondo_lookup.count():,}")
print(f"Orphanet lookup: {df_orphanet_lookup.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich Disease Information from Reference Tables
print("\nENRICHING DISEASE INFORMATION")
print("="*80)

# Enrich variants with disease names from reference tables
df_variants_enriched = (
    df_variants
    
    # Add OMIM disease names
    .join(
        df_omim_lookup.select(
            col("omim_id").alias("omim_id_lookup"),
            col("disease_name").alias("omim_disease_name")
        ),
        df_variants.omim_id == col("omim_id_lookup"),
        "left"
    )
    .drop("omim_id_lookup")
    
    # Add MONDO disease names
    .join(
        df_mondo_lookup.select(
            col("mondo_id").alias("mondo_id_lookup"),
            col("disease_name").alias("mondo_disease_name")
        ),
        df_variants.mondo_id == col("mondo_id_lookup"),
        "left"
    )
    .drop("mondo_id_lookup")
    
    # Add Orphanet disease names
    .join(
        df_orphanet_lookup.select(
            col("orphanet_id").alias("orphanet_id_lookup"),
            col("disease_name").alias("orphanet_disease_name")
        ),
        df_variants.orphanet_id == col("orphanet_id_lookup"),
        "left"
    )
    .drop("orphanet_id_lookup")
    
    # Create enriched disease name (use best available source)
    .withColumn("disease_name_enriched",
                coalesce(
                    col("disease_enriched"),
                    col("primary_disease"),
                    col("omim_disease_name"),
                    col("mondo_disease_name"),
                    col("orphanet_disease_name"),
                    lit("Unknown_Disease")
                ))
    
    # Disease database coverage flags
    .withColumn("has_omim_disease",
                col("omim_id").isNotNull() & col("omim_disease_name").isNotNull())
    
    .withColumn("has_mondo_disease",
                col("mondo_id").isNotNull() & col("mondo_disease_name").isNotNull())
    
    .withColumn("has_orphanet_disease",
                col("orphanet_id").isNotNull() & col("orphanet_disease_name").isNotNull())
    
    .withColumn("disease_db_coverage",
                when(col("has_omim_disease"), 1).otherwise(0) +
                when(col("has_mondo_disease"), 1).otherwise(0) +
                when(col("has_orphanet_disease"), 1).otherwise(0))
    
    .withColumn("disease_is_well_annotated",
                col("disease_db_coverage") >= 2)
    
    # Disease name quality
    .withColumn("disease_name_is_generic",
                col("disease_name_enriched").rlike("(?i)disease|disorder|syndrome") &
                (length(col("disease_name_enriched")) < 30))
)

print("Disease enrichment complete")

# COMMAND ----------

# DBTITLE 1,Use Case 4 - Disease Association Features
print("\nUSE CASE 4: DISEASE ASSOCIATION DISCOVERY")
print("="*80)

# Calculate gene-disease association statistics
# Note: gene_disease_links uses gene_symbol not gene_name
gene_disease_stats = (
    df_gene_disease
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(
        countDistinct("medgen_id").alias("disease_count"),
        countDistinct("omim_id").alias("omim_disease_count"),
        collect_list("disease_name").alias("disease_list"),
        collect_list("association_type").alias("association_types")
    )
    .withColumn("disease_count_category",
                when(col("disease_count") >= 10, lit("Many_Diseases"))
                .when(col("disease_count") >= 5, lit("Multiple_Diseases"))
                .when(col("disease_count") >= 2, lit("Few_Diseases"))
                .otherwise(lit("Single_Disease")))
)

# Derive disease association flags from genes table
df_genes_with_disease = (
    df_genes
    .select(
        col("gene_name").alias("gene_name_genes"),
        "official_symbol",
        "mim_id",
        col("chromosome").alias("chromosome_genes"),
        
        # Pharmacogene flags (available in schema)
        "is_pharmacogene",
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "is_gpcr",
        "is_transporter"
    )
    
    # Derive disease association from mim_id
    .withColumn("is_omim_gene",
                col("mim_id").isNotNull())
    
    # Join with gene-disease links to get disease association
    .join(
        gene_disease_stats,
        col("gene_name_genes") == gene_disease_stats.gene_name,
        "left"
    )
    .drop(gene_disease_stats.gene_name)
    
    # Create disease association flag
    .withColumn("is_disease_associated",
                col("disease_count").isNotNull() & (col("disease_count") > 0))
    
    .withColumn("is_multi_disease_gene",
                col("disease_count") > 1)
    
    .withColumn("disease_association_strength",
                when(~col("is_disease_associated"), lit("No_Association"))
                .when(col("disease_count") >= 5, lit("Strong_Association"))
                .when(col("disease_count") >= 2, lit("Moderate_Association"))
                .otherwise(lit("Weak_Association")))
)

# Join variants with disease features
df_disease = (
    df_variants_enriched
    .select(
        "variant_id", "gene_name", "chromosome", "position",
        "is_pathogenic", "is_benign", "is_vus",
        "clinical_significance_simple",
        "disease_enriched", "primary_disease", "disease_name_enriched",
        "omim_id", "mondo_id", "orphanet_id",
        "has_omim_disease", "has_mondo_disease", "has_orphanet_disease",
        "disease_db_coverage", "disease_is_well_annotated",
        "disease_name_is_generic"
    )
    .join(
        df_genes_with_disease.select(
            "gene_name_genes",
            "official_symbol",
            "is_pharmacogene",
            "is_kinase",
            "is_receptor",
            "is_enzyme",
            "is_gpcr",
            "is_transporter",
            "is_omim_gene",
            "disease_count",
            "omim_disease_count",
            "disease_count_category",
            "is_disease_associated",
            "is_multi_disease_gene",
            "disease_association_strength"
        ),
        col("gene_name") == col("gene_name_genes"),
        "left"
    )
    .drop("gene_name_genes")
    
    # Disease-variant relationship quality
    .withColumn("variant_disease_link_quality",
                when(col("disease_is_well_annotated") & col("is_disease_associated"),
                     lit("High_Quality"))
                .when(col("disease_is_well_annotated") | col("is_disease_associated"),
                     lit("Moderate_Quality"))
                .otherwise(lit("Low_Quality")))
)

print("Disease association features created")

# COMMAND ----------

# DBTITLE 1,Use Case 5 - Polygenic Risk Features
print("\nUSE CASE 5: MULTI-GENE DISEASE RISK (POLYGENIC)")
print("="*80)

# Calculate disease-level statistics (per disease, count genes and variants)
disease_variant_stats = (
    df_variants_enriched
    .filter(col("disease_name_enriched") != "Unknown_Disease")
    .groupBy("disease_name_enriched")
    .agg(
        count("*").alias("disease_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("disease_pathogenic_variants"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("disease_benign_variants"),
        spark_sum(when(col("is_vus"), 1).otherwise(0)).alias("disease_vus_variants"),
        countDistinct("gene_name").alias("disease_gene_count")
    )
    .withColumn("disease_pathogenic_ratio",
                col("disease_pathogenic_variants") / col("disease_total_variants"))
    
    # Polygenic vs monogenic classification
    .withColumn("is_polygenic_disease",
                col("disease_gene_count") >= 3)
    
    .withColumn("disease_complexity",
                when(col("disease_gene_count") >= 20, lit("Highly_Complex"))
                .when(col("disease_gene_count") >= 10, lit("Complex"))
                .when(col("disease_gene_count") >= 5, lit("Moderate_Complex"))
                .when(col("disease_gene_count") >= 2, lit("Oligogenic"))
                .otherwise(lit("Monogenic")))
    
    # Disease-level pathogenic burden
    .withColumn("disease_has_high_pathogenic_burden",
                col("disease_pathogenic_ratio") > 0.2)
    
    .withColumn("disease_complexity_score",
                when(col("disease_complexity") == "Highly_Complex", 5)
                .when(col("disease_complexity") == "Complex", 4)
                .when(col("disease_complexity") == "Moderate_Complex", 3)
                .when(col("disease_complexity") == "Oligogenic", 2)
                .when(col("disease_complexity") == "Monogenic", 1)
                .otherwise(0))
)

# Join back to main dataset
df_disease = (
    df_disease
    .join(
        disease_variant_stats.select(
            col("disease_name_enriched").alias("disease_name_enriched_stats"),
            "disease_total_variants",
            "disease_pathogenic_variants",
            "disease_benign_variants",
            "disease_vus_variants",
            "disease_gene_count",
            "disease_pathogenic_ratio",
            "is_polygenic_disease",
            "disease_complexity",
            "disease_has_high_pathogenic_burden",
            "disease_complexity_score"
        ),
        col("disease_name_enriched") == col("disease_name_enriched_stats"),
        "left"
    )
    .drop("disease_name_enriched_stats")
    
    # Individual variant contribution to polygenic risk
    .withColumn("polygenic_risk_contribution",
                when(col("is_pathogenic") & col("is_polygenic_disease"), lit(1.0))
                .when(col("is_vus") & col("is_polygenic_disease"), lit(0.5))
                .when(col("is_pathogenic") & ~col("is_polygenic_disease"), lit(0.8))
                .otherwise(lit(0.0)))
)

print("Polygenic risk features created")

# COMMAND ----------

# DBTITLE 1,Use Case 6 - Gene Prioritization Features
print("\nUSE CASE 6: GENE PRIORITIZATION (CLINICAL UTILITY)")
print("="*80)

# Calculate comprehensive gene-level clinical utility metrics
gene_clinical_utility = (
    df_variants_enriched
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("gene_pathogenic_count"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("gene_benign_count"),
        spark_sum(when(col("review_quality_score") >= 2, 1).otherwise(0)).alias("gene_high_quality_count"),
        countDistinct("disease_name_enriched").alias("gene_disease_diversity"),
        
        # Disease database coverage
        spark_sum(when(col("has_omim_disease"), 1).otherwise(0)).alias("gene_omim_variants"),
        spark_sum(when(col("has_mondo_disease"), 1).otherwise(0)).alias("gene_mondo_variants"),
        spark_sum(when(col("disease_is_well_annotated"), 1).otherwise(0)).alias("gene_well_annotated_variants")
    )
    
    # Clinical utility score calculation
    .withColumn("gene_clinical_utility_score",
                (col("gene_pathogenic_count") * 3) +
                (col("gene_high_quality_count") * 2) +
                (col("gene_disease_diversity") * 4) +
                (col("gene_well_annotated_variants") * 1))
    
    # Gene annotation completeness score
    .withColumn("gene_annotation_score",
                (col("gene_omim_variants") * 2) +
                (col("gene_mondo_variants") * 1) +
                (col("gene_well_annotated_variants") * 3))
)

# Join with main dataset
df_disease = (
    df_disease
    .join(
        gene_clinical_utility.select(
            col("gene_name").alias("gene_name_utility"),
            "gene_total_variants",
            "gene_pathogenic_count",
            "gene_benign_count",
            "gene_high_quality_count",
            "gene_disease_diversity",
            "gene_clinical_utility_score",
            "gene_annotation_score",
            "gene_omim_variants",
            "gene_mondo_variants",
            "gene_well_annotated_variants"
        ),
        col("gene_name") == col("gene_name_utility"),
        "left"
    )
    .drop("gene_name_utility")
    
    # Gene prioritization tiers
    .withColumn("gene_priority_tier",
                when(coalesce(col("gene_clinical_utility_score"), lit(0)) >= 200, lit("Tier_1_Critical"))
                .when(coalesce(col("gene_clinical_utility_score"), lit(0)) >= 100, lit("Tier_2_High"))
                .when(coalesce(col("gene_clinical_utility_score"), lit(0)) >= 50, lit("Tier_3_Moderate"))
                .when(coalesce(col("gene_clinical_utility_score"), lit(0)) >= 10, lit("Tier_4_Low"))
                .otherwise(lit("Tier_5_Minimal")))
    
    # Clinical actionability assessment
    .withColumn("is_clinically_actionable",
                (coalesce(col("gene_pathogenic_count"), lit(0)) >= 5) &
                (coalesce(col("gene_high_quality_count"), lit(0)) >= 3) &
                col("is_disease_associated") &
                col("is_omim_gene"))
    
    .withColumn("is_research_candidate",
                (coalesce(col("gene_disease_diversity"), lit(0)) >= 2) &
                (coalesce(col("gene_total_variants"), lit(0)) >= 50) &
                ~col("is_clinically_actionable"))
    
    # Pharmacogene prioritization
    .withColumn("is_pharmacogene_priority",
                col("is_pharmacogene") &
                col("is_disease_associated") &
                (coalesce(col("gene_pathogenic_count"), lit(0)) >= 3))
    
    # Annotation quality-based prioritization
    .withColumn("has_excellent_annotation",
                coalesce(col("gene_annotation_score"), lit(0)) >= 100)
    
    .withColumn("annotation_priority_level",
                when(col("has_excellent_annotation"), lit("Excellent_Annotation"))
                .when(coalesce(col("gene_annotation_score"), lit(0)) >= 50, lit("Good_Annotation"))
                .when(coalesce(col("gene_annotation_score"), lit(0)) >= 10, lit("Fair_Annotation"))
                .otherwise(lit("Poor_Annotation")))
)

print("Gene prioritization features created")

# COMMAND ----------

# DBTITLE 1,Create Final Disease Features Table
print("\nCREATING DISEASE ML FEATURES")
print("="*80)

# Select final feature set
disease_features = df_disease.select(
    # IDs
    "variant_id", "gene_name", "chromosome", "position",
    
    # Clinical significance
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple",
    
    # Use Case 4: Disease Association Features
    "disease_enriched",
    "primary_disease",
    "disease_name_enriched",
    "omim_id",
    "mondo_id",
    "orphanet_id",
    "has_omim_disease",
    "has_mondo_disease",
    "has_orphanet_disease",
    "disease_db_coverage",
    "disease_is_well_annotated",
    "disease_name_is_generic",
    "disease_count",
    "omim_disease_count",
    "disease_count_category",
    "is_disease_associated",
    "is_multi_disease_gene",
    "disease_association_strength",
    "is_omim_gene",
    "variant_disease_link_quality",
    
    # Use Case 5: Polygenic Risk Features
    "disease_total_variants",
    "disease_pathogenic_variants",
    "disease_benign_variants",
    "disease_vus_variants",
    "disease_pathogenic_ratio",
    "disease_gene_count",
    "is_polygenic_disease",
    "disease_complexity",
    "disease_complexity_score",
    "polygenic_risk_contribution",
    "disease_has_high_pathogenic_burden",
    
    # Use Case 6: Gene Prioritization Features
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_benign_count",
    "gene_high_quality_count",
    "gene_disease_diversity",
    "gene_clinical_utility_score",
    "gene_priority_tier",
    "is_clinically_actionable",
    "is_research_candidate",
    "is_pharmacogene",
    "is_pharmacogene_priority",
    "gene_annotation_score",
    "has_excellent_annotation",
    "annotation_priority_level",
    "gene_omim_variants",
    "gene_mondo_variants",
    "gene_well_annotated_variants"
)

feature_count = disease_features.count()
print(f"Disease ML features: {feature_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
disease_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.disease_ml_features")

print(f"Saved: {catalog_name}.gold.disease_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nDisease database coverage:")
disease_features.select(
    spark_sum(when(col("has_omim_disease"), 1).otherwise(0)).alias("has_omim"),
    spark_sum(when(col("has_mondo_disease"), 1).otherwise(0)).alias("has_mondo"),
    spark_sum(when(col("has_orphanet_disease"), 1).otherwise(0)).alias("has_orphanet"),
    spark_sum(when(col("disease_is_well_annotated"), 1).otherwise(0)).alias("well_annotated")
).show()

print("\nDisease association strength:")
disease_features.groupBy("disease_association_strength").count().orderBy("count", ascending=False).show()

print("\nDisease complexity:")
disease_features.groupBy("disease_complexity").count().orderBy("count", ascending=False).show()

print("\nGene priority tier:")
disease_features.groupBy("gene_priority_tier").count().orderBy("gene_priority_tier").show()

print("\nAnnotation priority:")
disease_features.groupBy("annotation_priority_level").count().orderBy("count", ascending=False).show()

print("\nClinical actionability:")
disease_features.select(
    spark_sum(when(col("is_clinically_actionable"), 1).otherwise(0)).alias("actionable"),
    spark_sum(when(col("is_research_candidate"), 1).otherwise(0)).alias("research_candidate"),
    spark_sum(when(col("is_pharmacogene_priority"), 1).otherwise(0)).alias("pharmacogene_priority")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("DISEASE FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {feature_count:,}")

print("\nUse Cases Covered:")
print("  4. Disease Association Discovery")
print("     - 20+ disease association features")
print("  5. Multi-Gene Disease Risk (Polygenic)")
print("     - Disease complexity classification")
print("     - Polygenic risk contribution scoring")
print("  6. Gene Prioritization (Clinical Utility)")
print("     - 5-tier prioritization system")
print("     - Clinical actionability assessment")

print("\nReference Tables Used:")
print("  - omim_disease_lookup (disease name enrichment)")
print("  - mondo_disease_lookup (disease name enrichment)")
print("  - orphanet_disease_lookup (disease name enrichment)")

print("\nData Quality Features:")
print("  - Disease database coverage tracking")
print("  - Annotation completeness scoring")
print("  - Variant-disease link quality assessment")

print("\nTable created:")
print(f"  {catalog_name}.gold.disease_ml_features")
