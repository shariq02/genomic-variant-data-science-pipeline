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
    max as spark_max, min as spark_min, collect_list, concat_ws,
    countDistinct, size, array_distinct
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
df_diseases = spark.table(f"{catalog_name}.silver.diseases")

print(f"Variants: {df_variants.count():,}")
print(f"Genes: {df_genes.count():,}")
print(f"Gene-disease links: {df_gene_disease.count():,}")
print(f"Diseases: {df_diseases.count():,}")

# COMMAND ----------

# DBTITLE 1,Use Case 4 - Disease Association Features
print("USE CASE 4: DISEASE ASSOCIATION DISCOVERY")
print("="*80)

# Count diseases per gene (MedGen-based, schema-correct)
gene_disease_stats = (
    df_gene_disease
    .filter(col("medgen_id").isNotNull())
    .groupBy("gene_id")
    .agg(
        countDistinct("medgen_id").alias("disease_count"),
        collect_list("medgen_id").alias("disease_list")
    )
)

# Join variants with disease statistics
df_disease = (
    df_variants
    .select(
        "variant_id",
        "gene_id",
        "gene_name",
        "chromosome",
        "position",
        "is_pathogenic",
        "is_benign",
        "is_vus",
        "clinical_significance_simple",
        "disease_enriched",
        "primary_disease",
        "omim_id",
        "mondo_id"
    )
    .join(gene_disease_stats, "gene_id", "left")
)

# ---- DERIVED FLAGS (no df_genes dependency) ----

df_disease = (
    df_disease
    .withColumn(
        "has_disease_association",
        col("disease_count").isNotNull() & (col("disease_count") > 0)
    )
    .withColumn(
        "is_multi_disease_gene",
        col("disease_count") > 1
    )
    .withColumn(
        "disease_association_strength",
        when(col("disease_count").isNull(), lit("None"))
        .when(col("disease_count") >= 5, lit("Strong"))
        .when(col("disease_count") >= 2, lit("Moderate"))
        .otherwise(lit("Weak"))
    )
    # Mendelian vs complex (proxy logic used in real pipelines)
    .withColumn(
        "is_mendelian_disease",
        col("omim_id").isNotNull() & (col("disease_count") == 1)
    )
    .withColumn(
        "is_complex_disease",
        col("disease_count") >= 2
    )
    .withColumn(
        "primary_disease_category",
        when(col("is_mendelian_disease"), lit("Mendelian"))
        .when(col("is_complex_disease"), lit("Complex"))
        .otherwise(lit("Other"))
    )
)

print("Disease association features created (schema-safe)")

# COMMAND ----------

# DBTITLE 1,Use Case 5 - Polygenic Risk Features
print("USE CASE 6: MULTI-GENE DISEASE RISK (POLYGENIC SCORES)")
print("="*80)

# Calculate disease-level statistics
disease_variant_stats = (
    df_variants
    .filter(col("disease_enriched").isNotNull())
    .groupBy("primary_disease")
    .agg(
        count("*").alias("disease_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("disease_pathogenic_variants"),
        countDistinct("gene_name").alias("disease_gene_count")
    )
    .withColumn("disease_pathogenic_ratio",
                col("disease_pathogenic_variants") / col("disease_total_variants"))
)

# Join with main dataset
df_disease = (
    df_disease
    .join(disease_variant_stats,
          df_disease.primary_disease == disease_variant_stats.primary_disease,
          "left")
    
    # Polygenic risk indicators
    .withColumn("is_polygenic_disease",
                col("disease_gene_count") >= 3)
    
    .withColumn("disease_complexity",
                when(col("disease_gene_count") >= 10, lit("Highly_Complex"))
                .when(col("disease_gene_count") >= 5, lit("Moderate_Complex"))
                .when(col("disease_gene_count") >= 2, lit("Oligogenic"))
                .otherwise(lit("Monogenic")))
    
    # Individual variant contribution to polygenic risk
    .withColumn("polygenic_risk_contribution",
                when(col("is_pathogenic") & col("is_polygenic_disease"), lit(1.0))
                .when(col("is_vus") & col("is_polygenic_disease"), lit(0.5))
                .otherwise(lit(0.0)))
    
    # Disease-specific pathogenic burden
    .withColumn("disease_has_high_pathogenic_burden",
                col("disease_pathogenic_ratio") > 0.2)
)

print("Polygenic risk features created")

# COMMAND ----------

# DBTITLE 1,Use Case 6 - Gene Prioritization Features
print("USE CASE 6: GENE PRIORITIZATION (CLINICAL UTILITY)")
print("="*80)

# Calculate gene-level clinical utility metrics
gene_clinical_utility = (
    df_variants
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("gene_pathogenic_count"),
        spark_sum(when(col("review_quality_score") >= 2, 1).otherwise(0)).alias("gene_high_quality_count"),
        countDistinct("primary_disease").alias("gene_disease_diversity")
    )
    .withColumn("gene_clinical_utility_score",
                (col("gene_pathogenic_count") * 2) +
                col("gene_high_quality_count") +
                (col("gene_disease_diversity") * 3))
)

# Join with main dataset and gene features
df_disease = (
    df_disease
    .join(gene_clinical_utility, "gene_name", "left")
    .join(df_genes.select(
        "gene_name",
        "is_haploinsufficient",
        "is_omim_gene",
        "is_orphanet_gene"
    ), "gene_name", "left")
    
    # Gene prioritization tiers
    .withColumn("gene_priority_tier",
                when(col("gene_clinical_utility_score") >= 100, lit("Tier_1_High"))
                .when(col("gene_clinical_utility_score") >= 50, lit("Tier_2_Moderate"))
                .when(col("gene_clinical_utility_score") >= 10, lit("Tier_3_Low"))
                .otherwise(lit("Tier_4_Minimal")))
    
    # Clinical actionability flags
    .withColumn("is_clinically_actionable",
                (col("gene_pathogenic_count") >= 5) &
                (col("gene_high_quality_count") >= 3) &
                col("has_disease_association"))
    
    .withColumn("is_research_priority",
                (col("gene_disease_diversity") >= 2) &
                (col("gene_total_variants") >= 100) &
                ~col("is_clinically_actionable"))
    
    # Haploinsufficiency-based prioritization
    .withColumn("haploinsufficiency_risk",
                when(col("is_haploinsufficient") & col("is_pathogenic"), lit("High"))
                .when(col("is_haploinsufficient") & col("is_vus"), lit("Moderate"))
                .otherwise(lit("Low")))
    
    # Database coverage score (0-3)
    .withColumn("gene_database_coverage_score",
                when(col("is_omim_gene"), 1).otherwise(0) +
                when(col("is_orphanet_gene"), 1).otherwise(0) +
                when(col("disease_count") > 0, 1).otherwise(0))
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
    
    # Clinical significance (for joining with other feature tables)
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple",
    
    # Use Case 2: Disease Association Features
    "disease_enriched",
    "primary_disease",
    "omim_id",
    "mondo_id",
    "disease_count",
    "has_disease_association",
    "is_multi_disease_gene",
    "disease_association_strength",
    "has_disease_association",
    "is_mendelian_disease",
    "is_complex_disease",
    "is_cancer_gene",
    "primary_disease_category",
    
    # Use Case 8: Polygenic Risk Features
    "disease_total_variants",
    "disease_pathogenic_variants",
    "disease_pathogenic_ratio",
    "disease_gene_count",
    "is_polygenic_disease",
    "disease_complexity",
    "polygenic_risk_contribution",
    "disease_has_high_pathogenic_burden",
    
    # Use Case 9: Gene Prioritization Features
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_high_quality_count",
    "gene_disease_diversity",
    "gene_clinical_utility_score",
    "gene_priority_tier",
    "is_clinically_actionable",
    "is_research_priority",
    "is_haploinsufficient",
    "haploinsufficiency_risk",
    "is_essential_gene",
    "is_omim_gene",
    "is_orphanet_gene",
    "gene_database_coverage_score"
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

print("\nDisease association strength distribution:")
disease_features.groupBy("disease_association_strength").count().show()

print("\nDisease complexity distribution:")
disease_features.groupBy("disease_complexity").count().show()

print("\nGene priority tier distribution:")
disease_features.groupBy("gene_priority_tier").count().orderBy("gene_priority_tier").show()

print("\nClinical actionability:")
disease_features.select(
    spark_sum(when(col("is_clinically_actionable"), 1).otherwise(0)).alias("actionable"),
    spark_sum(when(col("is_research_priority"), 1).otherwise(0)).alias("research_priority")
).show()

print("\nGene clinical utility score (percentiles):")
disease_features.select(
    spark_min("gene_clinical_utility_score").alias("min"),
    avg("gene_clinical_utility_score").alias("mean"),
    spark_max("gene_clinical_utility_score").alias("max")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("DISEASE FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {feature_count:,}")

print("\nUse Cases Covered:")
print("  2. Disease Association Discovery")
print("     - Features: disease count, association strength")
print("  8. Multi-Gene Disease Risk (Polygenic)")
print("     - Features: polygenic risk contribution, disease complexity")
print("  9. Gene Prioritization (Clinical Utility)")
print("     - Features: priority tier, clinical actionability")

print("\nKey Feature Groups:")
print("  - Disease association metrics")
print("  - Polygenic risk indicators")
print("  - Gene clinical utility scores")
print("  - Prioritization tiers")
print("  - Haploinsufficiency risk")

print("\nTable created:")
print(f"  {catalog_name}.gold.disease_ml_features")
