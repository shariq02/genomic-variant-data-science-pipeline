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

# Check if gene description features are available
gene_cols = df_genes.columns
has_description_features = 'primary_description' in gene_cols
if has_description_features:
    print("\n Gene description features available (16_gene_description_enrichmment was run)")
else:
    print("\n Gene description features NOT available (Run 16_gene_description_enrichmment first for full enhancement)")

# CRITICAL FIX: Check if Module 17 added comprehensive disease data
has_comprehensive_diseases = 'has_cancer_combined' in gene_cols and 'total_disease_count' in gene_cols
if has_comprehensive_diseases:
    print("\n Comprehensive disease data available (Module 17 + Module 00 were run)")
    print("   Will use comprehensive disease columns to avoid ambiguous column errors")
else:
    print("\n Comprehensive disease data NOT available")
    print("   Will compute disease statistics from gene_disease_links")

# COMMAND ----------

# DBTITLE 1,Enrich Disease Information from Reference Tables
print("\nENRICHING DISEASE INFORMATION")
print("="*80)

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

# Calculate gene-disease association statistics (CONDITIONAL based on Module 17)
if has_comprehensive_diseases:
    print("Using comprehensive disease data from genes_ultra_enriched (Module 17)")
    # Genes already have disease data - add derived columns
    gene_disease_stats = (
        df_genes
        .select(
            "gene_name",
            col("total_disease_count").alias("disease_count"),
            "omim_disease_count"  # Already exists from Module 17
        )
        .withColumn("disease_count_category",
                    when(col("disease_count") >= 10, lit("Highly_Associated"))
                    .when(col("disease_count") >= 5, lit("Moderately_Associated"))
                    .when(col("disease_count") >= 2, lit("Associated"))
                    .when(col("disease_count") == 1, lit("Single_Disease"))
                    .otherwise(lit("Not_Associated")))
        
        .withColumn("is_disease_associated",
                    col("disease_count") >= 1)
        
        .withColumn("is_multi_disease_gene",
                    col("disease_count") >= 3)
        
        .withColumn("disease_association_strength",
                    when(col("disease_count") >= 10, 5)
                    .when(col("disease_count") >= 5, 4)
                    .when(col("disease_count") >= 2, 3)
                    .when(col("disease_count") == 1, 2)
                    .otherwise(1))
        
        .withColumn("is_omim_gene",
                    col("omim_disease_count") >= 1)
    )
else:
    print("Computing disease statistics from gene_disease_links (OMIM only)")
    gene_disease_stats = (
        df_gene_disease
        .groupBy(col("gene_symbol").alias("gene_name"))
        .agg(
            countDistinct("medgen_id").alias("disease_count"),
            countDistinct(when(col("omim_id").isNotNull(), col("omim_id"))).alias("omim_disease_count")
        )
        .withColumn("disease_count_category",
                    when(col("disease_count") >= 10, lit("Highly_Associated"))
                    .when(col("disease_count") >= 5, lit("Moderately_Associated"))
                    .when(col("disease_count") >= 2, lit("Associated"))
                    .when(col("disease_count") == 1, lit("Single_Disease"))
                    .otherwise(lit("Not_Associated")))
        
        .withColumn("is_disease_associated",
                    col("disease_count") >= 1)
        
        .withColumn("is_multi_disease_gene",
                    col("disease_count") >= 3)
        
        .withColumn("disease_association_strength",
                    when(col("disease_count") >= 10, 5)
                    .when(col("disease_count") >= 5, 4)
                    .when(col("disease_count") >= 2, 3)
                    .when(col("disease_count") == 1, 2)
                    .otherwise(1))
        
        .withColumn("is_omim_gene",
                    col("omim_disease_count") >= 1)
    )
)

# Join gene-disease stats to gene table
df_genes_with_disease = (
    df_genes
    .join(gene_disease_stats, "gene_name", "left")
    .fillna({
        "disease_count": 0,
        "omim_disease_count": 0,
        "disease_association_strength": 1,
        "is_disease_associated": False,
        "is_multi_disease_gene": False,
        "is_omim_gene": False
    })
    .fillna("Not_Associated", ["disease_count_category"])
)

# Join variant data with enriched gene info
df_disease = (
    df_variants_enriched
    .drop("disease_count")  # Drop variants version to avoid conflict
    .join(
        df_genes_with_disease.select(
            col("gene_name").alias("gene_name_lookup"),
            "disease_count",
            "omim_disease_count",
            "disease_count_category",
            "is_disease_associated",
            "is_multi_disease_gene",
            "disease_association_strength",
            "is_omim_gene",
            "is_pharmacogene"
        ),
        df_variants_enriched.gene_name == col("gene_name_lookup"),
        "left"
    )
    .drop("gene_name_lookup")
    
    # Variant-disease link quality
    .withColumn("variant_disease_link_quality",
                (when(col("has_omim_disease"), 3).otherwise(0)) +
                (when(col("has_mondo_disease"), 2).otherwise(0)) +
                (when(col("has_orphanet_disease"), 2).otherwise(0)) +
                (when(col("is_disease_associated"), 2).otherwise(0)) +
                (when(col("disease_is_well_annotated"), 1).otherwise(0)))
)

print("Disease association features created")

# COMMAND ----------

# DBTITLE 1,Use Case 5 - Polygenic Risk Features
print("\nUSE CASE 5: POLYGENIC RISK ASSESSMENT")
print("="*80)

# Calculate disease-level variant statistics
disease_variant_stats = (
    df_disease
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
                when(col("disease_total_variants") > 0,
                     col("disease_pathogenic_variants") / col("disease_total_variants"))
                .otherwise(lit(0)))
    
    .withColumn("is_polygenic_disease",
                col("disease_gene_count") >= 3)
    
    .withColumn("disease_complexity",
                when(col("disease_gene_count") >= 10, lit("Complex_Polygenic"))
                .when(col("disease_gene_count") >= 5, lit("Moderate_Polygenic"))
                .when(col("disease_gene_count") >= 3, lit("Simple_Polygenic"))
                .otherwise(lit("Monogenic")))
    
    .withColumn("disease_has_high_pathogenic_burden",
                (col("disease_pathogenic_variants") >= 10) |
                (col("disease_pathogenic_ratio") >= 0.5))
    
    .withColumn("disease_complexity_score",
                col("disease_gene_count") * 2 +
                col("disease_total_variants"))
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

# DBTITLE 1,ENHANCEMENT - Add Gene Description Features
print("\nENHANCEMENT: ADDING GENE DESCRIPTION FEATURES")
print("="*80)

if has_description_features:
    # Join gene description features
    df_disease = (
        df_disease
        .join(
            df_genes.select(
                col("gene_name").alias("gene_name_desc"),
                "primary_description",
                "gene_importance_score",
                "gene_annotation_quality",
                "primary_gene_category",
                
                # Disease keywords
                "has_cancer_keyword",
                "has_syndrome_keyword",
                "has_neurological_keyword",
                "has_cardiovascular_keyword",
                "has_metabolic_keyword",
                "has_immune_keyword",
                "has_rare_disease_keyword",
                "is_disease_related_gene",
                
                # Clinical keywords
                "has_drug_target_in_desc",
                "has_biomarker_in_desc",
                "has_essential_in_desc",
                "clinical_importance_score",
                
                # Function
                "is_well_characterized",
                "function_keyword_count"
            ),
            col("gene_name") == col("gene_name_desc"),
            "left"
        )
        .drop("gene_name_desc")
        
        # Disease-gene relevance score (ENHANCED)
        .withColumn("disease_gene_relevance_score",
                    coalesce(col("gene_clinical_utility_score"), lit(0)) +
                    (coalesce(col("gene_importance_score"), lit(0)) * 5) +
                    (when(col("is_disease_related_gene"), 20).otherwise(0)) +
                    (when(col("has_drug_target_in_desc"), 15).otherwise(0)) +
                    (when(col("has_biomarker_in_desc"), 10).otherwise(0)) +
                    (when(col("has_essential_in_desc"), 10).otherwise(0)))
        
        # Enhanced gene priority tier
        .withColumn("enhanced_gene_priority_tier",
                    when(col("disease_gene_relevance_score") >= 300, lit("Tier_1_Critical"))
                    .when(col("disease_gene_relevance_score") >= 150, lit("Tier_2_High"))
                    .when(col("disease_gene_relevance_score") >= 75, lit("Tier_3_Moderate"))
                    .when(col("disease_gene_relevance_score") >= 25, lit("Tier_4_Low"))
                    .otherwise(lit("Tier_5_Minimal")))
        
        # Disease-specific gene flags
        .withColumn("is_cancer_gene_variant",
                    col("has_cancer_keyword") == True)
        
        .withColumn("is_neurological_gene_variant",
                    col("has_neurological_keyword") == True)
        
        .withColumn("is_cardiovascular_gene_variant",
                    col("has_cardiovascular_keyword") == True)
        
        .withColumn("is_metabolic_gene_variant",
                    col("has_metabolic_keyword") == True)
        
        .withColumn("is_rare_disease_gene_variant",
                    col("has_rare_disease_keyword") == True)
        
        # Drug development potential
        .withColumn("has_drug_development_potential",
                    (col("has_drug_target_in_desc") == True) |
                    (col("has_biomarker_in_desc") == True))
        
        # Enhanced clinical actionability
        .withColumn("is_highly_actionable",
                    (coalesce(col("gene_pathogenic_count"), lit(0)) >= 5) &
                    (col("disease_gene_relevance_score") >= 100) &
                    ((col("has_drug_target_in_desc") == True) |
                     (col("has_biomarker_in_desc") == True)))
    )
    
    print(" Gene description features integrated")
else:
    # Add placeholder columns if description features not available
    df_disease = df_disease.withColumn("disease_gene_relevance_score", lit(None).cast("integer"))
    df_disease = df_disease.withColumn("enhanced_gene_priority_tier", lit("Not_Available"))
    df_disease = df_disease.withColumn("is_cancer_gene_variant", lit(False))
    df_disease = df_disease.withColumn("is_neurological_gene_variant", lit(False))
    df_disease = df_disease.withColumn("is_cardiovascular_gene_variant", lit(False))
    df_disease = df_disease.withColumn("is_metabolic_gene_variant", lit(False))
    df_disease = df_disease.withColumn("is_rare_disease_gene_variant", lit(False))
    df_disease = df_disease.withColumn("has_drug_development_potential", lit(False))
    df_disease = df_disease.withColumn("is_highly_actionable", lit(False))
    
    print(" Placeholder columns added (16_gene_description_enrichmment for full enhancement)")

# COMMAND ----------

# DBTITLE 1,Create Final Disease Features Table
print("\nCREATING DISEASE ML FEATURES")
print("="*80)

# Build feature list dynamically based on available columns
base_features = [
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
]

# Add enhanced features
enhanced_features = [
    "disease_gene_relevance_score",
    "enhanced_gene_priority_tier",
    "is_cancer_gene_variant",
    "is_neurological_gene_variant",
    "is_cardiovascular_gene_variant",
    "is_metabolic_gene_variant",
    "is_rare_disease_gene_variant",
    "has_drug_development_potential",
    "is_highly_actionable"
]

# Add description features if available
if has_description_features:
    description_features = [
        "primary_description",
        "gene_importance_score",
        "gene_annotation_quality",
        "primary_gene_category",
        "has_cancer_keyword",
        "has_syndrome_keyword",
        "has_neurological_keyword",
        "has_cardiovascular_keyword",
        "has_metabolic_keyword",
        "has_immune_keyword",
        "has_rare_disease_keyword",
        "is_disease_related_gene",
        "has_drug_target_in_desc",
        "has_biomarker_in_desc",
        "has_essential_in_desc",
        "clinical_importance_score",
        "is_well_characterized",
        "function_keyword_count"
    ]
    all_features = base_features + enhanced_features + description_features
else:
    all_features = base_features + enhanced_features

# Select final feature set
disease_features = df_disease.select(*all_features)

feature_count = disease_features.count()
print(f"Disease ML features: {feature_count:,} variants")
print(f"Feature columns: {len(all_features)}")

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

total = disease_features.count()

print(f"\nDisease Association (Use Case 4):")
disease_features.groupBy("disease_count_category").count().show()

print("\nPolygenic Risk (Use Case 5):")
disease_features.groupBy("disease_complexity").count().show()

print("\nGene Prioritization (Use Case 6):")
disease_features.groupBy("gene_priority_tier").count().show()

if has_description_features:
    print("\nENHANCED: Priority tiers (with description features):")
    disease_features.groupBy("enhanced_gene_priority_tier").count().show()
    
    print("\nENHANCED: Disease-specific genes:")
    print(f"  Cancer genes: {disease_features.filter(col('is_cancer_gene_variant')).count():,}")
    print(f"  Neurological genes: {disease_features.filter(col('is_neurological_gene_variant')).count():,}")
    print(f"  Cardiovascular genes: {disease_features.filter(col('is_cardiovascular_gene_variant')).count():,}")
    print(f"  Highly actionable: {disease_features.filter(col('is_highly_actionable')).count():,}")

# COMMAND ----------

# DBTITLE 1,Summary
print("DISEASE FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal variants: {total:,}")
print(f"Total features: {len(all_features)}")

if has_description_features:
    print("\nFeature categories (ENHANCED):")
    print("  - Disease Association (Use Case 4): 20 features")
    print("  - Polygenic Risk (Use Case 5): 11 features")
    print("  - Gene Prioritization (Use Case 6): 16 features")
    print("  - Gene Description Enhancement: 27 features")
    print("  Total: 74 features")
else:
    print("\nFeature categories (BASE):")
    print("  - Disease Association (Use Case 4): 20 features")
    print("  - Polygenic Risk (Use Case 5): 11 features")
    print("  - Gene Prioritization (Use Case 6): 16 features")
    print("  Total: 47 features")
    print("\n  Run 16_gene_description_enrichmment first for 27 additional description features!")

print(f"\nTable created:")
print(f"  {catalog_name}.gold.disease_ml_features")
