# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - CLINICAL USE CASES
# MAGIC ##### Module 1: Clinical Pathogenicity, Inheritance Patterns, Population Frequency
# MAGIC
# MAGIC **DNA Gene Mapping Project**            
# MAGIC **Author:** Sharique Mohammad    
# MAGIC **Date:** January 27, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 1: Clinical Pathogenicity Prediction (Pathogenic vs Benign)
# MAGIC - Use Case 2: Inheritance Pattern Analysis (Dominant/Recessive)
# MAGIC - Use Case 3: Population Frequency Analysis (gnomAD)
# MAGIC
# MAGIC **Creates:** gold.clinical_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, stddev, countDistinct
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR CLINICAL FEATURE ENGINEERING - MODULE 1")

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("LOADING TABLES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"Variants: {df_variants.count():,}")
print(f"Variant-protein impact: {df_variant_impact.count():,}")
print(f"Genes: {df_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Use Case 1 - Clinical Pathogenicity Features
print("\nUSE CASE 1: CLINICAL PATHOGENICITY PREDICTION")
print("="*80)

df_clinical = (
    df_variant_impact
    .select(
        # IDs
        "variant_id", "gene_name", "chromosome", "position",
        
        # TARGET VARIABLE (for ML training)
        col("is_pathogenic").alias("target_is_pathogenic"),
        col("is_benign").alias("target_is_benign"),
        col("is_vus").alias("target_is_vus"),
        
        # Clinical significance features
        "clinical_significance_simple",
        "clinvar_pathogenicity_class",
        "review_status",
        "review_quality_score",
        
        # Mutation type features
        "variant_type",
        "is_missense_variant",
        "is_frameshift_variant",
        "is_nonsense_variant",
        "is_splice_variant",
        "is_snv",
        "is_insertion",
        "is_deletion",
        
        # Impact scores
        "mutation_severity_score",
        "pathogenicity_score",
        "protein_impact_category",
        
        # Conservation features
        "phylop_score",
        "phastcons_score",
        "cadd_phred",
        "conservation_level",
        "is_highly_conserved",
        "is_constrained",
        "is_likely_deleterious",
        
        # Binary ML flags
        "is_high_impact",
        "is_very_high_impact",
        "is_conservation_constrained",
        "is_domain_affecting",
        "is_loss_of_function",
        "is_deleterious_by_cadd",
        
        # Protein domain features
        "has_functional_domain",
        "domain_count",
        "has_kinase_domain",
        "has_receptor_domain"
    )
    
    # Add derived pathogenicity features
    .withColumn("is_coding_variant",
                col("is_missense_variant") | 
                col("is_frameshift_variant") | 
                col("is_nonsense_variant"))
    
    .withColumn("is_regulatory_variant",
                col("is_splice_variant"))
    
    .withColumn("has_strong_evidence",
                col("review_quality_score") >= 2)
    
    # Combined risk score (0-10)
    .withColumn("combined_pathogenicity_risk",
                coalesce(col("mutation_severity_score"), lit(0)) +
                coalesce(col("conservation_level"), lit(0)) +
                when(col("is_domain_affecting"), 2).otherwise(0) +
                when(col("is_deleterious_by_cadd"), 2).otherwise(0))
)

print("Clinical pathogenicity features created")

# COMMAND ----------

# DBTITLE 1,Use Case 2 - Inheritance Pattern Features
print("USE CASE 2: INHERITANCE PATTERN ANALYSIS")
print("="*80)

# Note: Inheritance pattern columns not available in genes_ultra_enriched
# Deriving basic patterns from chromosome location instead

df_clinical = (
    df_clinical
    
    # Derive basic inheritance patterns from chromosome
    .withColumn("inheritance_pattern",
                when(col("chromosome") == "X", lit("X_Linked"))
                .when(col("chromosome") == "Y", lit("Y_Linked"))
                .when(col("chromosome") == "MT", lit("Mitochondrial"))
                .otherwise(lit("Autosomal")))
    
    # X-linked risk considerations (affects males more)
    .withColumn("x_linked_risk_modifier",
                when((col("chromosome") == "X") & col("is_pathogenic"),
                     lit("Male_High_Risk"))
                .when((col("chromosome") == "X") & col("is_vus"),
                     lit("Male_Moderate_Risk"))
                .otherwise(lit("Standard_Risk")))
    
    # Mitochondrial inheritance (maternal)
    .withColumn("is_mitochondrial_variant",
                col("chromosome") == "MT")
    
    # Y-linked (male-only)
    .withColumn("is_y_linked_variant",
                col("chromosome") == "Y")
)

print("Inheritance pattern features created (based on chromosome location)")

# COMMAND ----------

# DBTITLE 1,Use Case 3 - Population Frequency Features
print("USE CASE 3: POPULATION FREQUENCY ANALYSIS")
print("="*80)

# Calculate gene-level variant statistics using df_variants (not df_variant_impact)
gene_stats = (
    df_variants
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("gene_pathogenic_count"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("gene_benign_count"),
        countDistinct("chromosome", "position").alias("gene_unique_positions")
    )
    .withColumn("gene_pathogenic_ratio",
                col("gene_pathogenic_count") / col("gene_total_variants"))
)

# Join with main features
df_clinical = (
    df_clinical
    .join(gene_stats, "gene_name", "left")
    
    # Population frequency categories
    # Note: gnomad_af not in current schema, using placeholder
    .withColumn("variant_frequency_class", lit("No_Population_Data"))
    
    # Gene mutation burden
    .withColumn("gene_mutation_burden",
                when(col("gene_total_variants") > 1000, lit("High"))
                .when(col("gene_total_variants") > 100, lit("Moderate"))
                .otherwise(lit("Low")))
    
    # Gene pathogenicity enrichment
    .withColumn("gene_is_pathogenic_enriched",
                coalesce(col("gene_pathogenic_ratio"), lit(0.0)) > 0.1)
)

print("Population frequency features created")

# COMMAND ----------

# DBTITLE 1,Create Final Clinical Features Table
print("\nCREATING CLINICAL ML FEATURES")
print("="*80)

# Select final feature set
clinical_features = df_clinical.select(
    # IDs
    "variant_id", "gene_name", "chromosome", "position",
    
    # TARGET VARIABLES (for supervised learning)
    "target_is_pathogenic",
    "target_is_benign",
    "target_is_vus",
    
    # Use Case 1: Pathogenicity Features
    "clinical_significance_simple",
    "clinvar_pathogenicity_class",
    "review_quality_score",
    "has_strong_evidence",
    "mutation_severity_score",
    "pathogenicity_score",
    "combined_pathogenicity_risk",
    "protein_impact_category",
    "is_coding_variant",
    "is_regulatory_variant",
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "phylop_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "is_likely_deleterious",
    "is_high_impact",
    "is_very_high_impact",
    "is_domain_affecting",
    "is_loss_of_function",
    "is_deleterious_by_cadd",
    "has_functional_domain",
    "domain_count",
    
    # Use Case 5: Inheritance Features
    "inheritance_pattern",
    "x_linked_risk_modifier",
    "is_mitochondrial_variant",
    "is_y_linked_variant",
    
    # Use Case 6: Population Frequency Features
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_pathogenic_ratio",
    "gene_mutation_burden",
    "gene_is_pathogenic_enriched",
    "variant_frequency_class"
)

feature_count = clinical_features.count()
print(f"Clinical ML features: {feature_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
clinical_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.clinical_ml_features")

print(f"Saved: {catalog_name}.gold.clinical_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nTarget variable distribution:")
clinical_features.select(
    spark_sum(when(col("target_is_pathogenic"), 1).otherwise(0)).alias("pathogenic"),
    spark_sum(when(col("target_is_benign"), 1).otherwise(0)).alias("benign"),
    spark_sum(when(col("target_is_vus"), 1).otherwise(0)).alias("vus")
).show()

print("\nInheritance pattern distribution:")
clinical_features.groupBy("inheritance_pattern").count().orderBy("count", ascending=False).show()

print("\nGene mutation burden distribution:")
clinical_features.groupBy("gene_mutation_burden").count().show()

print("\nCombined pathogenicity risk (percentiles):")
clinical_features.select(
    spark_min("combined_pathogenicity_risk").alias("min"),
    avg("combined_pathogenicity_risk").alias("mean"),
    spark_max("combined_pathogenicity_risk").alias("max")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("CLINICAL FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {feature_count:,}")

print("\nUse Cases Covered:")
print("  1. Clinical Pathogenicity Prediction")
print("     - Target: is_pathogenic, is_benign, is_vus")
print("     - Features: 30+ pathogenicity scores and flags")
print("  5. Inheritance Pattern Analysis")
print("     - Features: inheritance pattern, risk modifiers")
print("  6. Population Frequency Analysis")
print("     - Features: gene mutation burden, pathogenic ratio")

print("\nKey Feature Groups:")
print("  - Mutation severity scores")
print("  - Conservation scores (PhyloP, CADD)")
print("  - Protein domain impact flags")
print("  - Inheritance pattern classifications")
print("  - Gene-level statistics")

print("\nTable created:")
print(f"  {catalog_name}.gold.clinical_ml_features")
