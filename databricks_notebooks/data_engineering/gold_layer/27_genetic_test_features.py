# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - GENETIC TEST AVAILABILITY (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Gene-Level Test Availability Features
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC 
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive test availability profiling
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 5: Genetic Test Availability
# MAGIC - Use Case 27: Clinical Test Discovery
# MAGIC 
# MAGIC **Creates:** gold.gene_test_availability_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD GENETIC TEST FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core tables
df_gtr = spark.table(f"{catalog_name}.silver.gtr_gene_disease_tests")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # ENHANCED: enriched genes

# Additional enrichment tables
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population = spark.table(f"{catalog_name}.silver.population_frequencies")

print(f"GTR tests: {df_gtr.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")

# COMMAND ----------

# DBTITLE 1,Calculate Gene Test Statistics
print("\nCALCULATING GENE TEST STATISTICS")
print("="*80)

df_gene_tests = (
    df_gtr
    .groupBy("gene_symbol")
    .agg(
        count("gtr_test_id").alias("total_test_count"),
        countDistinct("gtr_test_id").alias("unique_test_count"),
        countDistinct("disease_name").alias("disease_count"),
        spark_sum(when(col("is_genetic_test"), 1).otherwise(0)).alias("genetic_test_count"),
        spark_sum(when(col("has_gene_info"), 1).otherwise(0)).alias("tests_with_gene_info"),
        spark_sum(when(col("has_disease_info"), 1).otherwise(0)).alias("tests_with_disease_info"),
        spark_sum(when(col("is_complete_record"), 1).otherwise(0)).alias("complete_test_count"),
        spark_sum(when(col("is_frequently_tested"), 1).otherwise(0)).alias("frequent_test_count")
    )
)

print(f"Genes with test data: {df_gene_tests.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Test Availability Classification
print("\nADDING TEST AVAILABILITY CLASSIFICATION")
print("="*80)

df_classified = (
    df_gene_tests
    .withColumn("has_clinical_test",
                when(col("unique_test_count") > 0, True).otherwise(False))
    
    .withColumn("has_multiple_tests",
                when(col("unique_test_count") >= 3, True).otherwise(False))
    
    .withColumn("has_comprehensive_testing",
                when(col("unique_test_count") >= 10, True).otherwise(False))
    
    .withColumn("is_well_tested_gene",
                when((col("complete_test_count") >= 5) & 
                     (col("disease_count") >= 2), True).otherwise(False))
    
    .withColumn("test_availability_category",
                when(col("unique_test_count") >= 10, lit("comprehensive"))
                .when(col("unique_test_count") >= 3, lit("multiple"))
                .when(col("unique_test_count") >= 1, lit("limited"))
                .otherwise(lit("none")))
)

print("Classification added")

# COMMAND ----------

# DBTITLE 1,Calculate Test Availability Scores
print("\nCALCULATING TEST AVAILABILITY SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("test_accessibility_score",
                (col("unique_test_count") * 2) +
                when(col("has_multiple_tests"), 5).otherwise(0) +
                when(col("has_comprehensive_testing"), 10).otherwise(0))
    
    .withColumn("clinical_utility_score",
                (col("complete_test_count") * 3) +
                (col("disease_count") * 2) +
                when(col("is_well_tested_gene"), 8).otherwise(0))
    
    .withColumn("test_quality_score",
                (col("tests_with_gene_info") * 1) +
                (col("tests_with_disease_info") * 2) +
                (col("complete_test_count") * 3))
)

print("Scores calculated")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Context
print("\nENRICHING WITH DISEASE CONTEXT")
print("="*80)

disease_test_correlation = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease")
    )
    .withColumn("disease_test_correlation",
                when(col("total_disease_count") >= 10, lit("High"))
                .when(col("total_disease_count") >= 5, lit("Medium"))
                .when(col("total_disease_count") >= 1, lit("Low"))
                .otherwise(lit("None")))
    .withColumn("multi_disease_testing",
                col("total_disease_count") >= 3)
)

print(f"Disease genes: {disease_test_correlation.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Variant Context
print("\nENRICHING WITH VARIANT CONTEXT")
print("="*80)

variant_test_coverage = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_variants_in_tested_gene"),
        count("*").alias("test_covered_variants")
    )
    .withColumn("variant_test_coverage_level",
                when(col("pathogenic_variants_in_tested_gene") >= 10, lit("High_Coverage"))
                .when(col("pathogenic_variants_in_tested_gene") >= 5, lit("Medium_Coverage"))
                .when(col("pathogenic_variants_in_tested_gene") >= 1, lit("Low_Coverage"))
                .otherwise(lit("No_Coverage")))
)

print(f"Genes with variants: {variant_test_coverage.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context (Hereditary Cancer Testing)
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

cancer_test_relevance = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("cancer_samples")
    )
    .withColumn("is_cancer_panel_gene",
                col("cancer_mutation_count") >= 50)
    .withColumn("hereditary_cancer_testing",
                col("cancer_samples") >= 10)
)

print(f"Cancer genes: {cancer_test_relevance.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Population Context (Carrier Screening)
print("\nENRICHING WITH POPULATION CONTEXT")
print("="*80)

# Count pathogenic rare variants per gene for carrier screening relevance
carrier_screening = (
    df_variant_impact
    .join(
        df_population.select("variant_id", "is_rare"),
        "variant_id",
        "inner"
    )
    .filter(col("is_rare") & col("is_pathogenic"))
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("rare_pathogenic_variants")
    )
    .withColumn("carrier_screening_relevant",
                col("rare_pathogenic_variants") >= 3)
    .withColumn("population_test_priority",
                when(col("rare_pathogenic_variants") >= 10, lit("High"))
                .when(col("rare_pathogenic_variants") >= 5, lit("Medium"))
                .otherwise(lit("Low")))
)

print(f"Carrier screening genes: {carrier_screening.count():,}")

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data and All Enrichments
print("\nJOINING WITH GENE MASTER DATA AND ENRICHMENTS")
print("="*80)

df_with_genes = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene")
    )
    .join(df_scored.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
          on="gene_symbol", how="left")
    
    # Join all enrichment tables
    .join(disease_test_correlation, on="gene_symbol", how="left")
    .join(variant_test_coverage, on="gene_symbol", how="left")
    .join(cancer_test_relevance, on="gene_symbol", how="left")
    .join(carrier_screening, on="gene_symbol", how="left")
    
    # Fill nulls
    .fillna({
        "total_test_count": 0,
        "unique_test_count": 0,
        "disease_count": 0,
        "genetic_test_count": 0,
        "tests_with_gene_info": 0,
        "tests_with_disease_info": 0,
        "complete_test_count": 0,
        "frequent_test_count": 0,
        "has_clinical_test": False,
        "has_multiple_tests": False,
        "has_comprehensive_testing": False,
        "is_well_tested_gene": False,
        "test_availability_category": "none",
        "test_accessibility_score": 0,
        "clinical_utility_score": 0,
        "test_quality_score": 0,
        "total_disease_count": 0,
        "has_cancer_disease": False,
        "has_cardiovascular_disease": False,
        "has_neurological_disease": False,
        "disease_test_correlation": "None",
        "multi_disease_testing": False,
        "pathogenic_variants_in_tested_gene": 0,
        "test_covered_variants": 0,
        "variant_test_coverage_level": "No_Coverage",
        "cancer_mutation_count": 0,
        "cancer_samples": 0,
        "is_cancer_panel_gene": False,
        "hereditary_cancer_testing": False,
        "rare_pathogenic_variants": 0,
        "carrier_screening_relevant": False,
        "population_test_priority": "Low"
    })
)

print(f"Genes with test features: {df_with_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Scores
print("\nADDING ENHANCED SCORES")
print("="*80)

df_enhanced_scores = (
    df_with_genes
    # Clinical test utility score (enhanced with disease+variant)
    .withColumn("clinical_test_utility_score",
                col("clinical_utility_score") +
                when(col("multi_disease_testing"), 5).otherwise(0) +
                when(col("pathogenic_variants_in_tested_gene") >= 10, 8).otherwise(0))
    
    # Variant-test coverage score
    .withColumn("variant_test_coverage_score",
                (col("pathogenic_variants_in_tested_gene") * 2) +
                when(col("variant_test_coverage_level") == "High_Coverage", 10).otherwise(0))
    
    # Population test relevance score
    .withColumn("population_test_relevance_score",
                when(col("carrier_screening_relevant"), 10).otherwise(0) +
                when(col("hereditary_cancer_testing"), 8).otherwise(0) +
                (col("rare_pathogenic_variants") * 1))
)

print("Enhanced scores calculated")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Priority Classification
print("\nADDING ENHANCED PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_enhanced_scores
    .withColumn("test_priority",
                when(col("clinical_test_utility_score") >= 30, lit("critical"))
                .when(col("clinical_test_utility_score") >= 20, lit("high"))
                .when(col("clinical_test_utility_score") >= 10, lit("medium"))
                .otherwise(lit("low")))
    
    .withColumn("is_high_priority_test_gene",
                when((col("has_comprehensive_testing")) & 
                     (col("is_well_tested_gene")), True).otherwise(False))
    
    # Enhanced: Test type classification
    .withColumn("primary_test_type",
                when(col("is_cancer_panel_gene") & col("hereditary_cancer_testing"),
                     lit("Hereditary_Cancer_Panel"))
                .when(col("carrier_screening_relevant"),
                     lit("Carrier_Screening"))
                .when(col("multi_disease_testing"),
                     lit("Multi_Disease_Panel"))
                .when(col("has_clinical_test"),
                     lit("Standard_Clinical"))
                .otherwise(lit("No_Testing")))
    
    # Enhanced: Test recommendation tier
    .withColumn("test_recommendation_tier",
                when(col("clinical_test_utility_score") >= 30, lit("Tier_1_Strongly_Recommended"))
                .when(col("clinical_test_utility_score") >= 20, lit("Tier_2_Recommended"))
                .when(col("clinical_test_utility_score") >= 10, lit("Tier_3_Consider"))
                .otherwise(lit("Tier_4_Not_Indicated")))
)

print("Enhanced priority classification added")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        # Gene identifiers
        col("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        
        # Gene type flags
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        
        # Test statistics
        col("total_test_count"),
        col("unique_test_count"),
        col("disease_count"),
        col("genetic_test_count"),
        col("tests_with_gene_info"),
        col("tests_with_disease_info"),
        col("complete_test_count"),
        col("frequent_test_count"),
        
        # Test classifications
        col("has_clinical_test"),
        col("has_multiple_tests"),
        col("has_comprehensive_testing"),
        col("is_well_tested_gene"),
        col("test_availability_category"),
        
        # Base scores
        col("test_accessibility_score"),
        col("clinical_utility_score"),
        col("test_quality_score"),
        
        # Disease context
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("disease_test_correlation"),
        col("multi_disease_testing"),
        
        # Variant context
        col("pathogenic_variants_in_tested_gene"),
        col("test_covered_variants"),
        col("variant_test_coverage_level"),
        
        # Cancer context
        col("cancer_mutation_count"),
        col("cancer_samples"),
        col("is_cancer_panel_gene"),
        col("hereditary_cancer_testing"),
        
        # Population context
        col("rare_pathogenic_variants"),
        col("carrier_screening_relevant"),
        col("population_test_priority"),
        
        # Enhanced scores
        col("clinical_test_utility_score"),
        col("variant_test_coverage_score"),
        col("population_test_relevance_score"),
        
        # Classifications
        col("test_priority"),
        col("is_high_priority_test_gene"),
        col("primary_test_type"),
        col("test_recommendation_tier")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} genes")
print(f"Total columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by Gene Symbol
print("\nDEDUPLICATING BY GENE_SYMBOL")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["gene_symbol"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Genetic Test Features
print("\nSAVING GOLD GENETIC TEST FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_test_availability_ml_features")

print(f"Saved: {catalog_name}.gold.gene_test_availability_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nGENETIC TEST FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.gene_test_availability_ml_features").count()
print(f"\nTable created: {result_count:,} genes")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Test statistics: 8 features")
print("  - Test classifications: 5 features")
print("  - Base scores: 3 features")
print("  - Disease context: 6 features")
print("  - Variant context: 3 features")
print("  - Cancer context: 4 features")
print("  - Population context: 3 features")
print("  - Enhanced scores: 3 features")
print("  - Classifications: 4 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.gene_test_availability_ml_features") \
    .groupBy("test_priority") \
    .count() \
    .orderBy("test_priority") \
    .show()

print("\nTest recommendation tier:")
spark.table(f"{catalog_name}.gold.gene_test_availability_ml_features") \
    .groupBy("test_recommendation_tier") \
    .count() \
    .orderBy("test_recommendation_tier") \
    .show()

print("\nPrimary test type:")
spark.table(f"{catalog_name}.gold.gene_test_availability_ml_features") \
    .groupBy("primary_test_type") \
    .count() \
    .orderBy("primary_test_type") \
    .show()

print("\nProcessing complete")
