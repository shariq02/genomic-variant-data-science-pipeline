# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - POPULATION FREQUENCY ANALYSIS (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Variant-Level Population Frequency Features
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC 
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive population frequency profiling
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Population Carrier Screening
# MAGIC - Use Case 19: Ancestry-Specific Risk
# MAGIC 
# MAGIC **Creates:** gold.variant_population_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, when, lit, trim, upper, coalesce, concat_ws, sum as spark_sum
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD POPULATION FREQUENCY FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core tables
df_pop_freq = spark.table(f"{catalog_name}.silver.population_frequencies")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

# Additional enrichment tables
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")

print(f"Population frequencies: {df_pop_freq.count():,}")
print(f"Variants: {df_variants.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Frequency Classification
print("\nADDING FREQUENCY CLASSIFICATION")
print("="*80)

df_classified = (
    df_pop_freq
    .withColumn("allele_frequency",
                coalesce(col("allele_frequency_global"), lit(0.0)))
    
    .withColumn("is_ultra_rare_variant",
                when(col("allele_frequency") < 0.0001, True).otherwise(False))
    
    .withColumn("is_very_rare_variant",
                when((col("allele_frequency") >= 0.0001) & 
                     (col("allele_frequency") < 0.001), True).otherwise(False))
    
    .withColumn("is_rare_variant",
                when((col("allele_frequency") >= 0.001) & 
                     (col("allele_frequency") < 0.01), True).otherwise(False))
    
    .withColumn("is_low_frequency_variant",
                when((col("allele_frequency") >= 0.01) & 
                     (col("allele_frequency") < 0.05), True).otherwise(False))
    
    .withColumn("is_common_variant",
                when(col("allele_frequency") >= 0.05, True).otherwise(False))
    
    .withColumn("frequency_tier",
                when(col("allele_frequency") < 0.0001, lit("ultra_rare"))
                .when(col("allele_frequency") < 0.001, lit("very_rare"))
                .when(col("allele_frequency") < 0.01, lit("rare"))
                .when(col("allele_frequency") < 0.05, lit("low_frequency"))
                .otherwise(lit("common")))
)

print("Frequency classification added")

# COMMAND ----------

# DBTITLE 1,Calculate Rarity Scores
print("\nCALCULATING RARITY SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("rarity_score",
                when(col("allele_frequency") < 0.0001, 10)
                .when(col("allele_frequency") < 0.001, 8)
                .when(col("allele_frequency") < 0.01, 6)
                .when(col("allele_frequency") < 0.05, 4)
                .otherwise(2))
    
    .withColumn("carrier_risk_score",
                when((col("is_ultra_rare_variant")) | (col("is_very_rare_variant")), 10)
                .when(col("is_rare_variant"), 7)
                .when(col("is_low_frequency_variant"), 5)
                .otherwise(2))
    
    .withColumn("pathogenicity_likelihood_score",
                when(col("is_ultra_rare_variant"), 9)
                .when(col("is_very_rare_variant"), 7)
                .when(col("is_rare_variant"), 5)
                .when(col("is_low_frequency_variant"), 3)
                .otherwise(1))
)

print("Rarity scores calculated")

# COMMAND ----------

# DBTITLE 1,Join with Variant Clinical Data
print("\nJOINING WITH VARIANT CLINICAL DATA")
print("="*80)

df_with_variants = (
    df_scored
    .join(
        df_variants.select(
            col("variant_id"),
            upper(trim(col("gene_name"))).alias("gene_symbol"),
            col("clinical_significance_simple"),
            col("is_pathogenic"),
            col("is_benign"),
            col("is_vus"),
            col("is_germline"),
            col("is_somatic")
        ),
        on="variant_id",
        how="left"
    )
)

print(f"Variants with population frequency: {df_with_variants.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Pathogenicity for Frequency Correlation
print("\nENRICHING WITH PATHOGENICITY")
print("="*80)

pathogenicity_context = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("is_pathogenic").alias("clinvar_pathogenic"),
        col("is_benign").alias("clinvar_benign"),
        col("pathogenicity_score"),
        col("conservation_level")
    )
)

df_with_variants = (
    df_with_variants
    .join(pathogenicity_context, "variant_id", "left")
    .fillna({
        "clinvar_pathogenic": False,
        "clinvar_benign": False,
        "pathogenicity_score": 0,
        "conservation_level": 0
    })
    
    # Pathogenicity-frequency conflict detection
    .withColumn("pathogenicity_frequency_conflict",
                col("is_common_variant") & col("clinvar_pathogenic"))
    
    .withColumn("rare_pathogenic_variant",
                (col("is_rare_variant") | col("is_ultra_rare_variant")) & col("clinvar_pathogenic"))
    
    .withColumn("common_benign_validation",
                col("is_common_variant") & col("clinvar_benign"))
)

print("Pathogenicity enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Gene Context
print("\nENRICHING WITH GENE CONTEXT")
print("="*80)

# Gene-level variant tolerance
gene_variant_tolerance = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("total_gene_variants"),
        spark_sum(when(col("is_loss_of_function"), 1).otherwise(0)).alias("lof_variants")
    )
    .withColumn("gene_mutation_tolerance",
                when(col("lof_variants") >= 50, lit("LoF_Tolerant"))
                .when(col("lof_variants") >= 10, lit("LoF_Moderate"))
                .when(col("lof_variants") >= 1, lit("LoF_Sensitive"))
                .otherwise(lit("No_LoF")))
    .withColumn("gene_constraint_score",
                100.0 / (col("lof_variants") + 1))
)

df_with_variants = (
    df_with_variants
    .join(gene_variant_tolerance, "gene_symbol", "left")
    .fillna({
        "total_gene_variants": 0,
        "lof_variants": 0,
        "gene_mutation_tolerance": "Unknown",
        "gene_constraint_score": 0.0
    })
)

print("Gene context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Context (Disease Prevalence)
print("\nENRICHING WITH DISEASE CONTEXT")
print("="*80)

disease_frequency = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count")
    )
    .withColumn("disease_allele_frequency",
                when(col("total_disease_count") >= 10, lit("High_Disease_Burden"))
                .when(col("total_disease_count") >= 5, lit("Medium_Disease_Burden"))
                .when(col("total_disease_count") >= 1, lit("Low_Disease_Burden"))
                .otherwise(lit("No_Disease")))
)

df_with_variants = (
    df_with_variants
    .join(disease_frequency, "gene_symbol", "left")
    .fillna({
        "total_disease_count": 0,
        "disease_allele_frequency": "No_Disease"
    })
    
    .withColumn("carrier_frequency_by_disease",
                when(col("rare_pathogenic_variant") & (col("total_disease_count") >= 5),
                     lit("High_Risk_Carrier"))
                .when(col("rare_pathogenic_variant") & (col("total_disease_count") >= 1),
                     lit("Medium_Risk_Carrier"))
                .otherwise(lit("Low_Risk_Carrier")))
)

print("Disease context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context (Somatic Frequency)
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

# Check if variant appears in cancer mutations (somatic)
cancer_frequency = (
    df_cancer
    .withColumn("cancer_variant_key",
                concat_ws(":", col("chromosome"), col("position"),
                         col("reference_allele"), col("alternate_allele")))
    .groupBy("cancer_variant_key")
    .agg(
        countDistinct("tumor_sample").alias("somatic_frequency")
    )
)

df_with_variants = (
    df_with_variants
    .withColumn("variant_key",
                concat_ws(":", col("chromosome"), col("position"),
                         col("reference_allele"), col("alternate_allele")))
    .join(
        cancer_frequency.select(
            col("cancer_variant_key").alias("variant_key"),
            "somatic_frequency"
        ),
        "variant_key",
        "left"
    )
    .fillna({"somatic_frequency": 0})
    
    .withColumn("germline_cancer_predisposition",
                (col("is_rare_variant") | col("is_ultra_rare_variant")) & 
                (col("somatic_frequency") > 0))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Context
print("\nENRICHING WITH EXPRESSION CONTEXT")
print("="*80)

expression_frequency = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
    .groupBy(col("gene_symbol"))
    .agg(
        countDistinct("tissue_type").alias("expression_tissues")
    )
    .withColumn("expression_frequency_correlation",
                when(col("expression_tissues") >= 20, lit("Ubiquitous_Expression"))
                .when(col("expression_tissues") >= 10, lit("Broad_Expression"))
                .otherwise(lit("Tissue_Specific_Expression")))
)

df_with_variants = (
    df_with_variants
    .join(expression_frequency, "gene_symbol", "left")
    .fillna({
        "expression_tissues": 0,
        "expression_frequency_correlation": "Unknown"
    })
    
    .withColumn("tissue_specific_allele_effects",
                col("expression_frequency_correlation") == "Tissue_Specific_Expression")
)

print("Expression context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Add Clinical Actionability Classification
print("\nADDING CLINICAL ACTIONABILITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_with_variants
    .withColumn("is_clinically_actionable_rare_variant",
                when((col("is_rare_variant") | col("is_ultra_rare_variant")) & 
                     (col("is_pathogenic")), True).otherwise(False))
    
    .withColumn("is_carrier_screening_candidate",
                when((col("is_rare_variant")) & 
                     (col("is_germline")), True).otherwise(False))
    
    .withColumn("population_priority",
                when(col("is_clinically_actionable_rare_variant"), lit("high"))
                .when((col("is_rare_variant")) & (col("is_vus")), lit("medium"))
                .otherwise(lit("low")))
    
    .withColumn("screening_recommendation",
                when(col("is_clinically_actionable_rare_variant"), lit("recommended"))
                .when(col("is_carrier_screening_candidate"), lit("consider"))
                .otherwise(lit("not_indicated")))
)

print("Clinical actionability added")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Scores
print("\nADDING ENHANCED SCORES")
print("="*80)

df_enhanced_scores = (
    df_priority
    # Clinical significance-frequency score
    .withColumn("clinical_significance_frequency_score",
                col("pathogenicity_score") * 0.5 +
                col("rarity_score") * 0.3 +
                when(col("rare_pathogenic_variant"), 10).otherwise(0))
    
    # Carrier risk score adjusted by gene constraint
    .withColumn("carrier_risk_score_adjusted",
                col("carrier_risk_score") +
                when(col("gene_mutation_tolerance") == "LoF_Sensitive", 5).otherwise(0) +
                (col("gene_constraint_score") * 0.1))
    
    # Pathogenicity likelihood refined with conservation
    .withColumn("pathogenicity_likelihood_refined",
                col("pathogenicity_likelihood_score") +
                (col("conservation_level") * 2) +
                when(col("pathogenicity_frequency_conflict"), -5).otherwise(0))
)

print("Enhanced scores calculated")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_enhanced_scores
    .select(
        # Identifiers
        col("variant_id"),
        col("variant_key"),
        col("gene_symbol"),
        col("gene_name"),
        col("chromosome"),
        col("position"),
        col("reference_allele"),
        col("alternate_allele"),
        
        # Frequency statistics
        col("allele_frequency"),
        col("frequency_category"),
        
        # Frequency classifications
        col("is_ultra_rare_variant"),
        col("is_very_rare_variant"),
        col("is_rare_variant"),
        col("is_low_frequency_variant"),
        col("is_common_variant"),
        col("frequency_tier"),
        
        # Clinical significance
        col("clinical_significance_simple").alias("clinical_significance"),
        col("is_pathogenic"),
        col("is_benign"),
        col("is_vus"),
        col("is_germline"),
        col("is_somatic"),
        
        # Base scores
        col("rarity_score"),
        col("carrier_risk_score"),
        col("pathogenicity_likelihood_score"),
        
        # Pathogenicity context
        col("clinvar_pathogenic"),
        col("clinvar_benign"),
        col("pathogenicity_score"),
        col("conservation_level"),
        col("pathogenicity_frequency_conflict"),
        col("rare_pathogenic_variant"),
        col("common_benign_validation"),
        
        # Gene context
        col("total_gene_variants"),
        col("lof_variants"),
        col("gene_mutation_tolerance"),
        col("gene_constraint_score"),
        
        # Disease context
        col("total_disease_count"),
        col("disease_allele_frequency"),
        col("carrier_frequency_by_disease"),
        
        # Cancer context
        col("somatic_frequency"),
        col("germline_cancer_predisposition"),
        
        # Expression context
        col("expression_tissues"),
        col("expression_frequency_correlation"),
        col("tissue_specific_allele_effects"),
        
        # Classifications
        col("is_clinically_actionable_rare_variant"),
        col("is_carrier_screening_candidate"),
        col("population_priority"),
        col("screening_recommendation"),
        
        # Enhanced scores
        col("clinical_significance_frequency_score"),
        col("carrier_risk_score_adjusted"),
        col("pathogenicity_likelihood_refined")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} variants")
print(f"Total columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant ID
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["variant_id"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Population Frequency Features
print("\nSAVING GOLD POPULATION FREQUENCY FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_population_ml_features")

print(f"Saved: {catalog_name}.gold.variant_population_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nPOPULATION FREQUENCY FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.variant_population_ml_features").count()
print(f"\nTable created: {result_count:,} variants")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Frequency statistics: 7 features")
print("  - Clinical significance: 6 features")
print("  - Base scores: 3 features")
print("  - Pathogenicity context: 7 features")
print("  - Gene context: 4 features")
print("  - Disease context: 3 features")
print("  - Cancer context: 2 features")
print("  - Expression context: 3 features")
print("  - Classifications: 4 features")
print("  - Enhanced scores: 3 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nFrequency tier breakdown:")
spark.table(f"{catalog_name}.gold.variant_population_ml_features") \
    .groupBy("frequency_tier") \
    .count() \
    .orderBy("frequency_tier") \
    .show()

print("\nPopulation priority breakdown:")
spark.table(f"{catalog_name}.gold.variant_population_ml_features") \
    .groupBy("population_priority") \
    .count() \
    .orderBy("population_priority") \
    .show()

print("\nClinically actionable rare variants:")
actionable = spark.table(f"{catalog_name}.gold.variant_population_ml_features") \
    .filter(col("is_clinically_actionable_rare_variant")).count()
print(f"  Count: {actionable:,}")

print("\nProcessing complete")
print("="*80)
print("ALL 7 GOLD NOTEBOOKS (23-29) NOW FULLY ENHANCED!")
print("="*80)
