# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - DRUG RESPONSE ANALYSIS (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Variant-Level Drug Response Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive drug response profiling
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 11: Treatment Response Prediction
# MAGIC
# MAGIC **Creates:** gold.variant_drug_response_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max,
    when, lit, trim, upper, lower, coalesce, split, size, array_contains, row_number, avg
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD DRUG RESPONSE FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core pharmacogene tables
df_pharmgkb_variants = spark.table(f"{catalog_name}.silver.pharmgkb_variants")
df_relationships = spark.table(f"{catalog_name}.silver.pharmgkb_relationships")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

# Additional enrichment tables
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_population = spark.table(f"{catalog_name}.silver.population_frequencies")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")

print(f"PharmGKB variants: {df_pharmgkb_variants.count():,}")
print(f"PharmGKB relationships: {df_relationships.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Variants ultra enriched: {df_variants.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Population frequencies: {df_population.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Extract Variant-Drug Relationships
print("\nEXTRACTING VARIANT-DRUG RELATIONSHIPS")
print("="*80)

df_variant_relationships = (
    df_relationships
    .filter(col("entity1_type") == "Variant")
    .select(
        col("entity1_id").alias("variant_pharmgkb_id"),
        col("entity1_name").alias("variant_name_rel"),
        col("entity2_type").alias("related_entity_type"),
        col("entity2_name").alias("related_entity_name"),
        col("evidence")
    )
)

df_variant_drug_counts = (
    df_variant_relationships
    .groupBy("variant_pharmgkb_id")
    .agg(
        count("*").alias("total_interactions"),
        countDistinct("related_entity_type").alias("interaction_type_count"),
        spark_sum(when(col("related_entity_type") == "Chemical", 1).otherwise(0)).alias("drug_interaction_count"),  
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_interaction_count"),
        spark_sum(when(col("evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count")
    )
)

print(f"Variants with interactions: {df_variant_drug_counts.count():,}")

# COMMAND ----------

# DBTITLE 1,Process PharmGKB Variant Annotations
print("\nPROCESSING PHARMGKB VARIANT ANNOTATIONS")
print("="*80)

df_pharmgkb_features = (
    df_pharmgkb_variants
    .select(
        col("variant_id").alias("variant_pharmgkb_id"),
        col("variant_name"),
        upper(trim(col("gene_symbols"))).alias("gene_symbol"),
        col("location").alias("variant_location")
    )
    .withColumn("has_annotation", lit(True))
)

print(f"PharmGKB variant features: {df_pharmgkb_features.count():,}")

# COMMAND ----------

# DBTITLE 1,Join with Variant Protein Impact
print("\nJOINING WITH VARIANT PROTEIN IMPACT")
print("="*80)

df_variant_impact_prep = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("variant_name").alias("clinvar_variant_name"),
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("clinical_significance_simple"),
        col("is_pathogenic"),
        col("is_benign"),
        col("is_vus"),
        col("is_missense_variant"),
        col("is_frameshift_variant"),
        col("is_nonsense_variant"),
        col("is_splice_variant"),
        col("has_functional_domain"),
        col("affects_functional_domain"),
        col("phylop_score"),
        col("cadd_phred"),
        col("conservation_level"),
        col("pathogenicity_score"),
        col("mutation_severity_score")
    )
)

# RIGHT JOIN to keep all ClinVar variants + add PharmGKB annotations
df_with_impact = (
    df_pharmgkb_features
    .join(df_variant_impact_prep, on="gene_symbol", how="right")
)

# Deduplicate by variant_id, keeping best PharmGKB annotation
window_spec = Window.partitionBy("variant_id").orderBy(col("variant_pharmgkb_id").desc_nulls_last())
df_with_impact = (
    df_with_impact
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print(f"Variants with impact: {df_with_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Data (Tissue-Specific Drug Effects)
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Gene-level expression for tissue-specific drug metabolism
gene_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
    .groupBy(col("gene_symbol"))
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("median_tpm").alias("max_expression_tpm")
    )
    # Check liver and kidney expression
    .join(
        df_gtex.filter((col("tissue_type") == "Liver") & (col("median_tpm") > 1.0))
               .select(col("gene_symbol").alias("liver_gene"), lit(True).alias("is_liver_expressed")),
        col("gene_symbol") == col("liver_gene"),
        "left"
    )
    .drop("liver_gene")
    .fillna({"is_liver_expressed": False})
    .withColumn("expression_breadth",
                when(col("tissues_expressed_count") >= 15, lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5, lit("Broad"))
                .otherwise(lit("Tissue_Specific")))
)

df_with_impact = (
    df_with_impact
    .join(gene_expression, "gene_symbol", "left")
    .fillna({
        "tissues_expressed_count": 0,
        "max_expression_tpm": 0.0,
        "is_liver_expressed": False,
        "expression_breadth": "Unknown"
    })
)

print("Expression data enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Population Frequencies
print("\nENRICHING WITH POPULATION FREQUENCIES")
print("="*80)

df_with_impact = (
    df_with_impact
    .join(
        df_population.select(
            "variant_id",
            col("global_af").alias("allele_frequency"),
            col("is_common").alias("is_common_variant"),
            col("is_rare").alias("is_rare_variant")
        ),
        "variant_id",
        "left"
    )
    .fillna({
        "allele_frequency": 0.0,
        "is_common_variant": False,
        "is_rare_variant": False
    })
    
    # Drug response frequency context
    .withColumn("drug_response_frequency_context",
                when(col("is_common_variant"), lit("Common_Drug_Response"))
                .when(col("is_rare_variant"), lit("Rare_Drug_Response"))
                .otherwise(lit("Standard_Frequency")))
)

print("Population frequency enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Associations (Drug Indications)
print("\nENRICHING WITH DISEASE ASSOCIATIONS")
print("="*80)

df_with_impact = (
    df_with_impact
    .join(
        df_gene_disease.select(
            upper(trim(col("gene_name"))).alias("gene_symbol"),
            col("total_disease_count"),
            col("has_cancer_disease"),
            col("has_cardiovascular_disease"),
            col("has_neurological_disease")
        ),
        "gene_symbol",
        "left"
    )
    .fillna({
        "total_disease_count": 0,
        "has_cancer_disease": False,
        "has_cardiovascular_disease": False,
        "has_neurological_disease": False
    })
    
    # Primary indication
    .withColumn("primary_indication_category",
                when(col("has_cancer_disease"), lit("Oncology"))
                .when(col("has_cardiovascular_disease"), lit("Cardiology"))
                .when(col("has_neurological_disease"), lit("Neurology"))
                .otherwise(lit("Other")))
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context (Drug Resistance)
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

# Check if variant's gene is a cancer gene (potential resistance variants)
cancer_genes = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count")
    )
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
)

df_with_impact = (
    df_with_impact
    .join(cancer_genes, "gene_symbol", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_cancer_gene": False
    })
    
    .withColumn("is_potential_resistance_variant",
                col("is_cancer_gene") & col("is_missense_variant"))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Gene Context
print("\nENRICHING WITH GENE CONTEXT")
print("="*80)

df_with_impact = (
    df_with_impact
    .join(
        df_genes.select(
            upper(trim(col("official_symbol"))).alias("gene_symbol"),
            col("is_pharmacogene"),
            col("druggability_score"),
            col("pharmacogene_category"),
            col("drug_metabolism_role")
        ),
        "gene_symbol",
        "left"
    )
    .fillna({
        "is_pharmacogene": False,
        "druggability_score": 0.0,
        "pharmacogene_category": "Unknown",
        "drug_metabolism_role": "Unknown"
    })
)

print("Gene context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Classification Flags
print("\nADDING ENHANCED CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_with_impact
    .withColumn("has_pharmgkb_annotation",
                when(col("variant_pharmgkb_id").isNotNull(), True).otherwise(False))
    
    .withColumn("has_high_conservation",
                when(col("conservation_level") >= 1, True).otherwise(False))
    
    .withColumn("affects_drug_metabolism",
                when((col("has_pharmgkb_annotation")) & 
                     (col("has_functional_domain")), True).otherwise(False))
    
    .withColumn("affects_drug_efficacy",
                when((col("has_pharmgkb_annotation")) & 
                     (col("is_missense_variant") | col("affects_functional_domain")), True).otherwise(False))
    
    .withColumn("is_high_impact_variant",
                when((col("is_pathogenic")) & (col("has_pharmgkb_annotation")), True).otherwise(False))
    
    # Enhanced: Tissue-specific drug metabolism
    .withColumn("is_hepatic_drug_metabolism_variant",
                col("is_liver_expressed") & col("affects_drug_metabolism"))
    
    # Enhanced: Population-specific drug response
    .withColumn("is_common_pharmacogene_variant",
                col("is_common_variant") & col("has_pharmgkb_annotation"))
)

print("Enhanced classification flags added")

# COMMAND ----------

# DBTITLE 1,Calculate Comprehensive Scores
print("\nCALCULATING COMPREHENSIVE SCORES")
print("="*80)

df_scored = (
    df_classified
    # Base pharmacogene annotation score
    .withColumn("pharmacogene_annotation_score",
                when(col("has_pharmgkb_annotation"), 10).otherwise(0))
    
    # Functional impact score (enhanced with conservation)
    .withColumn("functional_impact_score",
                when(col("affects_functional_domain"), 5).otherwise(0) +
                when(col("is_missense_variant"), 3).otherwise(0) +
                when(col("is_nonsense_variant"), 5).otherwise(0) +
                when(col("is_frameshift_variant"), 5).otherwise(0) +
                coalesce(col("conservation_level"), lit(0)) +
                coalesce(col("mutation_severity_score"), lit(0)))
    
    # Pathogenicity score
    .withColumn("pathogenicity_score",
                when(col("is_pathogenic"), 10)
                .when(col("is_benign"), -5)
                .when(col("is_vus"), 0)
                .otherwise(0))
    
    # Enhanced: Population-adjusted score
    .withColumn("population_adjusted_score",
                when(col("is_common_pharmacogene_variant"), 10).otherwise(0) +
                when(col("is_rare_variant") & col("has_pharmgkb_annotation"), 7).otherwise(0))
    
    # Enhanced: Tissue-specific response score
    .withColumn("tissue_specific_response_score",
                when(col("is_hepatic_drug_metabolism_variant"), 10).otherwise(0) +
                when(col("is_liver_expressed") & col("has_pharmgkb_annotation"), 5).otherwise(0))
    
    # Drug response priority score (enhanced formula)
    .withColumn("drug_response_priority_score",
                col("pharmacogene_annotation_score") * 0.4 +
                col("functional_impact_score") * 0.2 +
                col("pathogenicity_score") * 0.1 +
                col("population_adjusted_score") * 0.15 +
                col("tissue_specific_response_score") * 0.15)
)

print("Comprehensive scores calculated")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Priority Classification
print("\nADDING ENHANCED PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_scored
    .withColumn("drug_response_priority",
                when(col("drug_response_priority_score") >= 20, lit("critical"))
                .when(col("drug_response_priority_score") >= 15, lit("high"))
                .when(col("drug_response_priority_score") >= 8, lit("medium"))
                .otherwise(lit("low")))
    
    .withColumn("is_actionable_pharmacogene_variant",
                when(col("has_pharmgkb_annotation"), True).otherwise(False))
    
    .withColumn("drug_response_category",
                when(col("is_hepatic_drug_metabolism_variant"), lit("hepatic_metabolism"))
                .when(col("affects_drug_metabolism"), lit("metabolism"))
                .when(col("affects_drug_efficacy"), lit("efficacy"))
                .when(col("is_potential_resistance_variant"), lit("resistance"))
                .when(col("has_pharmgkb_annotation"), lit("pharmacogene_variant"))
                .otherwise(lit("unknown")))
    
    .withColumn("clinical_actionability",
                when((col("is_pathogenic")) & (col("has_pharmgkb_annotation")) & (col("is_pharmacogene")),
                     lit("tier_1_actionable"))
                .when((col("has_pharmgkb_annotation")) & (col("is_pharmacogene")),
                     lit("tier_2_high_evidence"))
                .when(col("has_pharmgkb_annotation"),
                     lit("tier_3_pharmgkb_annotated"))
                .otherwise(lit("tier_4_research_only")))
    
    # Enhanced: Indication-specific actionability
    .withColumn("indication_specific_actionability",
                when(col("primary_indication_category") != "Other", 
                     lit(True)).otherwise(lit(False)))
)

print("Enhanced priority classification added")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        # Identifiers
        col("variant_pharmgkb_id"),
        coalesce(col("variant_name"), col("clinvar_variant_name")).alias("variant_name"),
        col("variant_id"),
        col("gene_symbol"),
        col("variant_location"),
        
        # Clinical significance
        col("clinical_significance_simple"),
        col("is_pathogenic"),
        col("is_benign"),
        col("is_vus"),
        
        # Variant types
        col("is_missense_variant"),
        col("is_frameshift_variant"),
        col("is_nonsense_variant"),
        col("is_splice_variant"),
        
        # Functional impact
        col("has_functional_domain"),
        col("affects_functional_domain"),
        col("phylop_score"),
        col("cadd_phred"),
        col("conservation_level"),
        col("pathogenicity_score"),
        col("mutation_severity_score"),
        
        # PharmGKB flags
        col("has_pharmgkb_annotation"),
        col("has_high_conservation"),
        col("affects_drug_metabolism"),
        col("affects_drug_efficacy"),
        col("is_high_impact_variant"),
        
        # Enhanced flags
        col("is_hepatic_drug_metabolism_variant"),
        col("is_common_pharmacogene_variant"),
        col("is_potential_resistance_variant"),
        
        # Expression context
        col("tissues_expressed_count"),
        col("max_expression_tpm"),
        col("is_liver_expressed"),
        col("expression_breadth"),
        
        # Population context
        col("allele_frequency"),
        col("is_common_variant"),
        col("is_rare_variant"),
        col("drug_response_frequency_context"),
        
        # Disease context
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("primary_indication_category"),
        
        # Cancer context
        col("cancer_mutation_count"),
        col("is_cancer_gene"),
        
        # Gene context
        col("is_pharmacogene"),
        col("druggability_score"),
        col("pharmacogene_category"),
        col("drug_metabolism_role"),
        
        # Scores
        col("pharmacogene_annotation_score"),
        col("functional_impact_score"),
        col("population_adjusted_score"),
        col("tissue_specific_response_score"),
        col("drug_response_priority_score"),
        
        # Classifications
        col("drug_response_priority"),
        col("is_actionable_pharmacogene_variant"),
        col("drug_response_category"),
        col("clinical_actionability"),
        col("indication_specific_actionability")
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

# DBTITLE 1,Save Gold Drug Response Features
print("\nSAVING GOLD DRUG RESPONSE FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_drug_response_ml_features")

print(f"Saved: {catalog_name}.gold.variant_drug_response_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nDRUG RESPONSE FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.variant_drug_response_ml_features").count()
print(f"\nTable created: {result_count:,} variants")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Base pharmacogene: 10 features")
print("  - Expression context: 4 features")
print("  - Population context: 4 features")
print("  - Disease associations: 6 features")
print("  - Cancer context: 2 features")
print("  - Gene context: 4 features")
print("  - Enhanced scores: 5 features")
print("  - Classifications: 5 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.variant_drug_response_ml_features") \
    .groupBy("drug_response_priority") \
    .count() \
    .orderBy("drug_response_priority") \
    .show()

print("\nClinical actionability:")
spark.table(f"{catalog_name}.gold.variant_drug_response_ml_features") \
    .groupBy("clinical_actionability") \
    .count() \
    .orderBy("clinical_actionability") \
    .show()

print("\nProcessing complete")
