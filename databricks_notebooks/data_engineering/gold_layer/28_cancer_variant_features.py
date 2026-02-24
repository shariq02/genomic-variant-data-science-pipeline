# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - CANCER VARIANT ANALYSIS (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Variant and Gene-Level Cancer Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive cancer variant profiling
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 12: Cancer Variant Classification
# MAGIC
# MAGIC **Creates:** gold.variant_cancer_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    when, lit, trim, upper, lower, coalesce, concat_ws, max as spark_max
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD CANCER VARIANT FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core tables
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # ENHANCED: enriched genes

# Additional enrichment tables
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_population = spark.table(f"{catalog_name}.silver.population_frequencies")

print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")
print(f"Population frequencies: {df_population.count():,}")

# COMMAND ----------

# DBTITLE 1,Create Variant-Level Features
print("\nCREATING VARIANT-LEVEL FEATURES")
print("="*80)

df_variant_cancer = (
    df_cancer
    .withColumn("variant_key",
                concat_ws(":", col("chromosome"), col("position"), 
                         col("reference_allele"), col("alternate_allele")))
    
    .groupBy("gene_symbol", "variant_key", "chromosome", "position", 
             "reference_allele", "alternate_allele")
    .agg(
        count("tumor_sample").alias("sample_count"),
        spark_sum("mutation_count").alias("total_mutation_count"),
        countDistinct("variant_class").alias("variant_class_count"),
        countDistinct("variant_type").alias("variant_type_count"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_sample_count"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("truncating_sample_count"),
        spark_sum(when(col("is_silent"), 1).otherwise(0)).alias("silent_sample_count"),
        spark_sum(when(col("is_snv"), 1).otherwise(0)).alias("snv_sample_count"),
        spark_sum(when(col("is_indel"), 1).otherwise(0)).alias("indel_sample_count"),
        spark_sum(when(col("is_frequently_mutated"), 1).otherwise(0)).alias("frequent_mutation_samples")
    )
)

print(f"Unique cancer variants: {df_variant_cancer.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Variant Classification Flags
print("\nADDING VARIANT CLASSIFICATION FLAGS")
print("="*80)

df_variant_classified = (
    df_variant_cancer
    .withColumn("is_recurrent_mutation",
                when(col("sample_count") >= 3, True).otherwise(False))
    
    .withColumn("is_hotspot_mutation",
                when(col("sample_count") >= 10, True).otherwise(False))
    
    .withColumn("is_high_impact_cancer_variant",
                when((col("truncating_sample_count") > 0) & 
                     (col("sample_count") >= 2), True).otherwise(False))
    
    .withColumn("is_driver_candidate",
                when((col("is_hotspot_mutation")) | 
                     (col("is_high_impact_cancer_variant")), True).otherwise(False))
    
    .withColumn("mutation_frequency_category",
                when(col("sample_count") >= 10, lit("hotspot"))
                .when(col("sample_count") >= 3, lit("recurrent"))
                .when(col("sample_count") >= 2, lit("multiple"))
                .otherwise(lit("rare")))
)

print("Variant classification added")

# COMMAND ----------

# DBTITLE 1,Calculate Gene-Level Cancer Statistics
print("\nCALCULATING GENE-LEVEL CANCER STATISTICS")
print("="*80)

df_gene_cancer = (
    df_cancer
    .groupBy("gene_symbol")
    .agg(
        count("tumor_sample").alias("total_samples_affected"),
        countDistinct("tumor_sample").alias("unique_samples_affected"),
        countDistinct(concat_ws(":", col("chromosome"), col("position"))).alias("unique_mutation_sites"),
        spark_sum("mutation_count").alias("total_mutations"),
        avg("mutation_count").alias("avg_mutations_per_sample"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_mutations"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("truncating_mutations"),
        spark_sum(when(col("is_silent"), 1).otherwise(0)).alias("silent_mutations"),
        spark_sum(when(col("is_frequently_mutated"), 1).otherwise(0)).alias("frequent_mutations")
    )
)

print(f"Genes with cancer mutations: {df_gene_cancer.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Gene-Level Classification
print("\nADDING GENE-LEVEL CLASSIFICATION")
print("="*80)

df_gene_classified = (
    df_gene_cancer
    .withColumn("is_cancer_gene",
                when(col("unique_samples_affected") >= 5, True).otherwise(False))
    
    .withColumn("is_frequently_mutated_gene",
                when(col("unique_mutation_sites") >= 10, True).otherwise(False))
    
    .withColumn("is_tumor_suppressor_candidate",
                when((col("truncating_mutations") > col("missense_mutations")) & 
                     (col("unique_samples_affected") >= 3), True).otherwise(False))
    
    .withColumn("is_oncogene_candidate",
                when((col("missense_mutations") > col("truncating_mutations")) & 
                     (col("unique_samples_affected") >= 5), True).otherwise(False))
    
    .withColumn("gene_cancer_role",
                when(col("is_tumor_suppressor_candidate"), lit("tumor_suppressor"))
                .when(col("is_oncogene_candidate"), lit("oncogene"))
                .when(col("is_cancer_gene"), lit("cancer_associated"))
                .otherwise(lit("other")))
)

print("Gene classification added")

# COMMAND ----------

# DBTITLE 1,Calculate Cancer Scores
print("\nCALCULATING CANCER SCORES")
print("="*80)

df_gene_scored = (
    df_gene_classified
    .withColumn("cancer_mutation_burden_score",
                (col("unique_samples_affected") * 2) +
                (col("unique_mutation_sites") * 1))
    
    .withColumn("functional_impact_score",
                (col("truncating_mutations") * 3) +
                (col("missense_mutations") * 1) -
                (col("silent_mutations") * 0.5))
    
    .withColumn("cancer_priority_score",
                when(col("is_tumor_suppressor_candidate"), 10).otherwise(0) +
                when(col("is_oncogene_candidate"), 10).otherwise(0) +
                (col("unique_samples_affected") * 0.5))
)

print("Scores calculated")

# COMMAND ----------

# DBTITLE 1,Join Variant and Gene Features
print("\nJOINING VARIANT AND GENE FEATURES")
print("="*80)

df_combined = (
    df_variant_classified
    .withColumn("variant_gene_symbol", upper(trim(col("gene_symbol"))))  
    .drop("gene_symbol")
    .join(
        df_gene_scored.select(
            upper(trim(col("gene_symbol"))).alias("gene_symbol"),
            col("total_samples_affected").alias("gene_total_samples"),
            col("unique_mutation_sites").alias("gene_unique_sites"),
            col("is_cancer_gene"),
            col("is_tumor_suppressor_candidate"),
            col("is_oncogene_candidate"),
            col("gene_cancer_role"),
            col("cancer_mutation_burden_score"),
            col("cancer_priority_score")
        ),
        on=col("variant_gene_symbol") == col("gene_symbol"),
        how="left"
    )
    .withColumn("gene_symbol", col("variant_gene_symbol"))
    .drop("variant_gene_symbol")
)

print(f"Combined variant-gene features: {df_combined.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Clinical Variant Impact (Pathogenicity + Conservation)
print("\nENRICHING WITH CLINICAL VARIANT IMPACT")
print("="*80)

clinical_variant_impact = (
    df_variant_impact
    .select(
        concat_ws(":", col("chromosome"), col("position"), 
                 col("reference_allele"), col("alternate_allele")).alias("variant_key"),
        col("clinical_significance_simple").alias("clinvar_pathogenicity"),
        col("is_pathogenic").alias("clinvar_is_pathogenic"),
        col("phylop_score").alias("conservation_score"),
        col("cadd_phred"),
        col("mutation_severity_score").alias("functional_impact_prediction")
    )
)

df_combined = (
    df_combined
    .join(clinical_variant_impact, "variant_key", "left")
    .fillna({
        "clinvar_pathogenicity": "Unknown",
        "clinvar_is_pathogenic": False,
        "conservation_score": 0.0,
        "cadd_phred": 0.0,
        "functional_impact_prediction": 0
    })
)

print("Clinical variant impact enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Context (Tumor Expression)
print("\nENRICHING WITH EXPRESSION CONTEXT")
print("="*80)

tissue_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy(col("gene_name"))
    .agg(
        countDistinct("tissue_type").alias("tissue_expression_in_tumors"),
        spark_max("max_tpm").alias("max_tumor_expression")
    )
    .withColumn("expression_change_relevance",
                when(col("tissue_expression_in_tumors") >= 20, lit("Ubiquitous"))
                .when(col("tissue_expression_in_tumors") >= 10, lit("Broad"))
                .otherwise(lit("Tissue_Specific")))
)

df_combined = (
    df_combined
    .join(tissue_expression, col("gene_symbol") == tissue_expression["gene_name"], "left")
    .drop(tissue_expression["gene_name"])
    .fillna({
        "tissue_expression_in_tumors": 0,
        "max_tumor_expression": 0.0,
        "expression_change_relevance": "Unknown"
    })
)
print(f"Countfeatures: {df_combined.count():,}")
print("Expression context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Context (Cancer Disease Associations)
print("\nENRICHING WITH DISEASE CONTEXT")
print("="*80)

cancer_disease = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("has_cancer_disease").alias("cancer_disease_associations")
    )
)

df_combined = (
    df_combined
    .join(cancer_disease, "gene_symbol", "left")
    .fillna({"cancer_disease_associations": False})
    .withColumn("hereditary_cancer_syndrome",
                col("cancer_disease_associations") & 
                (col("gene_cancer_role").isin("tumor_suppressor", "oncogene")))
)

print("Disease context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Protein Context (Oncogenic Domains)
print("\nENRICHING WITH PROTEIN CONTEXT")
print("="*80)

oncogenic_domains = (
    df_protein_domains
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("has_kinase_domain_count")
    )
    .withColumn("affected_oncogenic_domains",
                col("has_kinase_domain_count") > 0)
    .withColumn("kinase_domain_mutations",
                col("has_kinase_domain_count") >= 1)
)

df_combined = (
    df_combined
    .join(oncogenic_domains, "gene_symbol", "left")
    .fillna({
        "has_kinase_domain_count": 0,
        "affected_oncogenic_domains": False,
        "kinase_domain_mutations": False
    })
)

print("Protein context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Population Context (Somatic vs Germline)
print("\nENRICHING WITH POPULATION CONTEXT")
print("="*80)

germline_frequency = (
    df_population
    .select(
        concat_ws(":", col("chromosome"), col("position"),
                 col("reference_allele"), col("alternate_allele")).alias("variant_key"),
        col("allele_frequency_global").alias("germline_variant_frequency"),
        col("is_rare")
    )
    .withColumn("somatic_vs_germline_classification",
                when(col("germline_variant_frequency") > 0.01, lit("Likely_Germline"))
                .when(col("is_rare"), lit("Rare_Germline"))
                .otherwise(lit("Likely_Somatic")))
)

df_combined = (
    df_combined
    .join(germline_frequency, "variant_key", "left")
    .fillna({
        "germline_variant_frequency": 0.0,
        "is_rare": False,
        "somatic_vs_germline_classification": "Unknown"
    })
)

print(f"Population context enrichment features: {df_combined.count():,}")
print("Population context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Scores
print("\nADDING ENHANCED SCORES")
print("="*80)

df_enhanced_scores = (
    df_combined
    # Driver likelihood score (enhanced with conservation + pathogenicity)
    .withColumn("driver_likelihood_score",
                when(col("is_driver_candidate"), 10).otherwise(0) +
                when(col("clinvar_is_pathogenic"), 8).otherwise(0) +
                when(col("conservation_score") > 2.7, 5).otherwise(0) +
                when(col("affected_oncogenic_domains"), 7).otherwise(0))
    
    # Therapeutic target score
    .withColumn("therapeutic_target_score",
                when(col("is_hotspot_mutation") & col("kinase_domain_mutations"), 15).otherwise(0) +
                when(col("is_oncogene_candidate"), 10).otherwise(0) +
                when(col("affected_oncogenic_domains"), 8).otherwise(0))
    
    # Prognostic value score
    .withColumn("prognostic_value_score",
                when(col("is_tumor_suppressor_candidate") & col("clinvar_is_pathogenic"), 12).otherwise(0) +
                when(col("hereditary_cancer_syndrome"), 10).otherwise(0) +
                (col("sample_count") * 0.5))
)

print("Enhanced scores calculated")

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data
print("\nJOINING WITH GENE MASTER DATA")
print("="*80)

df_with_genes = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome").alias("gene_chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene")
    )
    .join(df_enhanced_scores.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
          on="gene_symbol", how="right")
)

print(f"Final features with gene data: {df_with_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_with_genes
    .select(
        # Identifiers
        col("gene_symbol"),
        col("gene_name"),
        col("variant_key"),
        col("chromosome"),
        col("position"),
        col("reference_allele"),
        col("alternate_allele"),
        
        # Variant statistics
        col("sample_count"),
        col("total_mutation_count"),
        col("missense_sample_count"),
        col("truncating_sample_count"),
        col("silent_sample_count"),
        col("snv_sample_count"),
        col("indel_sample_count"),
        
        # Variant classifications
        col("is_recurrent_mutation"),
        col("is_hotspot_mutation"),
        col("is_high_impact_cancer_variant"),
        col("is_driver_candidate"),
        col("mutation_frequency_category"),
        
        # Gene statistics
        col("gene_total_samples"),
        col("gene_unique_sites"),
        col("is_cancer_gene"),
        col("is_tumor_suppressor_candidate"),
        col("is_oncogene_candidate"),
        col("gene_cancer_role"),
        col("cancer_mutation_burden_score"),
        col("cancer_priority_score"),
        
        # Clinical variant impact
        col("clinvar_pathogenicity"),
        col("clinvar_is_pathogenic"),
        col("conservation_score"),
        col("cadd_phred"),
        col("functional_impact_prediction"),
        
        # Expression context
        col("tissue_expression_in_tumors"),
        col("max_tumor_expression"),
        col("expression_change_relevance"),
        
        # Disease context
        col("cancer_disease_associations"),
        col("hereditary_cancer_syndrome"),
        
        # Protein context
        col("has_kinase_domain_count"),
        col("affected_oncogenic_domains"),
        col("kinase_domain_mutations"),
        
        # Population context
        col("germline_variant_frequency"),
        col("is_rare"),
        col("somatic_vs_germline_classification"),
        
        # Gene type flags
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        
        # Enhanced scores
        col("driver_likelihood_score"),
        col("therapeutic_target_score"),
        col("prognostic_value_score")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} cancer variants")
print(f"Total columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant Key
print("\nDEDUPLICATING BY VARIANT_KEY")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["variant_key"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Cancer Variant Features
print("\nSAVING GOLD CANCER VARIANT FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_cancer_ml_features")

print(f"Saved: {catalog_name}.gold.variant_cancer_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nCANCER VARIANT FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.variant_cancer_ml_features").count()
print(f"\nTable created: {result_count:,} variants")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Variant statistics: 7 features")
print("  - Variant classifications: 5 features")
print("  - Gene statistics: 8 features")
print("  - Clinical variant impact: 5 features")
print("  - Expression context: 3 features")
print("  - Disease context: 2 features")
print("  - Protein context: 3 features")
print("  - Population context: 3 features")
print("  - Enhanced scores: 3 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nMutation frequency breakdown:")
spark.table(f"{catalog_name}.gold.variant_cancer_ml_features") \
    .groupBy("mutation_frequency_category") \
    .count() \
    .orderBy("mutation_frequency_category") \
    .show()

print("\nGene cancer role breakdown:")
spark.table(f"{catalog_name}.gold.variant_cancer_ml_features") \
    .groupBy("gene_cancer_role") \
    .count() \
    .orderBy("gene_cancer_role") \
    .show()

print("\nDriver candidates:")
driver_final = spark.table(f"{catalog_name}.gold.variant_cancer_ml_features") \
    .filter(col("is_driver_candidate")).count()
print(f"  Driver candidates: {driver_final:,}")

print("\nProcessing complete")
