# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PROTEIN FAMILY ANALYSIS (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Gene-Level Protein Family Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive protein family profiling
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Protein Domain Analysis
# MAGIC - Use Case 7: Protein Family Conservation
# MAGIC
# MAGIC **Creates:** gold.gene_protein_family_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max, avg,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD PROTEIN FAMILY FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core tables
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_proteins_uniprot = spark.table(f"{catalog_name}.silver.proteins_uniprot")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # ENHANCED: enriched genes

# Additional enrichment tables
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

print(f"Protein domains: {df_protein_domains.count():,}")
print(f"Proteins uniprot: {df_proteins_uniprot.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")

# COMMAND ----------

# DBTITLE 1,Calculate Gene Domain Statistics
print("\nCALCULATING GENE DOMAIN STATISTICS")
print("="*80)

df_gene_domains = (
    df_protein_domains
    .groupBy("gene_symbol")
    .agg(
        countDistinct("uniprot_accession").alias("protein_count"),
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("proteins_with_kinase"),
        spark_sum(when(col("has_receptor_domain"), 1).otherwise(0)).alias("proteins_with_receptor"),
        spark_sum(when(col("has_zinc_finger"), 1).otherwise(0)).alias("proteins_with_zinc_finger"),
        spark_sum(when(col("has_sh2_domain"), 1).otherwise(0)).alias("proteins_with_sh2"),
        spark_sum(when(col("has_sh3_domain"), 1).otherwise(0)).alias("proteins_with_sh3"),
        spark_sum(when(col("has_ph_domain"), 1).otherwise(0)).alias("proteins_with_ph"),
        spark_sum(when(col("has_death_domain"), 1).otherwise(0)).alias("proteins_with_death"),
        spark_sum(when(col("has_leucine_zipper"), 1).otherwise(0)).alias("proteins_with_leucine_zipper"),
        spark_sum(when(col("has_helix_loop_helix"), 1).otherwise(0)).alias("proteins_with_helix_loop"),
        spark_sum(when(col("has_immunoglobulin"), 1).otherwise(0)).alias("proteins_with_ig"),
        spark_sum(when(col("has_functional_domain"), 1).otherwise(0)).alias("proteins_with_functional_domain")
    )
)

print(f"Genes with domain data: {df_gene_domains.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Domain Classification Flags
print("\nADDING DOMAIN CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_gene_domains
    .withColumn("has_signaling_domain",
                when((col("proteins_with_kinase") > 0) |
                     (col("proteins_with_sh2") > 0) |
                     (col("proteins_with_sh3") > 0), True).otherwise(False))
    
    .withColumn("has_dna_binding_domain",
                when((col("proteins_with_zinc_finger") > 0) |
                     (col("proteins_with_helix_loop") > 0) |
                     (col("proteins_with_leucine_zipper") > 0), True).otherwise(False))
    
    .withColumn("has_membrane_domain",
                when((col("proteins_with_receptor") > 0) |
                     (col("proteins_with_ph") > 0), True).otherwise(False))
    
    .withColumn("has_apoptosis_domain",
                when(col("proteins_with_death") > 0, True).otherwise(False))
    
    .withColumn("has_immune_domain",
                when(col("proteins_with_ig") > 0, True).otherwise(False))
    
    .withColumn("is_multi_domain_protein",
                when(col("max_domain_count") >= 5, True).otherwise(False))
)

print("Classification flags added")

# COMMAND ----------

# DBTITLE 1,Calculate Protein Family Scores
print("\nCALCULATING PROTEIN FAMILY SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("domain_diversity_score",
                col("max_domain_count") * 2 +
                when(col("has_signaling_domain"), 3).otherwise(0) +
                when(col("has_dna_binding_domain"), 3).otherwise(0) +
                when(col("has_membrane_domain"), 2).otherwise(0))
    
    .withColumn("functional_complexity_score",
                (col("proteins_with_functional_domain") * 2) +
                when(col("is_multi_domain_protein"), 5).otherwise(0))
    
    .withColumn("druggability_potential_score",
                when(col("proteins_with_kinase") > 0, 10).otherwise(0) +
                when(col("proteins_with_receptor") > 0, 8).otherwise(0) +
                when(col("has_signaling_domain"), 5).otherwise(0))
)

print("Scores calculated")

# COMMAND ----------

# DBTITLE 1,Enrich with Variant Impact on Domains
print("\nENRICHING WITH VARIANT IMPACT ON DOMAINS")
print("="*80)

variant_domain_impact = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("domain_affecting_variants"),
        spark_sum(when(col("affects_functional_domain") & col("is_pathogenic"), 1).otherwise(0))
            .alias("domain_pathogenic_variants"),
        spark_sum(when(col("affects_functional_domain") & col("is_very_high_impact"), 1).otherwise(0))
            .alias("critical_domain_variants")
    )
    .withColumn("has_domain_variants",
                col("domain_affecting_variants") > 0)
)

print(f"Genes with domain-affecting variants: {variant_domain_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Context
print("\nENRICHING WITH EXPRESSION CONTEXT")
print("="*80)

protein_family_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy(col("gene_name"))
    .agg(
        countDistinct("tissue_type").alias("protein_family_expression_breadth"),
        spark_max("max_tpm").alias("protein_max_expression")
    )
    .withColumn("tissue_specific_protein_expression",
                when(col("protein_family_expression_breadth") <= 5, True).otherwise(False))
)

print(f"Genes with expression: {protein_family_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

cancer_protein_family = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("cancer_missense_mutations"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("cancer_truncating_mutations"),
        countDistinct("tumor_sample").alias("cancer_samples_affected")
    )
    .withColumn("cancer_relevant_protein_family",
                col("cancer_samples_affected") >= 10)
    .withColumn("oncogenic_domain_alterations",
                when(col("cancer_missense_mutations") > col("cancer_truncating_mutations"),
                     lit("Likely_Oncogene"))
                .when(col("cancer_truncating_mutations") > col("cancer_missense_mutations"),
                     lit("Likely_TSG"))
                .otherwise(lit("Unknown")))
)

print(f"Cancer-related protein families: {cancer_protein_family.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Context
print("\nENRICHING WITH DISEASE CONTEXT")
print("="*80)

disease_protein_family = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease")
    )
    .withColumn("disease_associated_protein_family",
                col("total_disease_count") >= 5)
    .withColumn("disease_specific_domains",
                when(col("has_cancer_disease"), lit("Cancer_Related"))
                .when(col("has_neurological_disease"), lit("Neuro_Related"))
                .otherwise(lit("Other_Disease")))
)

print(f"Disease-associated protein families: {disease_protein_family.count():,}")

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
        col("protein_family"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        col("druggability_score")
    )
    .join(df_scored.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
          on="gene_symbol", how="left")
    
    # Join all enrichment tables
    .join(variant_domain_impact, on="gene_symbol", how="left")
    .join(protein_family_expression, col("gene_symbol") == protein_family_expression["gene_name"], how="left")
    .drop(protein_family_expression["gene_name"])
    .join(cancer_protein_family, on="gene_symbol", how="left")
    .join(disease_protein_family, on="gene_symbol", how="left")
    
    # Fill nulls
    .fillna({
        "protein_count": 0,
        "max_domain_count": 0,
        "proteins_with_kinase": 0,
        "proteins_with_receptor": 0,
        "proteins_with_zinc_finger": 0,
        "proteins_with_sh2": 0,
        "proteins_with_sh3": 0,
        "proteins_with_ph": 0,
        "proteins_with_death": 0,
        "proteins_with_leucine_zipper": 0,
        "proteins_with_helix_loop": 0,
        "proteins_with_ig": 0,
        "proteins_with_functional_domain": 0,
        "has_signaling_domain": False,
        "has_dna_binding_domain": False,
        "has_membrane_domain": False,
        "has_apoptosis_domain": False,
        "has_immune_domain": False,
        "is_multi_domain_protein": False,
        "domain_diversity_score": 0,
        "functional_complexity_score": 0,
        "druggability_potential_score": 0,
        "domain_affecting_variants": 0,
        "domain_pathogenic_variants": 0,
        "critical_domain_variants": 0,
        "has_domain_variants": False,
        "protein_family_expression_breadth": 0,
        "protein_max_expression": 0.0,
        "tissue_specific_protein_expression": False,
        "cancer_missense_mutations": 0,
        "cancer_truncating_mutations": 0,
        "cancer_samples_affected": 0,
        "cancer_relevant_protein_family": False,
        "oncogenic_domain_alterations": "Unknown",
        "total_disease_count": 0,
        "has_cancer_disease": False,
        "has_neurological_disease": False,
        "disease_associated_protein_family": False,
        "disease_specific_domains": "Unknown"
    })
)

print(f"Genes with protein family features: {df_with_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Scores
print("\nADDING ENHANCED SCORES")
print("="*80)

df_enhanced_scores = (
    df_with_genes
    # Variant-domain impact score
    .withColumn("variant_domain_impact_score",
                (col("domain_affecting_variants") * 2) +
                (col("domain_pathogenic_variants") * 5) +
                (col("critical_domain_variants") * 10))
    
    # Cancer protein family score
    .withColumn("cancer_protein_family_score",
                when(col("cancer_relevant_protein_family") & col("has_signaling_domain"), 15).otherwise(0) +
                when(col("cancer_relevant_protein_family"), 10).otherwise(0) +
                (col("cancer_samples_affected") * 0.1))
    
    # Disease protein family score
    .withColumn("disease_protein_family_score",
                when(col("disease_associated_protein_family") & col("is_multi_domain_protein"), 12).otherwise(0) +
                when(col("disease_associated_protein_family"), 8).otherwise(0) +
                (col("total_disease_count") * 0.5))
)

print("Enhanced scores calculated")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Priority Classification
print("\nADDING ENHANCED PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_enhanced_scores
    .withColumn("protein_family_priority",
                when(col("druggability_potential_score") + col("cancer_protein_family_score") >= 25, lit("critical"))
                .when(col("druggability_potential_score") >= 15, lit("high"))
                .when(col("druggability_potential_score") >= 8, lit("medium"))
                .otherwise(lit("low")))
    
    .withColumn("is_high_value_protein_family",
                when((col("has_signaling_domain")) & 
                     (col("is_multi_domain_protein")), True).otherwise(False))
    
    .withColumn("protein_functional_category",
                when(col("has_signaling_domain"), lit("signaling"))
                .when(col("has_dna_binding_domain"), lit("transcription"))
                .when(col("has_membrane_domain"), lit("membrane"))
                .when(col("has_immune_domain"), lit("immune"))
                .otherwise(lit("other")))
    
    # Enhanced: Variant-disease-domain correlation
    .withColumn("variant_disease_domain_correlation",
                when(col("has_domain_variants") & col("disease_associated_protein_family") & 
                     col("has_signaling_domain"), lit("High_Impact_Signaling_Disease"))
                .when(col("has_domain_variants") & col("disease_associated_protein_family"),
                     lit("Disease_Domain_Variants"))
                .when(col("has_domain_variants"), lit("Domain_Variants_Only"))
                .otherwise(lit("No_Domain_Variants")))
    
    # Enhanced: Cancer-specific protein family classification
    .withColumn("cancer_protein_classification",
                when(col("cancer_relevant_protein_family") & (col("oncogenic_domain_alterations") == "Likely_Oncogene"),
                     lit("Oncogene_Candidate"))
                .when(col("cancer_relevant_protein_family") & (col("oncogenic_domain_alterations") == "Likely_TSG"),
                     lit("TSG_Candidate"))
                .when(col("cancer_relevant_protein_family"),
                     lit("Cancer_Associated"))
                .otherwise(lit("Not_Cancer_Related")))
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
        col("protein_family"),
        
        # Gene type flags
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene"),
        col("druggability_score"),
        
        # Domain statistics
        col("protein_count"),
        col("max_domain_count"),
        col("proteins_with_kinase"),
        col("proteins_with_receptor"),
        col("proteins_with_zinc_finger"),
        col("proteins_with_sh2"),
        col("proteins_with_sh3"),
        col("proteins_with_ph"),
        col("proteins_with_death"),
        col("proteins_with_leucine_zipper"),
        col("proteins_with_helix_loop"),
        col("proteins_with_ig"),
        col("proteins_with_functional_domain"),
        
        # Domain classifications
        col("has_signaling_domain"),
        col("has_dna_binding_domain"),
        col("has_membrane_domain"),
        col("has_apoptosis_domain"),
        col("has_immune_domain"),
        col("is_multi_domain_protein"),
        
        # Base scores
        col("domain_diversity_score"),
        col("functional_complexity_score"),
        col("druggability_potential_score"),
        
        # Variant impact on domains
        col("domain_affecting_variants"),
        col("domain_pathogenic_variants"),
        col("critical_domain_variants"),
        col("has_domain_variants"),
        
        # Expression context
        col("protein_family_expression_breadth"),
        col("protein_max_expression"),
        col("tissue_specific_protein_expression"),
        
        # Cancer context
        col("cancer_missense_mutations"),
        col("cancer_truncating_mutations"),
        col("cancer_samples_affected"),
        col("cancer_relevant_protein_family"),
        col("oncogenic_domain_alterations"),
        
        # Disease context
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_neurological_disease"),
        col("disease_associated_protein_family"),
        col("disease_specific_domains"),
        
        # Enhanced scores
        col("variant_domain_impact_score"),
        col("cancer_protein_family_score"),
        col("disease_protein_family_score"),
        
        # Classifications
        col("protein_family_priority"),
        col("is_high_value_protein_family"),
        col("protein_functional_category"),
        col("variant_disease_domain_correlation"),
        col("cancer_protein_classification")
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

# DBTITLE 1,Save Gold Protein Family Features
print("\nSAVING GOLD PROTEIN FAMILY FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_protein_family_ml_features")

print(f"Saved: {catalog_name}.gold.gene_protein_family_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nPROTEIN FAMILY FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.gene_protein_family_ml_features").count()
print(f"\nTable created: {result_count:,} genes")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Domain statistics: 13 features")
print("  - Domain classifications: 6 features")
print("  - Base scores: 3 features")
print("  - Variant impact: 4 features")
print("  - Expression context: 3 features")
print("  - Cancer context: 5 features")
print("  - Disease context: 5 features")
print("  - Enhanced scores: 3 features")
print("  - Classifications: 5 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.gene_protein_family_ml_features") \
    .groupBy("protein_family_priority") \
    .count() \
    .orderBy("protein_family_priority") \
    .show()

print("\nCancer protein classification:")
spark.table(f"{catalog_name}.gold.gene_protein_family_ml_features") \
    .groupBy("cancer_protein_classification") \
    .count() \
    .orderBy("cancer_protein_classification") \
    .show()

print("\nProcessing complete")
