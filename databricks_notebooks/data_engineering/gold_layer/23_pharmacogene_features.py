# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PHARMACOGENE ANALYSIS (FULLY ENHANCED)
# MAGIC ##### Module: Comprehensive Gene-Level Pharmacogene Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **ENHANCED:** Uses all available silver tables for comprehensive pharmacogene profiling
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 14: Drug Target Identification
# MAGIC
# MAGIC **Creates:** gold.gene_pharmacogene_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max,
    when, lit, trim, upper, lower, coalesce, array_contains, split, size
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD PHARMACOGENE FEATURES (FULLY ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

# Core pharmacogene tables
df_pharmgkb_genes = spark.table(f"{catalog_name}.silver.pharmgkb_genes")
df_relationships = spark.table(f"{catalog_name}.silver.pharmgkb_relationships")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # ENHANCED: enriched genes

# Additional enrichment tables
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")

print(f"PharmGKB genes: {df_pharmgkb_genes.count():,}")
print(f"PharmGKB relationships: {df_relationships.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")

# COMMAND ----------

# DBTITLE 1,Extract Gene Relationships
print("\nEXTRACTING GENE RELATIONSHIPS")
print("="*80)

df_gene_relationships = (
    df_relationships
    .filter(col("entity1_type") == "Gene")
    .select(
        col("entity1_name").alias("gene_symbol"),
        col("entity2_type").alias("related_entity_type"),
        col("entity2_name").alias("related_entity_name"),
        col("evidence")
    )
)

# Calculate relationship counts
df_relationship_counts = (
    df_gene_relationships
    .groupBy("gene_symbol")
    .agg(
        count("*").alias("total_relationships"),
        countDistinct("related_entity_type").alias("entity_type_count"),
        spark_sum(when(col("related_entity_type") == "Chemical", 1).otherwise(0)).alias("drug_relationships"),  
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_relationships"),
        spark_sum(when(col("related_entity_type") == "Variant", 1).otherwise(0)).alias("variant_relationships"),
        spark_sum(when(col("evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count")
    )
)

print(f"Genes with relationships: {df_relationship_counts.count():,}")

# COMMAND ----------

# DBTITLE 1,Calculate Variant-Level Pharmacogene Impact
print("\nCALCULATING VARIANT-LEVEL PHARMACOGENE IMPACT")
print("="*80)

# Gene-level variant statistics for pharmacogenes
gene_variant_stats = (
    df_variant_impact
    .groupBy(upper(trim(col("gene_name"))).alias("gene_symbol"))
    .agg(
        count("*").alias("total_gene_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_variants"),
        spark_sum(when(col("is_missense_variant"), 1).otherwise(0)).alias("missense_variants"),
        spark_sum(when(col("is_loss_of_function"), 1).otherwise(0)).alias("lof_variants"),
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("domain_affecting_variants"),
        avg("pathogenicity_score").alias("avg_pathogenicity_score")
    )
    .withColumn("has_pharmacogene_variants",
                col("total_gene_variants") > 0)
    .withColumn("variant_impact_burden",
                when(col("pathogenic_variants") >= 10, lit("High"))
                .when(col("pathogenic_variants") >= 5, lit("Medium"))
                .otherwise(lit("Low")))
)

print(f"Genes with variant statistics: {gene_variant_stats.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Data (Tissue-Specific Metabolism)
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Calculate tissue expression for drug metabolism context
gene_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
    .groupBy(col("gene_symbol"))
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("median_tpm").alias("max_expression_tpm"),
        avg("median_tpm").alias("avg_expression_tpm")
    )
    # Liver and kidney are critical for drug metabolism
    .join(
        df_gtex.filter((col("tissue_type") == "Liver") & (col("median_tpm") > 1.0))
               .select(col("gene_symbol").alias("liver_gene"), lit(True).alias("is_liver_expressed")),
        col("gene_symbol") == col("liver_gene"),
        "left"
    )
    .drop("liver_gene")
    .join(
        df_gtex.filter((col("tissue_type") == "Kidney") & (col("median_tpm") > 1.0))
               .select(col("gene_symbol").alias("kidney_gene"), lit(True).alias("is_kidney_expressed")),
        col("gene_symbol") == col("kidney_gene"),
        "left"
    )
    .drop("kidney_gene")
    .fillna({"is_liver_expressed": False, "is_kidney_expressed": False})
    .withColumn("expression_breadth",
                when(col("tissues_expressed_count") >= 15, lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5, lit("Broad"))
                .otherwise(lit("Tissue_Specific")))
    .withColumn("drug_metabolism_tissue_expression",
                when(col("is_liver_expressed") & col("is_kidney_expressed"), lit("Hepato_Renal"))
                .when(col("is_liver_expressed"), lit("Hepatic"))
                .when(col("is_kidney_expressed"), lit("Renal"))
                .otherwise(lit("Other")))
)

print(f"Genes with expression data: {gene_expression.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context (Oncology Drug Targets)
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

cancer_genes = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        count("*").alias("cancer_mutation_count"),
        countDistinct("tumor_sample").alias("unique_tumor_samples")
    )
    .withColumn("is_oncology_drug_target",
                col("cancer_mutation_count") >= 50)
    .withColumn("cancer_mutation_burden",
                when(col("unique_tumor_samples") >= 100, lit("Very_High"))
                .when(col("unique_tumor_samples") >= 50, lit("High"))
                .when(col("unique_tumor_samples") >= 10, lit("Medium"))
                .otherwise(lit("Low")))
)

print(f"Genes with cancer data: {cancer_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Associations (Drug Indications)
print("\nENRICHING WITH DISEASE ASSOCIATIONS")
print("="*80)

disease_genes = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease")
    )
    .withColumn("primary_indication_category",
                when(col("has_cancer_disease"), lit("Oncology"))
                .when(col("has_cardiovascular_disease"), lit("Cardiology"))
                .when(col("has_neurological_disease"), lit("Neurology"))
                .when(col("has_metabolic_disease"), lit("Metabolism"))
                .otherwise(lit("Other")))
)

print(f"Genes with disease associations: {disease_genes.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich with Protein Domain Complexity
print("\nENRICHING WITH PROTEIN DOMAIN COMPLEXITY")
print("="*80)

protein_complexity = (
    df_protein_domains
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("has_kinase_domain_count")
    )
    .withColumn("is_complex_drug_target",
                col("max_domain_count") >= 5)
)

print(f"Genes with protein domain data: {protein_complexity.count():,}")

# COMMAND ----------

# DBTITLE 1,Join PharmGKB Gene Data
print("\nJOINING PHARMGKB GENE DATA")
print("="*80)

df_pharmgkb_features = (
    df_pharmgkb_genes
    .select(
        upper(trim(col("gene_symbol"))).alias("gene_symbol"),
        col("gene_name").alias("pharmgkb_name"),
        col("pharmgkb_gene_id")
    )
    .join(df_relationship_counts, on="gene_symbol", how="left")
)

# Filter to genes with PharmGKB data AND relationships
df_pharmgkb_with_data = df_pharmgkb_features.filter(
    (col("pharmgkb_gene_id").isNotNull()) & 
    (col("total_relationships").isNotNull()) &
    (col("total_relationships") > 0)
)

print(f"PharmGKB genes with relationships: {df_pharmgkb_with_data.count():,}")

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data and All Enrichments
print("\nJOINING WITH GENE MASTER DATA AND ENRICHMENTS")
print("="*80)

df_gene_pharmacogene = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name").alias("gene_full_name"),
        col("description"),
        col("chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transporter"),
        col("is_metabolic"),
        col("is_pharmacogene"),
        col("druggability_score"),
        col("pharmacogene_category"),
        col("drug_metabolism_role")
    )
    .join(df_pharmgkb_with_data, on="gene_symbol", how="inner")
    
    # Join all enrichment tables
    .join(gene_variant_stats, on="gene_symbol", how="left")
    .join(gene_expression, on="gene_symbol", how="left")
    .join(cancer_genes, on="gene_symbol", how="left")
    .join(disease_genes, on="gene_symbol", how="left")
    .join(protein_complexity, on="gene_symbol", how="left")
    
    # Fill nulls
    .fillna({
        "total_gene_variants": 0,
        "pathogenic_variants": 0,
        "missense_variants": 0,
        "lof_variants": 0,
        "domain_affecting_variants": 0,
        "avg_pathogenicity_score": 0.0,
        "has_pharmacogene_variants": False,
        "variant_impact_burden": "Low",
        "tissues_expressed_count": 0,
        "max_expression_tpm": 0.0,
        "avg_expression_tpm": 0.0,
        "is_liver_expressed": False,
        "is_kidney_expressed": False,
        "expression_breadth": "Unknown",
        "drug_metabolism_tissue_expression": "Unknown",
        "cancer_mutation_count": 0,
        "unique_tumor_samples": 0,
        "is_oncology_drug_target": False,
        "cancer_mutation_burden": "None",
        "total_disease_count": 0,
        "has_cancer_disease": False,
        "has_cardiovascular_disease": False,
        "has_neurological_disease": False,
        "has_metabolic_disease": False,
        "primary_indication_category": "Unknown",
        "max_domain_count": 0,
        "has_kinase_domain_count": 0,
        "is_complex_drug_target": False
    })
)

print(f"Gene pharmacogene joined: {df_gene_pharmacogene.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Classification Flags
print("\nADDING ENHANCED CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_gene_pharmacogene
    .withColumn("has_pharmgkb_annotation",
                when(col("pharmgkb_gene_id").isNotNull(), True).otherwise(False))
    
    .withColumn("is_drug_metabolizer",
                when(col("is_metabolic") & (col("drug_relationships") > 0), True).otherwise(False))
    
    .withColumn("is_drug_transporter_gene",
                when(col("is_transporter") & (col("drug_relationships") > 0), True).otherwise(False))
    
    .withColumn("is_drug_target_gene",
                when((col("is_kinase") | col("is_receptor") | col("is_enzyme")) & 
                     (col("drug_relationships") > 0), True).otherwise(False))
    
    .withColumn("has_high_druggability",
                when(col("druggability_score") >= 0.7, True).otherwise(False))
    
    # Enhanced: Tissue-specific drug metabolism
    .withColumn("is_hepatic_metabolizer",
                col("is_liver_expressed") & col("is_drug_metabolizer"))
    
    .withColumn("is_renal_transporter",
                col("is_kidney_expressed") & col("is_drug_transporter_gene"))
    
    # Enhanced: Cancer drug target
    .withColumn("is_validated_cancer_target",
                col("is_oncology_drug_target") & 
                (col("is_kinase") | col("is_receptor")))
)

print("Added enhanced classification flags")

# COMMAND ----------

# DBTITLE 1,Calculate Comprehensive Scores
print("\nCALCULATING COMPREHENSIVE SCORES")
print("="*80)

df_evidence = (
    df_classified
    # Base pharmacogene evidence score
    .withColumn("pharmacogene_evidence_score",
                coalesce(col("evidence_count"), lit(0)) +
                when(col("has_pharmgkb_annotation"), 5).otherwise(0) +
                when(col("has_high_druggability"), 3).otherwise(0) +
                when(col("has_pharmacogene_variants"), 2).otherwise(0))
    
    # Drug interaction score (enhanced with variant and expression data)
    .withColumn("drug_interaction_score",
                coalesce(col("drug_relationships"), lit(0)) * 2 +
                coalesce(col("evidence_count"), lit(0)) +
                when(col("is_liver_expressed") | col("is_kidney_expressed"), 3).otherwise(0) +
                when(col("pathogenic_variants") > 0, 2).otherwise(0))
    
    # Clinical utility score (enhanced with disease and cancer data)
    .withColumn("clinical_utility_score",
                when(col("has_pharmgkb_annotation"), 10).otherwise(0) +
                when(col("has_high_druggability"), 5).otherwise(0) +
                (coalesce(col("drug_relationships"), lit(0)) * 0.5) +
                when(col("total_disease_count") >= 5, 5).otherwise(0) +
                when(col("is_oncology_drug_target"), 5).otherwise(0))
    
    # Variant impact score
    .withColumn("pharmacogene_variant_impact_score",
                (coalesce(col("pathogenic_variants"), lit(0)) * 3) +
                (coalesce(col("domain_affecting_variants"), lit(0)) * 2) +
                coalesce(col("lof_variants"), lit(0)))
    
    # Tissue-specific metabolism score
    .withColumn("metabolism_context_score",
                when(col("is_hepatic_metabolizer"), 10).otherwise(0) +
                when(col("is_renal_transporter"), 8).otherwise(0) +
                when(col("is_liver_expressed"), 3).otherwise(0))
)

print("Added comprehensive scores")

# COMMAND ----------

# DBTITLE 1,Add Enhanced Priority Classification
print("\nADDING ENHANCED PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_evidence
    .withColumn("pharmacogene_priority",
                when(col("clinical_utility_score") >= 20, lit("critical"))
                .when(col("clinical_utility_score") >= 15, lit("high"))
                .when(col("clinical_utility_score") >= 8, lit("medium"))
                .otherwise(lit("low")))
    
    .withColumn("is_high_priority_pharmacogene",
                when(col("pharmacogene_priority").isin("critical", "high"), True).otherwise(False))
    
    .withColumn("pharmacogene_category_enhanced",
                when((col("is_hepatic_metabolizer")) & (col("drug_relationships") > 0), lit("hepatic_metabolizer"))
                .when((col("is_renal_transporter")) & (col("drug_relationships") > 0), lit("renal_transporter"))
                .when((col("is_drug_target_gene")) & (col("is_oncology_drug_target")), lit("oncology_target"))
                .when((col("is_drug_target_gene")) & (col("drug_relationships") > 0), lit("drug_target"))
                .when((col("is_drug_metabolizer")) & (col("drug_relationships") > 0), lit("metabolizer"))
                .when((col("is_drug_transporter_gene")) & (col("drug_relationships") > 0), lit("transporter"))
                .when(col("drug_relationships") > 0, lit("interaction"))
                .otherwise(lit("other")))
    
    # Clinical actionability tier
    .withColumn("clinical_actionability_tier",
                when((col("pharmacogene_priority") == "critical") & 
                     (col("has_pharmacogene_variants")), lit("Tier_1_Actionable"))
                .when(col("pharmacogene_priority").isin("critical", "high"), lit("Tier_2_High_Evidence"))
                .when(col("has_pharmgkb_annotation"), lit("Tier_3_PharmGKB_Annotated"))
                .otherwise(lit("Tier_4_Research")))
)

print("Added enhanced priority classification")

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        # Gene identifiers
        col("gene_symbol"),
        col("gene_full_name"),
        col("pharmgkb_name"),
        col("description"),
        col("chromosome"),
        col("pharmgkb_gene_id"),
        
        # Basic pharmacogene flags
        col("has_pharmgkb_annotation"),
        col("is_drug_metabolizer"),
        col("is_drug_transporter_gene"),
        col("is_drug_target_gene"),
        col("has_high_druggability"),
        col("is_pharmacogene"),
        
        # Enhanced flags
        col("is_hepatic_metabolizer"),
        col("is_renal_transporter"),
        col("is_validated_cancer_target"),
        
        # Protein type flags
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transporter"),
        col("is_metabolic"),
        
        # Base scores
        col("druggability_score"),
        
        # Relationship counts
        col("total_relationships"),
        col("entity_type_count"),
        col("drug_relationships"),
        col("disease_relationships"),
        col("variant_relationships"),
        col("evidence_count"),
        
        # Variant impact features
        col("total_gene_variants"),
        col("pathogenic_variants"),
        col("missense_variants"),
        col("lof_variants"),
        col("domain_affecting_variants"),
        col("avg_pathogenicity_score"),
        col("has_pharmacogene_variants"),
        col("variant_impact_burden"),
        
        # Expression features
        col("tissues_expressed_count"),
        col("max_expression_tpm"),
        col("avg_expression_tpm"),
        col("is_liver_expressed"),
        col("is_kidney_expressed"),
        col("expression_breadth"),
        col("drug_metabolism_tissue_expression"),
        
        # Cancer features
        col("cancer_mutation_count"),
        col("unique_tumor_samples"),
        col("is_oncology_drug_target"),
        col("cancer_mutation_burden"),
        
        # Disease features
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("has_metabolic_disease"),
        col("primary_indication_category"),
        
        # Protein domain features
        col("max_domain_count"),
        col("has_kinase_domain_count"),
        col("is_complex_drug_target"),
        
        # Comprehensive scores
        col("pharmacogene_evidence_score"),
        col("drug_interaction_score"),
        col("clinical_utility_score"),
        col("pharmacogene_variant_impact_score"),
        col("metabolism_context_score"),
        
        # Classifications
        col("pharmacogene_priority"),
        col("is_high_priority_pharmacogene"),
        col("pharmacogene_category"),
        col("pharmacogene_category_enhanced"),
        col("drug_metabolism_role"),
        col("clinical_actionability_tier")
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

# DBTITLE 1,Save Gold Pharmacogene Features
print("\nSAVING GOLD PHARMACOGENE FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_pharmacogene_ml_features")

print(f"Saved: {catalog_name}.gold.gene_pharmacogene_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nPHARMACOGENE FEATURES COMPLETE (FULLY ENHANCED)")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.gene_pharmacogene_ml_features").count()
print(f"\nTable created: {result_count:,} genes")
print(f"Total columns: {len(df_final.columns)}")

print("\nFeature categories:")
print("  - Base pharmacogene: 15 features")
print("  - Variant impact: 8 features")
print("  - Expression context: 8 features")
print("  - Cancer context: 4 features")
print("  - Disease associations: 6 features")
print("  - Protein domains: 3 features")
print("  - Comprehensive scores: 5 features")
print("  - Classifications: 6 features")
print(f"  Total: {len(df_final.columns)} features")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.gene_pharmacogene_ml_features") \
    .groupBy("pharmacogene_priority") \
    .count() \
    .orderBy("pharmacogene_priority") \
    .show()

print("\nClinical actionability:")
spark.table(f"{catalog_name}.gold.gene_pharmacogene_ml_features") \
    .groupBy("clinical_actionability_tier") \
    .count() \
    .orderBy("clinical_actionability_tier") \
    .show()

print("\nProcessing complete")
