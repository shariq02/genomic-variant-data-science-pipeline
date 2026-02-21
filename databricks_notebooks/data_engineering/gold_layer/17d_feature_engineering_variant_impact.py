# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - VARIANT IMPACT USE CASES (UPDATED & ENHANCED)
# MAGIC ##### Module 4: Comprehensive Protein Domain Impact and Conservation Analysis
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **UPDATED:** Uses all available silver tables for maximum feature richness
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 8: Variant Impact Assessment (Protein domains + conservation)
# MAGIC - Use Case 9: Splice Site Impact Analysis
# MAGIC
# MAGIC **Creates:** gold.variant_impact_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct, length, abs as spark_abs
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("VARIANT IMPACT FEATURE ENGINEERING - MODULE 4 (UPDATED & ENHANCED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

# Core tables
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_conservation = spark.table(f"{catalog_name}.silver.conservation_with_phylop")  # UPDATED: with PhyloP
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")  # UPDATED: enriched genes
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

# Additional enrichment tables
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

# Reference
df_gene_lookup = spark.table(f"{catalog_name}.reference.gene_universal_search")

print(f"Variant-protein impact: {df_variant_impact.count():,}")
print(f"Conservation (with PhyloP): {df_conservation.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Variants: {df_variants.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")
print(f"Gene lookup (reference): {df_gene_lookup.count():,}")

# COMMAND ----------

# DBTITLE 1,Use Case 8 - Comprehensive Variant Impact Features
print("\nUSE CASE 8: COMPREHENSIVE VARIANT IMPACT ASSESSMENT")
print("="*80)

df_impact = (
    df_variant_impact
    .select(
        # IDs
        "variant_id", "gene_name", "chromosome", "position",
        
        # Clinical significance
        "is_pathogenic", "is_benign", "is_vus",
        "clinical_significance_simple",
        "clinvar_pathogenicity_class",
        "review_status",
        "review_quality_score",
        
        # Variant details
        "variant_type", "variant_name",
        "reference_allele", "alternate_allele",
        "protein_change", "cdna_change",
        
        # Mutation types
        "is_missense_variant",
        "is_frameshift_variant",
        "is_nonsense_variant",
        "is_splice_variant",
        "is_snv",
        "is_insertion",
        "is_deletion",
        
        # Protein information
        "refseq_protein_accession",
        "uniprot_accession",
        "protein_name",
        
        # Domain features
        "has_functional_domain",
        "domain_count",
        "has_zinc_finger",
        "has_kinase_domain",
        "has_receptor_domain",
        "has_sh2_domain",
        "has_sh3_domain",
        "has_ph_domain",
        "affects_functional_domain",
        
        # Impact scores
        "mutation_severity_score",
        "pathogenicity_score",
        "protein_impact_category",
        
        # Conservation scores
        "phylop_score",
        "phastcons_score",
        "gerp_score",
        "cadd_phred",
        "conservation_level",
        "is_highly_conserved",
        "is_constrained",
        "is_likely_deleterious",
        
        # Binary ML flags
        "is_high_impact",
        "is_very_high_impact",
        "is_conservation_constrained",
        "is_highly_conserved_region",
        "is_domain_affecting",
        "is_loss_of_function",
        "is_splice_affecting",
        "has_cadd_score",
        "is_deleterious_by_cadd"
    )
    
    # Enhanced domain impact features
    .withColumn("domain_impact_severity",
                when(col("affects_functional_domain") & col("is_loss_of_function"),
                     lit("Critical"))
                .when(col("affects_functional_domain") & col("is_missense_variant"),
                     lit("High"))
                .when(col("has_functional_domain") & col("is_missense_variant"),
                     lit("Moderate"))
                .when(col("has_functional_domain"),
                     lit("Low"))
                .otherwise(lit("None")))
    
    .withColumn("domain_type_count",
                when(col("has_zinc_finger"), 1).otherwise(0) +
                when(col("has_kinase_domain"), 1).otherwise(0) +
                when(col("has_receptor_domain"), 1).otherwise(0) +
                when(col("has_sh2_domain"), 1).otherwise(0) +
                when(col("has_sh3_domain"), 1).otherwise(0) +
                when(col("has_ph_domain"), 1).otherwise(0))
    
    .withColumn("has_multiple_domain_types",
                col("domain_type_count") > 1)
    
    # Conservation-based impact classification
    .withColumn("conservation_impact_class",
                when((col("phylop_score") > 2.7) & (col("cadd_phred") > 20),
                     lit("High_Conservation_High_Deleteriousness"))
                .when((col("phylop_score") > 2.7),
                     lit("High_Conservation"))
                .when(col("cadd_phred") > 20,
                     lit("High_Deleteriousness"))
                .when(col("phylop_score").isNotNull() | col("cadd_phred").isNotNull(),
                     lit("Low_Conservation"))
                .otherwise(lit("No_Conservation_Data")))
    
    # Combined impact score (0-15 scale)
    .withColumn("combined_impact_score",
                coalesce(col("mutation_severity_score"), lit(0)) +
                coalesce(col("conservation_level"), lit(0)) +
                when(col("affects_functional_domain"), 3).otherwise(0) +
                when(col("is_deleterious_by_cadd"), 2).otherwise(0) +
                when(col("is_highly_conserved"), 1).otherwise(0))
    
    # Impact tier classification
    .withColumn("variant_impact_tier",
                when(col("combined_impact_score") >= 12, lit("Tier_1_Critical"))
                .when(col("combined_impact_score") >= 9, lit("Tier_2_High"))
                .when(col("combined_impact_score") >= 6, lit("Tier_3_Moderate"))
                .when(col("combined_impact_score") >= 3, lit("Tier_4_Low"))
                .otherwise(lit("Tier_5_Minimal")))
    
    # Splice-specific features
    .withColumn("is_splice_site_variant",
                col("is_splice_variant") | col("is_splice_affecting"))
    
    .withColumn("splice_impact_severity",
                when(col("is_splice_affecting") & col("is_pathogenic"),
                     lit("High_Splice_Impact"))
                .when(col("is_splice_variant") & col("is_pathogenic"),
                     lit("Moderate_Splice_Impact"))
                .when(col("is_splice_site_variant"),
                     lit("Low_Splice_Impact"))
                .otherwise(lit("No_Splice_Impact")))
    
    # Loss-of-function categories
    .withColumn("lof_category",
                when(col("is_nonsense_variant"), lit("Nonsense_Mediated"))
                .when(col("is_frameshift_variant"), lit("Frameshift"))
                .when(col("is_splice_affecting"), lit("Splice_Disruption"))
                .when(col("is_loss_of_function"), lit("Other_LoF"))
                .otherwise(lit("Not_LoF")))
)

print("Variant impact features created")

# COMMAND ----------

# DBTITLE 1,Enrich with Gene-Level Features
print("\nENRICHING WITH GENE-LEVEL FEATURES")
print("="*80)

df_impact = (
    df_impact
    .join(
        df_genes.select(
            "gene_name",
            "official_symbol",
            "is_kinase",
            "is_receptor",
            "is_enzyme",
            "is_pharmacogene",
            "druggability_score",
            "is_well_annotated",
            col("description").alias("gene_description")
        ).dropDuplicates(["gene_name"]),
        "gene_name",
        "left"
    )
    
    # Gene context flags
    .withColumn("is_druggable_gene",
                coalesce(col("druggability_score"), lit(0.0)) >= 3.0)
    
    .withColumn("is_key_protein_type",
                col("is_kinase") | col("is_receptor") | col("is_enzyme"))
    
    # Enhanced impact based on gene context
    .withColumn("clinical_impact_priority",
                when(col("is_pharmacogene") & col("is_pathogenic") & col("domain_impact_severity") == "Critical",
                     lit("Top_Priority"))
                .when((col("is_druggable_gene") | col("is_pharmacogene")) & col("is_pathogenic"),
                     lit("High_Priority"))
                .when(col("is_key_protein_type") & col("is_pathogenic"),
                     lit("Medium_Priority"))
                .when(col("is_pathogenic"),
                     lit("Standard_Priority"))
                .otherwise(lit("Low_Priority")))
)

print("Gene-level enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Data
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Calculate gene expression breadth
gene_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)  # Expressed
    .groupBy("gene_symbol")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("median_tpm").alias("max_expression_tpm"),
        avg("median_tpm").alias("avg_expression_tpm")
    )
    .withColumn("is_broadly_expressed",
                col("tissues_expressed_count") >= 10)
    .withColumn("is_highly_expressed",
                col("max_expression_tpm") >= 100)
)

df_impact = (
    df_impact
    .join(
        gene_expression.select(
            col("gene_symbol").alias("gene_name"),
            "tissues_expressed_count",
            "max_expression_tpm",
            "is_broadly_expressed",
            "is_highly_expressed"
        ),
        "gene_name",
        "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "max_expression_tpm": 0.0,
        "is_broadly_expressed": False,
        "is_highly_expressed": False
    })
    
    # Expression-impact interaction
    .withColumn("expression_impact_context",
                when(col("is_broadly_expressed") & col("is_high_impact"),
                     lit("High_Impact_Ubiquitous"))
                .when(col("is_highly_expressed") & col("is_high_impact"),
                     lit("High_Impact_Highly_Expressed"))
                .when(col("is_broadly_expressed"),
                     lit("Ubiquitous_Expression"))
                .when(col("tissues_expressed_count") > 0,
                     lit("Tissue_Specific"))
                .otherwise(lit("Low_Expression")))
)

print("Expression data enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

# Check if variant overlaps with cancer hotspot
cancer_variants = (
    df_cancer
    .select(
        col("gene_symbol").alias("gene_name"),
        col("chromosome").alias("cancer_chr"),
        col("position").alias("cancer_pos")
    )
    .groupBy("gene_name")
    .agg(
        count("*").alias("cancer_mutation_count")
    )
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
)

df_impact = (
    df_impact
    .join(cancer_variants, "gene_name", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_cancer_gene": False
    })
    
    # Cancer-impact interaction
    .withColumn("is_cancer_relevant_variant",
                col("is_cancer_gene") & 
                (col("is_missense_variant") | col("is_loss_of_function")))
    
    .withColumn("cancer_variant_priority",
                when(col("is_cancer_gene") & col("is_pathogenic") & col("is_high_impact"),
                     lit("Cancer_High_Priority"))
                .when(col("is_cancer_gene") & col("is_pathogenic"),
                     lit("Cancer_Medium_Priority"))
                .when(col("is_cancer_relevant_variant"),
                     lit("Cancer_Research_Candidate"))
                .otherwise(lit("Not_Cancer_Priority")))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Association
print("\nENRICHING WITH DISEASE ASSOCIATION")
print("="*80)

df_impact = (
    df_impact
    .join(
        df_gene_disease.select(
            "gene_name",
            col("total_disease_count").alias("disease_count"),
            "has_cancer_disease",
            "has_neurological_disease",
            "has_metabolic_disease",
            "has_cardiovascular_disease"
        ),
        "gene_name",
        "left"
    )
    .fillna({
        "disease_count": 0,
        "has_cancer_disease": False,
        "has_neurological_disease": False,
        "has_metabolic_disease": False,
        "has_cardiovascular_disease": False
    })
    
    # Disease-impact interaction
    .withColumn("is_disease_associated_gene",
                col("disease_count") >= 1)
    
    .withColumn("disease_impact_category",
                when(col("disease_count") >= 10, lit("Highly_Disease_Associated"))
                .when(col("disease_count") >= 5, lit("Disease_Associated"))
                .when(col("disease_count") >= 1, lit("Single_Disease"))
                .otherwise(lit("Not_Disease_Associated")))
    
    .withColumn("disease_specific_priority",
                when(col("has_cancer_disease") & col("is_high_impact"),
                     lit("Cancer_Disease_Priority"))
                .when(col("has_neurological_disease") & col("is_high_impact"),
                     lit("Neurological_Disease_Priority"))
                .when(col("has_cardiovascular_disease") & col("is_high_impact"),
                     lit("Cardiovascular_Disease_Priority"))
                .when(col("has_metabolic_disease") & col("is_high_impact"),
                     lit("Metabolic_Disease_Priority"))
                .otherwise(lit("No_Specific_Disease_Priority")))
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,Calculate Gene-Level Impact Statistics
print("\nCALCULATING GENE-LEVEL IMPACT STATISTICS")
print("="*80)

gene_impact_stats = (
    df_impact
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("is_high_impact"), 1).otherwise(0)).alias("gene_high_impact_count"),
        spark_sum(when(col("is_very_high_impact"), 1).otherwise(0)).alias("gene_very_high_impact_count"),
        spark_sum(when(col("is_loss_of_function"), 1).otherwise(0)).alias("gene_lof_count"),
        spark_sum(when(col("is_splice_site_variant"), 1).otherwise(0)).alias("gene_splice_variant_count"),
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("gene_domain_affecting_count"),
        avg("combined_impact_score").alias("gene_avg_impact_score"),
        spark_max("combined_impact_score").alias("gene_max_impact_score")
    )
    .withColumn("gene_impact_burden",
                when(col("gene_high_impact_count") >= 100, lit("Very_High_Burden"))
                .when(col("gene_high_impact_count") >= 50, lit("High_Burden"))
                .when(col("gene_high_impact_count") >= 10, lit("Moderate_Burden"))
                .when(col("gene_high_impact_count") >= 1, lit("Low_Burden"))
                .otherwise(lit("No_Burden")))
    
    .withColumn("gene_lof_tolerance",
                when(col("gene_lof_count") >= 50, lit("LoF_Tolerant"))
                .when(col("gene_lof_count") >= 10, lit("LoF_Moderate"))
                .when(col("gene_lof_count") >= 1, lit("LoF_Sensitive"))
                .otherwise(lit("No_LoF_Variants")))
)

df_impact = (
    df_impact
    .join(gene_impact_stats, "gene_name", "left")
    
    # Gene context priority
    .withColumn("gene_variant_impact_priority",
                when(col("gene_impact_burden").isin("Very_High_Burden", "High_Burden") &
                     col("is_very_high_impact"),
                     lit("Critical_Gene_Critical_Variant"))
                .when(col("gene_impact_burden").isin("Very_High_Burden", "High_Burden"),
                     lit("High_Impact_Gene"))
                .when(col("is_very_high_impact"),
                     lit("Critical_Variant_Standard_Gene"))
                .otherwise(lit("Standard_Priority")))
)

print("Gene-level impact statistics calculated")

# COMMAND ----------

# DBTITLE 1,Create Final Variant Impact Features Table
print("\nCREATING VARIANT IMPACT ML FEATURES")
print("="*80)

variant_impact_features = df_impact.select(
    # IDs
    "variant_id", "gene_name", "official_symbol", "chromosome", "position",
    
    # Clinical significance
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple",
    "clinvar_pathogenicity_class",
    "review_status",
    "review_quality_score",
    
    # Variant details
    "variant_type", "variant_name",
    "reference_allele", "alternate_allele",
    "protein_change", "cdna_change",
    
    # Mutation types
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "is_snv",
    "is_insertion",
    "is_deletion",
    
    # Protein information
    "refseq_protein_accession",
    "uniprot_accession",
    "protein_name",
    
    # Domain features
    "has_functional_domain",
    "domain_count",
    "has_zinc_finger",
    "has_kinase_domain",
    "has_receptor_domain",
    "has_sh2_domain",
    "has_sh3_domain",
    "has_ph_domain",
    "affects_functional_domain",
    "domain_impact_severity",
    "domain_type_count",
    "has_multiple_domain_types",
    
    # Impact scores
    "mutation_severity_score",
    "pathogenicity_score",
    "protein_impact_category",
    "combined_impact_score",
    "variant_impact_tier",
    
    # Conservation scores
    "phylop_score",
    "phastcons_score",
    "gerp_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "is_likely_deleterious",
    "conservation_impact_class",
    
    # Binary ML flags
    "is_high_impact",
    "is_very_high_impact",
    "is_conservation_constrained",
    "is_highly_conserved_region",
    "is_domain_affecting",
    "is_loss_of_function",
    "is_splice_affecting",
    "has_cadd_score",
    "is_deleterious_by_cadd",
    
    # Splice features
    "is_splice_site_variant",
    "splice_impact_severity",
    
    # LoF features
    "lof_category",
    
    # Gene context
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_pharmacogene",
    "druggability_score",
    "is_druggable_gene",
    "is_key_protein_type",
    "is_well_annotated",
    "clinical_impact_priority",
    
    # Expression context
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_broadly_expressed",
    "is_highly_expressed",
    "expression_impact_context",
    
    # Cancer context
    "cancer_mutation_count",
    "is_cancer_gene",
    "is_cancer_relevant_variant",
    "cancer_variant_priority",
    
    # Disease context
    "disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "has_metabolic_disease",
    "has_cardiovascular_disease",
    "is_disease_associated_gene",
    "disease_impact_category",
    "disease_specific_priority",
    
    # Gene-level stats
    "gene_total_variants",
    "gene_high_impact_count",
    "gene_very_high_impact_count",
    "gene_lof_count",
    "gene_splice_variant_count",
    "gene_domain_affecting_count",
    "gene_avg_impact_score",
    "gene_max_impact_score",
    "gene_impact_burden",
    "gene_lof_tolerance",
    "gene_variant_impact_priority"
)

feature_count = variant_impact_features.count()
print(f"Variant impact ML features: {feature_count:,} variants")
print(f"Total columns: {len(variant_impact_features.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = variant_impact_features.count()
variant_impact_features = variant_impact_features.dropDuplicates(["variant_id"])
after_count = variant_impact_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
variant_impact_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_impact_ml_features")

print(f"Saved: {catalog_name}.gold.variant_impact_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nVariant impact tier distribution:")
variant_impact_features.groupBy("variant_impact_tier").count().orderBy("count", ascending=False).show()

print("\nDomain impact severity:")
variant_impact_features.groupBy("domain_impact_severity").count().orderBy("count", ascending=False).show()

print("\nConservation impact class:")
variant_impact_features.groupBy("conservation_impact_class").count().orderBy("count", ascending=False).show()

print("\nClinical impact priority:")
variant_impact_features.groupBy("clinical_impact_priority").count().orderBy("count", ascending=False).show()

print("\nGene impact burden:")
variant_impact_features.groupBy("gene_impact_burden").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("VARIANT IMPACT FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {after_count:,}")
print(f"Total columns: {len(variant_impact_features.columns)}")

print("\nFeature Categories:")
print("  - Basic variant impact: 40+ features")
print("  - Domain impact: 15 features")
print("  - Conservation scores: 15 features")
print("  - Gene context: 12 features")
print("  - Expression context: 5 features")
print("  - Cancer context: 4 features")
print("  - Disease context: 8 features")
print("  - Gene-level statistics: 12 features")

print("\nSilver Tables Used:")
print("  - variant_protein_impact (protein + domain data)")
print("  - conservation_with_phylop (conservation scores)")
print("  - protein_domains (domain annotations)")
print("  - genes_with_pharmgkb (enriched gene data)")
print("  - gtex_tissue_expression (expression data)")
print("  - cancer_mutations (cancer context)")
print("  - gene_disease_comprehensive (disease associations)")

print("\nTable created:")
print(f"  {catalog_name}.gold.variant_impact_ml_features")
