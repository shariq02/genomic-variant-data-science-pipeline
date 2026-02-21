# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - CLINICAL USE CASES (UPDATED)
# MAGIC ##### Module 1: Clinical Pathogenicity, Inheritance Patterns, Gene Statistics
# MAGIC
# MAGIC **DNA Gene Mapping Project**            
# MAGIC **Author:** Sharique Mohammad    
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **UPDATED:** Uses actual silver table schemas (genes_with_pharmgkb, conservation_with_phylop)
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 1: Clinical Pathogenicity Prediction (Pathogenic vs Benign)
# MAGIC - Use Case 2: Inheritance Pattern Analysis (Dominant/Recessive)
# MAGIC - Use Case 3: Gene-Level Statistics
# MAGIC
# MAGIC **Creates:** gold.clinical_ml_features_v1

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct, length
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("CLINICAL FEATURE ENGINEERING - MODULE 1 (UPDATED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

# Core tables
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_lookup = spark.table(f"{catalog_name}.reference.gene_universal_search")

# Additional enrichment tables for enhanced features
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population = spark.table(f"{catalog_name}.silver.population_frequencies")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

print(f"Variants: {df_variants.count():,}")
print(f"Variant-protein impact: {df_variant_impact.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Gene lookup (reference): {df_gene_lookup.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")

# COMMAND ----------

# DBTITLE 1,Schema Validation
print("\nVALIDATING REQUIRED COLUMNS")
print("="*80)

# Check variant_protein_impact has required columns
REQUIRED_COLUMNS_IMPACT = [
    "variant_id", "gene_name", "chromosome", "position",
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple", "clinvar_pathogenicity_class",
    "review_status", "review_quality_score",
    "variant_type", "is_missense_variant", "is_frameshift_variant",
    "is_nonsense_variant", "is_splice_variant", "is_snv",
    "is_insertion", "is_deletion",
    "mutation_severity_score", "pathogenicity_score", "protein_impact_category",
    "phylop_score", "phastcons_score", "cadd_phred", "conservation_level",
    "is_highly_conserved", "is_constrained", "is_likely_deleterious",
    "is_high_impact", "is_very_high_impact", "is_conservation_constrained",
    "is_domain_affecting", "is_loss_of_function", "is_deleterious_by_cadd",
    "has_functional_domain", "domain_count", "has_kinase_domain", "has_receptor_domain"
]

missing_cols = set(REQUIRED_COLUMNS_IMPACT) - set(df_variant_impact.columns)
if missing_cols:
    print(f"WARNING: Missing columns in variant_protein_impact: {missing_cols}")
else:
    print("All required columns present in variant_protein_impact")

# COMMAND ----------

# DBTITLE 1,Use Case 1 - Clinical Pathogenicity Features
print("\nUSE CASE 1: CLINICAL PATHOGENICITY PREDICTION")
print("="*80)

df_clinical = (
    df_variant_impact
    .select(
        # IDs
        "variant_id", "gene_name", "chromosome", "position",
        
        # TARGET VARIABLES (for ML training)
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
        
        # Conservation features (from variant_protein_impact)
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
    
    # Data quality flags
    .withColumn("has_conservation_data",
                col("phylop_score").isNotNull() | col("cadd_phred").isNotNull())
    
    .withColumn("has_complete_annotation",
                col("protein_impact_category").isNotNull() &
                col("mutation_severity_score").isNotNull() &
                col("has_conservation_data"))
    
    # Clinical significance quality
    .withColumn("clinical_sig_is_uncertain",
                col("clinical_significance_simple").rlike("(?i)uncertain|conflicting|not provided"))
)

print("Clinical pathogenicity features created")

# COMMAND ----------

# DBTITLE 1,Enrich with Gene Data
print("\nENRICHING WITH GENE DATA")
print("="*80)

# Join with enriched gene table
df_clinical = (
    df_clinical
    .join(
        df_genes.select(
            col("gene_name"),
            col("official_symbol").alias("official_gene_symbol"),
            col("gene_id").alias("validated_gene_id"),
            col("chromosome").alias("gene_chromosome"),
            col("mim_id").alias("gene_mim_id"),
            col("ensembl_id").alias("gene_ensembl_id"),
            col("description"),
            col("is_pharmacogene"),
            col("druggability_score")
        ).dropDuplicates(["gene_name"]),
        "gene_name",
        "left"
    )
    
    # Gene validation flags
    .withColumn("gene_is_validated",
                col("validated_gene_id").isNotNull())
    
    .withColumn("gene_has_omim",
                col("gene_mim_id").isNotNull())
    
    .withColumn("gene_has_ensembl",
                col("gene_ensembl_id").isNotNull())
    
    .withColumn("gene_description_length",
                when(col("description").isNotNull(), length(col("description"))).otherwise(0))
    
    .withColumn("gene_is_well_characterized",
                col("gene_description_length") > 50)
)

print("Gene enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Data
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Calculate gene expression breadth
gene_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
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

df_clinical = (
    df_clinical
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
    
    # Expression context
    .withColumn("expression_context",
                when(col("is_broadly_expressed") & col("is_highly_expressed"),
                     lit("Ubiquitous_High"))
                .when(col("is_broadly_expressed"),
                     lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5,
                     lit("Moderate_Breadth"))
                .when(col("tissues_expressed_count") > 0,
                     lit("Tissue_Specific"))
                .otherwise(lit("Low_Expression")))
)

print("Expression data enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

# Cancer gene flags
cancer_genes = (
    df_cancer
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(
        count("*").alias("cancer_mutation_count")
    )
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
)

df_clinical = (
    df_clinical
    .join(cancer_genes, "gene_name", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_cancer_gene": False
    })
    
    .withColumn("is_cancer_relevant",
                col("is_cancer_gene") & col("target_is_pathogenic"))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Population Frequencies
print("\nENRICHING WITH POPULATION FREQUENCIES")
print("="*80)

df_clinical = (
    df_clinical
    .join(
        df_population.select(
            col("variant_id"),
            col("global_af").alias("population_allele_frequency"),
            col("is_common").alias("is_common_in_population"),
            col("is_rare").alias("is_rare_in_population")
        ),
        "variant_id",
        "left"
    )
    .fillna({
        "population_allele_frequency": 0.0,
        "is_common_in_population": False,
        "is_rare_in_population": False
    })
    
    # Frequency-pathogenicity interaction
    .withColumn("frequency_pathogenicity_conflict",
                col("is_common_in_population") & col("target_is_pathogenic"))
)

print("Population frequency enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Associations
print("\nENRICHING WITH DISEASE ASSOCIATIONS")
print("="*80)

df_clinical = (
    df_clinical
    .join(
        df_gene_disease.select(
            "gene_name",
            col("total_disease_count").alias("disease_count"),
            "has_cancer_disease",
            "has_neurological_disease"
        ),
        "gene_name",
        "left"
    )
    .fillna({
        "disease_count": 0,
        "has_cancer_disease": False,
        "has_neurological_disease": False
    })
    
    .withColumn("is_disease_gene",
                col("disease_count") >= 1)
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,Use Case 2 - Inheritance Pattern Features
print("\nUSE CASE 2: INHERITANCE PATTERN ANALYSIS")
print("="*80)

df_clinical = (
    df_clinical
    
    # Basic inheritance patterns from chromosome
    .withColumn("inheritance_pattern",
                when(col("chromosome") == "X", lit("X_Linked"))
                .when(col("chromosome") == "Y", lit("Y_Linked"))
                .when(col("chromosome") == "MT", lit("Mitochondrial"))
                .otherwise(lit("Autosomal")))
    
    # X-linked risk considerations
    .withColumn("x_linked_risk_modifier",
                when((col("chromosome") == "X") & col("target_is_pathogenic"),
                     lit("Male_High_Risk"))
                .when((col("chromosome") == "X") & col("target_is_vus"),
                     lit("Male_Moderate_Risk"))
                .otherwise(lit("Standard_Risk")))
    
    # Specific inheritance flags
    .withColumn("is_mitochondrial_variant",
                col("chromosome") == "MT")
    
    .withColumn("is_y_linked_variant",
                col("chromosome") == "Y")
    
    .withColumn("is_x_linked_variant",
                col("chromosome") == "X")
    
    .withColumn("is_autosomal_variant",
                ~col("chromosome").isin("X", "Y", "MT"))
    
    # Inheritance-based pathogenicity modifiers
    .withColumn("inheritance_pathogenicity_modifier",
                when(col("is_mitochondrial_variant") & col("target_is_pathogenic"),
                     lit("Maternal_Inheritance_Risk"))
                .when(col("is_y_linked_variant") & col("target_is_pathogenic"),
                     lit("Male_Only_Risk"))
                .when(col("is_x_linked_variant") & col("target_is_pathogenic"),
                     lit("X_Linked_Risk"))
                .otherwise(lit("Standard_Mendelian")))
)

print("Inheritance pattern features created")

# COMMAND ----------

# DBTITLE 1,Use Case 3 - Gene-Level Statistics
print("\nUSE CASE 3: GENE-LEVEL STATISTICS")
print("="*80)

# Calculate gene-level statistics
gene_stats = (
    df_clinical
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("target_is_pathogenic"), 1).otherwise(0)).alias("gene_pathogenic_count"),
        spark_sum(when(col("target_is_benign"), 1).otherwise(0)).alias("gene_benign_count"),
        spark_sum(when(col("target_is_vus"), 1).otherwise(0)).alias("gene_vus_count"),
        spark_sum(when(col("is_missense_variant"), 1).otherwise(0)).alias("gene_missense_count"),
        spark_sum(when(col("is_frameshift_variant"), 1).otherwise(0)).alias("gene_frameshift_count"),
        spark_sum(when(col("is_nonsense_variant"), 1).otherwise(0)).alias("gene_nonsense_count"),
        spark_sum(when(col("is_splice_variant"), 1).otherwise(0)).alias("gene_splice_count"),
        avg(col("review_quality_score")).alias("gene_avg_review_quality")
    )
    
    # Calculate ratios
    .withColumn("gene_pathogenic_ratio",
                col("gene_pathogenic_count") / col("gene_total_variants"))
    
    .withColumn("gene_benign_ratio",
                col("gene_benign_count") / col("gene_total_variants"))
    
    .withColumn("gene_vus_ratio",
                col("gene_vus_count") / col("gene_total_variants"))
    
    .withColumn("gene_lof_variant_ratio",
                (col("gene_frameshift_count") + col("gene_nonsense_count")) / 
                col("gene_total_variants"))
)

# Join with main features
df_clinical = (
    df_clinical
    .join(gene_stats, "gene_name", "left")
    
    # Gene mutation burden classification
    .withColumn("gene_mutation_burden",
                when(col("gene_total_variants") >= 1000, lit("Very_High"))
                .when(col("gene_total_variants") >= 500, lit("High"))
                .when(col("gene_total_variants") >= 100, lit("Moderate"))
                .when(col("gene_total_variants") >= 10, lit("Low"))
                .otherwise(lit("Very_Low")))
    
    # Gene pathogenicity enrichment
    .withColumn("gene_is_pathogenic_enriched",
                coalesce(col("gene_pathogenic_ratio"), lit(0.0)) > 0.1)
    
    .withColumn("gene_is_benign_enriched",
                coalesce(col("gene_benign_ratio"), lit(0.0)) > 0.5)
    
    .withColumn("gene_is_vus_enriched",
                coalesce(col("gene_vus_ratio"), lit(0.0)) > 0.5)
    
    # Gene variant profile
    .withColumn("gene_variant_profile",
                when(col("gene_is_pathogenic_enriched"), lit("Pathogenic_Enriched"))
                .when(col("gene_is_benign_enriched"), lit("Benign_Enriched"))
                .when(col("gene_is_vus_enriched"), lit("VUS_Enriched"))
                .otherwise(lit("Mixed_Profile")))
    
    # Loss-of-function tolerance indicator
    .withColumn("gene_has_high_lof_burden",
                coalesce(col("gene_lof_variant_ratio"), lit(0.0)) > 0.1)
    
    # Data quality at gene level
    .withColumn("gene_has_quality_annotations",
                coalesce(col("gene_avg_review_quality"), lit(0.0)) >= 1.0)
)

print("Gene-level statistics created")

# COMMAND ----------

# DBTITLE 1,Create Final Clinical Features Table
print("\nCREATING CLINICAL ML FEATURES")
print("="*80)

# Select final feature set
clinical_features = df_clinical.select(
    # IDs
    "variant_id", "gene_name", "chromosome", "position",
    
    # Gene reference enrichment
    "official_gene_symbol",
    "gene_is_validated",
    "gene_has_omim",
    "gene_has_ensembl",
    "gene_is_well_characterized",
    "is_pharmacogene",
    "druggability_score",
    
    # TARGET VARIABLES
    "target_is_pathogenic",
    "target_is_benign",
    "target_is_vus",
    
    # Use Case 1: Pathogenicity Features
    "clinical_significance_simple",
    "clinvar_pathogenicity_class",
    "clinical_sig_is_uncertain",
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
    "has_conservation_data",
    "has_complete_annotation",
    
    # Use Case 2: Inheritance Features
    "inheritance_pattern",
    "x_linked_risk_modifier",
    "inheritance_pathogenicity_modifier",
    "is_mitochondrial_variant",
    "is_y_linked_variant",
    "is_x_linked_variant",
    "is_autosomal_variant",
    
    # Use Case 3: Gene Statistics
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_benign_count",
    "gene_vus_count",
    "gene_pathogenic_ratio",
    "gene_benign_ratio",
    "gene_vus_ratio",
    "gene_mutation_burden",
    "gene_is_pathogenic_enriched",
    "gene_is_benign_enriched",
    "gene_is_vus_enriched",
    "gene_variant_profile",
    "gene_has_high_lof_burden",
    "gene_avg_review_quality",
    "gene_has_quality_annotations",
    "gene_missense_count",
    "gene_frameshift_count",
    "gene_nonsense_count",
    "gene_splice_count",
    "gene_lof_variant_ratio",
    
    # Expression context
    "tissues_expressed_count",
    "max_expression_tpm",
    "is_broadly_expressed",
    "is_highly_expressed",
    "expression_context",
    
    # Cancer context
    "cancer_mutation_count",
    "is_cancer_gene",
    "is_cancer_relevant",
    
    # Population frequencies
    "population_allele_frequency",
    "is_common_in_population",
    "is_rare_in_population",
    "frequency_pathogenicity_conflict",
    
    # Disease associations
    "disease_count",
    "has_cancer_disease",
    "has_neurological_disease",
    "is_disease_gene"
)

feature_count = clinical_features.count()
print(f"Clinical ML features: {feature_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = clinical_features.count()
clinical_features = clinical_features.dropDuplicates(["variant_id"])
after_count = clinical_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

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

print("\nGene validation:")
clinical_features.select(
    spark_sum(when(col("gene_is_validated"), 1).otherwise(0)).alias("validated"),
    spark_sum(when(col("gene_has_omim"), 1).otherwise(0)).alias("has_omim"),
    spark_sum(when(col("gene_is_well_characterized"), 1).otherwise(0)).alias("well_characterized")
).show()

print("\nData quality:")
clinical_features.select(
    spark_sum(when(col("has_conservation_data"), 1).otherwise(0)).alias("has_conservation"),
    spark_sum(when(col("has_complete_annotation"), 1).otherwise(0)).alias("complete_annotation"),
    spark_sum(when(col("clinical_sig_is_uncertain"), 1).otherwise(0)).alias("uncertain_sig")
).show()

print("\nInheritance pattern distribution:")
clinical_features.groupBy("inheritance_pattern").count().orderBy("count", ascending=False).show()

print("\nGene variant profile:")
clinical_features.groupBy("gene_variant_profile").count().orderBy("count", ascending=False).show()

print("\nGene mutation burden:")
clinical_features.groupBy("gene_mutation_burden").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("CLINICAL FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {after_count:,}")
print(f"Total columns: {len(clinical_features.columns)}")

print("\nUse Cases Covered:")
print("  1. Clinical Pathogenicity Prediction")
print("     - 40+ pathogenicity features")
print("     - Gene validation and enrichment")
print("  2. Inheritance Pattern Analysis")
print("     - Chromosome-based inheritance patterns")
print("     - Risk modifiers by inheritance type")
print("  3. Gene-Level Statistics")
print("     - Comprehensive gene-level statistics")
print("     - Variant profile classification")

print("\nSilver Tables Used:")
print("  - variants_ultra_enriched (base variants)")
print("  - variant_protein_impact (protein impact + conservation)")
print("  - genes_with_pharmgkb (enriched gene data)")
print("  - conservation_with_phylop (conservation scores)")

print("\nTable created:")
print(f"  {catalog_name}.gold.clinical_ml_features")
