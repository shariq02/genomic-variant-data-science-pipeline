# Databricks notebook source
# MAGIC %md
# MAGIC #### CREATE VARIANT-PROTEIN IMPACT TABLE
# MAGIC ##### Map variants to protein changes and functional domains
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 27, 2026
# MAGIC
# MAGIC **Purpose:** Create comprehensive variant-protein impact gold table
# MAGIC
# MAGIC **Integrates:**
# MAGIC - variants_ultra_enriched (genomic variants)
# MAGIC - protein_domains (functional domains)
# MAGIC - conservation_scores 
# MAGIC - proteins_refseq, proteins_uniprot
# MAGIC
# MAGIC **Creates:** silver.variant_protein_impact

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, upper, trim, concat_ws, sum as spark_sum
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("INITIALIZED SPARK FOR VARIANT-PROTEIN IMPACT ANALYSIS")

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_proteins_refseq = spark.table(f"{catalog_name}.silver.proteins_refseq")
df_proteins_uniprot = spark.table(f"{catalog_name}.silver.proteins_uniprot")

print(f"Variants: {df_variants.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")
print(f"RefSeq proteins: {df_proteins_refseq.count():,}")
print(f"UniProt proteins: {df_proteins_uniprot.count():,}")

# Check if conservation scores exist
try:
    df_conservation = spark.table(f"{catalog_name}.silver.conservation_scores")
    has_conservation = True
    print(f"Conservation scores: {df_conservation.count():,}")
except:
    has_conservation = False
    print("Conservation scores: Not available (optional)")

# COMMAND ----------

# DBTITLE 1,Join Variants with Proteins
print("\nJOINING VARIANTS WITH PROTEINS")
print("="*80)

# Join variants with RefSeq proteins
df_variant_protein = (
    df_variants
    .join(df_proteins_refseq,
          df_variants.gene_name == df_proteins_refseq.gene_symbol,
          "left")
    .select(
        df_variants["*"],
        df_proteins_refseq.protein_accession.alias("refseq_protein_accession")
    )
)

# Also join with UniProt for additional protein info
df_variant_protein = (
    df_variant_protein
    .join(df_proteins_uniprot,
          df_variant_protein.gene_name == df_proteins_uniprot.gene_symbol,
          "left")
    .select(
        df_variant_protein["*"],
        df_proteins_uniprot.uniprot_accession,
        df_proteins_uniprot.protein_name
    )
)

print(f"Variants with protein info: {df_variant_protein.count():,}")

# COMMAND ----------

# DBTITLE 1,Add Protein Domain Information
print("ADDING PROTEIN DOMAIN INFORMATION")
print("="*80)

df_variant_protein_domains = (
    df_variant_protein
    .join(df_protein_domains,
          df_variant_protein.uniprot_accession == df_protein_domains.uniprot_accession,
          "left")
    .select(
        df_variant_protein["*"],
        df_protein_domains.has_zinc_finger,
        df_protein_domains.has_kinase_domain,
        df_protein_domains.has_receptor_domain,
        df_protein_domains.has_sh2_domain,
        df_protein_domains.has_sh3_domain,
        df_protein_domains.has_ph_domain,
        df_protein_domains.has_death_domain,
        df_protein_domains.has_functional_domain,
        df_protein_domains.domain_count
    )
)

print("Domain information added")

# COMMAND ----------

# DBTITLE 1,Add Conservation Scores
if has_conservation:
    print("ADDING CONSERVATION SCORES")
    print("="*80)
    
    df_variant_impact = (
        df_variant_protein_domains
        .join(df_conservation,
              (df_variant_protein_domains.variant_id == df_conservation.variant_id),
              "left")
        .select(
            df_variant_protein_domains["*"],
            df_conservation.phylop_score,
            df_conservation.phastcons_score,
            df_conservation.gerp_score,
            df_conservation.cadd_phred,
            df_conservation.is_highly_conserved,
            df_conservation.is_constrained,
            df_conservation.is_likely_deleterious,
            df_conservation.conservation_level
        )
    )
    
    print("Conservation scores added")
else:
    print("SKIPPING CONSERVATION SCORES (not available)")
    df_variant_impact = df_variant_protein_domains

# COMMAND ----------

# DBTITLE 1,Calculate Protein Impact Score
print("\nCALCULATING PROTEIN IMPACT SCORES")
print("="*80)

df_variant_impact = (
    df_variant_impact
    # Mutation severity (0-3)
    .withColumn("mutation_severity_score",
                when(col("is_frameshift_variant") | col("is_nonsense_variant"), 3)
                .when(col("is_splice_variant"), 2)
                .when(col("is_missense_variant"), 1)
                .otherwise(0))
    
    # Domain impact flag
    .withColumn("affects_functional_domain",
                when(col("has_functional_domain") & 
                     (col("is_missense_variant") | col("is_frameshift_variant") | col("is_nonsense_variant")),
                     True).otherwise(False))
    
    # Protein impact category
    .withColumn("protein_impact_category",
                when(col("is_frameshift_variant") | col("is_nonsense_variant"),
                     lit("Loss_of_Function"))
                .when(col("is_splice_variant"),
                     lit("Splicing_Altered"))
                .when(col("is_missense_variant") & col("affects_functional_domain"),
                     lit("Domain_Disrupted"))
                .when(col("is_missense_variant"),
                     lit("Amino_Acid_Change"))
                .otherwise(lit("Unknown")))
)

# Add conservation-based impact (if available)
if has_conservation:
    df_variant_impact = (
        df_variant_impact
        # Combined pathogenicity score (0-6)
        .withColumn("pathogenicity_score",
                    col("mutation_severity_score") +
                    coalesce(col("conservation_level"), lit(0)))
        
        # Predicted pathogenicity (computed from scores)
        .withColumn("predicted_pathogenicity",
                    when(col("pathogenicity_score") >= 5, lit("Likely_Pathogenic"))
                    .when(col("pathogenicity_score") >= 3, lit("Possibly_Pathogenic"))
                    .when(col("pathogenicity_score") >= 1, lit("Uncertain"))
                    .otherwise(lit("Likely_Benign")))
        
        # ClinVar-based classification (TRUE LABELS)
        .withColumn("clinvar_pathogenicity_class",
                    when(col("is_pathogenic"), lit("Pathogenic"))
                    .when(col("is_benign"), lit("Benign"))
                    .when(col("is_vus"), lit("VUS"))
                    .when(col("clinical_significance_simple").contains("Risk"), lit("Risk_Factor"))
                    .when(col("clinical_significance_simple").contains("Drug"), lit("Drug_Response"))
                    .when(col("clinical_significance_simple").contains("Protective"), lit("Protective"))
                    .when(col("clinical_significance_simple").contains("Affects"), lit("Affects"))
                    .when(col("clinical_significance_simple").contains("Association"), lit("Association"))
                    .otherwise(lit("Not_Provided")))
        
        # Binary ML-ready flags
        .withColumn("is_high_impact", 
                    col("pathogenicity_score") >= 3)
        .withColumn("is_very_high_impact",
                    col("pathogenicity_score") >= 5)
        .withColumn("is_conservation_constrained",
                    col("conservation_level") >= 1)
        .withColumn("is_highly_conserved_region",
                    col("is_highly_conserved") == True)
        .withColumn("is_domain_affecting",
                    col("affects_functional_domain") == True)
        .withColumn("is_loss_of_function",
                    col("protein_impact_category") == "Loss_of_Function")
        .withColumn("is_splice_affecting",
                    col("protein_impact_category") == "Splicing_Altered")
        .withColumn("has_cadd_score",
                    col("cadd_phred").isNotNull())
        .withColumn("is_deleterious_by_cadd",
                    when(col("cadd_phred") > 20, True).otherwise(False))
    )
else:
    # Without conservation, use mutation type only
    df_variant_impact = (
        df_variant_impact
        .withColumn("pathogenicity_score", col("mutation_severity_score"))
        .withColumn("predicted_pathogenicity",
                    when(col("mutation_severity_score") >= 3, lit("Likely_Pathogenic"))
                    .when(col("mutation_severity_score") >= 2, lit("Possibly_Pathogenic"))
                    .when(col("mutation_severity_score") >= 1, lit("Uncertain"))
                    .otherwise(lit("Likely_Benign")))
        
        # ClinVar-based classification (always available)
        .withColumn("clinvar_pathogenicity_class",
                    when(col("is_pathogenic"), lit("Pathogenic"))
                    .when(col("is_benign"), lit("Benign"))
                    .when(col("is_vus"), lit("VUS"))
                    .when(col("clinical_significance_simple").contains("Risk"), lit("Risk_Factor"))
                    .when(col("clinical_significance_simple").contains("Drug"), lit("Drug_Response"))
                    .when(col("clinical_significance_simple").contains("Protective"), lit("Protective"))
                    .when(col("clinical_significance_simple").contains("Affects"), lit("Affects"))
                    .when(col("clinical_significance_simple").contains("Association"), lit("Association"))
                    .otherwise(lit("Not_Provided")))
        
        # Binary ML-ready flags (without conservation)
        .withColumn("is_high_impact", 
                    col("pathogenicity_score") >= 2)
        .withColumn("is_very_high_impact",
                    col("pathogenicity_score") >= 3)
        .withColumn("is_conservation_constrained", lit(False))
        .withColumn("is_highly_conserved_region", lit(False))
        .withColumn("is_domain_affecting",
                    col("affects_functional_domain") == True)
        .withColumn("is_loss_of_function",
                    col("protein_impact_category") == "Loss_of_Function")
        .withColumn("is_splice_affecting",
                    col("protein_impact_category") == "Splicing_Altered")
        .withColumn("has_cadd_score", lit(False))
        .withColumn("is_deleterious_by_cadd", lit(False))
    )

print("Protein impact scores calculated")
print("Added:")
print("  - predicted_pathogenicity (computed from scores)")
print("  - clinvar_pathogenicity_class (TRUE labels from ClinVar)")
print("  - Binary ML flags (is_high_impact, is_domain_affecting, etc.)")

# COMMAND ----------

# DBTITLE 1,Create Final Silver Table
print("\nCREATING SILVER TABLE")
print("="*80)

# Select ALL relevant columns for silver layer (keep comprehensive data)
silver_columns = [
    # Variant identification
    "variant_id", "allele_id", "gene_id", "gene_name", "chromosome", "position",
    "reference_allele", "alternate_allele", "variant_type", "variant_name",
    
    # Clinical significance
    "clinical_significance_original", "clinical_significance_simple", 
    "is_pathogenic", "is_benign", "is_vus",
    "review_status", "review_quality_score",
    
    # Disease information
    "disease_enriched", "primary_disease", "omim_id", "mondo_id",
    
    # Protein information
    "refseq_protein_accession", "uniprot_accession", "protein_name",
    "protein_change", "cdna_change",
    
    # Mutation details
    "is_missense_variant", "is_frameshift_variant", "is_nonsense_variant",
    "is_splice_variant", "is_insertion", "is_deletion", "is_snv",
    
    # Domain information
    "has_functional_domain", "domain_count",
    "has_kinase_domain", "has_receptor_domain", "has_zinc_finger",
    "has_sh2_domain", "has_sh3_domain", "has_ph_domain",
    "affects_functional_domain",
    
    # Impact scores
    "mutation_severity_score", "protein_impact_category",
    "pathogenicity_score", "predicted_pathogenicity",
    
    # ClinVar-based classification (TRUE LABELS for ML)
    "clinvar_pathogenicity_class",
    
    # Binary ML-ready flags
    "is_high_impact", "is_very_high_impact",
    "is_conservation_constrained", "is_highly_conserved_region",
    "is_domain_affecting", "is_loss_of_function", "is_splice_affecting",
    "has_cadd_score", "is_deleterious_by_cadd"
]

# Add conservation columns if available
if has_conservation:
    conservation_columns = [
        "phylop_score", "phastcons_score", "gerp_score", "cadd_phred",
        "is_highly_conserved", "is_constrained", "is_likely_deleterious", 
        "conservation_level"
    ]
    silver_columns.extend(conservation_columns)

df_variant_protein_impact_silver = df_variant_impact.select(silver_columns)

variant_impact_count = df_variant_protein_impact_silver.count()
print(f"Variant-protein impact records: {variant_impact_count:,}")

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
df_variant_protein_impact_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.variant_protein_impact")

print(f" Saved: {catalog_name}.silver.variant_protein_impact")

# COMMAND ----------

# DBTITLE 1,Verify Silver Table
print("\nVERIFYING SILVER TABLE")
print("="*80)

print("Protein impact category distribution:")
df_variant_protein_impact_silver.groupBy("protein_impact_category").count().orderBy("count", ascending=False).show()

print("Predicted pathogenicity distribution (computed):")
df_variant_protein_impact_silver.groupBy("predicted_pathogenicity").count().orderBy("count", ascending=False).show()

print("ClinVar pathogenicity class (TRUE LABELS from ClinVar):")
df_variant_protein_impact_silver.groupBy("clinvar_pathogenicity_class").count().orderBy("count", ascending=False).show()

print("Binary ML flags distribution:")
ml_flag_stats = df_variant_protein_impact_silver.select(
    spark_sum(when(col("is_high_impact"), 1).otherwise(0)).alias("high_impact"),
    spark_sum(when(col("is_very_high_impact"), 1).otherwise(0)).alias("very_high_impact"),
    spark_sum(when(col("is_conservation_constrained"), 1).otherwise(0)).alias("conserved"),
    spark_sum(when(col("is_domain_affecting"), 1).otherwise(0)).alias("domain_affecting"),
    spark_sum(when(col("is_loss_of_function"), 1).otherwise(0)).alias("loss_of_function"),
    spark_sum(when(col("is_deleterious_by_cadd"), 1).otherwise(0)).alias("deleterious_cadd")
).collect()[0]

print(f"  High impact (score >= 3): {ml_flag_stats['high_impact']:,}")
print(f"  Very high impact (score >= 5): {ml_flag_stats['very_high_impact']:,}")
print(f"  Conservation constrained: {ml_flag_stats['conserved']:,}")
print(f"  Domain affecting: {ml_flag_stats['domain_affecting']:,}")
print(f"  Loss of function: {ml_flag_stats['loss_of_function']:,}")
print(f"  Deleterious by CADD: {ml_flag_stats['deleterious_cadd']:,}")

print("Variants affecting functional domains:")
domain_impact_count = df_variant_protein_impact_silver.filter(col("affects_functional_domain")).count()
print(f"  {domain_impact_count:,} variants ({domain_impact_count / variant_impact_count * 100:.2f}%)")

print("Sample high-impact variants:")
df_variant_protein_impact_silver.filter(
    (col("is_very_high_impact")) | 
    (col("clinvar_pathogenicity_class") == "Pathogenic")
).select(
    "gene_name",
    "variant_type",
    "protein_impact_category",
    "clinvar_pathogenicity_class",
    "pathogenicity_score",
    "is_domain_affecting"
).show(10, truncate=60)

# COMMAND ----------

# DBTITLE 1,Summary
print("VARIANT-PROTEIN IMPACT ANALYSIS COMPLETE")
print("="*80)

print(f"Total variant-protein records: {variant_impact_count:,}")
print(f"Variants with protein info: {df_variant_protein_impact_silver.filter(col('uniprot_accession').isNotNull()).count():,}")
print(f"Variants with domain info: {df_variant_protein_impact_silver.filter(col('has_functional_domain')).count():,}")
if has_conservation:
    print(f"Variants with conservation scores: {df_variant_protein_impact_silver.filter(col('phylop_score').isNotNull()).count():,}")

print("\nKey features added:")
print("  1. PROTEIN INFORMATION:")
print("     - Protein accessions (RefSeq, UniProt)")
print("     - Functional domain flags (zinc finger, kinase, receptor, etc.)")
print("  2. IMPACT SCORING:")
print("     - Mutation severity scores (0-3)")
print("     - Protein impact categories (Loss_of_Function, Domain_Disrupted, etc.)")
print("     - Pathogenicity scores (0-6, combining mutation + conservation)")
print("  3. PREDICTIONS (computed from scores):")
print("     - predicted_pathogenicity (Likely_Pathogenic, Possibly_Pathogenic, etc.)")
print("  4. TRUE LABELS (from ClinVar - for ML training):")
print("     - clinvar_pathogenicity_class (Pathogenic, Benign, VUS, etc.)")
print("     - Use is_pathogenic as ML TARGET")
print("  5. BINARY ML FLAGS (for feature engineering):")
print("     - is_high_impact, is_very_high_impact")
print("     - is_conservation_constrained, is_highly_conserved_region")
print("     - is_domain_affecting, is_loss_of_function, is_splice_affecting")
print("     - has_cadd_score, is_deleterious_by_cadd")
if has_conservation:
    print("  6. CONSERVATION SCORES:")
    print("     - PhyloP, PhastCons, CADD")
    print("     - Conservation-based pathogenicity scoring")

print("ML USAGE GUIDE:")
print("  TARGET (label to predict):")
print("    - is_pathogenic (TRUE label from ClinVar)")
print("  FEATURES (use these for prediction):")
print("    - pathogenicity_score, mutation_severity_score")
print("    - is_high_impact, is_domain_affecting, is_loss_of_function")
print("    - conservation_level, phylop_score, cadd_phred")
print("    - protein_impact_category, domain_count")
print("  COMPARE:")
print("    - predicted_pathogenicity (our prediction)")
print("    - clinvar_pathogenicity_class (ClinVar's classification)")

print("\nTable created:")
print(f"  {catalog_name}.silver.variant_protein_impact")
print("  Layer: SILVER (enriched/processed data)")
