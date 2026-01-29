# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - VARIANT IMPACT USE CASES
# MAGIC ##### Module 4: Protein Domain Impact and Splice Site Analysis
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 28, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 8: Variant Impact Assessment (Protein domains + conservation)
# MAGIC - Use Case 9: Splice Site Impact (Splice variants)
# MAGIC
# MAGIC **Creates:** gold.variant_impact_ml_features

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

print("SPARK INITIALIZED FOR VARIANT IMPACT FEATURE ENGINEERING - MODULE 4")

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_conservation = spark.table(f"{catalog_name}.silver.conservation_scores")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")

# Load reference for gene enrichment
df_gene_lookup = spark.table(f"{catalog_name}.reference.gene_universal_search")

print(f"Variant-protein impact: {df_variant_impact.count():,}")
print(f"Conservation scores: {df_conservation.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")
print(f"Gene lookup (reference): {df_gene_lookup.count():,}")

# COMMAND ----------

# DBTITLE 1,Use Case 8 - Protein Domain Impact Features
print("\nUSE CASE 8: VARIANT IMPACT ASSESSMENT")
print("="*80)

df_impact = (
    df_variant_impact
    .select(
        # IDs
        "variant_id", "gene_name", "chromosome", "position",
        
        # Clinical significance
        "is_pathogenic", "is_benign", "is_vus",
        "clinical_significance_simple",
        
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
                .when(col("phylop_score") > 2.7,
                     lit("High_Conservation"))
                .when(col("cadd_phred") > 20,
                     lit("High_Deleteriousness"))
                .when(col("phylop_score").isNotNull() | col("cadd_phred").isNotNull(),
                     lit("Low_Conservation"))
                .otherwise(lit("No_Conservation_Data")))
    
    # Combined functional impact score (0-15)
    .withColumn("functional_impact_score",
                coalesce(col("mutation_severity_score"), lit(0)) +
                coalesce(col("conservation_level"), lit(0)) +
                when(col("affects_functional_domain"), 3).otherwise(0) +
                when(col("is_deleterious_by_cadd"), 2).otherwise(0) +
                when(col("has_multiple_domain_types"), 1).otherwise(0))
    
    # Protein length change indicator
    .withColumn("alters_protein_length",
                col("is_frameshift_variant") | 
                col("is_nonsense_variant") |
                col("is_insertion") |
                col("is_deletion"))
    
    # Missense in conserved domain
    .withColumn("is_missense_in_conserved_domain",
                col("is_missense_variant") & 
                col("affects_functional_domain") &
                col("is_highly_conserved"))
    
    # Data quality flags
    .withColumn("has_protein_annotation",
                col("protein_change").isNotNull() | col("uniprot_accession").isNotNull())
    
    .withColumn("has_conservation_scores",
                col("phylop_score").isNotNull() | 
                col("phastcons_score").isNotNull() |
                col("cadd_phred").isNotNull())
    
    .withColumn("annotation_completeness_score",
                when(col("has_protein_annotation"), 1).otherwise(0) +
                when(col("has_functional_domain"), 1).otherwise(0) +
                when(col("has_conservation_scores"), 1).otherwise(0))
)

print("Protein domain impact features created")

# COMMAND ----------

# DBTITLE 1,Use Case 9 - Splice Site Impact Features
print("\nUSE CASE 9: SPLICE SITE IMPACT ANALYSIS")
print("="*80)

df_impact = (
    df_impact
    
    # Splice variant classification
    .withColumn("splice_variant_type",
                when(col("is_splice_variant") & col("variant_name").rlike("(?i)splice donor"),
                     lit("Splice_Donor"))
                .when(col("is_splice_variant") & col("variant_name").rlike("(?i)splice acceptor"),
                     lit("Splice_Acceptor"))
                .when(col("is_splice_variant") & col("variant_name").rlike("(?i)splice region"),
                     lit("Splice_Region"))
                .when(col("is_splice_variant") & col("variant_name").rlike("(?i)intron"),
                     lit("Intronic"))
                .when(col("is_splice_variant"),
                     lit("Splice_Site_General"))
                .otherwise(lit("Not_Splice_Variant")))
    
    # Splice impact severity
    .withColumn("splice_impact_severity",
                when(col("splice_variant_type").isin(["Splice_Donor", "Splice_Acceptor"]) &
                     col("is_pathogenic"), lit("Critical_Splice_Impact"))
                .when(col("splice_variant_type").isin(["Splice_Donor", "Splice_Acceptor"]),
                     lit("High_Splice_Impact"))
                .when(col("splice_variant_type") == "Splice_Region",
                     lit("Moderate_Splice_Impact"))
                .when(col("is_splice_variant"),
                     lit("Low_Splice_Impact"))
                .otherwise(lit("No_Splice_Impact")))
    
    # Predicted splicing outcome
    .withColumn("predicted_splicing_outcome",
                when(col("splice_variant_type").isin(["Splice_Donor", "Splice_Acceptor"]),
                     lit("Exon_Skipping_Likely"))
                .when(col("splice_variant_type") == "Splice_Region",
                     lit("Altered_Splicing_Possible"))
                .when(col("splice_variant_type") == "Intronic",
                     lit("Intronic_Effect_Uncertain"))
                .otherwise(lit("Normal_Splicing")))
    
    # Combined splice risk score (0-5)
    .withColumn("splice_risk_score",
                when(col("splice_variant_type").isin(["Splice_Donor", "Splice_Acceptor"]), 3)
                .when(col("splice_variant_type") == "Splice_Region", 2)
                .when(col("is_splice_variant"), 1)
                .otherwise(0) +
                when(col("is_pathogenic") & col("is_splice_variant"), 2)
                .otherwise(0))
    
    # Critical splice variant flag
    .withColumn("is_critical_splice_variant",
                col("is_splice_variant") &
                col("splice_variant_type").isin(["Splice_Donor", "Splice_Acceptor"]) &
                (col("is_pathogenic") | col("is_high_impact")))
    
    # Splice site location quality
    .withColumn("splice_site_is_well_defined",
                col("splice_variant_type").isin(["Splice_Donor", "Splice_Acceptor", "Splice_Region"]))
)

print("Splice site impact features created")

# COMMAND ----------

# DBTITLE 1,Calculate Gene-Level Impact Statistics
print("\nCALCULATING GENE-LEVEL IMPACT STATISTICS")
print("="*80)

gene_impact_stats = (
    df_impact
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("affects_functional_domain"), 1).otherwise(0))
            .alias("gene_domain_affecting_variants"),
        spark_sum(when(col("is_splice_variant"), 1).otherwise(0))
            .alias("gene_splice_variants"),
        spark_sum(when(col("is_critical_splice_variant"), 1).otherwise(0))
            .alias("gene_critical_splice_variants"),
        spark_sum(when(col("is_loss_of_function"), 1).otherwise(0))
            .alias("gene_lof_variants"),
        spark_sum(when(col("is_missense_in_conserved_domain"), 1).otherwise(0))
            .alias("gene_conserved_domain_missense"),
        avg("functional_impact_score").alias("gene_avg_functional_impact"),
        spark_max("functional_impact_score").alias("gene_max_functional_impact"),
        
        # Conservation statistics
        avg("phylop_score").alias("gene_avg_phylop"),
        avg("cadd_phred").alias("gene_avg_cadd"),
        
        # Annotation quality
        spark_sum(when(col("has_protein_annotation"), 1).otherwise(0))
            .alias("gene_annotated_variants")
    )
    .withColumn("gene_has_high_impact_burden",
                (col("gene_domain_affecting_variants") >= 100) |
                (col("gene_critical_splice_variants") >= 10) |
                (col("gene_lof_variants") >= 50))
    
    .withColumn("gene_annotation_quality",
                col("gene_annotated_variants") / col("gene_total_variants"))
    
    .withColumn("gene_is_well_annotated",
                col("gene_annotation_quality") > 0.5)
)

# Join back to main dataset
df_impact = (
    df_impact
    .join(gene_impact_stats, "gene_name", "left")
    
    # Gene-level risk classification
    .withColumn("gene_variant_impact_class",
                when(col("gene_has_high_impact_burden"), lit("High_Impact_Gene"))
                .when(col("gene_domain_affecting_variants") >= 10, lit("Moderate_Impact_Gene"))
                .otherwise(lit("Low_Impact_Gene")))
)

print("Gene-level impact statistics calculated")

# COMMAND ----------

# DBTITLE 1,Enrich with Gene Reference Data
print("\nENRICHING WITH GENE REFERENCE DATA")
print("="*80)

df_impact = (
    df_impact
    .join(
        df_gene_lookup.select(
            col("mapped_gene_name").alias("gene_name"),
            col("mapped_official_symbol").alias("validated_gene_symbol"),
            "description"
        ).dropDuplicates(["gene_name"]),
        "gene_name",
        "left"
    )
    
    .withColumn("gene_is_validated",
                col("validated_gene_symbol").isNotNull())
    
    .withColumn("gene_description_length",
                when(col("description").isNotNull(), length(col("description"))).otherwise(0))
)

print("Gene reference enrichment complete")

# COMMAND ----------

# DBTITLE 1,Create Final Variant Impact Features Table
print("\nCREATING VARIANT IMPACT ML FEATURES")
print("="*80)

# Select final feature set
variant_impact_features = df_impact.select(
    # IDs
    "variant_id", "gene_name", "chromosome", "position",
    
    # Gene validation
    "validated_gene_symbol",
    "gene_is_validated",
    "gene_description_length",
    
    # Clinical significance
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple",
    
    # Variant details
    "variant_type",
    "is_missense_variant",
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "is_snv",
    "alters_protein_length",
    "protein_change",
    "cdna_change",
    
    # Use Case 8: Protein Domain Impact Features
    "has_functional_domain",
    "domain_count",
    "domain_type_count",
    "has_multiple_domain_types",
    "has_zinc_finger",
    "has_kinase_domain",
    "has_receptor_domain",
    "has_sh2_domain",
    "has_sh3_domain",
    "has_ph_domain",
    "affects_functional_domain",
    "domain_impact_severity",
    "is_missense_in_conserved_domain",
    
    # Conservation features
    "phylop_score",
    "phastcons_score",
    "gerp_score",
    "cadd_phred",
    "conservation_level",
    "is_highly_conserved",
    "is_constrained",
    "is_likely_deleterious",
    "conservation_impact_class",
    
    # Impact scores
    "mutation_severity_score",
    "pathogenicity_score",
    "functional_impact_score",
    "protein_impact_category",
    
    # Binary ML flags
    "is_high_impact",
    "is_very_high_impact",
    "is_conservation_constrained",
    "is_domain_affecting",
    "is_loss_of_function",
    "is_deleterious_by_cadd",
    
    # Data quality
    "has_protein_annotation",
    "has_conservation_scores",
    "annotation_completeness_score",
    
    # Use Case 9: Splice Site Features
    "splice_variant_type",
    "splice_impact_severity",
    "predicted_splicing_outcome",
    "splice_risk_score",
    "is_critical_splice_variant",
    "splice_site_is_well_defined",
    
    # Gene-level statistics
    "gene_total_variants",
    "gene_domain_affecting_variants",
    "gene_splice_variants",
    "gene_critical_splice_variants",
    "gene_lof_variants",
    "gene_conserved_domain_missense",
    "gene_avg_functional_impact",
    "gene_max_functional_impact",
    "gene_avg_phylop",
    "gene_avg_cadd",
    "gene_has_high_impact_burden",
    "gene_variant_impact_class",
    "gene_annotation_quality",
    "gene_is_well_annotated"
)

feature_count = variant_impact_features.count()
print(f"Variant impact ML features: {feature_count:,} variants")

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

print("\nDomain impact severity distribution:")
variant_impact_features.groupBy("domain_impact_severity").count().orderBy("count", ascending=False).show()

print("\nConservation impact class distribution:")
variant_impact_features.groupBy("conservation_impact_class").count().orderBy("count", ascending=False).show()

print("\nSplice variant type distribution:")
variant_impact_features.groupBy("splice_variant_type").count().orderBy("count", ascending=False).show()

print("\nSplice impact severity distribution:")
variant_impact_features.groupBy("splice_impact_severity").count().orderBy("count", ascending=False).show()

print("\nGene variant impact class distribution:")
variant_impact_features.groupBy("gene_variant_impact_class").count().show()

print("\nFunctional impact score distribution:")
variant_impact_features.select(
    spark_min("functional_impact_score").alias("min"),
    avg("functional_impact_score").alias("mean"),
    spark_max("functional_impact_score").alias("max")
).show()

print("\nData quality metrics:")
variant_impact_features.select(
    spark_sum(when(col("has_protein_annotation"), 1).otherwise(0)).alias("has_protein"),
    spark_sum(when(col("has_conservation_scores"), 1).otherwise(0)).alias("has_conservation"),
    spark_sum(when(col("gene_is_well_annotated"), 1).otherwise(0)).alias("gene_well_annotated"),
    spark_sum(when(col("gene_is_validated"), 1).otherwise(0)).alias("gene_validated")
).show()

print("\nKey impact counts:")
variant_impact_features.select(
    spark_sum(when(col("affects_functional_domain"), 1).otherwise(0)).alias("domain_affecting"),
    spark_sum(when(col("is_critical_splice_variant"), 1).otherwise(0)).alias("critical_splice"),
    spark_sum(when(col("is_missense_in_conserved_domain"), 1).otherwise(0)).alias("missense_conserved"),
    spark_sum(when(col("alters_protein_length"), 1).otherwise(0)).alias("length_altering")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("VARIANT IMPACT FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {feature_count:,}")

print("\nUse Cases Covered:")
print("  8. Variant Impact Assessment")
print("     - Protein domain impact analysis")
print("     - Conservation-based impact scoring")
print("     - Functional impact classification")
print("  9. Splice Site Impact")
print("     - Splice variant type classification")
print("     - Splicing outcome prediction")
print("     - Critical splice variant identification")

print("\nReference Tables Used:")
print("  - gene_universal_search (gene validation)")

print("\nKey Feature Groups:")
print("  - Domain impact severity and types (15+ features)")
print("  - Conservation impact classification")
print("  - Functional impact scores (0-15)")
print("  - Splice variant classification and risk")
print("  - Gene-level impact burden")
print("  - Data quality and annotation completeness")

print("\nTable created:")
print(f"  {catalog_name}.gold.variant_impact_ml_features")
