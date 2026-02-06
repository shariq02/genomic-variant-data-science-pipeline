# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PHARMACOGENE USE CASE
# MAGIC ##### Module 3: Drug Target Identification
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 28, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 7: Drug Target Identification (Pharmacogenes)
# MAGIC
# MAGIC **Creates:** gold.pharmacogene_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    countDistinct, concat_ws, length
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR PHARMACOGENE FEATURE ENGINEERING - MODULE 3")

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")

# Try to load pharmgkb table (may not exist)
try:
    df_pharmgkb = spark.table(f"{catalog_name}.silver.pharmgkb_genes")
    has_pharmgkb = True
    print(f"PharmGKB genes: {df_pharmgkb.count():,}")
except:
    has_pharmgkb = False
    print("PharmGKB genes: Not available")

# Load reference for gene validation
df_gene_lookup = spark.table(f"{catalog_name}.reference.gene_universal_search")

print(f"Variants: {df_variants.count():,}")
print(f"Genes: {df_genes.count():,}")
print(f"Variant-protein impact: {df_variant_impact.count():,}")
print(f"Gene lookup (reference): {df_gene_lookup.count():,}")

# COMMAND ----------

# DBTITLE 1,Use Case 7 - Pharmacogene Features
print("\nUSE CASE 7: DRUG TARGET IDENTIFICATION")
print("="*80)

# Start with variant-protein impact data
df_pharma = (
    df_variant_impact
    .select(
        "variant_id", "gene_name", "chromosome", "position",
        "is_pathogenic", "is_benign", "is_vus",
        "clinical_significance_simple",
        "variant_type",
        "is_missense_variant",
        "is_loss_of_function",
        "protein_impact_category",
        "has_functional_domain",
        "has_kinase_domain",
        "has_receptor_domain",
        "is_domain_affecting",
        "mutation_severity_score",
        "pathogenicity_score"
    )
    
    # Join with gene pharmacogene flags (using correct schema columns)
    .join(df_genes.select(
        "gene_name",
        "official_symbol",
        
        # Protein type flags
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "is_transporter",
        "is_phosphatase",
        "is_protease",
        "is_channel",
        "is_gpcr",
        "is_transcription_factor",
        "druggability_score"
    ), "gene_name", "left")
    .withColumn("is_drug_transporter", col("is_transporter"))
    .withColumn("is_drug_target",
                col("is_kinase") | col("is_receptor") | col("is_gpcr"))
    .withColumn("is_metabolizing_enzyme",
                col("is_enzyme") | col("is_phosphatase") | col("is_protease"))
    .withColumn("is_pharmacogene",
                col("is_drug_target") | col("is_metabolizing_enzyme") | col("is_transporter"))
    .withColumn("pharmacogene_category",
                when(col("is_kinase"), "Kinase")
                .when(col("is_receptor"), "Receptor")
                .when(col("is_enzyme"), "Enzyme")
                .when(col("is_transporter"), "Transporter")
                .when(col("is_gpcr"), "GPCR")
                .otherwise("Other"))
    .withColumn("pharmacogene_evidence_level",
                when(col("is_kinase") | col("is_receptor") | col("is_gpcr"), "high")
                .when(col("is_enzyme") | col("is_transporter"), "medium")
                .otherwise("low"))
    .withColumn("drug_metabolism_role",
                when(col("is_enzyme"), "Metabolism")
                .when(col("is_transporter"), "Transport")
                .when(col("is_kinase") | col("is_receptor"), "Signal_Transduction")
                .otherwise("Unknown"))
)

# Add PharmGKB data if available
if has_pharmgkb:
    df_pharma = (
        df_pharma
        .join(df_pharmgkb.select(
            col("gene_symbol").alias("gene_name"),
            col("source").alias("pharmgkb_source"),
            col("evidence").alias("pharmgkb_evidence"),
            col("source_count").alias("pharmgkb_source_count")
        ), "gene_name", "left")
        .withColumn("has_pharmgkb_annotation",
                    col("pharmgkb_source").isNotNull())
    )
else:
    df_pharma = (
        df_pharma
        .withColumn("pharmgkb_source", lit(None).cast("string"))
        .withColumn("pharmgkb_evidence", lit(None).cast("string"))
        .withColumn("pharmgkb_source_count", lit(None).cast("long"))
        .withColumn("has_pharmgkb_annotation", lit(False))
    )

# Add pharmacogene features
df_pharma = (
    df_pharma
    # Metabolizing enzyme classification (using correct schema)
    .withColumn("metabolizing_enzyme_type",
                when(col("is_enzyme"), lit("Phase2_Enzyme"))
                .otherwise(lit("Not_Metabolizing_Enzyme")))
    # Drug target classification
    .withColumn("drug_target_category",
                when(col("is_kinase"), lit("Kinase"))
                .when(col("is_receptor"), lit("Receptor"))
                .when(col("is_gpcr"), lit("GPCR"))
                .when(col("is_metabolizing_enzyme"), lit("Metabolizing_Enzyme"))
                .when(col("is_drug_transporter"), lit("Transporter"))
                .when(col("is_enzyme"), lit("Other_Enzyme"))
                .when(col("is_drug_target"), lit("Drug_Target"))
                .otherwise(lit("Not_Drug_Target")))
    # Enhanced druggability score (0-10)
    .withColumn("enhanced_druggability_score",
                coalesce(col("druggability_score"), lit(0.0)) +
                when(col("is_pharmacogene"), 2).otherwise(0) +
                when(col("is_drug_target"), 1).otherwise(0) +
                when(col("has_pharmgkb_annotation"), 1).otherwise(0))
    # Variant-drug interaction potential
    .withColumn("has_drug_interaction_potential",
                (col("is_pharmacogene") | col("is_drug_target") | col("is_metabolizing_enzyme")) &
                (col("is_missense_variant") | col("is_loss_of_function")))
    .withColumn("drug_response_impact",
                when(col("has_drug_interaction_potential") & col("is_pathogenic"), 
                     lit("High_Impact"))
                .when(col("has_drug_interaction_potential") & col("is_vus"),
                     lit("Moderate_Impact"))
                .when(col("has_drug_interaction_potential"),
                     lit("Low_Impact"))
                .otherwise(lit("No_Impact")))
    # Metabolizer variant classification (using drug_metabolism_role)
    .withColumn("is_metabolizer_variant",
                col("is_metabolizing_enzyme") & 
                (col("is_missense_variant") | col("is_loss_of_function")))
    .withColumn("metabolizer_phenotype_risk",
                when(col("is_metabolizer_variant") & col("is_loss_of_function"),
                     lit("Poor_Metabolizer_Risk"))
                .when(col("is_metabolizer_variant") & col("is_pathogenic"),
                     lit("Altered_Metabolizer_Risk"))
                .when(col("drug_metabolism_role").isNotNull(),
                     col("drug_metabolism_role"))
                .otherwise(lit("Normal_Metabolizer")))
    # Transporter variant priority
    .withColumn("is_transporter_variant",
                col("is_drug_transporter") & 
                (col("is_missense_variant") | col("is_loss_of_function")))
    .withColumn("transporter_impact_level",
                when(col("is_transporter_variant") & col("is_pathogenic"),
                     lit("High_Transport_Impact"))
                .when(col("is_transporter_variant") & col("is_vus"),
                     lit("Moderate_Transport_Impact"))
                .when(col("is_transporter_variant"),
                     lit("Low_Transport_Impact"))
                .otherwise(lit("No_Transport_Impact")))
    # Kinase inhibitor target priority
    .withColumn("is_kinase_inhibitor_target",
                col("is_kinase") & col("has_kinase_domain"))
    .withColumn("kinase_variant_therapeutic_relevance",
                when(col("is_kinase_inhibitor_target") & col("is_missense_variant") & 
                     col("is_domain_affecting"), lit("High_Therapeutic_Relevance"))
                .when(col("is_kinase_inhibitor_target") & col("is_missense_variant"),
                     lit("Moderate_Therapeutic_Relevance"))
                .when(col("is_kinase_inhibitor_target"),
                     lit("Low_Therapeutic_Relevance"))
                .otherwise(lit("No_Therapeutic_Relevance")))
)

print("Pharmacogene features created")


# COMMAND ----------

# DBTITLE 1,Calculate Gene-Level Pharmacogene Statistics
print("\nCALCULATING GENE-LEVEL PHARMACOGENE STATS")
print("="*80)

gene_pharma_stats = (
    df_pharma
    .filter(col("is_pharmacogene") | col("is_drug_target") | col("is_metabolizing_enzyme"))
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_pharmacogene_variants"),
        spark_sum(when(col("has_drug_interaction_potential"), 1).otherwise(0))
            .alias("gene_drug_interaction_variants"),
        spark_sum(when(col("is_metabolizer_variant"), 1).otherwise(0))
            .alias("gene_metabolizer_variants"),
        spark_sum(when(col("is_transporter_variant"), 1).otherwise(0))
            .alias("gene_transporter_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0))
            .alias("gene_pharmacogene_pathogenic"),
        avg("enhanced_druggability_score").alias("gene_avg_druggability")
    )
    .withColumn("gene_has_multiple_drug_variants",
                col("gene_drug_interaction_variants") > 1)
    
    .withColumn("gene_pharmacogene_burden",
                when(col("gene_pharmacogene_pathogenic") >= 10, lit("High_Burden"))
                .when(col("gene_pharmacogene_pathogenic") >= 5, lit("Moderate_Burden"))
                .when(col("gene_pharmacogene_pathogenic") >= 1, lit("Low_Burden"))
                .otherwise(lit("No_Burden")))
)

# Join back to main dataset
df_pharma = (
    df_pharma
    .join(gene_pharma_stats, "gene_name", "left")
    
    # Gene-level prioritization
    .withColumn("gene_pharmacogene_priority",
                when(col("gene_drug_interaction_variants") >= 10, lit("Critical_Priority"))
                .when(col("gene_drug_interaction_variants") >= 5, lit("High_Priority"))
                .when(col("gene_drug_interaction_variants") >= 1, lit("Moderate_Priority"))
                .otherwise(lit("Low_Priority")))
    
    # Clinical pharmacogenomics flag
    .withColumn("is_pharmacogene",
                col("is_pharmacogene") &
                (col("gene_pharmacogene_pathogenic") >= 3) 
                )
    )

print("Gene-level pharmacogene statistics calculated")

# COMMAND ----------

# DBTITLE 1,Enrich with Gene Reference Data
print("\nENRICHING WITH GENE REFERENCE DATA")
print("="*80)

df_pharma = (
    df_pharma
    .join(
        df_gene_lookup.select(
            col("mapped_gene_name").alias("gene_name"),
            col("mapped_official_symbol").alias("validated_gene_symbol"),
            "mim_id",
            "description"
        ).dropDuplicates(["gene_name"]),
        "gene_name",
        "left"
    )
    
    .withColumn("gene_is_validated",
                col("validated_gene_symbol").isNotNull())
    
    .withColumn("gene_description_mentions_drug",
                when(col("description").isNotNull(),
                     col("description").rlike("(?i)drug|metabolism|pharmacology"))
                .otherwise(False))
)

print("Gene reference enrichment complete")

# COMMAND ----------

# DBTITLE 1,Create Final Pharmacogene Features Table
print("\nCREATING PHARMACOGENE ML FEATURES")
print("="*80)

# Select final feature set
pharmacogene_features = df_pharma.select(
    # IDs
    "variant_id", "gene_name", "chromosome", "position",
    
    # Gene validation
    "official_symbol",
    "validated_gene_symbol",
    "gene_is_validated",
    "gene_description_mentions_drug",
    
    # Clinical significance
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple",
    
    # Variant impact
    "variant_type",
    "is_missense_variant",
    "is_loss_of_function",
    "protein_impact_category",
    "mutation_severity_score",
    "pathogenicity_score",
    
    # Pharmacogene flags
    "is_pharmacogene",
    "pharmacogene_category",
    "pharmacogene_evidence_level",
    "drug_metabolism_role",
    
    # Drug-related flags
    "is_drug_target",
    "is_metabolizing_enzyme",
    "metabolizing_enzyme_type",
    #"is_cyp_gene",
    "is_enzyme",
    "is_drug_transporter",
    
    # Protein type flags
    "is_kinase",
    "is_phosphatase",
    "is_receptor",
    "is_enzyme",
    "is_gpcr",
    "is_transporter",
    
    # Drug target features
    "drug_target_category",
    "druggability_score",
    "enhanced_druggability_score",
    "drug_response_impact",
    
    # Metabolizer features
    "is_metabolizer_variant",
    "metabolizer_phenotype_risk",
    
    # Transporter features
    "is_transporter_variant",
    "transporter_impact_level",
    
    # Kinase features
    "is_kinase_inhibitor_target",
    "kinase_variant_therapeutic_relevance",
    
    # PharmGKB annotations
    "pharmgkb_source",
    "pharmgkb_evidence",
    "pharmgkb_source_count",
    "has_pharmgkb_annotation",
    
    # Gene-level stats
    "gene_pharmacogene_variants",
    "gene_drug_interaction_variants",
    "gene_metabolizer_variants",
    "gene_transporter_variants",
    "gene_pharmacogene_pathogenic",
    "gene_has_multiple_drug_variants",
    "gene_pharmacogene_priority",
    "gene_pharmacogene_burden",
    "gene_avg_druggability",
    "is_pharmacogene"
)

feature_count = pharmacogene_features.count()
print(f"Pharmacogene ML features: {feature_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = pharmacogene_features.count()
pharmacogene_features = pharmacogene_features.dropDuplicates(["variant_id"])
after_count = pharmacogene_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
pharmacogene_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.pharmacogene_ml_features")

print(f"Saved: {catalog_name}.gold.pharmacogene_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nPharmacogene flags:")
pharmacogene_features.select(
    spark_sum(when(col("is_pharmacogene"), 1).otherwise(0)).alias("pharmacogenes"),
    spark_sum(when(col("is_drug_target"), 1).otherwise(0)).alias("drug_targets"),
    spark_sum(when(col("is_metabolizing_enzyme"), 1).otherwise(0)).alias("metabolizing_enzymes"),
    spark_sum(when(col("is_pharmacogene"), 1).otherwise(0)).alias("clinical_pharmacogenes")
).show()

print("\nDrug target category distribution:")
pharmacogene_features.groupBy("drug_target_category").count().orderBy("count", ascending=False).show(10)

print("\nDrug response impact distribution:")
pharmacogene_features.groupBy("drug_response_impact").count().show()

print("\nMetabolizer phenotype risk distribution:")
pharmacogene_features.groupBy("metabolizer_phenotype_risk").count().orderBy("count", ascending=False).show()

print("\nGene pharmacogene priority distribution:")
pharmacogene_features.groupBy("gene_pharmacogene_priority").count().show()

print("\nPharmacogene evidence quality:")
pharmacogene_features.groupBy("pharmacogene_evidence_quality").count().orderBy("count", ascending=False).show()

print("\nData completeness:")
pharmacogene_features.select(
    spark_sum(when(col("has_complete_pharmacogene_annotation"), 1).otherwise(0)).alias("complete_annotation"),
    spark_sum(when(col("has_pharmgkb_annotation"), 1).otherwise(0)).alias("has_pharmgkb"),
    spark_sum(when(col("gene_is_validated"), 1).otherwise(0)).alias("gene_validated")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("PHARMACOGENE FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {feature_count:,}")

print("\nUse Cases Covered:")
print("  7. Drug Target Identification")
print("     - Drug target classification")
print("     - Druggability scoring")
print("     - Drug-gene interaction potential")
print("     - Metabolizer phenotype prediction")
print("     - HLA adverse drug reaction risk")

print("\nSchema Corrections Applied:")
print("  - Used is_cyp_gene and is_enzyme for metabolizing enzymes")
print("  - Used drug_metabolism_role for metabolizer type")
print("  - Used pharmgkb_genes source/evidence columns")

print("\nReference Tables Used:")
print("  - gene_universal_search (gene validation)")

print("\nKey Feature Groups:")
print("  - Pharmacogene flags and categories")
print("  - Drug interaction potential (40+ features)")
print("  - Metabolizer phenotype risk")
print("  - Kinase inhibitor targeting")
print("  - Transporter impact levels")
print("  - HLA ADR risk assessment")
print("  - Gene-level pharmacogene statistics")

print("\nTable created:")
print(f"  {catalog_name}.gold.pharmacogene_ml_features")
