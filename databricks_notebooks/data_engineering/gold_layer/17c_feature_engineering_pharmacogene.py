# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PHARMACOGENE USE CASE (UPDATED)
# MAGIC ##### Module 3: Drug Target Identification
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **UPDATED:** Uses genes_with_pharmgkb (final enriched gene table with PharmGKB data)
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

print("PHARMACOGENE FEATURE ENGINEERING - MODULE 3 (UPDATED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_pharmgkb_genes = spark.table(f"{catalog_name}.silver.pharmgkb_genes")
df_gene_lookup = spark.table(f"{catalog_name}.reference.gene_universal_search")

# Additional enrichment tables
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population = spark.table(f"{catalog_name}.silver.population_frequencies")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

print(f"Variants: {df_variants.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Variant-protein impact: {df_variant_impact.count():,}")
print(f"PharmGKB genes: {df_pharmgkb_genes.count():,}")
print(f"Gene lookup (reference): {df_gene_lookup.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")
print(f"Gene-disease: {df_gene_disease.count():,}")

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
    
    # Join with enriched gene data (has pharmgkb columns built-in)
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
        "druggability_score",
        
        # PharmGKB columns (already in genes_with_pharmgkb)
        "is_pharmacogene",
        "pharmgkb_sources",
        "pharmgkb_evidence",
        "pharmgkb_source_count",
        "pharmacogene_category",
        "pharmacogene_evidence_level",
        "drug_metabolism_role"
    ).dropDuplicates(["gene_name"]), "gene_name", "left")
    
    # Create derived pharmacogene features
    .withColumn("is_drug_transporter", col("is_transporter"))
    
    .withColumn("is_drug_target",
                col("is_kinase") | col("is_receptor") | col("is_gpcr"))
    
    .withColumn("is_metabolizing_enzyme",
                col("is_enzyme") | col("is_phosphatase") | col("is_protease"))
    
    # Enhanced classifications
    .withColumn("metabolizing_enzyme_type",
                when(col("is_enzyme"), lit("Phase2_Enzyme"))
                .otherwise(lit("Not_Metabolizing_Enzyme")))
    
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
                when(col("pharmgkb_sources").isNotNull(), 1).otherwise(0))
    
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
    
    # Metabolizer variant classification
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
    
    # PharmGKB annotation flag
    .withColumn("has_pharmgkb_annotation",
                col("pharmgkb_sources").isNotNull())
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
    
    # Gene-level prioritization
    .withColumn("gene_pharmacogene_priority",
                when(col("gene_drug_interaction_variants") >= 10, lit("Critical_Priority"))
                .when(col("gene_drug_interaction_variants") >= 5, lit("High_Priority"))
                .when(col("gene_drug_interaction_variants") >= 1, lit("Moderate_Priority"))
                .otherwise(lit("Low_Priority")))
)

# Join back to main dataset
df_pharma = df_pharma.join(gene_pharma_stats, "gene_name", "left")

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

# DBTITLE 1,Enrich with Expression Data (Tissue-Specific Drug Response)
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Gene expression for tissue-specific drug metabolism
gene_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
    .groupBy("gene_symbol")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("median_tpm").alias("max_expression_tpm"),
        collect_list("tissue_type").alias("expressed_tissues")
    )
    .withColumn("is_liver_expressed",
                array_contains(col("expressed_tissues"), "Liver"))
    .withColumn("is_kidney_expressed",
                array_contains(col("expressed_tissues"), "Kidney"))
    .withColumn("expression_breadth",
                when(col("tissues_expressed_count") >= 15, lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5, lit("Broad"))
                .otherwise(lit("Tissue_Specific")))
)

df_pharma = (
    df_pharma
    .join(
        gene_expression.select(
            col("gene_symbol").alias("gene_name"),
            "tissues_expressed_count",
            "is_liver_expressed",
            "is_kidney_expressed",
            "expression_breadth"
        ),
        "gene_name",
        "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "is_liver_expressed": False,
        "is_kidney_expressed": False,
        "expression_breadth": "Unknown"
    })
    
    # Metabolism context
    .withColumn("drug_metabolism_context",
                when(col("is_liver_expressed") & col("is_metabolizing_enzyme"),
                     lit("Hepatic_Metabolizer"))
                .when(col("is_kidney_expressed") & col("is_drug_transporter"),
                     lit("Renal_Transporter"))
                .when(col("is_metabolizing_enzyme"),
                     lit("Other_Metabolizer"))
                .otherwise(lit("Non_Metabolic")))
)

print("Expression data enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context (Oncology Drug Targets)
print("\nENRICHING WITH CANCER CONTEXT")
print("="*80)

cancer_genes = (
    df_cancer
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(
        count("*").alias("cancer_mutation_count")
    )
    .withColumn("is_oncology_target",
                col("cancer_mutation_count") >= 50)
)

df_pharma = (
    df_pharma
    .join(cancer_genes, "gene_name", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_oncology_target": False
    })
    
    .withColumn("is_cancer_drug_target",
                col("is_oncology_target") & 
                (col("is_kinase") | col("is_receptor")))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Population Frequencies (Drug Response Variants)
print("\nENRICHING WITH POPULATION FREQUENCIES")
print("="*80)

df_pharma = (
    df_pharma
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
                when(col("is_common_variant") & col("has_drug_interaction_potential"),
                     lit("Common_Drug_Response_Variant"))
                .when(col("is_rare_variant") & col("has_drug_interaction_potential"),
                     lit("Rare_Drug_Response_Variant"))
                .otherwise(lit("Standard_Frequency")))
)

print("Population frequency enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Associations (Drug Indications)
print("\nENRICHING WITH DISEASE ASSOCIATIONS")
print("="*80)

df_pharma = (
    df_pharma
    .join(
        df_gene_disease.select(
            "gene_name",
            col("total_disease_count").alias("disease_count"),
            "has_cancer_disease",
            "has_cardiovascular_disease",
            "has_neurological_disease"
        ),
        "gene_name",
        "left"
    )
    .fillna({
        "disease_count": 0,
        "has_cancer_disease": False,
        "has_cardiovascular_disease": False,
        "has_neurological_disease": False
    })
    
    # Indication mapping
    .withColumn("primary_indication_category",
                when(col("has_cancer_disease") & col("is_pharmacogene"),
                     lit("Oncology"))
                .when(col("has_cardiovascular_disease") & col("is_pharmacogene"),
                     lit("Cardiology"))
                .when(col("has_neurological_disease") & col("is_pharmacogene"),
                     lit("Neurology"))
                .when(col("is_pharmacogene"),
                     lit("Other_Indication"))
                .otherwise(lit("Not_Applicable")))
)

print("Disease association enrichment complete")

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
    
    # Pharmacogene flags (from genes_with_pharmgkb)
    "is_pharmacogene",
    "pharmacogene_category",
    "pharmacogene_evidence_level",
    "drug_metabolism_role",
    
    # Drug-related flags
    "is_drug_target",
    "is_metabolizing_enzyme",
    "metabolizing_enzyme_type",
    "is_enzyme",
    "is_drug_transporter",
    
    # Protein type flags
    "is_kinase",
    "is_phosphatase",
    "is_receptor",
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
    
    # PharmGKB annotations (from genes_with_pharmgkb)
    "pharmgkb_sources",
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
    
    # Expression context (tissue-specific metabolism)
    "tissues_expressed_count",
    "is_liver_expressed",
    "is_kidney_expressed",
    "expression_breadth",
    "drug_metabolism_context",
    
    # Cancer context (oncology targets)
    "cancer_mutation_count",
    "is_oncology_target",
    "is_cancer_drug_target",
    
    # Population frequencies (drug response)
    "allele_frequency",
    "is_common_variant",
    "is_rare_variant",
    "drug_response_frequency_context",
    
    # Disease associations (indications)
    "disease_count",
    "has_cancer_disease",
    "has_cardiovascular_disease",
    "has_neurological_disease",
    "primary_indication_category"
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
    spark_sum(when(col("is_metabolizing_enzyme"), 1).otherwise(0)).alias("metabolizing_enzymes")
).show()

print("\nDrug target category distribution:")
pharmacogene_features.groupBy("drug_target_category").count().orderBy("count", ascending=False).show(10)

print("\nDrug response impact distribution:")
pharmacogene_features.groupBy("drug_response_impact").count().show()

print("\nMetabolizer phenotype risk distribution:")
pharmacogene_features.groupBy("metabolizer_phenotype_risk").count().orderBy("count", ascending=False).show()

print("\nGene pharmacogene priority distribution:")
pharmacogene_features.groupBy("gene_pharmacogene_priority").count().show()

print("\nData completeness:")
pharmacogene_features.select(
    spark_sum(when(col("has_pharmgkb_annotation"), 1).otherwise(0)).alias("has_pharmgkb"),
    spark_sum(when(col("gene_is_validated"), 1).otherwise(0)).alias("gene_validated")
).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("PHARMACOGENE FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {after_count:,}")
print(f"Total columns: {len(pharmacogene_features.columns)}")

print("\nUse Cases Covered:")
print("  7. Drug Target Identification")
print("     - Drug target classification")
print("     - Druggability scoring")
print("     - Drug-gene interaction potential")
print("     - Metabolizer phenotype prediction")

print("\nSilver Tables Used:")
print("  - variants_ultra_enriched (base variants)")
print("  - variant_protein_impact (protein impact)")
print("  - genes_with_pharmgkb (enriched gene + PharmGKB data)")
print("  - pharmgkb_genes (PharmGKB annotations)")
print("  - gene_universal_search (gene validation)")

print("\nKey Feature Groups:")
print("  - Pharmacogene flags and categories (from genes_with_pharmgkb)")
print("  - Drug interaction potential")
print("  - Metabolizer phenotype risk")
print("  - Kinase inhibitor targeting")
print("  - Transporter impact levels")
print("  - Gene-level pharmacogene statistics")

print("\nTable created:")
print(f"  {catalog_name}.gold.pharmacogene_ml_features")
