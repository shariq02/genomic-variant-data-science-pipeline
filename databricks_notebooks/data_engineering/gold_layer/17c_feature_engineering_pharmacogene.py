# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PHARMACOGENE USE CASE
# MAGIC ##### Drug Target Identification
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 7: Drug Target Identification (Pharmacogenes)
# MAGIC
# MAGIC **Creates:**  
# MAGIC gold.pharmacogene_ml_features
# MAGIC
# MAGIC **NOTE:**   
# MAGIC Features-only gold table. No ML target column.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, countDistinct, collect_list, array_contains
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("PHARMACOGENE FEATURE ENGINEERING - MODULE 3")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_variants       = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_genes          = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_pharmgkb_genes = spark.table(f"{catalog_name}.silver.pharmgkb_genes")
df_gene_lookup    = spark.table(f"{catalog_name}.reference.gene_universal_search")
df_gtex           = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer         = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population     = spark.table(f"{catalog_name}.silver.population_frequencies")
df_gene_disease   = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

print(f"Variants:               {df_variants.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"PharmGKB genes:         {df_pharmgkb_genes.count():,}")
print(f"Gene lookup:            {df_gene_lookup.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 1: Pharmacogene Core Features
print("\nSTEP 1: PHARMACOGENE CORE FEATURES")
print("="*80)

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

    .join(
        df_genes.select(
            "gene_name",
            "official_symbol",
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
            "is_pharmacogene",
            "pharmgkb_sources",
            "pharmgkb_evidence",
            "pharmgkb_source_count",
            "pharmacogene_category",
            "pharmacogene_evidence_level",
            "drug_metabolism_role"
        ).dropDuplicates(["gene_name"]),
        "gene_name", "left"
    )

    .withColumn("is_drug_transporter", col("is_transporter"))

    .withColumn("is_drug_target",
                col("is_kinase") | col("is_receptor") | col("is_gpcr"))

    .withColumn("is_metabolizing_enzyme",
                col("is_enzyme") | col("is_phosphatase") | col("is_protease"))

    .withColumn("metabolizing_enzyme_type",
                when(col("is_enzyme"), lit("Phase2_Enzyme"))
                .otherwise(lit("Not_Metabolizing_Enzyme")))

    .withColumn("drug_target_category",
                when(col("is_kinase"), lit("Kinase"))
                .when(col("is_receptor"), lit("Receptor"))
                .when(col("is_gpcr"), lit("GPCR"))
                .when(col("is_enzyme") | col("is_phosphatase") | col("is_protease"),
                      lit("Metabolizing_Enzyme"))
                .when(col("is_transporter"), lit("Transporter"))
                .when(col("is_enzyme"), lit("Other_Enzyme"))
                .when(col("is_kinase") | col("is_receptor") | col("is_gpcr"),
                      lit("Drug_Target"))
                .otherwise(lit("Not_Drug_Target")))

    .withColumn("enhanced_druggability_score",
                coalesce(col("druggability_score"), lit(0.0)) +
                when(col("is_pharmacogene"), 2).otherwise(0) +
                when(col("is_kinase") | col("is_receptor") | col("is_gpcr"), 1).otherwise(0) +
                when(col("pharmgkb_sources").isNotNull(), 1).otherwise(0))

    .withColumn("has_drug_interaction_potential",
                (col("is_pharmacogene") |
                 col("is_kinase") | col("is_receptor") | col("is_gpcr") |
                 col("is_enzyme") | col("is_phosphatase") | col("is_protease")) &
                (col("is_missense_variant") | col("is_loss_of_function")))

    .withColumn("drug_response_impact",
                when(col("has_drug_interaction_potential") & col("is_pathogenic"),
                     lit("High_Impact"))
                .when(col("has_drug_interaction_potential") & col("is_vus"),
                     lit("Moderate_Impact"))
                .when(col("has_drug_interaction_potential"),
                     lit("Low_Impact"))
                .otherwise(lit("No_Impact")))

    .withColumn("is_metabolizer_variant",
                (col("is_enzyme") | col("is_phosphatase") | col("is_protease")) &
                (col("is_missense_variant") | col("is_loss_of_function")))

    .withColumn("metabolizer_phenotype_risk",
                when(col("is_metabolizer_variant") & col("is_loss_of_function"),
                     lit("Poor_Metabolizer_Risk"))
                .when(col("is_metabolizer_variant") & col("is_pathogenic"),
                     lit("Altered_Metabolizer_Risk"))
                .when(col("drug_metabolism_role").isNotNull(),
                     col("drug_metabolism_role"))
                .otherwise(lit("Normal_Metabolizer")))

    .withColumn("is_transporter_variant",
                col("is_transporter") &
                (col("is_missense_variant") | col("is_loss_of_function")))

    .withColumn("transporter_impact_level",
                when(col("is_transporter_variant") & col("is_pathogenic"),
                     lit("High_Transport_Impact"))
                .when(col("is_transporter_variant") & col("is_vus"),
                     lit("Moderate_Transport_Impact"))
                .when(col("is_transporter_variant"),
                     lit("Low_Transport_Impact"))
                .otherwise(lit("No_Transport_Impact")))

    .withColumn("is_kinase_inhibitor_target",
                col("is_kinase") & col("has_kinase_domain"))

    .withColumn("kinase_variant_therapeutic_relevance",
                when(col("is_kinase") & col("has_kinase_domain") &
                     col("is_missense_variant") & col("is_domain_affecting"),
                     lit("High_Therapeutic_Relevance"))
                .when(col("is_kinase") & col("has_kinase_domain") &
                      col("is_missense_variant"),
                     lit("Moderate_Therapeutic_Relevance"))
                .when(col("is_kinase") & col("has_kinase_domain"),
                     lit("Low_Therapeutic_Relevance"))
                .otherwise(lit("No_Therapeutic_Relevance")))

    .withColumn("has_pharmgkb_annotation",
                col("pharmgkb_sources").isNotNull())
)

print("Pharmacogene core features created")

# COMMAND ----------

# DBTITLE 1,Step 2: Gene-Level Pharmacogene Statistics
print("\nSTEP 2: GENE-LEVEL PHARMACOGENE STATISTICS")
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
    .withColumn("gene_pharmacogene_priority",
                when(col("gene_drug_interaction_variants") >= 10, lit("Critical_Priority"))
                .when(col("gene_drug_interaction_variants") >= 5, lit("High_Priority"))
                .when(col("gene_drug_interaction_variants") >= 1, lit("Moderate_Priority"))
                .otherwise(lit("Low_Priority")))
)

df_pharma = (
    df_pharma
    .join(gene_pharma_stats, "gene_name", "left")
    .fillna({
        "gene_pharmacogene_variants":     0,
        "gene_drug_interaction_variants": 0,
        "gene_metabolizer_variants":      0,
        "gene_transporter_variants":      0,
        "gene_pharmacogene_pathogenic":   0,
        "gene_has_multiple_drug_variants": False,
        "gene_avg_druggability":          0.0
    })
    .fillna("No_Burden",    ["gene_pharmacogene_burden"])
    .fillna("Low_Priority", ["gene_pharmacogene_priority"])
)

print("Gene-level statistics calculated")

# COMMAND ----------

# DBTITLE 1,Step 3: Gene Reference Enrichment
print("\nSTEP 3: GENE REFERENCE ENRICHMENT")
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
        "gene_name", "left"
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

# DBTITLE 1,Step 4: Expression Context
print("\nSTEP 4: EXPRESSION CONTEXT")
print("="*80)

gene_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy("gene_name")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("max_tpm").alias("max_expression_tpm"),
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
            "gene_name", "tissues_expressed_count",
            "is_liver_expressed", "is_kidney_expressed", "expression_breadth"
        ),
        "gene_name", "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "is_liver_expressed":      False,
        "is_kidney_expressed":     False,
        "expression_breadth":      "Unknown"
    })
    .withColumn("drug_metabolism_context",
                when(col("is_liver_expressed") &
                     (col("is_enzyme") | col("is_phosphatase") | col("is_protease")),
                     lit("Hepatic_Metabolizer"))
                .when(col("is_kidney_expressed") & col("is_transporter"),
                     lit("Renal_Transporter"))
                .when(col("is_enzyme") | col("is_phosphatase") | col("is_protease"),
                     lit("Other_Metabolizer"))
                .otherwise(lit("Non_Metabolic")))
)

print("Expression enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 5: Cancer Context
print("\nSTEP 5: CANCER CONTEXT")
print("="*80)

cancer_genes = (
    df_cancer
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(count("*").alias("cancer_mutation_count"))
    .withColumn("is_oncology_target",
                col("cancer_mutation_count") >= 50)
)

df_pharma = (
    df_pharma
    .join(cancer_genes, "gene_name", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_oncology_target":    False
    })
    .withColumn("is_cancer_drug_target",
                col("is_oncology_target") &
                (col("is_kinase") | col("is_receptor")))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 6: Population Frequencies
print("\nSTEP 6: POPULATION FREQUENCIES")
print("="*80)

df_pharma = (
    df_pharma
    .join(
        df_population.select(
            "variant_id",
            col("allele_frequency_global").alias("allele_frequency"),
            col("is_common").alias("is_common_variant"),
            col("is_rare").alias("is_rare_variant")
        ),
        "variant_id", "left"
    )
    .fillna({
        "allele_frequency":  0.0,
        "is_common_variant": False,
        "is_rare_variant":   False
    })
    .withColumn("drug_response_frequency_context",
                when(col("is_common_variant") & col("has_drug_interaction_potential"),
                     lit("Common_Drug_Response_Variant"))
                .when(col("is_rare_variant") & col("has_drug_interaction_potential"),
                     lit("Rare_Drug_Response_Variant"))
                .otherwise(lit("Standard_Frequency")))
)

print("Population frequency enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 7: Disease Associations
print("\nSTEP 7: DISEASE ASSOCIATIONS")
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
        "gene_name", "left"
    )
    .fillna({
        "disease_count":             0,
        "has_cancer_disease":        False,
        "has_cardiovascular_disease": False,
        "has_neurological_disease":  False
    })
    .withColumn("primary_indication_category",
                when(col("has_cancer_disease") & col("is_pharmacogene"),
                     lit("Oncology"))
                .when(col("has_cardiovascular_disease") & col("is_pharmacogene"),
                     lit("Cardiology"))
                .when(col("has_neurological_disease") & col("is_pharmacogene"),
                     lit("Neurology"))
                .when(col("is_pharmacogene"), lit("Other_Indication"))
                .otherwise(lit("Not_Applicable")))
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 8: Deduplicate by Variant ID
print("\nSTEP 8: DEDUPLICATE BY VARIANT_ID")
print("="*80)

before_count = df_pharma.count()
df_pharma    = df_pharma.dropDuplicates(["variant_id"])
after_count  = df_pharma.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_pharma.select(
    col("variant_id"),
    col("gene_name"),
    col("chromosome"),
    col("position"),
    col("official_symbol"),
    col("validated_gene_symbol"),
    col("gene_is_validated"),
    col("gene_description_mentions_drug"),
    col("is_pathogenic"),
    col("is_benign"),
    col("is_vus"),
    col("clinical_significance_simple"),
    col("variant_type"),
    col("is_missense_variant"),
    col("is_loss_of_function"),
    col("protein_impact_category"),
    col("mutation_severity_score"),
    col("pathogenicity_score"),
    col("is_pharmacogene"),
    col("pharmacogene_category"),
    col("pharmacogene_evidence_level"),
    col("drug_metabolism_role"),
    col("is_drug_target"),
    col("is_metabolizing_enzyme"),
    col("metabolizing_enzyme_type"),
    col("is_enzyme"),
    col("is_drug_transporter"),
    col("is_kinase"),
    col("is_phosphatase"),
    col("is_receptor"),
    col("is_gpcr"),
    col("is_transporter"),
    col("drug_target_category"),
    col("druggability_score"),
    col("enhanced_druggability_score"),
    col("drug_response_impact"),
    col("is_metabolizer_variant"),
    col("metabolizer_phenotype_risk"),
    col("is_transporter_variant"),
    col("transporter_impact_level"),
    col("is_kinase_inhibitor_target"),
    col("kinase_variant_therapeutic_relevance"),
    col("pharmgkb_sources"),
    col("pharmgkb_evidence"),
    col("pharmgkb_source_count"),
    col("has_pharmgkb_annotation"),
    col("gene_pharmacogene_variants"),
    col("gene_drug_interaction_variants"),
    col("gene_metabolizer_variants"),
    col("gene_transporter_variants"),
    col("gene_pharmacogene_pathogenic"),
    col("gene_has_multiple_drug_variants"),
    col("gene_pharmacogene_priority"),
    col("gene_pharmacogene_burden"),
    col("gene_avg_druggability"),
    col("tissues_expressed_count"),
    col("is_liver_expressed"),
    col("is_kidney_expressed"),
    col("expression_breadth"),
    col("drug_metabolism_context"),
    col("cancer_mutation_count"),
    col("is_oncology_target"),
    col("is_cancer_drug_target"),
    col("allele_frequency"),
    col("is_common_variant"),
    col("is_rare_variant"),
    col("drug_response_frequency_context"),
    col("disease_count"),
    col("has_cancer_disease"),
    col("has_cardiovascular_disease"),
    col("has_neurological_disease"),
    col("primary_indication_category")
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gold.pharmacogene_ml_features
print("\nWRITING gold.pharmacogene_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.pharmacogene_ml_features")

print(f"Saved: {catalog_name}.gold.pharmacogene_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

print(f"Rows:    {rows:,}")
print(f"Columns: {cols}")

print("\nDrug target category breakdown:")
df_check.groupBy("drug_target_category").count().orderBy("count", ascending=False).show(10)

print("\nProcessing complete")
