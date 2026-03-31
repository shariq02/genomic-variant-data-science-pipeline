# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - VARIANT IMPACT USE CASES
# MAGIC ##### Comprehensive Protein Domain Impact and Conservation Analysis
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 8: Variant Impact Assessment (Protein domains + conservation)
# MAGIC - Use Case 9: Splice Site Impact Analysis
# MAGIC
# MAGIC **Creates:** gold.variant_impact_ml_features
# MAGIC
# MAGIC **NOTE:**   
# MAGIC Features-only gold table. No ML target column.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, countDistinct
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("VARIANT IMPACT FEATURE ENGINEERING - MODULE 4")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_variant_impact  = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_conservation    = spark.table(f"{catalog_name}.silver.conservation_with_phylop")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_genes           = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_variants        = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_gtex            = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer          = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_gene_disease    = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_gene_lookup     = spark.table(f"{catalog_name}.reference.gene_universal_search")

print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Conservation:           {df_conservation.count():,}")
print(f"Protein domains:        {df_protein_domains.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Variants:               {df_variants.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")
print(f"Gene lookup:            {df_gene_lookup.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 1: Base Variant Impact Features
print("\nSTEP 1: BASE VARIANT IMPACT FEATURES")
print("="*80)

df_impact = (
    df_variant_impact
    .select(
        "variant_id", "gene_name", "chromosome", "position",
        "is_pathogenic", "is_benign", "is_vus",
        "clinical_significance_simple",
        "clinvar_pathogenicity_class",
        "review_status",
        "review_quality_score",
        "variant_type", "variant_name",
        "reference_allele", "alternate_allele",
        "protein_change", "cdna_change",
        "is_missense_variant",
        "is_frameshift_variant",
        "is_nonsense_variant",
        "is_splice_variant",
        "is_snv",
        "is_insertion",
        "is_deletion",
        "refseq_protein_accession",
        "uniprot_accession",
        "protein_name",
        "has_functional_domain",
        "domain_count",
        "has_zinc_finger",
        "has_kinase_domain",
        "has_receptor_domain",
        "has_sh2_domain",
        "has_sh3_domain",
        "has_ph_domain",
        "affects_functional_domain",
        "mutation_severity_score",
        "pathogenicity_score",
        "protein_impact_category",
        "phylop_score",
        "phastcons_score",
        "gerp_score",
        "cadd_phred",
        "conservation_level",
        "is_highly_conserved",
        "is_constrained",
        "is_likely_deleterious",
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

    .withColumn("domain_impact_severity",
                when(col("affects_functional_domain") & col("is_loss_of_function"),
                     lit("Critical"))
                .when(col("affects_functional_domain") & col("is_missense_variant"),
                     lit("High"))
                .when(col("has_functional_domain") & col("is_missense_variant"),
                     lit("Moderate"))
                .when(col("has_functional_domain"),
                     lit("Low"))
                .otherwise(lit("Unknown")))

    .withColumn("domain_type_count",
                when(col("has_zinc_finger"), 1).otherwise(0) +
                when(col("has_kinase_domain"), 1).otherwise(0) +
                when(col("has_receptor_domain"), 1).otherwise(0) +
                when(col("has_sh2_domain"), 1).otherwise(0) +
                when(col("has_sh3_domain"), 1).otherwise(0) +
                when(col("has_ph_domain"), 1).otherwise(0))

    .withColumn("has_multiple_domain_types",
                col("domain_type_count") > 1)

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

    .withColumn("combined_impact_score",
                coalesce(col("mutation_severity_score"), lit(0)) +
                coalesce(col("conservation_level"), lit(0)) +
                when(col("affects_functional_domain"), 3).otherwise(0) +
                when(col("is_deleterious_by_cadd"), 2).otherwise(0) +
                when(col("is_highly_conserved"), 1).otherwise(0))

    .withColumn("variant_impact_tier",
                when(col("combined_impact_score") >= 12, lit("Tier_1_Critical"))
                .when(col("combined_impact_score") >= 9, lit("Tier_2_High"))
                .when(col("combined_impact_score") >= 6, lit("Tier_3_Moderate"))
                .when(col("combined_impact_score") >= 3, lit("Tier_4_Low"))
                .otherwise(lit("Tier_5_Minimal")))

    .withColumn("is_splice_site_variant",
                col("is_splice_variant") | col("is_splice_affecting"))

    .withColumn("splice_impact_severity",
                when(col("is_splice_affecting") & col("is_pathogenic"),
                     lit("High_Splice_Impact"))
                .when(col("is_splice_variant") & col("is_pathogenic"),
                     lit("Moderate_Splice_Impact"))
                .when(col("is_splice_variant") | col("is_splice_affecting"),
                     lit("Low_Splice_Impact"))
                .otherwise(lit("No_Splice_Impact")))

    .withColumn("lof_category",
                when(col("is_nonsense_variant"), lit("Nonsense_Mediated"))
                .when(col("is_frameshift_variant"), lit("Frameshift"))
                .when(col("is_splice_affecting"), lit("Splice_Disruption"))
                .when(col("is_loss_of_function"), lit("Other_LoF"))
                .otherwise(lit("Not_LoF")))
)

print("Base impact features created")

# COMMAND ----------

# DBTITLE 1,Step 2: Gene-Level Features
print("\nSTEP 2: GENE-LEVEL FEATURES")
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
        "gene_name", "left"
    )
    .withColumn("is_druggable_gene",
                coalesce(col("druggability_score"), lit(0.0)) >= 3.0)
    .withColumn("is_key_protein_type",
                col("is_kinase") | col("is_receptor") | col("is_enzyme"))
    .withColumn("clinical_impact_priority",
                when(col("is_pharmacogene") & col("is_pathogenic") &
                     col("domain_impact_severity").eqNullSafe(lit("Critical")),
                     lit("Top_Priority"))
                .when((col("is_druggable_gene") | col("is_pharmacogene")) &
                      col("is_pathogenic"),
                     lit("High_Priority"))
                .when(col("is_key_protein_type") & col("is_pathogenic"),
                     lit("Medium_Priority"))
                .when(col("is_pathogenic"),
                     lit("Standard_Priority"))
                .otherwise(lit("Low_Priority")))
)

print("Gene-level enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 3: Expression Context
print("\nSTEP 3: EXPRESSION CONTEXT")
print("="*80)

gene_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy("gene_name")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("max_tpm").alias("max_expression_tpm"),
        avg("max_tpm").alias("avg_expression_tpm")
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
            "gene_name", "tissues_expressed_count", "max_expression_tpm",
            "is_broadly_expressed", "is_highly_expressed"
        ),
        "gene_name", "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "max_expression_tpm":      0.0,
        "is_broadly_expressed":    False,
        "is_highly_expressed":     False
    })
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

print("Expression enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 4: Cancer Context
print("\nSTEP 4: CANCER CONTEXT")
print("="*80)

cancer_variants = (
    df_cancer
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(count("*").alias("cancer_mutation_count"))
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
)

df_impact = (
    df_impact
    .join(cancer_variants, "gene_name", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_cancer_gene":        False
    })
    .withColumn("is_cancer_relevant_variant",
                col("is_cancer_gene") &
                (col("is_missense_variant") | col("is_loss_of_function")))
    .withColumn("cancer_variant_priority",
                when(col("is_cancer_gene") & col("is_pathogenic") & col("is_high_impact"),
                     lit("Cancer_High_Priority"))
                .when(col("is_cancer_gene") & col("is_pathogenic"),
                     lit("Cancer_Medium_Priority"))
                .when(col("is_cancer_gene") &
                      (col("is_missense_variant") | col("is_loss_of_function")),
                     lit("Cancer_Research_Candidate"))
                .otherwise(lit("Not_Cancer_Priority")))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 5: Disease Association
print("\nSTEP 5: DISEASE ASSOCIATION")
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
        "gene_name", "left"
    )
    .fillna({
        "disease_count":             0,
        "has_cancer_disease":        False,
        "has_neurological_disease":  False,
        "has_metabolic_disease":     False,
        "has_cardiovascular_disease": False
    })
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

# DBTITLE 1,Step 6: Gene-Level Impact Statistics
print("\nSTEP 6: GENE-LEVEL IMPACT STATISTICS")
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

# DBTITLE 1,Step 7: Deduplicate by Variant ID
print("\nSTEP 7: DEDUPLICATE BY VARIANT_ID")
print("="*80)

before_count = df_impact.count()
df_impact    = df_impact.dropDuplicates(["variant_id"])
after_count  = df_impact.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_impact.select(
    col("variant_id"),
    col("gene_name"),
    col("official_symbol"),
    col("chromosome"),
    col("position"),
    col("is_pathogenic"),
    col("is_benign"),
    col("is_vus"),
    col("clinical_significance_simple"),
    col("clinvar_pathogenicity_class"),
    col("review_status"),
    col("review_quality_score"),
    col("variant_type"),
    col("variant_name"),
    col("reference_allele"),
    col("alternate_allele"),
    col("protein_change"),
    col("cdna_change"),
    col("is_missense_variant"),
    col("is_frameshift_variant"),
    col("is_nonsense_variant"),
    col("is_splice_variant"),
    col("is_snv"),
    col("is_insertion"),
    col("is_deletion"),
    col("refseq_protein_accession"),
    col("uniprot_accession"),
    col("protein_name"),
    col("has_functional_domain"),
    col("domain_count"),
    col("has_zinc_finger"),
    col("has_kinase_domain"),
    col("has_receptor_domain"),
    col("has_sh2_domain"),
    col("has_sh3_domain"),
    col("has_ph_domain"),
    col("affects_functional_domain"),
    col("domain_impact_severity"),
    col("domain_type_count"),
    col("has_multiple_domain_types"),
    col("mutation_severity_score"),
    col("pathogenicity_score"),
    col("protein_impact_category"),
    col("combined_impact_score"),
    col("variant_impact_tier"),
    col("phylop_score"),
    col("phastcons_score"),
    col("gerp_score"),
    col("cadd_phred"),
    col("conservation_level"),
    col("is_highly_conserved"),
    col("is_constrained"),
    col("is_likely_deleterious"),
    col("conservation_impact_class"),
    col("is_high_impact"),
    col("is_very_high_impact"),
    col("is_conservation_constrained"),
    col("is_highly_conserved_region"),
    col("is_domain_affecting"),
    col("is_loss_of_function"),
    col("is_splice_affecting"),
    col("has_cadd_score"),
    col("is_deleterious_by_cadd"),
    col("is_splice_site_variant"),
    col("splice_impact_severity"),
    col("lof_category"),
    col("is_kinase"),
    col("is_receptor"),
    col("is_enzyme"),
    col("is_pharmacogene"),
    col("druggability_score"),
    col("is_druggable_gene"),
    col("is_key_protein_type"),
    col("is_well_annotated"),
    col("clinical_impact_priority"),
    col("tissues_expressed_count"),
    col("max_expression_tpm"),
    col("is_broadly_expressed"),
    col("is_highly_expressed"),
    col("expression_impact_context"),
    col("cancer_mutation_count"),
    col("is_cancer_gene"),
    col("is_cancer_relevant_variant"),
    col("cancer_variant_priority"),
    col("disease_count"),
    col("has_cancer_disease"),
    col("has_neurological_disease"),
    col("has_metabolic_disease"),
    col("has_cardiovascular_disease"),
    col("is_disease_associated_gene"),
    col("disease_impact_category"),
    col("disease_specific_priority"),
    col("gene_total_variants"),
    col("gene_high_impact_count"),
    col("gene_very_high_impact_count"),
    col("gene_lof_count"),
    col("gene_splice_variant_count"),
    col("gene_domain_affecting_count"),
    col("gene_avg_impact_score"),
    col("gene_max_impact_score"),
    col("gene_impact_burden"),
    col("gene_lof_tolerance"),
    col("gene_variant_impact_priority")
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gold.variant_impact_ml_features
print("\nWRITING gold.variant_impact_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_impact_ml_features")

print(f"Saved: {catalog_name}.gold.variant_impact_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.variant_impact_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

print(f"Rows:    {rows:,}")
print(f"Columns: {cols}")

print("\nVariant impact tier breakdown:")
df_check.groupBy("variant_impact_tier").count().orderBy("variant_impact_tier").show()

print("\nProcessing complete")
