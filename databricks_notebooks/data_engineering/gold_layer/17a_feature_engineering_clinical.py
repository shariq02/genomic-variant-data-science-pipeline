# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - CLINICAL USE CASES
# MAGIC #####Clinical Pathogenicity, Inheritance Patterns, Gene Statistics
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 1: Clinical Pathogenicity Prediction (Pathogenic vs Benign)
# MAGIC - Use Case 2: Inheritance Pattern Analysis (Dominant/Recessive)
# MAGIC - Use Case 3: Gene-Level Statistics
# MAGIC
# MAGIC **Creates:** gold.clinical_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only from raw silver measurements. No target reference.
# MAGIC - Pass 2: Targets only from ClinVar significance. Independent of Pass 1.
# MAGIC - Final:  Features and targets joined on variant_id. Written to gold.
# MAGIC
# MAGIC **NOTE ON LEAKAGE COLUMNS:**  
# MAGIC The following columns are written to gold per schema definition.
# MAGIC Notebook 29b scans and drops them dynamically before split notebooks run:
# MAGIC   clinical_significance_simple, clinvar_pathogenicity_class,
# MAGIC   protein_impact_category, is_cancer_relevant, x_linked_risk_modifier,
# MAGIC   inheritance_pathogenicity_modifier, frequency_pathogenicity_conflict,
# MAGIC   expression_context

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, countDistinct, length
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("CLINICAL FEATURE ENGINEERING (TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_variants       = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_genes          = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_lookup    = spark.table(f"{catalog_name}.reference.gene_universal_search")
df_gtex           = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer         = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_population     = spark.table(f"{catalog_name}.silver.population_frequencies")
df_gene_disease   = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

print(f"Variants:               {df_variants.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Gene lookup:            {df_gene_lookup.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Population frequencies: {df_population.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Core Variant Impact Features
print("\nPASS 1 - STEP 1: CORE VARIANT IMPACT FEATURES")
print("="*80)

df_features = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("gene_name"),
        col("chromosome"),
        col("position"),
        col("variant_type"),
        col("is_missense_variant"),
        col("is_frameshift_variant"),
        col("is_nonsense_variant"),
        col("is_splice_variant"),
        col("is_snv"),
        col("is_insertion"),
        col("is_deletion"),
        col("mutation_severity_score"),
        col("pathogenicity_score"),
        col("phylop_score"),
        col("phastcons_score"),
        col("cadd_phred"),
        col("conservation_level"),
        col("is_highly_conserved"),
        col("is_constrained"),
        col("is_likely_deleterious"),
        col("is_high_impact"),
        col("is_very_high_impact"),
        col("is_conservation_constrained"),
        col("is_domain_affecting"),
        col("is_loss_of_function"),
        col("is_deleterious_by_cadd"),
        col("has_functional_domain"),
        col("domain_count"),
        col("has_kinase_domain"),
        col("has_receptor_domain"),
        col("review_status"),
        col("review_quality_score"),
        # Schema columns - leakage candidates written for schema compliance
        # 29b drops these dynamically before split notebooks run
        col("clinical_significance_simple"),
        col("clinvar_pathogenicity_class"),
        col("protein_impact_category")
    )
    .withColumn("is_coding_variant",
                col("is_missense_variant") |
                col("is_frameshift_variant") |
                col("is_nonsense_variant"))
    .withColumn("is_regulatory_variant",
                col("is_splice_variant"))
    .withColumn("has_strong_evidence",
                col("review_quality_score") >= 2)
    .withColumn("combined_pathogenicity_risk",
                coalesce(col("mutation_severity_score"), lit(0)) +
                coalesce(col("conservation_level"), lit(0)) +
                when(col("is_domain_affecting"), 2).otherwise(0) +
                when(col("is_deleterious_by_cadd"), 2).otherwise(0))
    .withColumn("has_conservation_data",
                col("phylop_score").isNotNull() | col("cadd_phred").isNotNull())
    .withColumn("has_complete_annotation",
                col("mutation_severity_score").isNotNull() &
                col("has_conservation_data"))
    .withColumn("clinical_sig_is_uncertain",
                col("review_quality_score") == 0)
)

print(f"Core variant features: {df_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: Gene Data Enrichment
print("\nPASS 1 - STEP 2: GENE DATA ENRICHMENT")
print("="*80)

df_features = (
    df_features
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
        "gene_name", "left"
    )
    .withColumn("gene_is_validated",      col("validated_gene_id").isNotNull())
    .withColumn("gene_has_omim",          col("gene_mim_id").isNotNull())
    .withColumn("gene_has_ensembl",       col("gene_ensembl_id").isNotNull())
    .withColumn("gene_description_length",
                when(col("description").isNotNull(), length(col("description"))).otherwise(0))
    .withColumn("gene_is_well_characterized",
                col("gene_description_length") > 50)
)

print("Gene enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Expression Data
print("\nPASS 1 - STEP 3: EXPRESSION DATA")
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
    # Schema column - leakage candidate, written for schema compliance
    # 29b drops dynamically before split notebooks run
    .withColumn("expression_context",
                when(col("tissues_expressed_count") >= 10, lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5, lit("Broad"))
                .otherwise(lit("Tissue_Specific")))
)

df_features = (
    df_features
    .join(
        gene_expression.select(
            "gene_name", "tissues_expressed_count", "max_expression_tpm",
            "is_broadly_expressed", "is_highly_expressed", "expression_context"
        ),
        "gene_name", "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "max_expression_tpm":      0.0,
        "is_broadly_expressed":    False,
        "is_highly_expressed":     False,
        "expression_context":      "Unknown"
    })
)

print("Expression enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Cancer Context
print("\nPASS 1 - STEP 4: CANCER CONTEXT")
print("="*80)

cancer_genes = (
    df_cancer
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(count("*").alias("cancer_mutation_count"))
    .withColumn("is_cancer_gene", col("cancer_mutation_count") >= 10)
)

df_features = (
    df_features
    .join(cancer_genes, "gene_name", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_cancer_gene":        False
    })
    # Schema column - leakage candidate, written for schema compliance
    # 29b drops dynamically before split notebooks run
    .withColumn("is_cancer_relevant", col("is_cancer_gene"))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Population Frequencies
print("\nPASS 1 - STEP 5: POPULATION FREQUENCIES")
print("="*80)

df_features = (
    df_features
    .join(
        df_population.select(
            col("variant_id"),
            col("allele_frequency_global").alias("population_allele_frequency"),
            col("is_common").alias("is_common_in_population"),
            col("is_rare").alias("is_rare_in_population")
        ),
        "variant_id", "left"
    )
    .fillna({
        "population_allele_frequency": 0.0,
        "is_common_in_population":     False,
        "is_rare_in_population":       False
    })
    # Schema column - leakage candidate, written for schema compliance
    # 29b drops dynamically before split notebooks run
    .withColumn("frequency_pathogenicity_conflict",
                col("is_common_in_population"))
)

print("Population frequency enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Disease Associations
print("\nPASS 1 - STEP 6: DISEASE ASSOCIATIONS")
print("="*80)

df_features = (
    df_features
    .join(
        df_gene_disease.select(
            "gene_name",
            col("total_disease_count").alias("disease_count"),
            "has_cancer_disease",
            "has_neurological_disease"
        ),
        "gene_name", "left"
    )
    .fillna({
        "disease_count":            0,
        "has_cancer_disease":       False,
        "has_neurological_disease": False
    })
    .withColumn("is_disease_gene", col("disease_count") >= 1)
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: Inheritance Pattern Features
print("\nPASS 1 - STEP 7: INHERITANCE PATTERN FEATURES")
print("="*80)

df_features = (
    df_features
    .withColumn("inheritance_pattern",
                when(col("chromosome") == "X", lit("X_Linked"))
                .when(col("chromosome") == "Y", lit("Y_Linked"))
                .when(col("chromosome") == "MT", lit("Mitochondrial"))
                .otherwise(lit("Autosomal")))
    .withColumn("is_mitochondrial_variant", col("chromosome") == "MT")
    .withColumn("is_y_linked_variant",      col("chromosome") == "Y")
    .withColumn("is_x_linked_variant",      col("chromosome") == "X")
    .withColumn("is_autosomal_variant",
                ~col("chromosome").isin("X", "Y", "MT"))
    # Schema columns - leakage candidates, written for schema compliance
    # 29b drops dynamically before split notebooks run
    .withColumn("x_linked_risk_modifier",
                when(col("chromosome") == "X", lit("X_Linked_Risk"))
                .otherwise(lit("No_X_Risk")))
    .withColumn("inheritance_pathogenicity_modifier",
                when(col("chromosome") == "MT", lit("Mitochondrial_Risk"))
                .when(col("chromosome") == "X", lit("X_Linked_Risk"))
                .otherwise(lit("Standard")))
)

print("Inheritance pattern features created")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 8: Gene-Level Statistics
print("\nPASS 1 - STEP 8: GENE-LEVEL STATISTICS")
print("="*80)

gene_stats = (
    df_variant_impact
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("gene_pathogenic_count"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("gene_benign_count"),
        spark_sum(when(col("is_vus"), 1).otherwise(0)).alias("gene_vus_count"),
        spark_sum(when(col("is_missense_variant"), 1).otherwise(0)).alias("gene_missense_count"),
        spark_sum(when(col("is_frameshift_variant"), 1).otherwise(0)).alias("gene_frameshift_count"),
        spark_sum(when(col("is_nonsense_variant"), 1).otherwise(0)).alias("gene_nonsense_count"),
        spark_sum(when(col("is_splice_variant"), 1).otherwise(0)).alias("gene_splice_count"),
        avg(col("review_quality_score")).alias("gene_avg_review_quality")
    )
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

df_features = (
    df_features
    .join(gene_stats, "gene_name", "left")
    .withColumn("gene_mutation_burden",
                when(col("gene_total_variants") >= 1000, lit("Very_High"))
                .when(col("gene_total_variants") >= 500,  lit("High"))
                .when(col("gene_total_variants") >= 100,  lit("Moderate"))
                .when(col("gene_total_variants") >= 10,   lit("Low"))
                .otherwise(lit("Very_Low")))
    .withColumn("gene_is_pathogenic_enriched",
                coalesce(col("gene_pathogenic_ratio"), lit(0.0)) > 0.1)
    .withColumn("gene_is_benign_enriched",
                coalesce(col("gene_benign_ratio"), lit(0.0)) > 0.5)
    .withColumn("gene_is_vus_enriched",
                coalesce(col("gene_vus_ratio"), lit(0.0)) > 0.5)
    .withColumn("gene_variant_profile",
                when(col("gene_is_pathogenic_enriched"), lit("Pathogenic_Enriched"))
                .when(col("gene_is_benign_enriched"),    lit("Benign_Enriched"))
                .when(col("gene_is_vus_enriched"),       lit("VUS_Enriched"))
                .otherwise(lit("Mixed_Profile")))
    .withColumn("gene_has_high_lof_burden",
                coalesce(col("gene_lof_variant_ratio"), lit(0.0)) > 0.1)
    .withColumn("gene_has_quality_annotations",
                coalesce(col("gene_avg_review_quality"), lit(0.0)) >= 1.0)
)

print("Gene-level statistics created")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Deduplicate by Variant ID
print("\nPASS 1 - STEP 9: DEDUPLICATE BY VARIANT_ID")
print("="*80)

before_count = df_features.count()
df_features  = df_features.dropDuplicates(["variant_id"])
after_count  = df_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")
print(f"Feature columns:      {len(df_features.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 2 - TARGETS ONLY

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variables
print("\nPASS 2 - DERIVING TARGET VARIABLES")
print("="*80)
print("Targets: target_is_pathogenic, target_is_benign, target_is_vus")

df_target = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("is_pathogenic").alias("target_is_pathogenic"),
        col("is_benign").alias("target_is_benign"),
        col("is_vus").alias("target_is_vus")
    )
    .dropDuplicates(["variant_id"])
)

target_stats = df_target.agg(
    spark_sum(when(col("target_is_pathogenic"), 1).otherwise(0)).alias("pathogenic"),
    spark_sum(when(col("target_is_benign"), 1).otherwise(0)).alias("benign"),
    spark_sum(when(col("target_is_vus"), 1).otherwise(0)).alias("vus"),
    count("*").alias("total")
).collect()[0]

total    = target_stats["total"]
path_pct = target_stats["pathogenic"] / total * 100 if total > 0 else 0
ben_pct  = target_stats["benign"]     / total * 100 if total > 0 else 0
vus_pct  = target_stats["vus"]        / total * 100 if total > 0 else 0

print(f"  target_is_pathogenic: {target_stats['pathogenic']:,} ({path_pct:.2f}%)")
print(f"  target_is_benign:     {target_stats['benign']:,} ({ben_pct:.2f}%)")
print(f"  target_is_vus:        {target_stats['vus']:,} ({vus_pct:.2f}%)")

for label, count_val, pct in [
    ("target_is_pathogenic", target_stats["pathogenic"], path_pct),
    ("target_is_benign",     target_stats["benign"],     ben_pct),
    ("target_is_vus",        target_stats["vus"],        vus_pct),
]:
    if count_val == 0:
        raise ValueError(f"{label} has zero positives. Fix threshold.")
    elif pct < 1.0:
        print(f"WARN: {label} positive rate {pct:.2f}% is very low.")
    else:
        print(f"OK: {label} positive rate {pct:.2f}%.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL JOIN - Features + Targets

# COMMAND ----------

# DBTITLE 1,Final Join: Features + Targets
print("\nFINAL JOIN - FEATURES AND TARGETS")
print("="*80)

df_joined = (
    df_features
    .join(df_target, on="variant_id", how="left")
    .fillna({
        "target_is_pathogenic": False,
        "target_is_benign":     False,
        "target_is_vus":        False
    })
)

print(f"Joined rows:    {df_joined.count():,}")
print(f"Joined columns: {len(df_joined.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_joined.select(
    col("variant_id"),
    col("gene_name"),
    col("chromosome"),
    col("position"),
    col("official_gene_symbol"),
    col("gene_is_validated"),
    col("gene_has_omim"),
    col("gene_has_ensembl"),
    col("gene_is_well_characterized"),
    col("is_pharmacogene"),
    col("druggability_score"),
    col("target_is_pathogenic"),
    col("target_is_benign"),
    col("target_is_vus"),
    col("clinical_significance_simple"),   
    col("clinvar_pathogenicity_class"),    
    col("clinical_sig_is_uncertain"),
    col("review_quality_score"),
    col("has_strong_evidence"),
    col("mutation_severity_score"),
    col("pathogenicity_score"),
    col("combined_pathogenicity_risk"),
    col("protein_impact_category"),        
    col("is_coding_variant"),
    col("is_regulatory_variant"),
    col("is_missense_variant"),
    col("is_frameshift_variant"),
    col("is_nonsense_variant"),
    col("is_splice_variant"),
    col("phylop_score"),
    col("cadd_phred"),
    col("conservation_level"),
    col("is_highly_conserved"),
    col("is_constrained"),
    col("is_likely_deleterious"),
    col("is_high_impact"),
    col("is_very_high_impact"),
    col("is_domain_affecting"),
    col("is_loss_of_function"),
    col("is_deleterious_by_cadd"),
    col("has_functional_domain"),
    col("domain_count"),
    col("has_conservation_data"),
    col("has_complete_annotation"),
    col("inheritance_pattern"),
    col("x_linked_risk_modifier"),         
    col("inheritance_pathogenicity_modifier"), 
    col("is_mitochondrial_variant"),
    col("is_y_linked_variant"),
    col("is_x_linked_variant"),
    col("is_autosomal_variant"),
    col("gene_total_variants"),
    col("gene_pathogenic_count"),
    col("gene_benign_count"),
    col("gene_vus_count"),
    col("gene_pathogenic_ratio"),
    col("gene_benign_ratio"),
    col("gene_vus_ratio"),
    col("gene_mutation_burden"),
    col("gene_is_pathogenic_enriched"),
    col("gene_is_benign_enriched"),
    col("gene_is_vus_enriched"),
    col("gene_variant_profile"),
    col("gene_has_high_lof_burden"),
    col("gene_avg_review_quality"),
    col("gene_has_quality_annotations"),
    col("gene_missense_count"),
    col("gene_frameshift_count"),
    col("gene_nonsense_count"),
    col("gene_splice_count"),
    col("gene_lof_variant_ratio"),
    col("tissues_expressed_count"),
    col("max_expression_tpm"),
    col("is_broadly_expressed"),
    col("is_highly_expressed"),
    col("expression_context"),            
    col("cancer_mutation_count"),
    col("is_cancer_gene"),
    col("is_cancer_relevant"),             
    col("population_allele_frequency"),
    col("is_common_in_population"),
    col("is_rare_in_population"),
    col("frequency_pathogenicity_conflict"), 
    col("disease_count"),
    col("has_cancer_disease"),
    col("has_neurological_disease"),
    col("is_disease_gene")
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant ID
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = df_final.count()
df_final     = df_final.dropDuplicates(["variant_id"])
after_count  = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Write gold.clinical_ml_features
print("\nWRITING gold.clinical_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.clinical_ml_features")

print(f"Saved: {catalog_name}.gold.clinical_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.clinical_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

target_stats = df_check.agg(
    spark_sum(when(col("target_is_pathogenic"), 1).otherwise(0)).alias("pathogenic"),
    spark_sum(when(col("target_is_benign"), 1).otherwise(0)).alias("benign"),
    spark_sum(when(col("target_is_vus"), 1).otherwise(0)).alias("vus"),
    count("*").alias("total")
).collect()[0]

total    = target_stats["total"]
path_pct = target_stats["pathogenic"] / total * 100 if total > 0 else 0
ben_pct  = target_stats["benign"]     / total * 100 if total > 0 else 0
vus_pct  = target_stats["vus"]        / total * 100 if total > 0 else 0

print(f"Rows:    {rows:,}")
print(f"Columns: {cols}")
print(f"  target_is_pathogenic: {target_stats['pathogenic']:,} ({path_pct:.2f}%)")
print(f"  target_is_benign:     {target_stats['benign']:,} ({ben_pct:.2f}%)")
print(f"  target_is_vus:        {target_stats['vus']:,} ({vus_pct:.2f}%)")
print("\nProcessing complete")
