# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - DRUG RESPONSE ANALYSIS
# MAGIC ##### Module: Comprehensive Variant-Level Drug Response Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 11: Treatment Response Prediction
# MAGIC
# MAGIC **Creates:** gold.variant_drug_response_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: All features from raw silver measurements. No target reference.
# MAGIC - Pass 2: Target only. is_actionable_pharmacogene_variant derived independently.
# MAGIC - Final:  Features and target joined on variant_id. Written to gold.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max,
    when, lit, trim, upper, coalesce, row_number, avg
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD DRUG RESPONSE FEATURES - TWO-PASS")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_pharmgkb_variants = spark.table(f"{catalog_name}.silver.pharmgkb_variants")
df_relationships     = spark.table(f"{catalog_name}.silver.pharmgkb_relationships")
df_variant_impact    = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_variants          = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_gtex              = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_population        = spark.table(f"{catalog_name}.silver.population_frequencies")
df_gene_disease      = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_cancer            = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_genes             = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")

print(f"PharmGKB variants:  {df_pharmgkb_variants.count():,}")
print(f"PharmGKB rels:      {df_relationships.count():,}")
print(f"Variant impact:     {df_variant_impact.count():,}")
print(f"Variants enriched:  {df_variants.count():,}")
print(f"GTEx expression:    {df_gtex.count():,}")
print(f"Population freqs:   {df_population.count():,}")
print(f"Gene-disease:       {df_gene_disease.count():,}")
print(f"Cancer mutations:   {df_cancer.count():,}")
print(f"Genes (enriched):   {df_genes.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Variant-Drug Relationships
print("\nPASS 1 - STEP 1: VARIANT-DRUG RELATIONSHIPS")
print("="*80)

df_variant_relationships = (
    df_relationships
    .filter(col("entity1_type") == "Variant")
    .select(
        col("entity1_id").alias("variant_pharmgkb_id"),
        col("entity1_name").alias("variant_name_rel"),
        col("entity2_type").alias("related_entity_type"),
        col("entity2_name").alias("related_entity_name"),
        col("evidence")
    )
)

df_variant_drug_counts = (
    df_variant_relationships
    .groupBy("variant_pharmgkb_id")
    .agg(
        count("*").alias("total_interactions"),
        countDistinct("related_entity_type").alias("interaction_type_count"),
        spark_sum(when(col("related_entity_type") == "Chemical", 1).otherwise(0)).alias("drug_interaction_count"),
        spark_sum(when(col("related_entity_type") == "Disease",  1).otherwise(0)).alias("disease_interaction_count"),
        spark_sum(when(col("evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count")
    )
)

print(f"Variants with interactions: {df_variant_drug_counts.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: PharmGKB Variant Annotations
print("\nPASS 1 - STEP 2: PHARMGKB VARIANT ANNOTATIONS")
print("="*80)

df_pharmgkb_features = (
    df_pharmgkb_variants
    .select(
        col("variant_id").alias("variant_pharmgkb_id"),
        col("variant_name"),
        upper(trim(col("gene_symbols"))).alias("gene_symbol"),
        col("location").alias("variant_location")
    )
    .withColumn("has_annotation", lit(True))
)

print(f"PharmGKB variant features: {df_pharmgkb_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Join Variant Impact
print("\nPASS 1 - STEP 3: JOIN VARIANT IMPACT")
print("="*80)

df_variant_impact_prep = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("variant_name").alias("clinvar_variant_name"),
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("clinical_significance_simple"),
        col("is_pathogenic"),
        col("is_benign"),
        col("is_vus"),
        col("is_missense_variant"),
        col("is_frameshift_variant"),
        col("is_nonsense_variant"),
        col("is_splice_variant"),
        col("has_functional_domain"),
        col("affects_functional_domain"),
        col("phylop_score"),
        col("cadd_phred"),
        col("conservation_level"),
        col("pathogenicity_score"),
        col("mutation_severity_score")
    )
)

df_with_impact = (
    df_pharmgkb_features
    .join(df_variant_impact_prep, on="gene_symbol", how="right")
)

window_spec    = Window.partitionBy("variant_id").orderBy(col("variant_pharmgkb_id").desc_nulls_last())
df_with_impact = (
    df_with_impact
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print(f"Variants with impact: {df_with_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Expression Data
print("\nPASS 1 - STEP 4: EXPRESSION DATA")
print("="*80)

liver_genes = (
    df_gtex
    .filter((col("tissue_type") == "Liver") & (col("max_tpm") > 1.0))
    .select(col("gene_name"))
    .distinct()
    .withColumn("is_liver_expressed", lit(True))
)

gene_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy("gene_name")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("max_tpm").alias("max_expression_tpm")
    )
    .join(liver_genes, "gene_name", "left")
    .fillna({"is_liver_expressed": False})
    .withColumn("expression_breadth",
                when(col("tissues_expressed_count") >= 15, lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5,  lit("Broad"))
                .otherwise(lit("Tissue_Specific")))
)

df_with_impact = (
    df_with_impact
    .join(gene_expression,
          df_with_impact["gene_symbol"] == gene_expression["gene_name"], "left")
    .drop(gene_expression["gene_name"])
    .fillna({
        "tissues_expressed_count": 0,
        "max_expression_tpm":      0.0,
        "is_liver_expressed":      False,
        "expression_breadth":      "Unknown"
    })
)

print("Expression enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Population Frequencies
print("\nPASS 1 - STEP 5: POPULATION FREQUENCIES")
print("="*80)

df_with_impact = (
    df_with_impact
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
        "allele_frequency":   0.0,
        "is_common_variant":  False,
        "is_rare_variant":    False
    })
    .withColumn("drug_response_frequency_context",
                when(col("is_common_variant"), lit("Common_Drug_Response"))
                .when(col("is_rare_variant"),  lit("Rare_Drug_Response"))
                .otherwise(lit("Standard_Frequency")).cast(StringType()))
)

print("Population frequency enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Disease Associations
print("\nPASS 1 - STEP 6: DISEASE ASSOCIATIONS")
print("="*80)

df_with_impact = (
    df_with_impact
    .join(
        df_gene_disease.select(
            upper(trim(col("gene_name"))).alias("gene_symbol"),
            col("total_disease_count"),
            col("has_cancer_disease"),
            col("has_cardiovascular_disease"),
            col("has_neurological_disease")
        ),
        "gene_symbol", "left"
    )
    .fillna({
        "total_disease_count":        0,
        "has_cancer_disease":         False,
        "has_cardiovascular_disease": False,
        "has_neurological_disease":   False
    })
    .withColumn("primary_indication_category",
                when(col("has_cancer_disease"),          lit("Oncology"))
                .when(col("has_cardiovascular_disease"), lit("Cardiology"))
                .when(col("has_neurological_disease"),   lit("Neurology"))
                .otherwise(lit("Other")))
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: Cancer Context
print("\nPASS 1 - STEP 7: CANCER CONTEXT")
print("="*80)

cancer_genes = (
    df_cancer
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(count("*").alias("cancer_mutation_count"))
    .withColumn("is_cancer_gene", col("cancer_mutation_count") >= 10)
)

df_with_impact = (
    df_with_impact
    .join(cancer_genes, "gene_symbol", "left")
    .fillna({
        "cancer_mutation_count": 0,
        "is_cancer_gene":        False
    })
    .withColumn("is_potential_resistance_variant",
                col("is_cancer_gene") & col("is_missense_variant"))
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 8: Gene Context
print("\nPASS 1 - STEP 8: GENE CONTEXT")
print("="*80)

df_with_impact = (
    df_with_impact
    .join(
        df_genes.select(
            upper(trim(col("official_symbol"))).alias("gene_symbol"),
            col("is_pharmacogene"),
            col("druggability_score"),
            col("pharmacogene_category"),
            col("drug_metabolism_role")
        ),
        "gene_symbol", "left"
    )
    .fillna({
        "is_pharmacogene":      False,
        "druggability_score":   0.0,
        "pharmacogene_category": "Unknown",
        "drug_metabolism_role": "Unknown"
    })
)

print("Gene context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Classification Flags and Scores
print("\nPASS 1 - STEP 9: CLASSIFICATION FLAGS AND SCORES")
print("="*80)

df_features = (
    df_with_impact
    .withColumn("has_pharmgkb_annotation",
                when(col("variant_pharmgkb_id").isNotNull(), True).otherwise(False))
    .withColumn("has_high_conservation",
                when(col("conservation_level") >= 1, True).otherwise(False))
    .withColumn("affects_drug_metabolism",
                when(col("has_pharmgkb_annotation") & col("has_functional_domain"),
                     True).otherwise(False))
    .withColumn("affects_drug_efficacy",
                when(col("has_pharmgkb_annotation") &
                     (col("is_missense_variant") | col("affects_functional_domain")),
                     True).otherwise(False))
    .withColumn("is_high_impact_variant",
                when(col("is_pathogenic") & col("has_pharmgkb_annotation"),
                     True).otherwise(False))
    .withColumn("is_hepatic_drug_metabolism_variant",
                col("is_liver_expressed") & col("affects_drug_metabolism"))
    .withColumn("is_common_pharmacogene_variant",
                col("is_common_variant") & col("has_pharmgkb_annotation"))
    .withColumn("pharmacogene_annotation_score",
                when(col("has_pharmgkb_annotation"), 10).otherwise(0))
    .withColumn("functional_impact_score",
                when(col("affects_functional_domain"), 5).otherwise(0) +
                when(col("is_missense_variant"),       3).otherwise(0) +
                when(col("is_nonsense_variant"),       5).otherwise(0) +
                when(col("is_frameshift_variant"),     5).otherwise(0) +
                coalesce(col("conservation_level"), lit(0)) +
                coalesce(col("mutation_severity_score"), lit(0)))
    .withColumn("population_adjusted_score",
                when(col("is_common_pharmacogene_variant"), 10).otherwise(0) +
                when(col("is_rare_variant") & col("has_pharmgkb_annotation"), 7).otherwise(0))
    .withColumn("tissue_specific_response_score",
                when(col("is_hepatic_drug_metabolism_variant"), 10).otherwise(0) +
                when(col("is_liver_expressed") & col("has_pharmgkb_annotation"), 5).otherwise(0))
    .withColumn("drug_response_priority_score",
                col("pharmacogene_annotation_score") * 0.4 +
                col("functional_impact_score") * 0.2 +
                when(col("is_pathogenic"), 10).when(col("is_benign"), -5).otherwise(0) * 0.1 +
                col("population_adjusted_score") * 0.15 +
                col("tissue_specific_response_score") * 0.15)
    .withColumn("drug_response_priority",
                when(col("drug_response_priority_score") >= 20, lit("critical"))
                .when(col("drug_response_priority_score") >= 15, lit("high"))
                .when(col("drug_response_priority_score") >= 8,  lit("medium"))
                .otherwise(lit("low")))
    .withColumn("drug_response_category",
                when(col("is_hepatic_drug_metabolism_variant"), lit("hepatic_metabolism"))
                .when(col("affects_drug_metabolism"),  lit("metabolism"))
                .when(col("affects_drug_efficacy"),    lit("efficacy"))
                .when(col("is_potential_resistance_variant"), lit("resistance"))
                .when(col("has_pharmgkb_annotation"), lit("pharmacogene_variant"))
                .otherwise(lit("unknown")))
    .withColumn("clinical_actionability",
                when(col("is_pathogenic") & col("has_pharmgkb_annotation") & col("is_pharmacogene"),
                     lit("tier_1_actionable"))
                .when(col("has_pharmgkb_annotation") & col("is_pharmacogene"),
                     lit("tier_2_high_evidence"))
                .when(col("has_pharmgkb_annotation"),
                     lit("tier_3_pharmgkb_annotated"))
                .otherwise(lit("tier_4_research_only")))
    .withColumn("indication_specific_actionability",
                when(col("primary_indication_category") != "Other", lit(True)).otherwise(lit(False)))
)

print("Classification flags and scores complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10: Deduplicate by Variant ID
print("\nPASS 1 - STEP 10: DEDUPLICATE BY VARIANT_ID")
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
# MAGIC ### PASS 2 - TARGET ONLY

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variable
print("\nPASS 2 - DERIVING TARGET VARIABLE")
print("="*80)
print("Target: is_actionable_pharmacogene_variant")
print("Definition: has_pharmgkb_annotation AND (affects_functional_domain OR is_pathogenic OR cadd_phred >= 20)")

df_target = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("is_pathogenic").alias("_target_is_pathogenic"),
        col("affects_functional_domain").alias("_target_affects_domain"),
        col("cadd_phred").alias("_target_cadd_phred")
    )
    .join(
        df_pharmgkb_variants
        .select(col("variant_id").alias("_pharmgkb_vid"), lit(True).alias("_is_pharmgkb_annotated"))
        .dropDuplicates(["_pharmgkb_vid"]),
        col("variant_id") == col("_pharmgkb_vid"), "left"
    )
    .drop("_pharmgkb_vid")
    .fillna({"_is_pharmgkb_annotated": False})
    .withColumn("is_actionable_pharmacogene_variant",
                when(
                    col("_is_pharmgkb_annotated") &
                    (col("_target_affects_domain") |
                     col("_target_is_pathogenic") |
                     (coalesce(col("_target_cadd_phred"), lit(0.0)) >= 20)),
                    True
                ).otherwise(False))
    .select("variant_id", "is_actionable_pharmacogene_variant")
    .dropDuplicates(["variant_id"])
)

target_counts  = df_target.groupBy("is_actionable_pharmacogene_variant").count().collect()
total          = sum(r["count"] for r in target_counts)
positives      = [r["count"] for r in target_counts if r["is_actionable_pharmacogene_variant"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

for row in sorted(target_counts, key=lambda r: str(r["is_actionable_pharmacogene_variant"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_actionable_pharmacogene_variant']}: {row['count']:,} ({pct:.2f}%)")

if positive_count == 0:
    raise ValueError("Target has zero positives. Fix threshold.")
elif positive_pct < 1.0:
    raise ValueError(f"Target positive rate {positive_pct:.2f}% too low.")
elif positive_pct > 30.0:
    print(f"WARN: Positive rate {positive_pct:.2f}% above 30%.")
else:
    print(f"OK: Positive rate {positive_pct:.2f}%. Proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL JOIN - Features + Target

# COMMAND ----------

# DBTITLE 1,Final Join: Features + Target
print("\nFINAL JOIN - FEATURES AND TARGET")
print("="*80)

df_joined = (
    df_features
    .join(df_target, on="variant_id", how="left")
    .fillna({"is_actionable_pharmacogene_variant": False})
)

print(f"Joined rows:    {df_joined.count():,}")
print(f"Joined columns: {len(df_joined.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_joined.select(
    col("variant_pharmgkb_id"),
    coalesce(col("variant_name"), col("clinvar_variant_name")).alias("variant_name"),
    col("variant_id"),
    col("gene_symbol"),
    col("variant_location"),
    col("clinical_significance_simple"),
    col("is_pathogenic"),
    col("is_benign"),
    col("is_vus"),
    col("is_missense_variant"),
    col("is_frameshift_variant"),
    col("is_nonsense_variant"),
    col("is_splice_variant"),
    col("has_functional_domain"),
    col("affects_functional_domain"),
    col("phylop_score"),
    col("cadd_phred"),
    col("conservation_level"),
    col("pathogenicity_score"),
    col("mutation_severity_score"),
    col("has_pharmgkb_annotation"),
    col("has_high_conservation"),
    col("affects_drug_metabolism"),
    col("affects_drug_efficacy"),
    col("is_high_impact_variant"),
    col("is_hepatic_drug_metabolism_variant"),
    col("is_common_pharmacogene_variant"),
    col("is_potential_resistance_variant"),
    col("tissues_expressed_count"),
    col("max_expression_tpm"),
    col("is_liver_expressed"),
    col("expression_breadth"),
    col("allele_frequency"),
    col("is_common_variant"),
    col("is_rare_variant"),
    col("drug_response_frequency_context"),
    col("total_disease_count"),
    col("has_cancer_disease"),
    col("has_cardiovascular_disease"),
    col("has_neurological_disease"),
    col("primary_indication_category"),
    col("cancer_mutation_count"),
    col("is_cancer_gene"),
    col("is_pharmacogene"),
    col("druggability_score"),
    col("pharmacogene_category"),
    col("drug_metabolism_role"),
    col("pharmacogene_annotation_score"),
    col("functional_impact_score"),
    col("population_adjusted_score"),
    col("tissue_specific_response_score"),
    col("drug_response_priority_score"),
    col("drug_response_priority"),
    col("is_actionable_pharmacogene_variant"),
    col("drug_response_category"),
    col("clinical_actionability"),
    col("indication_specific_actionability")
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

# DBTITLE 1,Write gold.variant_drug_response_ml_features
print("\nWRITING gold.variant_drug_response_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_drug_response_ml_features")

print(f"Saved: {catalog_name}.gold.variant_drug_response_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.variant_drug_response_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

print(f"Rows:    {rows:,}")
print(f"Columns: {cols}")

df_check.groupBy("is_actionable_pharmacogene_variant").count().orderBy("is_actionable_pharmacogene_variant").show()
print("\nProcessing complete")
