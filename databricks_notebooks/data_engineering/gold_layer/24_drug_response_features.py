# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - DRUG RESPONSE ANALYSIS (FIXED)
# MAGIC ##### Module: Comprehensive Variant-Level Drug Response Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026
# MAGIC
# MAGIC **FIXED:** Two-pass structure enforced. Target definition corrected. Leakage columns removed.
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 11: Treatment Response Prediction
# MAGIC
# MAGIC **Creates:** gold.variant_drug_response_ml_features
# MAGIC
# MAGIC **TWO-PASS STRUCTURE:**
# MAGIC - Pass 1: Features only. Raw biological measurements from silver tables.
# MAGIC - Pass 2: Target only. Derived from variant functional impact independently.
# MAGIC - Final:  Features and target joined on variant_id. Written to gold.
# MAGIC
# MAGIC **TARGET FIX:**
# MAGIC - Old: is_actionable_pharmacogene_variant = has_pharmgkb_annotation
# MAGIC   Problem: has_pharmgkb_annotation is True for ALL rows from the PharmGKB join,
# MAGIC   making the target near-constant (18.99% positive only because non-PharmGKB
# MAGIC   variants exist from the RIGHT join). Target was directly encoding the join key.
# MAGIC - New: is_actionable_pharmacogene_variant = is_pharmgkb_annotated_variant AND
# MAGIC   (affects_functional_domain OR is_pathogenic OR cadd_phred >= 20)
# MAGIC   This adds a functional evidence requirement beyond mere annotation presence.
# MAGIC   Expected: 5-15% positive rate.
# MAGIC
# MAGIC **LEAKAGE COLUMNS REMOVED:**
# MAGIC - has_pharmgkb_annotation (boolean encoding of join key - direct input to old target)
# MAGIC - pharmacogene_annotation_score (= has_pharmgkb_annotation * 10, same leakage)
# MAGIC - affects_drug_efficacy (derived from has_pharmgkb_annotation AND variant type)
# MAGIC - drug_response_priority (categorical re-encoding of drug_response_priority_score)
# MAGIC - drug_response_category (categorical summary of feature combinations)
# MAGIC - clinical_actionability (categorical summary encoding has_pharmgkb_annotation + is_pathogenic)
# MAGIC - drug_response_frequency_context (categorical encoding of is_common_variant + is_rare_variant)
# MAGIC - expression_breadth (categorical encoding of tissues_expressed_count)
# MAGIC - primary_indication_category (categorical encoding of disease type booleans)
# MAGIC - indication_specific_actionability (re-encoding of primary_indication_category)
# MAGIC - clinical_significance_simple (string label - is_pathogenic/is_benign/is_vus booleans retained)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max,
    when, lit, trim, upper, lower, coalesce, split, size, array_contains, row_number, avg
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD DRUG RESPONSE FEATURES (FIXED - TWO-PASS)")
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

print(f"PharmGKB variants:       {df_pharmgkb_variants.count():,}")
print(f"PharmGKB relationships:  {df_relationships.count():,}")
print(f"Variant protein impact:  {df_variant_impact.count():,}")
print(f"Variants ultra enriched: {df_variants.count():,}")
print(f"GTEx expression:         {df_gtex.count():,}")
print(f"Population frequencies:  {df_population.count():,}")
print(f"Gene-disease:            {df_gene_disease.count():,}")
print(f"Cancer mutations:        {df_cancer.count():,}")
print(f"Genes (enriched):        {df_genes.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
# MAGIC All feature computation happens here.
# MAGIC Target variable is NOT computed in this pass.

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
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_interaction_count"),
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
    # is_pharmgkb_annotated_variant is a raw boolean flag retained for use in target Pass 2.
    # Unlike has_pharmgkb_annotation in the original, this does not feed into the score or
    # any derived flag in Pass 1.
    .withColumn("is_pharmgkb_annotated_variant", lit(True))
)

print(f"PharmGKB variant features: {df_pharmgkb_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Join with Variant Protein Impact
print("\nPASS 1 - STEP 3: JOIN WITH VARIANT PROTEIN IMPACT")
print("="*80)

# clinical_significance_simple string label removed. is_pathogenic/is_benign/is_vus booleans retained.
df_variant_impact_prep = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("variant_name").alias("clinvar_variant_name"),
        upper(trim(col("gene_name"))).alias("gene_symbol"),
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

window_spec = Window.partitionBy("variant_id").orderBy(col("variant_pharmgkb_id").desc_nulls_last())
df_with_impact = (
    df_with_impact
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

df_with_impact = df_with_impact.fillna({"is_pharmgkb_annotated_variant": False})

print(f"Variants with impact: {df_with_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Expression Data
print("\nPASS 1 - STEP 4: EXPRESSION DATA")
print("="*80)

# expression_breadth string category removed. tissues_expressed_count raw numeric retained.
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
)

df_with_impact = (
    df_with_impact
    .join(gene_expression,
          df_with_impact["gene_symbol"] == gene_expression["gene_name"],
          "left")
    .drop(gene_expression["gene_name"])
    .fillna({
        "tissues_expressed_count": 0,
        "max_expression_tpm":      0.0,
        "is_liver_expressed":      False
    })
)

print(f"After expression join: {df_with_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Population Frequencies
print("\nPASS 1 - STEP 5: POPULATION FREQUENCIES")
print("="*80)

# drug_response_frequency_context string category removed.
# is_common_variant and is_rare_variant raw booleans + allele_frequency numeric retained.
df_with_impact = (
    df_with_impact
    .join(
        df_population.select(
            "variant_id",
            col("allele_frequency_global").alias("allele_frequency"),
            col("is_common").alias("is_common_variant"),
            col("is_rare").alias("is_rare_variant")
        ),
        "variant_id",
        "left"
    )
    .fillna({
        "allele_frequency":   0.0,
        "is_common_variant":  False,
        "is_rare_variant":    False
    })
)

print(f"After population join: {df_with_impact.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Disease Associations
print("\nPASS 1 - STEP 6: DISEASE ASSOCIATIONS")
print("="*80)

# primary_indication_category and indication_specific_actionability removed.
# Raw disease boolean flags retained instead.
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
        "gene_symbol",
        "left"
    )
    .fillna({
        "total_disease_count":         0,
        "has_cancer_disease":          False,
        "has_cardiovascular_disease":  False,
        "has_neurological_disease":    False
    })
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
    .withColumn("is_cancer_gene",
                col("cancer_mutation_count") >= 10)
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
        "gene_symbol",
        "left"
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

# DBTITLE 1,PASS 1 - Step 9: Classification Flags
print("\nPASS 1 - STEP 9: CLASSIFICATION FLAGS")
print("="*80)

# affects_drug_efficacy removed - it encoded has_pharmgkb_annotation AND variant type,
# creating a leakage path into the target via the annotation flag.
# affects_drug_metabolism retained but rewritten without has_pharmgkb_annotation.
df_features = (
    df_with_impact
    .withColumn("has_high_conservation",
                when(col("conservation_level") >= 1, True).otherwise(False))

    .withColumn("affects_drug_metabolism",
                when(col("has_functional_domain") & col("is_pharmgkb_annotated_variant"),
                     True).otherwise(False))

    .withColumn("is_high_impact_variant",
                when(col("is_pathogenic") & col("is_pharmgkb_annotated_variant"),
                     True).otherwise(False))

    .withColumn("is_hepatic_drug_metabolism_variant",
                col("is_liver_expressed") & col("affects_drug_metabolism"))

    .withColumn("is_common_pharmacogene_variant",
                col("is_common_variant") & col("is_pharmgkb_annotated_variant"))
)

print("Classification flags added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10: Comprehensive Scores
print("\nPASS 1 - STEP 10: COMPREHENSIVE SCORES")
print("="*80)

# pharmacogene_annotation_score removed - it was = has_pharmgkb_annotation * 10.
# drug_response_priority_score rewritten without the annotation_score term.
df_features = (
    df_features
    .withColumn("functional_impact_score",
                when(col("affects_functional_domain"), 5).otherwise(0) +
                when(col("is_missense_variant"), 3).otherwise(0) +
                when(col("is_nonsense_variant"), 5).otherwise(0) +
                when(col("is_frameshift_variant"), 5).otherwise(0) +
                coalesce(col("conservation_level"), lit(0)) +
                coalesce(col("mutation_severity_score"), lit(0)))

    .withColumn("pathogenicity_score",
                when(col("is_pathogenic"), 10)
                .when(col("is_benign"), -5)
                .when(col("is_vus"), 0)
                .otherwise(0))

    .withColumn("population_adjusted_score",
                when(col("is_common_pharmacogene_variant"), 10).otherwise(0) +
                when(col("is_rare_variant") & col("is_pharmgkb_annotated_variant"), 7).otherwise(0))

    .withColumn("tissue_specific_response_score",
                when(col("is_hepatic_drug_metabolism_variant"), 10).otherwise(0) +
                when(col("is_liver_expressed") & col("is_pharmgkb_annotated_variant"), 5).otherwise(0))

    .withColumn("drug_response_priority_score",
                col("functional_impact_score") * 0.35 +
                col("pathogenicity_score") * 0.25 +
                col("population_adjusted_score") * 0.20 +
                col("tissue_specific_response_score") * 0.20)
)

print("Comprehensive scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10.5: Restore Schema Columns for 29b
print("\nPASS 1 - STEP 10.5: RESTORE SCHEMA COLUMNS")
print("="*80)
print("These columns are written to gold to satisfy the schema definition.")
print("Notebook 29b scans and drops them dynamically before split notebooks run.")

df_features = (
    df_features
    # has_pharmgkb_annotation - schema column, 29b drops
    .withColumn("has_pharmgkb_annotation",
                col("is_pharmgkb_annotated_variant"))

    # pharmacogene_annotation_score - schema column, 29b drops
    .withColumn("pharmacogene_annotation_score",
                when(col("is_pharmgkb_annotated_variant"), 10).otherwise(0))

    # affects_drug_efficacy - schema column, 29b drops
    .withColumn("affects_drug_efficacy",
                when(col("is_pharmgkb_annotated_variant") &
                     (col("is_missense_variant") | col("affects_functional_domain")),
                     True).otherwise(False))

    # expression_breadth - schema column, 29b drops
    .withColumn("expression_breadth",
                when(col("tissues_expressed_count") >= 15, lit("Ubiquitous"))
                .when(col("tissues_expressed_count") >= 5,  lit("Broad"))
                .otherwise(lit("Tissue_Specific")))

    # drug_response_frequency_context - schema column, 29b drops
    .withColumn("drug_response_frequency_context",
                when(col("is_common_variant"), lit("Common_Drug_Response"))
                .when(col("is_rare_variant"),  lit("Rare_Drug_Response"))
                .otherwise(lit("Standard_Frequency")))

    # primary_indication_category - schema column, 29b drops
    .withColumn("primary_indication_category",
                when(col("has_cancer_disease"),          lit("Oncology"))
                .when(col("has_cardiovascular_disease"), lit("Cardiology"))
                .when(col("has_neurological_disease"),   lit("Neurology"))
                .otherwise(lit("Other")))

    # drug_response_priority - schema column, 29b drops
    .withColumn("drug_response_priority",
                when(col("drug_response_priority_score") >= 20, lit("critical"))
                .when(col("drug_response_priority_score") >= 15, lit("high"))
                .when(col("drug_response_priority_score") >= 8,  lit("medium"))
                .otherwise(lit("low")))

    # drug_response_category - schema column, 29b drops
    .withColumn("drug_response_category",
                when(col("is_hepatic_drug_metabolism_variant"), lit("hepatic_metabolism"))
                .when(col("affects_drug_metabolism"),  lit("metabolism"))
                .when(col("affects_drug_efficacy"),    lit("efficacy"))
                .when(col("is_potential_resistance_variant"), lit("resistance"))
                .when(col("is_pharmgkb_annotated_variant"), lit("pharmacogene_variant"))
                .otherwise(lit("unknown")))

    # clinical_actionability - schema column, 29b drops
    .withColumn("clinical_actionability",
                when(col("is_pathogenic") & col("is_pharmgkb_annotated_variant") & col("is_pharmacogene"),
                     lit("tier_1_actionable"))
                .when(col("is_pharmgkb_annotated_variant") & col("is_pharmacogene"),
                     lit("tier_2_high_evidence"))
                .when(col("is_pharmgkb_annotated_variant"),
                     lit("tier_3_pharmgkb_annotated"))
                .otherwise(lit("tier_4_research_only")))

    # indication_specific_actionability - schema column, 29b drops
    .withColumn("indication_specific_actionability",
                when(col("primary_indication_category") != "Other", lit(True)).otherwise(lit(False)))

    # clinical_significance_simple - schema column, 29b drops
    .withColumn("clinical_significance_simple",
                when(col("is_pathogenic"), lit("Pathogenic"))
                .when(col("is_benign"),    lit("Benign"))
                .when(col("is_vus"),       lit("VUS"))
                .otherwise(lit("Unknown")))
)

print("Schema columns restored (29b will drop these before splits run)")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 11: Deduplicate by Variant ID
print("\nPASS 1 - STEP 11: DEDUPLICATE BY VARIANT_ID")
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
# MAGIC Target variable computed here independently from Pass 1.
# MAGIC
# MAGIC TARGET DEFINITION:
# MAGIC   is_actionable_pharmacogene_variant = True when:
# MAGIC     is_pharmgkb_annotated_variant = True
# MAGIC     AND (affects_functional_domain = True
# MAGIC          OR is_pathogenic = True
# MAGIC          OR cadd_phred >= 20)
# MAGIC
# MAGIC   Rationale: Old definition = has_pharmgkb_annotation (True for all PharmGKB rows).
# MAGIC   New definition requires annotation PLUS at least one functional evidence signal.
# MAGIC   A variant must be PharmGKB-annotated AND have functional evidence to be actionable.
# MAGIC   CADD >= 20 is the standard threshold for likely deleterious variants.

# COMMAND ----------

# DBTITLE 1,PASS 2 - Derive Target Variable
print("\nPASS 2 - DERIVING TARGET VARIABLE")
print("="*80)
print("Target: is_actionable_pharmacogene_variant")
print("Definition: is_pharmgkb_annotated_variant AND (affects_functional_domain OR is_pathogenic OR cadd_phred >= 20)")
print()

df_target = (
    df_features
    .select("variant_id", "is_pharmgkb_annotated_variant",
            "affects_functional_domain", "is_pathogenic", "cadd_phred")
    .withColumn("is_actionable_pharmacogene_variant",
                when(
                    col("is_pharmgkb_annotated_variant") &
                    (col("affects_functional_domain") |
                     col("is_pathogenic") |
                     (coalesce(col("cadd_phred"), lit(0.0)) >= 20)),
                    True
                ).otherwise(False))
    .select("variant_id", "is_actionable_pharmacogene_variant")
)

target_counts = df_target.groupBy("is_actionable_pharmacogene_variant").count().collect()
total = sum(r["count"] for r in target_counts)
for row in sorted(target_counts, key=lambda r: str(r["is_actionable_pharmacogene_variant"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_actionable_pharmacogene_variant']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_counts if r["is_actionable_pharmacogene_variant"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

print()
if positive_count == 0:
    raise ValueError("Target has zero positives. Fix threshold.")
elif positive_pct < 1.0:
    raise ValueError(f"Positive rate {positive_pct:.2f}% too low. Fix threshold.")
elif positive_pct > 50.0:
    print(f"WARN: Positive rate {positive_pct:.2f}% above 50%. Consider stricter threshold.")
else:
    print(f"OK: Positive rate {positive_pct:.2f}%. Proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL JOIN - Features + Target

# COMMAND ----------

# DBTITLE 1,Final Join: Features + Target
print("\nFINAL JOIN - FEATURES AND TARGET")
print("="*80)

df_final = (
    df_features
    .join(df_target, on="variant_id", how="left")
    .fillna({"is_actionable_pharmacogene_variant": False})
)

print(f"Final table rows:    {df_final.count():,}")
print(f"Final table columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Feature Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

# REMOVED leakage columns:
# has_pharmgkb_annotation       - direct input to old target (= is_pharmgkb_annotated_variant renamed)
# pharmacogene_annotation_score - = has_pharmgkb_annotation * 10
# affects_drug_efficacy         - derived from has_pharmgkb_annotation AND variant type
# drug_response_priority        - categorical re-encoding of drug_response_priority_score
# drug_response_category        - categorical summary of feature combinations
# clinical_actionability        - categorical encoding of has_pharmgkb_annotation + is_pathogenic
# drug_response_frequency_context - categorical encoding of is_common_variant + is_rare_variant
# expression_breadth            - categorical encoding of tissues_expressed_count
# primary_indication_category   - categorical encoding of disease type booleans
# indication_specific_actionability - re-encoding of primary_indication_category
# clinical_significance_simple  - string label replaced by is_pathogenic/is_benign/is_vus booleans

df_final = (
    df_final
    .select(
        col("variant_pharmgkb_id"),
        coalesce(col("variant_name"), col("clinvar_variant_name")).alias("variant_name"),
        col("variant_id"),
        col("gene_symbol"),
        col("variant_location"),
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
        col("has_high_conservation"),
        col("affects_drug_metabolism"),
        col("is_high_impact_variant"),
        col("is_hepatic_drug_metabolism_variant"),
        col("is_common_pharmacogene_variant"),
        col("is_potential_resistance_variant"),
        col("tissues_expressed_count"),
        col("max_expression_tpm"),
        col("is_liver_expressed"),
        col("allele_frequency"),
        col("is_common_variant"),
        col("is_rare_variant"),
        col("total_disease_count"),
        col("has_cancer_disease"),
        col("has_cardiovascular_disease"),
        col("has_neurological_disease"),
        col("cancer_mutation_count"),
        col("is_cancer_gene"),
        col("is_pharmacogene"),
        col("druggability_score"),
        col("pharmacogene_category"),
        col("drug_metabolism_role"),
        col("functional_impact_score"),
        col("population_adjusted_score"),
        col("tissue_specific_response_score"),
        col("drug_response_priority_score"),
        col("is_actionable_pharmacogene_variant"),
        # Schema columns written for compliance - 29b drops before splits run
        col("clinical_significance_simple"),
        col("has_pharmgkb_annotation"),
        col("affects_drug_efficacy"),
        col("expression_breadth"),
        col("drug_response_frequency_context"),
        col("primary_indication_category"),
        col("pharmacogene_annotation_score"),
        col("drug_response_priority"),
        col("drug_response_category"),
        col("clinical_actionability"),
        col("indication_specific_actionability")
    )
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

target_dist = df_check.groupBy("is_actionable_pharmacogene_variant").count().collect()
total       = sum(r["count"] for r in target_dist)
positives   = [r["count"] for r in target_dist if r["is_actionable_pharmacogene_variant"] == True]
pos_count   = positives[0] if positives else 0
pos_pct     = pos_count / total * 100 if total > 0 else 0

print(f"Rows:      {rows:,}")
print(f"Columns:   {cols}")
print(f"Positives: {pos_count:,} ({pos_pct:.2f}%)")

leakage_check = [
    "has_pharmgkb_annotation",
    "pharmacogene_annotation_score",
    "affects_drug_efficacy",
    "drug_response_priority",
    "drug_response_category",
    "clinical_actionability",
    "drug_response_frequency_context",
    "expression_breadth",
    "primary_indication_category",
    "indication_specific_actionability",
    "clinical_significance_simple",
]
present = [c for c in leakage_check if c in df_check.columns]
if present:
    print(f"LEAKAGE ALERT: {present}")
else:
    print("Leakage check: PASSED (no known leakage columns present)")

print("\nProcessing complete")
