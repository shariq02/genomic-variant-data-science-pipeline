# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - CANCER VARIANT ANALYSIS
# MAGIC ##### Module: Comprehensive Variant and Gene-Level Cancer Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 27, 2026  
# MAGIC **Updated:** March 14, 2026 (Gold V2 - Target Fix)
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 15: Cancer Molecular Classification
# MAGIC
# MAGIC **Creates:** gold.cancer_variant_ml_features
# MAGIC
# MAGIC **GOLD V2 TARGET FIX (March 14, 2026):**
# MAGIC Old target: gene_cancer_role (multiclass: oncogene/tumor_suppressor/cancer_associated/other)
# MAGIC Result: 97.95% oncogene, 1.13% cancer_associated, 0.87% TSG, 0.05% other (BROKEN)
# MAGIC
# MAGIC New target: is_driver_gene (binary: driver vs passenger)
# MAGIC Logic: (oncogene OR tumor_suppressor) AND high_mutation_burden
# MAGIC Expected: 20-30% positive rate
# MAGIC Rationale: Collapse multiclass to binary driver/passenger classification

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    when, lit, trim, upper, lower, coalesce, concat_ws, max as spark_max
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD CANCER VARIANT FEATURES (TWO-PASS)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_cancer          = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_genes           = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_variant_impact  = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_gtex            = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_gene_disease    = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_population      = spark.table(f"{catalog_name}.silver.population_frequencies")

print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Gene-disease:           {df_gene_disease.count():,}")
print(f"Protein domains:        {df_protein_domains.count():,}")
print(f"Population frequencies: {df_population.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PASS 1 - FEATURES ONLY
# MAGIC Target variable is NOT computed in this pass.

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 1: Variant-Level Cancer Features
print("\nPASS 1 - STEP 1: VARIANT-LEVEL CANCER FEATURES")
print("="*80)

df_variant_cancer = (
    df_cancer
    .withColumn("variant_key",
                concat_ws(":", col("chromosome"), col("position"),
                         col("reference_allele"), col("alternate_allele")))
    .groupBy("gene_symbol", "variant_key", "chromosome", "position",
             "reference_allele", "alternate_allele")
    .agg(
        count("tumor_sample").alias("sample_count"),
        spark_sum("mutation_count").alias("total_mutation_count"),
        countDistinct("variant_class").alias("variant_class_count"),
        countDistinct("variant_type").alias("variant_type_count"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_sample_count"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("truncating_sample_count"),
        spark_sum(when(col("is_silent"), 1).otherwise(0)).alias("silent_sample_count"),
        spark_sum(when(col("is_snv"), 1).otherwise(0)).alias("snv_sample_count"),
        spark_sum(when(col("is_indel"), 1).otherwise(0)).alias("indel_sample_count"),
        spark_sum(when(col("is_frequently_mutated"), 1).otherwise(0)).alias("frequent_mutation_samples")
    )
)

print(f"Unique cancer variants: {df_variant_cancer.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 2: Variant Classification Flags
print("\nPASS 1 - STEP 2: VARIANT CLASSIFICATION FLAGS")
print("="*80)

df_variant_classified = (
    df_variant_cancer
    .withColumn("is_recurrent_mutation",
                when(col("sample_count") >= 3, True).otherwise(False))

    .withColumn("is_hotspot_mutation",
                when(col("sample_count") >= 10, True).otherwise(False))

    .withColumn("is_high_impact_cancer_variant",
                when((col("truncating_sample_count") >= 1) &
                     (col("sample_count") >= 2), True).otherwise(False))

    .withColumn("mutation_frequency_category",
                when(col("sample_count") >= 10, lit("hotspot"))
                .when(col("sample_count") >= 3, lit("recurrent"))
                .when(col("sample_count") >= 2, lit("low_recurrent"))
                .otherwise(lit("singleton")))
)

print("Variant classification added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 3: Gene-Level Cancer Statistics
print("\nPASS 1 - STEP 3: GENE-LEVEL CANCER STATISTICS")
print("="*80)

df_gene_cancer = (
    df_cancer
    .groupBy("gene_symbol")
    .agg(
        count("tumor_sample").alias("total_samples_affected"),
        countDistinct("tumor_sample").alias("unique_samples_affected"),
        countDistinct(concat_ws(":", col("chromosome"), col("position"))).alias("unique_mutation_sites"),
        spark_sum("mutation_count").alias("total_mutations"),
        avg("mutation_count").alias("avg_mutations_per_sample"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_mutations"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("truncating_mutations"),
        spark_sum(when(col("is_silent"), 1).otherwise(0)).alias("silent_mutations"),
        spark_sum(when(col("is_frequently_mutated"), 1).otherwise(0)).alias("frequent_mutations")
    )
)

print(f"Genes with cancer mutations: {df_gene_cancer.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 4: Gene-Level Classification
print("\nPASS 1 - STEP 4: GENE-LEVEL CLASSIFICATION")
print("="*80)

df_gene_classified = (
    df_gene_cancer
    .withColumn("is_cancer_gene",
                when(col("unique_samples_affected") >= 5, True).otherwise(False))

    .withColumn("is_frequently_mutated_gene",
                when(col("unique_mutation_sites") >= 10, True).otherwise(False))

    .withColumn("is_tumor_suppressor_candidate",
                when((col("truncating_mutations") > col("missense_mutations")) &
                     (col("unique_samples_affected") >= 3), True).otherwise(False))

    .withColumn("is_oncogene_candidate",
                when((col("missense_mutations") > col("truncating_mutations")) &
                     (col("unique_samples_affected") >= 5), True).otherwise(False))

    # GOLD V2 TARGET (March 14, 2026):
    # Binary driver gene classification replaces multiclass gene_cancer_role
    # Driver = (oncogene OR tumor_suppressor) AND significant mutation burden
    .withColumn("is_driver_gene",
                when((col("is_oncogene_candidate") | col("is_tumor_suppressor_candidate")) &
                     (col("unique_samples_affected") >= 10), True)
                .otherwise(False))
    
    # Keep old multiclass for reference (will be excluded by cleanup notebook)
    .withColumn("gene_cancer_role",
                when((col("truncating_mutations") > col("missense_mutations")) &
                     (col("unique_samples_affected") >= 3), lit("tumor_suppressor"))
                .when((col("missense_mutations") > col("truncating_mutations")) &
                      (col("unique_samples_affected") >= 5), lit("oncogene"))
                .when(col("unique_samples_affected") >= 1, lit("cancer_associated"))
                .otherwise(lit("other")))
)

print("Gene classification added")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 5: Cancer Scores
print("\nPASS 1 - STEP 5: CANCER SCORES")
print("="*80)

df_gene_scored = (
    df_gene_classified
    .withColumn("cancer_mutation_burden_score",
                (col("unique_samples_affected") * 2) +
                (col("unique_mutation_sites") * 1))

    .withColumn("functional_impact_score",
                (col("truncating_mutations") * 3) +
                (col("missense_mutations") * 1) -
                (col("silent_mutations") * 0.5))

    .withColumn("cancer_priority_score",
                when(col("is_tumor_suppressor_candidate"), 10).otherwise(0) +
                when(col("is_oncogene_candidate"), 10).otherwise(0) +
                (col("unique_samples_affected") * 0.5))
)

print("Scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 6: Join Variant and Gene Features
print("\nPASS 1 - STEP 6: JOIN VARIANT AND GENE FEATURES")
print("="*80)

df_combined = (
    df_variant_classified
    .withColumn("variant_gene_symbol", upper(trim(col("gene_symbol"))))
    .drop("gene_symbol")
    .join(
        df_gene_scored.select(
            upper(trim(col("gene_symbol"))).alias("gene_symbol"),
            col("total_samples_affected").alias("gene_total_samples"),
            col("unique_mutation_sites").alias("gene_unique_sites"),
            col("is_cancer_gene"),
            col("is_tumor_suppressor_candidate"),
            col("is_oncogene_candidate"),
            col("gene_cancer_role"),
            col("cancer_mutation_burden_score"),
            col("cancer_priority_score")
        ),
        on=col("variant_gene_symbol") == col("gene_symbol"),
        how="left"
    )
    .withColumn("gene_symbol", col("variant_gene_symbol"))
    .drop("variant_gene_symbol")
)

print(f"Combined variant-gene features: {df_combined.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 7: Clinical Variant Impact
print("\nPASS 1 - STEP 7: CLINICAL VARIANT IMPACT")
print("="*80)

clinical_variant_impact = (
    df_variant_impact
    .select(
        concat_ws(":", col("chromosome"), col("position"),
                 col("reference_allele"), col("alternate_allele")).alias("variant_key"),
        col("is_pathogenic").alias("clinvar_is_pathogenic"),
        col("phylop_score").alias("conservation_score"),
        col("cadd_phred"),
        col("mutation_severity_score").alias("functional_impact_prediction"),
        when(col("is_pathogenic"), lit("pathogenic"))
        .when(col("is_benign"), lit("benign"))
        .otherwise(lit("uncertain")).alias("clinvar_pathogenicity")
    )
)

df_combined = (
    df_combined
    .join(clinical_variant_impact, "variant_key", "left")
    .fillna({
        "clinvar_is_pathogenic":        False,
        "conservation_score":           0.0,
        "cadd_phred":                   0.0,
        "functional_impact_prediction": 0,
        "clinvar_pathogenicity":        "uncertain"
    })
)

print("Clinical variant impact enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 8: Expression Context
print("\nPASS 1 - STEP 8: EXPRESSION CONTEXT")
print("="*80)

tissue_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy(col("gene_name"))
    .agg(
        countDistinct("tissue_type").alias("tissue_expression_in_tumors"),
        spark_max("max_tpm").alias("max_tumor_expression")
    )
    .withColumn("expression_change_relevance",
                when(col("tissue_expression_in_tumors") <= 5, lit("tissue_specific"))
                .when(col("tissue_expression_in_tumors") <= 20, lit("moderately_expressed"))
                .otherwise(lit("broadly_expressed")))
)

df_combined = (
    df_combined
    .join(tissue_expression, col("gene_symbol") == tissue_expression["gene_name"], "left")
    .drop(tissue_expression["gene_name"])
    .fillna({
        "tissue_expression_in_tumors": 0,
        "max_tumor_expression":        0.0,
        "expression_change_relevance": "broadly_expressed"
    })
)

print("Expression context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 9: Disease Context
print("\nPASS 1 - STEP 9: DISEASE CONTEXT")
print("="*80)

cancer_disease = (
    df_gene_disease
    .select(
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("has_cancer_disease").alias("cancer_disease_associations")
    )
)

df_combined = (
    df_combined
    .join(cancer_disease, "gene_symbol", "left")
    .fillna({"cancer_disease_associations": False})
    .withColumn("hereditary_cancer_syndrome",
                col("cancer_disease_associations") &
                (col("is_tumor_suppressor_candidate") | col("is_oncogene_candidate")))
)

print("Disease context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 10: Protein Context
print("\nPASS 1 - STEP 10: PROTEIN CONTEXT")
print("="*80)

oncogenic_domains = (
    df_protein_domains
    .groupBy(upper(trim(col("gene_symbol"))).alias("gene_symbol"))
    .agg(
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("has_kinase_domain_count")
    )
    .withColumn("affected_oncogenic_domains",
                col("has_kinase_domain_count") > 0)
    .withColumn("kinase_domain_mutations",
                col("has_kinase_domain_count") >= 1)
)

df_combined = (
    df_combined
    .join(oncogenic_domains, "gene_symbol", "left")
    .fillna({
        "has_kinase_domain_count":    0,
        "affected_oncogenic_domains": False,
        "kinase_domain_mutations":    False
    })
)

print("Protein context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 11: Population Context
print("\nPASS 1 - STEP 11: POPULATION CONTEXT")
print("="*80)

germline_frequency = (
    df_population
    .select(
        concat_ws(":", col("chromosome"), col("position"),
                 col("reference_allele"), col("alternate_allele")).alias("variant_key"),
        col("allele_frequency_global").alias("germline_variant_frequency"),
        col("is_rare")
    )
    .withColumn("somatic_vs_germline_classification",
                when(col("germline_variant_frequency") > 0.01, lit("likely_germline"))
                .when(col("is_rare"), lit("likely_somatic"))
                .otherwise(lit("unknown")))
)

df_combined = (
    df_combined
    .join(germline_frequency, "variant_key", "left")
    .fillna({
        "germline_variant_frequency":         0.0,
        "is_rare":                            False,
        "somatic_vs_germline_classification": "unknown"
    })
)

print("Population context enrichment complete")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 12: Enhanced Scores
print("\nPASS 1 - STEP 12: ENHANCED SCORES")
print("="*80)

df_features = (
    df_combined
    .withColumn("driver_likelihood_score",
                when(col("is_recurrent_mutation"), 10).otherwise(0) +
                when(col("clinvar_is_pathogenic"), 8).otherwise(0) +
                when(col("conservation_score") > 2.7, 5).otherwise(0) +
                when(col("affected_oncogenic_domains"), 7).otherwise(0))

    .withColumn("therapeutic_target_score",
                when(col("is_hotspot_mutation") & col("kinase_domain_mutations"), 15).otherwise(0) +
                when(col("is_oncogene_candidate"), 10).otherwise(0) +
                when(col("affected_oncogenic_domains"), 8).otherwise(0))

    .withColumn("prognostic_value_score",
                when(col("is_tumor_suppressor_candidate") & col("clinvar_is_pathogenic"), 12).otherwise(0) +
                when(col("hereditary_cancer_syndrome"), 10).otherwise(0) +
                (col("sample_count") * 0.5))
)

print("Enhanced scores calculated")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 13: Join with Gene Master Data
print("\nPASS 1 - STEP 13: JOIN WITH GENE MASTER DATA")
print("="*80)

df_features = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_pharmacogene")
    )
    .join(df_features.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
          on="gene_symbol", how="right")
)

print(f"Final features with gene data: {df_features.count():,}")

# COMMAND ----------

# DBTITLE 1,PASS 1 - Step 14: Deduplicate by Variant Key
print("\nPASS 1 - STEP 14: DEDUPLICATE BY VARIANT_KEY")
print("="*80)

before_count = df_features.count()
df_features  = df_features.dropDuplicates(["variant_key"])
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
print("Target: is_driver_candidate")
print("Definition: sample_count >= 3 OR (truncating_sample_count >= 1 AND sample_count >= 2)")
print()

df_target = (
    df_features
    .select("variant_key", "sample_count", "truncating_sample_count")
    .withColumn("is_driver_candidate",
                when(
                    (col("sample_count") >= 3) |
                    ((col("truncating_sample_count") >= 1) & (col("sample_count") >= 2)),
                    True
                ).otherwise(False))
    .select("variant_key", "is_driver_candidate")
)

target_counts  = df_target.groupBy("is_driver_candidate").count().collect()
total          = sum(r["count"] for r in target_counts)
positives      = [r["count"] for r in target_counts if r["is_driver_candidate"] == True]
positive_count = positives[0] if positives else 0
positive_pct   = positive_count / total * 100 if total > 0 else 0

for row in sorted(target_counts, key=lambda r: str(r["is_driver_candidate"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_driver_candidate']}: {row['count']:,} ({pct:.2f}%)")

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
    .join(df_target, on="variant_key", how="left")
    .fillna({"is_driver_candidate": False})
    .dropDuplicates(["variant_key"])
)

print(f"Final rows:    {df_final.count():,}")
print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_final.select(
    col("gene_symbol"),
    col("gene_name"),
    col("variant_key"),
    col("chromosome"),
    col("position"),
    col("reference_allele"),
    col("alternate_allele"),
    col("sample_count"),
    col("total_mutation_count"),
    col("missense_sample_count"),
    col("truncating_sample_count"),
    col("silent_sample_count"),
    col("snv_sample_count"),
    col("indel_sample_count"),
    col("is_recurrent_mutation"),
    col("is_hotspot_mutation"),
    col("is_high_impact_cancer_variant"),
    col("is_driver_candidate"),
    col("mutation_frequency_category"),
    col("gene_total_samples"),
    col("gene_unique_sites"),
    col("is_cancer_gene"),
    col("is_tumor_suppressor_candidate"),
    col("is_oncogene_candidate"),
    col("is_driver_gene"),  # GOLD V2: new binary target
    col("gene_cancer_role"),  # Keep for reference (cleanup will exclude)
    col("cancer_mutation_burden_score"),
    col("cancer_priority_score"),
    col("clinvar_pathogenicity"),
    col("clinvar_is_pathogenic"),
    col("conservation_score"),
    col("cadd_phred"),
    col("functional_impact_prediction"),
    col("tissue_expression_in_tumors"),
    col("max_tumor_expression"),
    col("expression_change_relevance"),
    col("cancer_disease_associations"),
    col("hereditary_cancer_syndrome"),
    col("has_kinase_domain_count"),
    col("affected_oncogenic_domains"),
    col("kinase_domain_mutations"),
    col("germline_variant_frequency"),
    col("is_rare"),
    col("somatic_vs_germline_classification"),
    col("is_kinase"),
    col("is_receptor"),
    col("is_enzyme"),
    col("is_pharmacogene"),
    col("driver_likelihood_score"),
    col("therapeutic_target_score"),
    col("prognostic_value_score")
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gold.variant_cancer_ml_features
print("\nWRITING gold.variant_cancer_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.variant_cancer_ml_features")

print(f"Saved: {catalog_name}.gold.variant_cancer_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.variant_cancer_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

# GOLD V2 TARGET VALIDATION
print("\n" + "="*80)
print("GOLD V2 TARGET VALIDATION - is_driver_gene")
print("="*80)

target_dist = df_check.groupBy("is_driver_gene").count().collect()
total = sum(r["count"] for r in target_dist)
for row in sorted(target_dist, key=lambda r: str(r["is_driver_gene"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_driver_gene']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_dist if r["is_driver_gene"] == True]
pos_count = positives[0] if positives else 0
pos_pct = pos_count / total * 100 if total > 0 else 0

print()
if pos_pct < 15.0:
    print(f"WARNING: Positive rate {pos_pct:.2f}% below expected range (20-30%). Review threshold.")
elif pos_pct > 40.0:
    print(f"WARNING: Positive rate {pos_pct:.2f}% above expected range (20-30%). Review threshold.")
elif 20.0 <= pos_pct <= 30.0:
    print(f"SUCCESS: Positive rate {pos_pct:.2f}% within target range (20-30%)!")
else:
    print(f"OK: Positive rate {pos_pct:.2f}% acceptable (15-40% range).")

print("="*80)

print(f"\nRows:      {rows:,}")
print(f"Columns:   {cols}")

# Show old multiclass distribution for comparison
print("\nOld multiclass distribution (gene_cancer_role):")
df_check.groupBy("gene_cancer_role").count().orderBy("count", ascending=False).show()

print("\nProcessing complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD V2 VALIDATION SQL
# MAGIC Run these queries to verify binary target quality

# COMMAND ----------

# DBTITLE 1,Binary Target Distribution
# MAGIC %sql
# MAGIC -- UC15: Binary Driver Gene Target
# MAGIC -- Expected: 20-30% positive
# MAGIC SELECT 
# MAGIC   is_driver_gene,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
# MAGIC FROM workspace.gold.cancer_variant_ml_features
# MAGIC GROUP BY is_driver_gene;

# COMMAND ----------

# DBTITLE 1,Multiclass vs Binary Comparison
# MAGIC %sql
# MAGIC -- Compare old multiclass to new binary
# MAGIC SELECT 
# MAGIC   gene_cancer_role,
# MAGIC   is_driver_gene,
# MAGIC   COUNT(*) as count
# MAGIC FROM workspace.gold.cancer_variant_ml_features
# MAGIC GROUP BY gene_cancer_role, is_driver_gene
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# DBTITLE 1,Sample Driver Genes
# MAGIC %sql
# MAGIC -- Sample driver genes to verify logic
# MAGIC SELECT 
# MAGIC   gene_name,
# MAGIC   is_oncogene_candidate,
# MAGIC   is_tumor_suppressor_candidate,
# MAGIC   unique_samples_affected,
# MAGIC   gene_cancer_role,
# MAGIC   is_driver_gene
# MAGIC FROM workspace.gold.cancer_variant_ml_features
# MAGIC WHERE is_driver_gene = TRUE
# MAGIC LIMIT 20;
