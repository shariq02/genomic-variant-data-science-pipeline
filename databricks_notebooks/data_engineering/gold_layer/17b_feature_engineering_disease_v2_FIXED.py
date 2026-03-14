# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - DISEASE USE CASES
# MAGIC ##### Disease Association, Polygenic Risk, Gene Prioritization
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026  
# MAGIC **Updated:** March 14, 2026 (Gold V2 - Target Fix)
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Disease Association Discovery (Gene-Disease links)
# MAGIC - Use Case 5: Multi-Gene Disease Risk (Polygenic scores)
# MAGIC - Use Case 6: Gene Prioritization (Clinical utility ranking)
# MAGIC
# MAGIC **Creates:** gold.disease_ml_features
# MAGIC
# MAGIC **GOLD V2 TARGET FIX (March 14, 2026):**
# MAGIC Old target: is_disease_associated = (gene_disease_count >= 1) → 100% positive
# MAGIC New target: is_high_confidence_disease_gene
# MAGIC Logic: (omim_disease_count >= 2) OR (has_clinvar_pathogenic_disease_variant)
# MAGIC Expected: 15-25% positive rate
# MAGIC Rationale: Binary threshold too permissive, requires high-confidence evidence

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct,
    concat_ws, length, regexp_replace
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("DISEASE FEATURE ENGINEERING ")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_variants          = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_genes             = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease_comp = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_omim_lookup       = spark.table(f"{catalog_name}.reference.omim_disease_lookup")
df_mondo_lookup      = spark.table(f"{catalog_name}.reference.mondo_disease_lookup")
df_orphanet_lookup   = spark.table(f"{catalog_name}.reference.orphanet_disease_lookup")
df_gtex              = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer            = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_conservation      = spark.table(f"{catalog_name}.silver.conservation_with_phylop")
df_protein_domains   = spark.table(f"{catalog_name}.silver.protein_domains")

print(f"Variants:               {df_variants.count():,}")
print(f"Genes (enriched):       {df_genes.count():,}")
print(f"Gene-disease:           {df_gene_disease_comp.count():,}")
print(f"OMIM lookup:            {df_omim_lookup.count():,}")
print(f"MONDO lookup:           {df_mondo_lookup.count():,}")
print(f"Orphanet lookup:        {df_orphanet_lookup.count():,}")
print(f"GTEx expression:        {df_gtex.count():,}")
print(f"Cancer mutations:       {df_cancer.count():,}")
print(f"Conservation:           {df_conservation.count():,}")
print(f"Protein domains:        {df_protein_domains.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 1: Enrich Disease Information from Reference Tables
print("\nSTEP 1: ENRICHING DISEASE INFORMATION")
print("="*80)

df_variants_enriched = (
    df_variants

    .join(
        df_omim_lookup.select(
            col("omim_id").alias("omim_id_lookup"),
            col("disease_name").alias("omim_disease_name")
        ),
        df_variants.omim_id == col("omim_id_lookup"),
        "left"
    )
    .drop("omim_id_lookup")

    .join(
        df_mondo_lookup.select(
            col("mondo_id").alias("mondo_id_lookup"),
            col("disease_name").alias("mondo_disease_name")
        ),
        df_variants.mondo_id == col("mondo_id_lookup"),
        "left"
    )
    .drop("mondo_id_lookup")

    .join(
        df_orphanet_lookup.select(
            col("orphanet_id").alias("orphanet_id_lookup"),
            col("disease_name").alias("orphanet_disease_name")
        ),
        df_variants.orphanet_id == col("orphanet_id_lookup"),
        "left"
    )
    .drop("orphanet_id_lookup")

    .withColumn("disease_name_enriched",
                coalesce(
                    col("disease_enriched"),
                    col("primary_disease"),
                    col("omim_disease_name"),
                    col("mondo_disease_name"),
                    col("orphanet_disease_name"),
                    lit("Unknown_Disease")
                ))
    .withColumn("disease_name_enriched",
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(col("disease_name_enriched"), '"', ''),
                            '\n', ' '
                        ),
                        '\r', ' '
                    ),
                    ',', ';'
                ))

    .withColumn("has_omim_disease",
                col("omim_id").isNotNull() & col("omim_disease_name").isNotNull())

    .withColumn("has_mondo_disease",
                col("mondo_id").isNotNull() & col("mondo_disease_name").isNotNull())

    .withColumn("has_orphanet_disease",
                col("orphanet_id").isNotNull() & col("orphanet_disease_name").isNotNull())

    .withColumn("disease_db_coverage",
                when(col("has_omim_disease"), 1).otherwise(0) +
                when(col("has_mondo_disease"), 1).otherwise(0) +
                when(col("has_orphanet_disease"), 1).otherwise(0))

    .withColumn("disease_is_well_annotated",
                col("disease_db_coverage") >= 2)

    .withColumn("disease_name_is_generic",
                col("disease_name_enriched").rlike("(?i)disease|disorder|syndrome") &
                (length(col("disease_name_enriched")) < 30))
)

print("Disease enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 2: Disease Association Features (UC4)
print("\nSTEP 2: DISEASE ASSOCIATION FEATURES (UC4)")
print("="*80)

df_genes_with_disease = (
    df_gene_disease_comp
    .select(
        "gene_name",
        col("total_disease_count").alias("gene_disease_count"),
        col("omim_disease_count")
    )
    .withColumn("disease_count_category",
                when(col("gene_disease_count") >= 10, lit("Highly_Associated"))
                .when(col("gene_disease_count") >= 5, lit("Moderately_Associated"))
                .when(col("gene_disease_count") >= 2, lit("Associated"))
                .when(col("gene_disease_count") == 1, lit("Single_Disease"))
                .otherwise(lit("Not_Associated")))
    
    # GOLD V2 TARGET (March 14, 2026):
    # New high-confidence target replaces old is_disease_associated
    # Requires multiple OMIM diseases OR ClinVar pathogenic variant
    .withColumn("is_high_confidence_disease_gene",
                when(col("omim_disease_count") >= 3, True)
                .otherwise(False))  # Will add ClinVar check after join
    
    .withColumn("is_multi_disease_gene",  col("gene_disease_count") >= 3)
    .withColumn("disease_association_strength",
                when(col("gene_disease_count") >= 10, 5)
                .when(col("gene_disease_count") >= 5, 4)
                .when(col("gene_disease_count") >= 2, 3)
                .when(col("gene_disease_count") == 1, 2)
                .otherwise(1))
    .withColumn("is_omim_gene", col("omim_disease_count") >= 1)
)

df_disease = (
    df_variants_enriched
    .join(df_genes_with_disease, "gene_name", "left")
    .fillna({
        "gene_disease_count":               0,
        "omim_disease_count":               0,
        "disease_association_strength":     1,
        "is_high_confidence_disease_gene":  False,  # GOLD V2: new target
        "is_multi_disease_gene":            False,
        "is_omim_gene":                     False
    })
    .fillna("Not_Associated", ["disease_count_category"])
    
    # GOLD V2: Add ClinVar pathogenic check to target (OR logic)
    .withColumn("is_high_confidence_disease_gene",
                when(col("is_high_confidence_disease_gene") == True, True)  # Already True from OMIM
                .when(col("is_pathogenic") == True, True)  # OR ClinVar pathogenic
                .otherwise(False))

    .withColumn("variant_disease_link_quality",
                when(col("has_omim_disease") & col("is_omim_gene"), lit("High_Quality"))
                .when(col("disease_db_coverage") >= 2, lit("Medium_Quality"))
                .when(col("disease_db_coverage") >= 1, lit("Low_Quality"))
                .otherwise(lit("No_Link")))
)

print("Disease association features created")

# COMMAND ----------

# DBTITLE 1,Step 3: Polygenic Risk Features (UC5)
print("\nSTEP 3: POLYGENIC RISK FEATURES (UC5)")
print("="*80)

disease_stats = (
    df_disease
    .filter(col("disease_name_enriched") != "Unknown_Disease")
    .groupBy("disease_name_enriched")
    .agg(
        count("*").alias("disease_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("disease_pathogenic_variants"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("disease_benign_variants"),
        spark_sum(when(col("is_vus"), 1).otherwise(0)).alias("disease_vus_variants"),
        countDistinct("gene_name").alias("disease_gene_count")
    )
    .withColumn("disease_pathogenic_ratio",
                col("disease_pathogenic_variants") / col("disease_total_variants"))
    .withColumn("is_polygenic_disease",
                col("disease_gene_count") >= 3)
    .withColumn("disease_complexity",
                when(col("disease_gene_count") >= 10, lit("Highly_Complex"))
                .when(col("disease_gene_count") >= 5, lit("Complex"))
                .when(col("disease_gene_count") >= 3, lit("Moderately_Complex"))
                .when(col("disease_gene_count") >= 2, lit("Oligogenic"))
                .otherwise(lit("Monogenic")))
    .withColumn("disease_complexity_score",
                when(col("disease_gene_count") >= 10, 5)
                .when(col("disease_gene_count") >= 5, 4)
                .when(col("disease_gene_count") >= 3, 3)
                .when(col("disease_gene_count") >= 2, 2)
                .otherwise(1))
    .withColumn("disease_has_high_pathogenic_burden",
                coalesce(col("disease_pathogenic_ratio"), lit(0.0)) > 0.2)
)

df_disease = (
    df_disease
    .join(disease_stats, "disease_name_enriched", "left")
    .fillna({
        "disease_total_variants":             0,
        "disease_pathogenic_variants":         0,
        "disease_benign_variants":             0,
        "disease_vus_variants":                0,
        "disease_gene_count":                  0,
        "disease_pathogenic_ratio":            0.0,
        "is_polygenic_disease":                False,
        "disease_complexity_score":            1,
        "disease_has_high_pathogenic_burden":  False
    })
    .fillna("Unknown", ["disease_complexity"])

    .withColumn("polygenic_risk_contribution",
                when(col("is_pathogenic") & col("is_polygenic_disease"), lit("High_Risk_Contributor"))
                .when(col("is_vus") & col("is_polygenic_disease"), lit("Moderate_Risk_Contributor"))
                .when(col("is_polygenic_disease"), lit("Low_Risk_Contributor"))
                .otherwise(lit("Not_Applicable")))
)

print("Polygenic risk features created")

# COMMAND ----------

# DBTITLE 1,Step 4: Gene Prioritization Features (UC6)
print("\nSTEP 4: GENE PRIORITIZATION FEATURES (UC6)")
print("="*80)

gene_stats = (
    df_disease
    .groupBy("gene_name")
    .agg(
        count("*").alias("gene_total_variants"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("gene_pathogenic_count"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("gene_benign_count"),
        spark_sum(when(col("review_quality_score") >= 2, 1).otherwise(0)).alias("gene_high_quality_count"),
        countDistinct("disease_name_enriched").alias("gene_disease_diversity"),
        spark_sum(when(col("has_omim_disease"), 1).otherwise(0)).alias("gene_omim_variants"),
        spark_sum(when(col("has_mondo_disease"), 1).otherwise(0)).alias("gene_mondo_variants"),
        spark_sum(when(col("disease_is_well_annotated"), 1).otherwise(0)).alias("gene_well_annotated_variants")
    )
    .withColumn("gene_clinical_utility_score",
                (col("gene_pathogenic_count") * 3) +
                (col("gene_high_quality_count") * 2) +
                (col("gene_disease_diversity") * 2))
    .withColumn("gene_priority_tier",
                when(col("gene_clinical_utility_score") >= 20, lit("Tier_1_Critical"))
                .when(col("gene_clinical_utility_score") >= 10, lit("Tier_2_High"))
                .when(col("gene_clinical_utility_score") >= 5, lit("Tier_3_Medium"))
                .when(col("gene_clinical_utility_score") >= 1, lit("Tier_4_Low"))
                .otherwise(lit("Tier_5_Minimal")))
    .withColumn("is_clinically_actionable",
                (col("gene_pathogenic_count") >= 5) & (col("gene_high_quality_count") >= 3))
    .withColumn("is_research_candidate",
                (col("gene_total_variants") >= 10) & (col("gene_disease_diversity") >= 2))
    .withColumn("gene_annotation_score",
                col("gene_omim_variants") +
                col("gene_mondo_variants") +
                col("gene_well_annotated_variants"))
    .withColumn("has_excellent_annotation",
                col("gene_annotation_score") >= 10)
    .withColumn("annotation_priority_level",
                when(col("gene_annotation_score") >= 20, lit("Excellent"))
                .when(col("gene_annotation_score") >= 10, lit("Good"))
                .when(col("gene_annotation_score") >= 5, lit("Moderate"))
                .otherwise(lit("Poor")))
)

df_disease = (
    df_disease
    .join(gene_stats, "gene_name", "left")
    .fillna({
        "gene_total_variants":          0,
        "gene_pathogenic_count":        0,
        "gene_benign_count":            0,
        "gene_high_quality_count":      0,
        "gene_disease_diversity":       0,
        "gene_clinical_utility_score":  0,
        "is_clinically_actionable":     False,
        "is_research_candidate":        False,
        "gene_annotation_score":        0,
        "has_excellent_annotation":     False,
        "gene_omim_variants":           0,
        "gene_mondo_variants":          0,
        "gene_well_annotated_variants": 0
    })
    .fillna("Tier_5_Minimal", ["gene_priority_tier"])
    .fillna("Poor",           ["annotation_priority_level"])
)

print("Gene prioritization features created")

# COMMAND ----------

# DBTITLE 1,Step 5: Expression Context
print("\nSTEP 5: EXPRESSION CONTEXT")
print("="*80)

gene_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy("gene_name")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("max_tpm").alias("max_expression_tpm")
    )
    .withColumn("is_broadly_expressed",
                col("tissues_expressed_count") >= 10)
)

df_disease = (
    df_disease
    .join(
        gene_expression.select("gene_name", "tissues_expressed_count", "is_broadly_expressed"),
        "gene_name", "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "is_broadly_expressed":    False
    })
)

print("Expression enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 6: Cancer Context
print("\nSTEP 6: CANCER CONTEXT")
print("="*80)

cancer_genes = (
    df_cancer
    .groupBy(col("gene_symbol").alias("gene_name"))
    .agg(
        count("*").alias("cancer_mutation_count")
    )
    .withColumn("is_cancer_hotspot_gene",
                col("cancer_mutation_count") >= 100)
)

df_disease = (
    df_disease
    .join(cancer_genes, "gene_name", "left")
    .fillna({
        "cancer_mutation_count":  0,
        "is_cancer_hotspot_gene": False
    })
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 7: Conservation Scores
print("\nSTEP 7: CONSERVATION SCORES")
print("="*80)

df_disease = (
    df_disease
    .join(
        df_conservation.select(
            "variant_id",
            "phylop_score",
            "cadd_phred",
            "is_highly_conserved"
        ),
        "variant_id", "left"
    )
    .fillna({
        "phylop_score":       0.0,
        "cadd_phred":         0.0,
        "is_highly_conserved": False
    })

    .withColumn("has_high_conservation",
                (col("phylop_score") > 2.7) | (col("cadd_phred") > 20))
)

print("Conservation enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 8: Protein Domain Context
print("\nSTEP 8: PROTEIN DOMAIN CONTEXT")
print("="*80)

gene_domains = (
    df_protein_domains
    .groupBy(col("protein_name").alias("gene_name"))
    .agg(
        spark_max("domain_count").alias("gene_domain_count"),
        spark_max("has_kinase_domain").cast("int").alias("has_kinase_domain_int")
    )
    .withColumn("is_complex_protein",
                col("gene_domain_count") >= 5)
)

df_disease = (
    df_disease
    .join(gene_domains, "gene_name", "left")
    .fillna({
        "gene_domain_count": 0,
        "is_complex_protein": False
    })
)

print("Protein domain enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 9: Deduplicate by Variant ID
print("\nSTEP 9: DEDUPLICATE BY VARIANT_ID")
print("="*80)

before_count = df_disease.count()
df_disease   = df_disease.dropDuplicates(["variant_id"])
after_count  = df_disease.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_disease.select(
    col("variant_id"),
    col("gene_name"),
    col("chromosome"),
    col("position"),
    col("is_pathogenic"),
    col("is_benign"),
    col("is_vus"),
    col("clinical_significance_simple"),
    col("disease_enriched"),
    col("primary_disease"),
    col("disease_name_enriched"),
    col("omim_id"),
    col("mondo_id"),
    col("orphanet_id"),
    col("has_omim_disease"),
    col("has_mondo_disease"),
    col("has_orphanet_disease"),
    col("disease_db_coverage"),
    col("disease_is_well_annotated"),
    col("disease_name_is_generic"),
    col("disease_count"),
    col("omim_disease_count"),
    col("disease_count_category"),
    col("is_high_confidence_disease_gene"),  # GOLD V2: new target
    col("is_multi_disease_gene"),
    col("disease_association_strength"),
    col("is_omim_gene"),
    col("variant_disease_link_quality"),
    col("disease_total_variants"),
    col("disease_pathogenic_variants"),
    col("disease_benign_variants"),
    col("disease_vus_variants"),
    col("disease_pathogenic_ratio"),
    col("disease_gene_count"),
    col("is_polygenic_disease"),
    col("disease_complexity"),
    col("disease_complexity_score"),
    col("polygenic_risk_contribution"),
    col("disease_has_high_pathogenic_burden"),
    col("gene_total_variants"),
    col("gene_pathogenic_count"),
    col("gene_benign_count"),
    col("gene_high_quality_count"),
    col("gene_disease_diversity"),
    col("gene_clinical_utility_score"),
    col("gene_priority_tier"),
    col("is_clinically_actionable"),
    col("is_research_candidate"),
    col("gene_annotation_score"),
    col("has_excellent_annotation"),
    col("annotation_priority_level"),
    col("gene_omim_variants"),
    col("gene_mondo_variants"),
    col("gene_well_annotated_variants"),
    col("tissues_expressed_count"),
    col("is_broadly_expressed"),
    col("cancer_mutation_count"),
    col("is_cancer_hotspot_gene"),
    col("phylop_score"),
    col("cadd_phred"),
    col("is_highly_conserved"),
    col("has_high_conservation"),
    col("gene_domain_count"),
    col("is_complex_protein")
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gold.disease_ml_features
print("\nWRITING gold.disease_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.disease_ml_features")

print(f"Saved: {catalog_name}.gold.disease_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.disease_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

print(f"Rows:    {rows:,}")
print(f"Columns: {cols}")

# GOLD V2 TARGET VALIDATION
print("\n" + "="*80)
print("GOLD V2 TARGET VALIDATION - is_high_confidence_disease_gene")
print("="*80)
target_dist = df_check.groupBy("is_high_confidence_disease_gene").count().collect()
total = sum(r["count"] for r in target_dist)
for row in sorted(target_dist, key=lambda r: str(r["is_high_confidence_disease_gene"])):
    pct = row["count"] / total * 100
    print(f"  {row['is_high_confidence_disease_gene']}: {row['count']:,} ({pct:.2f}%)")

positives = [r["count"] for r in target_dist if r["is_high_confidence_disease_gene"] == True]
pos_count = positives[0] if positives else 0
pos_pct = pos_count / total * 100 if total > 0 else 0

print()
if pos_pct < 10.0:
    print(f"WARNING: Positive rate {pos_pct:.2f}% below expected range (15-25%). Review threshold.")
elif pos_pct > 30.0:
    print(f"WARNING: Positive rate {pos_pct:.2f}% above expected range (15-25%). Review threshold.")
elif 15.0 <= pos_pct <= 25.0:
    print(f"SUCCESS: Positive rate {pos_pct:.2f}% within target range (15-25%)!")
else:
    print(f"OK: Positive rate {pos_pct:.2f}% acceptable (10-30% range).")

print("="*80)

print("\nDisease count category breakdown:")
df_check.groupBy("disease_count_category").count().orderBy("count", ascending=False).show()

print("\nGene priority tier breakdown:")
df_check.groupBy("gene_priority_tier").count().orderBy("count", ascending=False).show()

print("\nProcessing complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD V2 VALIDATION SQL
# MAGIC Run these queries to verify target quality

# COMMAND ----------

# DBTITLE 1,Target Distribution Check
# MAGIC %sql
# MAGIC -- UC02: Target Distribution Validation
# MAGIC -- Expected: 15-25% positive
# MAGIC SELECT 
# MAGIC   is_high_confidence_disease_gene,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
# MAGIC FROM workspace.gold.disease_ml_features
# MAGIC GROUP BY is_high_confidence_disease_gene;

# COMMAND ----------

# DBTITLE 1,Sample Positive Rows
# MAGIC %sql
# MAGIC -- Sample high-confidence disease genes
# MAGIC SELECT 
# MAGIC   gene_name,
# MAGIC   omim_disease_count,
# MAGIC   is_pathogenic,
# MAGIC  -- gene_disease_count,
# MAGIC   is_high_confidence_disease_gene
# MAGIC FROM workspace.gold.disease_ml_features
# MAGIC WHERE is_high_confidence_disease_gene = TRUE
# MAGIC LIMIT 20;
