# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### FEATURE ENGINEERING - DISEASE USE CASES (UPDATED)
# MAGIC ##### Module 2: Disease Association, Polygenic Risk, Gene Prioritization
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **UPDATED:** Uses genes_with_pharmgkb (final enriched gene table)
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Disease Association Discovery (Gene-Disease links)
# MAGIC - Use Case 5: Multi-Gene Disease Risk (Polygenic scores)
# MAGIC - Use Case 6: Gene Prioritization (Clinical utility ranking)
# MAGIC
# MAGIC **Creates:** gold.disease_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, countDistinct, collect_list,
    concat_ws, size, array_distinct, length
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("DISEASE FEATURE ENGINEERING - MODULE 2 (UPDATED)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Required Tables
print("\nLOADING TABLES")
print("="*80)

df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")
df_genes = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease_comp = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")

# Load reference tables for disease enrichment
df_omim_lookup = spark.table(f"{catalog_name}.reference.omim_disease_lookup")
df_mondo_lookup = spark.table(f"{catalog_name}.reference.mondo_disease_lookup")
df_orphanet_lookup = spark.table(f"{catalog_name}.reference.orphanet_disease_lookup")

# Additional enrichment tables
df_gtex = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_conservation = spark.table(f"{catalog_name}.silver.conservation_with_phylop")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")

print(f"Variants: {df_variants.count():,}")
print(f"Genes (enriched): {df_genes.count():,}")
print(f"Gene-disease comprehensive: {df_gene_disease_comp.count():,}")
print(f"OMIM lookup: {df_omim_lookup.count():,}")
print(f"MONDO lookup: {df_mondo_lookup.count():,}")
print(f"Orphanet lookup: {df_orphanet_lookup.count():,}")
print(f"GTEx expression: {df_gtex.count():,}")
print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Conservation: {df_conservation.count():,}")
print(f"Protein domains: {df_protein_domains.count():,}")

# COMMAND ----------

# DBTITLE 1,Enrich Disease Information from Reference Tables
print("\nENRICHING DISEASE INFORMATION")
print("="*80)

df_variants_enriched = (
    df_variants
    
    # Add OMIM disease names
    .join(
        df_omim_lookup.select(
            col("omim_id").alias("omim_id_lookup"),
            col("disease_name").alias("omim_disease_name")
        ),
        df_variants.omim_id == col("omim_id_lookup"),
        "left"
    )
    .drop("omim_id_lookup")
    
    # Add MONDO disease names
    .join(
        df_mondo_lookup.select(
            col("mondo_id").alias("mondo_id_lookup"),
            col("disease_name").alias("mondo_disease_name")
        ),
        df_variants.mondo_id == col("mondo_id_lookup"),
        "left"
    )
    .drop("mondo_id_lookup")
    
    # Add Orphanet disease names
    .join(
        df_orphanet_lookup.select(
            col("orphanet_id").alias("orphanet_id_lookup"),
            col("disease_name").alias("orphanet_disease_name")
        ),
        df_variants.orphanet_id == col("orphanet_id_lookup"),
        "left"
    )
    .drop("orphanet_id_lookup")
    
    # Create enriched disease name
    .withColumn("disease_name_enriched",
                coalesce(
                    col("disease_enriched"),
                    col("primary_disease"),
                    col("omim_disease_name"),
                    col("mondo_disease_name"),
                    col("orphanet_disease_name"),
                    lit("Unknown_Disease")
                ))
    
    # Disease database coverage flags
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

# DBTITLE 1,USE CASE 4 - Disease Association Discovery
print("\nUSE CASE 4: DISEASE ASSOCIATION DISCOVERY")
print("="*80)

# Use gene_disease_comprehensive for disease counts
df_genes_with_disease = (
    df_gene_disease_comp
    .select(
        "gene_name",
        col("total_disease_count").alias("disease_count"),
        "omim_disease_count"
    )
    .withColumn("disease_count_category",
                when(col("disease_count") >= 10, lit("Highly_Associated"))
                .when(col("disease_count") >= 5, lit("Moderately_Associated"))
                .when(col("disease_count") >= 2, lit("Associated"))
                .when(col("disease_count") == 1, lit("Single_Disease"))
                .otherwise(lit("Not_Associated")))
    .withColumn("is_disease_associated", col("disease_count") >= 1)
    .withColumn("is_multi_disease_gene", col("disease_count") >= 3)
    .withColumn("disease_association_strength",
                when(col("disease_count") >= 10, 5)
                .when(col("disease_count") >= 5, 4)
                .when(col("disease_count") >= 2, 3)
                .when(col("disease_count") == 1, 2)
                .otherwise(1))
    .withColumn("is_omim_gene", col("omim_disease_count") >= 1)
)

print("Disease association features created")

# COMMAND ----------

# DBTITLE 1,Join Variants with Disease Features
print("\nJOINING VARIANTS WITH DISEASE FEATURES")
print("="*80)

df_disease = (
    df_variants_enriched
    .join(df_genes_with_disease, "gene_name", "left")
    .fillna({
        "disease_count": 0,
        "omim_disease_count": 0,
        "disease_association_strength": 1,
        "is_disease_associated": False,
        "is_multi_disease_gene": False,
        "is_omim_gene": False
    })
    .fillna("Not_Associated", ["disease_count_category"])
    
    # Variant-disease link quality
    .withColumn("variant_disease_link_quality",
                when(col("has_omim_disease") & col("is_omim_gene"), lit("High_Quality"))
                .when(col("disease_db_coverage") >= 2, lit("Medium_Quality"))
                .when(col("disease_db_coverage") >= 1, lit("Low_Quality"))
                .otherwise(lit("No_Link")))
)

print("Variant-disease join complete")

# COMMAND ----------

# DBTITLE 1,USE CASE 5 - Polygenic Risk Features
print("\nUSE CASE 5: POLYGENIC RISK SCORES")
print("="*80)

# Calculate disease-level statistics
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

# Join with main dataframe
df_disease = (
    df_disease
    .join(disease_stats, "disease_name_enriched", "left")
    .fillna({
        "disease_total_variants": 0,
        "disease_pathogenic_variants": 0,
        "disease_benign_variants": 0,
        "disease_vus_variants": 0,
        "disease_gene_count": 0,
        "disease_pathogenic_ratio": 0.0,
        "is_polygenic_disease": False,
        "disease_complexity_score": 1,
        "disease_has_high_pathogenic_burden": False
    })
    .fillna("Unknown", ["disease_complexity"])
    
    # Polygenic risk contribution
    .withColumn("polygenic_risk_contribution",
                when(col("is_pathogenic") & col("is_polygenic_disease"), lit("High_Risk_Contributor"))
                .when(col("is_vus") & col("is_polygenic_disease"), lit("Moderate_Risk_Contributor"))
                .when(col("is_polygenic_disease"), lit("Low_Risk_Contributor"))
                .otherwise(lit("Not_Applicable")))
)

print("Polygenic risk features created")

# COMMAND ----------

# DBTITLE 1,USE CASE 6 - Gene Prioritization Features
print("\nUSE CASE 6: GENE PRIORITIZATION")
print("="*80)

# Calculate gene-level clinical utility statistics
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
                col("gene_omim_variants") + col("gene_mondo_variants") + col("gene_well_annotated_variants"))
    .withColumn("has_excellent_annotation",
                col("gene_annotation_score") >= 10)
    .withColumn("annotation_priority_level",
                when(col("gene_annotation_score") >= 20, lit("Excellent"))
                .when(col("gene_annotation_score") >= 10, lit("Good"))
                .when(col("gene_annotation_score") >= 5, lit("Moderate"))
                .otherwise(lit("Poor")))
)

# Join with main dataframe
df_disease = (
    df_disease
    .join(gene_stats, "gene_name", "left")
    .fillna({
        "gene_total_variants": 0,
        "gene_pathogenic_count": 0,
        "gene_benign_count": 0,
        "gene_high_quality_count": 0,
        "gene_disease_diversity": 0,
        "gene_clinical_utility_score": 0,
        "is_clinically_actionable": False,
        "is_research_candidate": False,
        "has_drug_development_potential": False,
        "gene_annotation_score": 0,
        "has_excellent_annotation": False,
        "gene_omim_variants": 0,
        "gene_mondo_variants": 0,
        "gene_well_annotated_variants": 0
    })
    .fillna("Tier_5_Minimal", ["gene_priority_tier"])
    .fillna("Poor", ["annotation_priority_level"])
)

print("Gene prioritization features created")

# COMMAND ----------

# DBTITLE 1,Enrich with Expression Data
print("\nENRICHING WITH EXPRESSION DATA")
print("="*80)

# Gene expression breadth
gene_expression = (
    df_gtex
    .filter(col("median_tpm") > 1.0)
    .groupBy("gene_symbol")
    .agg(
        countDistinct("tissue_type").alias("tissues_expressed_count"),
        spark_max("median_tpm").alias("max_expression_tpm")
    )
    .withColumn("is_broadly_expressed",
                col("tissues_expressed_count") >= 10)
)

df_disease = (
    df_disease
    .join(
        gene_expression.select(
            col("gene_symbol").alias("gene_name"),
            "tissues_expressed_count",
            "is_broadly_expressed"
        ),
        "gene_name",
        "left"
    )
    .fillna({
        "tissues_expressed_count": 0,
        "is_broadly_expressed": False
    })
)

print("Expression data enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Cancer Context
print("\nENRICHING WITH CANCER CONTEXT")
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
        "cancer_mutation_count": 0,
        "is_cancer_hotspot_gene": False
    })
)

print("Cancer context enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Conservation Scores
print("\nENRICHING WITH CONSERVATION SCORES")
print("="*80)

# Variant-level conservation
df_disease = (
    df_disease
    .join(
        df_conservation.select(
            "variant_id",
            "phylop_score",
            "cadd_phred",
            "is_highly_conserved"
        ),
        "variant_id",
        "left"
    )
    .fillna({
        "phylop_score": 0.0,
        "cadd_phred": 0.0,
        "is_highly_conserved": False
    })
    
    .withColumn("has_high_conservation",
                (col("phylop_score") > 2.7) | (col("cadd_phred") > 20))
)

print("Conservation enrichment complete")

# COMMAND ----------

# DBTITLE 1,Enrich with Protein Domain Data
print("\nENRICHING WITH PROTEIN DOMAIN DATA")
print("="*80)

# Gene-level domain complexity
gene_domains = (
    df_protein_domains
    .groupBy(col("protein_id").alias("gene_name"))
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

# DBTITLE 1,Create Final Disease Features Table
print("\nCREATING DISEASE ML FEATURES")
print("="*80)

disease_features = df_disease.select(
    # IDs
    "variant_id", "gene_name", "chromosome", "position",
    
    # Clinical significance
    "is_pathogenic", "is_benign", "is_vus",
    "clinical_significance_simple",
    
    # Use Case 4: Disease Association Features
    "disease_enriched",
    "primary_disease",
    "disease_name_enriched",
    "omim_id",
    "mondo_id",
    "orphanet_id",
    "has_omim_disease",
    "has_mondo_disease",
    "has_orphanet_disease",
    "disease_db_coverage",
    "disease_is_well_annotated",
    "disease_name_is_generic",
    "disease_count",
    "omim_disease_count",
    "disease_count_category",
    "is_disease_associated",
    "is_multi_disease_gene",
    "disease_association_strength",
    "is_omim_gene",
    "variant_disease_link_quality",
    
    # Use Case 5: Polygenic Risk Features
    "disease_total_variants",
    "disease_pathogenic_variants",
    "disease_benign_variants",
    "disease_vus_variants",
    "disease_pathogenic_ratio",
    "disease_gene_count",
    "is_polygenic_disease",
    "disease_complexity",
    "disease_complexity_score",
    "polygenic_risk_contribution",
    "disease_has_high_pathogenic_burden",
    
    # Use Case 6: Gene Prioritization Features
    "gene_total_variants",
    "gene_pathogenic_count",
    "gene_benign_count",
    "gene_high_quality_count",
    "gene_disease_diversity",
    "gene_clinical_utility_score",
    "gene_priority_tier",
    "is_clinically_actionable",
    "is_research_candidate",
    "gene_annotation_score",
    "has_excellent_annotation",
    "annotation_priority_level",
    "gene_omim_variants",
    "gene_mondo_variants",
    "gene_well_annotated_variants",
    
    # Expression context
    "tissues_expressed_count",
    "is_broadly_expressed",
    
    # Cancer context
    "cancer_mutation_count",
    "is_cancer_hotspot_gene",
    
    # Conservation scores
    "phylop_score",
    "cadd_phred",
    "is_highly_conserved",
    "has_high_conservation",
    
    # Protein domain data
    "gene_domain_count",
    "is_complex_protein"
)

feature_count = disease_features.count()
print(f"Disease ML features: {feature_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Deduplicate by variant_id
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = disease_features.count()
disease_features = disease_features.dropDuplicates(["variant_id"])
after_count = disease_features.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
disease_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.disease_ml_features")

print(f"Saved: {catalog_name}.gold.disease_ml_features")

# COMMAND ----------

# DBTITLE 1,Feature Statistics
print("\nFEATURE STATISTICS")
print("="*80)

print("\nDisease Association (Use Case 4):")
disease_features.groupBy("disease_count_category").count().orderBy("count", ascending=False).show()

print("\nPolygenic Risk (Use Case 5):")
disease_features.groupBy("disease_complexity").count().orderBy("count", ascending=False).show()

print("\nGene Prioritization (Use Case 6):")
disease_features.groupBy("gene_priority_tier").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Summary
print("DISEASE FEATURE ENGINEERING COMPLETE")
print("="*80)

print(f"\nTotal features created: {after_count:,}")
print(f"Total columns: {len(disease_features.columns)}")

print("\nUse Cases Covered:")
print("  - Disease Association (Use Case 4): 20 features")
print("  - Polygenic Risk (Use Case 5): 11 features")
print("  - Gene Prioritization (Use Case 6): 15 features")

print("\nTable created:")
print(f"  {catalog_name}.gold.disease_ml_features")
