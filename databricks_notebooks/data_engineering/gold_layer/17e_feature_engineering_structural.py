# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - STRUCTURAL VARIANT USE CASE
# MAGIC ##### Module 5: Comprehensive Structural Variant Impact Analysis with Gene Mapping
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 22, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Structural Variant Impact (CNVs, SVs, Gene Disruption)
# MAGIC
# MAGIC **Creates:** gold.structural_variant_ml_features
# MAGIC
# MAGIC **NOTE:** Features-only gold table. No ML target column.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, avg,
    max as spark_max, countDistinct, abs as spark_abs,
    collect_list, concat_ws
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("STRUCTURAL VARIANT FEATURE ENGINEERING - MODULE 5")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Check Table Availability
print("\nCHECKING TABLE AVAILABILITY")
print("="*80)

try:
    df_structural     = spark.table(f"{catalog_name}.silver.structural_variants")
    structural_count  = df_structural.count()
    print(f"Structural variants: {structural_count:,}")
except Exception as e:
    print("Structural variants: Not available")
    print(f"Error: {str(e)}")
    dbutils.notebook.exit("SKIPPED: structural_variants table not found")

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_genes          = spark.table(f"{catalog_name}.silver.genes_with_pharmgkb")
df_gene_disease   = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
df_gtex           = spark.table(f"{catalog_name}.silver.gtex_tissue_expression")
df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")

print(f"Structural variants: {structural_count:,}")
print(f"Genes (enriched):    {df_genes.count():,}")
print(f"Gene-disease:        {df_gene_disease.count():,}")
print(f"GTEx expression:     {df_gtex.count():,}")
print(f"Protein domains:     {df_protein_domains.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 1: Basic SV Features
print("\nSTEP 1: BASIC SV FEATURES")
print("="*80)

df_sv = (
    df_structural
    .select(
        col("variant_id").alias("sv_id"),
        "study_id",
        "variant_name",
        "variant_type",
        "chromosome",
        col("start_position").alias("start_pos"),
        col("end_position").alias("end_pos"),
        "assembly"
    )

    .withColumn("sv_size",
                spark_abs(col("end_pos") - col("start_pos")))

    .withColumn("sv_type_class",
                when(col("variant_type").rlike("(?i)deletion|loss|del"), lit("Deletion"))
                .when(col("variant_type").rlike("(?i)duplication|gain|dup"), lit("Duplication"))
                .when(col("variant_type").rlike("(?i)inversion|inv"), lit("Inversion"))
                .when(col("variant_type").rlike("(?i)translocation|trans"), lit("Translocation"))
                .when(col("variant_type").rlike("(?i)insertion|ins"), lit("Insertion"))
                .when(col("variant_type").rlike("(?i)copy|cnv"), lit("Copy_Number_Variant"))
                .otherwise(lit("Other_SV")))

    .withColumn("sv_size_category",
                when(col("sv_size") < 1000, lit("Small"))
                .when(col("sv_size") < 10000, lit("Medium"))
                .when(col("sv_size") < 100000, lit("Large"))
                .when(col("sv_size") < 1000000, lit("Very_Large"))
                .otherwise(lit("Mega")))

    .withColumn("sv_pathogenicity_risk",
                when((col("sv_type_class") == "Deletion") & (col("sv_size") > 100000),
                     lit("High_Risk"))
                .when(col("sv_type_class").isin("Deletion", "Duplication") &
                      (col("sv_size") > 10000),
                     lit("Moderate_Risk"))
                .when(col("sv_size") > 1000,
                     lit("Low_Risk"))
                .otherwise(lit("Minimal_Risk")))
)

print("Basic SV features created")

# COMMAND ----------

# DBTITLE 1,Step 2: Map SVs to Genes via Coordinate Overlap
print("\nSTEP 2: MAP SVs TO GENES VIA COORDINATE OVERLAP")
print("="*80)

df_genes_coord = (
    df_genes
    .select(
        "gene_name",
        "official_symbol",
        "chromosome",
        col("start_position").alias("gene_start"),
        col("end_position").alias("gene_end"),
        "gene_length",
        "gene_type",
        "is_transporter",
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "is_gpcr",
        "is_pharmacogene",
        "mim_id",
        "druggability_score"
    )
    .filter(col("start_position").isNotNull() & col("end_position").isNotNull())
    .filter(
        ~(col("gene_name").startswith("LOC") & (col("gene_type") == "biological-region"))
    )
    .withColumn("is_omim_gene", col("mim_id").isNotNull())
)

print(f"Genes with coordinates: {df_genes_coord.count():,}")

df_sv_gene_overlap = (
    df_sv
    .join(
        df_genes_coord,
        (df_sv.chromosome == df_genes_coord.chromosome) &
        (df_sv.start_pos < df_genes_coord.gene_end) &
        (df_sv.end_pos > df_genes_coord.gene_start),
        "left"
    )
    .withColumn("overlap_start",
                when(col("start_pos") > col("gene_start"), col("start_pos"))
                .otherwise(col("gene_start")))
    .withColumn("overlap_end",
                when(col("end_pos") < col("gene_end"), col("end_pos"))
                .otherwise(col("gene_end")))
    .withColumn("overlap_length",
                when(col("gene_name").isNotNull(),
                     col("overlap_end") - col("overlap_start"))
                .otherwise(lit(0)))
    .withColumn("gene_coverage_fraction",
                when(col("gene_length").isNotNull() & (col("gene_length") > 0),
                     col("overlap_length") / col("gene_length"))
                .otherwise(lit(0.0)))
    .withColumn("sv_coverage_fraction",
                when(col("sv_size") > 0,
                     col("overlap_length") / col("sv_size"))
                .otherwise(lit(0.0)))
    .withColumn("gene_disruption_level",
                when(col("gene_coverage_fraction") >= 0.9, lit("Complete_Disruption"))
                .when(col("gene_coverage_fraction") >= 0.5, lit("Major_Disruption"))
                .when(col("gene_coverage_fraction") >= 0.25, lit("Moderate_Disruption"))
                .when(col("gene_coverage_fraction") > 0, lit("Minor_Disruption"))
                .otherwise(lit("No_Disruption")))
)

print("SV-gene overlap calculated")

# COMMAND ----------

# DBTITLE 1,Step 3: Aggregate Gene-Level SV Features
print("\nSTEP 3: AGGREGATE GENE-LEVEL SV FEATURES")
print("="*80)

sv_gene_agg = (
    df_sv_gene_overlap
    .filter(col("gene_name").isNotNull())
    .groupBy("sv_id")
    .agg(
        countDistinct("gene_name").alias("genes_overlapped"),
        concat_ws(",", collect_list("gene_name")).alias("gene_list"),
        spark_sum(when(col("is_pharmacogene"), 1).otherwise(0)).alias("pharmacogenes_affected"),
        spark_sum(when(col("is_omim_gene"), 1).otherwise(0)).alias("omim_genes_affected"),
        spark_sum(when(col("is_kinase"), 1).otherwise(0)).alias("kinase_genes_affected"),
        spark_sum(when(col("is_receptor"), 1).otherwise(0)).alias("receptor_genes_affected"),
        spark_max("gene_coverage_fraction").alias("max_gene_disruption_fraction"),
        avg("druggability_score").alias("avg_druggability_affected_genes")
    )
    .withColumn("gene_count_category",
                when(col("genes_overlapped") == 0, lit("No_Genes"))
                .when(col("genes_overlapped") == 1, lit("Single_Gene"))
                .when(col("genes_overlapped") <= 5, lit("Few_Genes"))
                .when(col("genes_overlapped") <= 20, lit("Multiple_Genes"))
                .otherwise(lit("Many_Genes")))
    .withColumn("has_critical_gene_disruption",
                (col("pharmacogenes_affected") > 0) | (col("omim_genes_affected") > 0))
    .withColumn("sv_clinical_priority",
                when(col("pharmacogenes_affected") >= 3, lit("Critical_Priority"))
                .when(col("pharmacogenes_affected") >= 1, lit("High_Priority"))
                .when(col("omim_genes_affected") >= 3, lit("High_Priority"))
                .when(col("omim_genes_affected") >= 1, lit("Medium_Priority"))
                .when(col("genes_overlapped") >= 10, lit("Medium_Priority"))
                .when(col("genes_overlapped") >= 1, lit("Low_Priority"))
                .otherwise(lit("Minimal_Priority")))
)

df_sv = (
    df_sv
    .join(sv_gene_agg, "sv_id", "left")
    .fillna({
        "genes_overlapped":               0,
        "gene_list":                      "",
        "pharmacogenes_affected":         0,
        "omim_genes_affected":            0,
        "kinase_genes_affected":          0,
        "receptor_genes_affected":        0,
        "max_gene_disruption_fraction":   0.0,
        "avg_druggability_affected_genes": 0.0,
        "has_critical_gene_disruption":   False
    })
    .fillna("No_Genes",       ["gene_count_category"])
    .fillna("Minimal_Priority", ["sv_clinical_priority"])
)

print("Gene-level SV features aggregated")

# COMMAND ----------

# DBTITLE 1,Step 4: Disease Association
print("\nSTEP 4: DISEASE ASSOCIATION")
print("="*80)

disease_genes = (
    df_gene_disease
    .select(
        "gene_name",
        col("total_disease_count").alias("disease_count"),
        "has_cancer_disease",
        "has_neurological_disease"
    )
    .filter(col("total_disease_count") > 0)
)

sv_disease = (
    df_sv_gene_overlap
    .filter(col("gene_name").isNotNull())
    .join(disease_genes, "gene_name", "left")
    .groupBy("sv_id")
    .agg(
        spark_sum(
            when(col("disease_count").isNotNull(), col("disease_count")).otherwise(0)
        ).alias("total_disease_associations"),
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0)).alias("cancer_genes_affected"),
        spark_sum(when(col("has_neurological_disease"), 1).otherwise(0)).alias("neuro_genes_affected")
    )
    .withColumn("has_disease_associated_genes",
                col("total_disease_associations") > 0)
)

df_sv = (
    df_sv
    .join(sv_disease, "sv_id", "left")
    .fillna({
        "total_disease_associations":  0,
        "cancer_genes_affected":       0,
        "neuro_genes_affected":        0,
        "has_disease_associated_genes": False
    })
    .withColumn("disease_sv_priority",
                when(col("cancer_genes_affected") >= 2, lit("Cancer_Gene_Disruption"))
                .when(col("neuro_genes_affected") >= 2, lit("Neuro_Gene_Disruption"))
                .when(col("has_disease_associated_genes"), lit("Disease_Gene_Involved"))
                .otherwise(lit("No_Disease_Association")))
)

print("Disease association enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 5: Expression Context
print("\nSTEP 5: EXPRESSION CONTEXT")
print("="*80)

broad_expression = (
    df_gtex
    .filter(col("max_tpm") > 1.0)
    .groupBy("gene_name")
    .agg(countDistinct("tissue_type").alias("tissues_expressed"))
    .filter(col("tissues_expressed") >= 10)
    .select("gene_name", "tissues_expressed")
)

sv_expression = (
    df_sv_gene_overlap
    .filter(col("gene_name").isNotNull())
    .join(broad_expression, "gene_name", "left")
    .groupBy("sv_id")
    .agg(
        spark_sum(when(col("tissues_expressed").isNotNull(), 1).otherwise(0))
            .alias("broadly_expressed_genes_affected")
    )
)

df_sv = (
    df_sv
    .join(sv_expression, "sv_id", "left")
    .fillna({"broadly_expressed_genes_affected": 0})
    .withColumn("affects_essential_genes",
                col("broadly_expressed_genes_affected") >= 1)
)

print("Expression enrichment complete")

# COMMAND ----------

# DBTITLE 1,Step 6: Combined SV Impact Score
print("\nSTEP 6: COMBINED SV IMPACT SCORE")
print("="*80)

df_sv = (
    df_sv
    .withColumn("sv_combined_impact_score",
                when(col("genes_overlapped") >= 20, 5)
                .when(col("genes_overlapped") >= 10, 4)
                .when(col("genes_overlapped") >= 5, 3)
                .when(col("genes_overlapped") >= 1, 2)
                .otherwise(1) +
                when(col("pharmacogenes_affected") >= 3, 3)
                .when(col("pharmacogenes_affected") >= 1, 2)
                .otherwise(0) +
                when(col("omim_genes_affected") >= 3, 2)
                .when(col("omim_genes_affected") >= 1, 1)
                .otherwise(0) +
                when(col("sv_size") >= 1000000, 2)
                .when(col("sv_size") >= 100000, 1)
                .otherwise(0) +
                when(col("sv_type_class") == "Deletion", 1)
                .otherwise(0))

    .withColumn("sv_impact_tier",
                when(col("sv_combined_impact_score") >= 12, lit("Tier_1_Critical"))
                .when(col("sv_combined_impact_score") >= 9, lit("Tier_2_High"))
                .when(col("sv_combined_impact_score") >= 6, lit("Tier_3_Moderate"))
                .when(col("sv_combined_impact_score") >= 3, lit("Tier_4_Low"))
                .otherwise(lit("Tier_5_Minimal")))

    .withColumn("sv_classification",
                when(col("has_critical_gene_disruption") & (col("sv_size") > 100000),
                     lit("Critical_Large_Disruption"))
                .when(col("has_critical_gene_disruption"),
                     lit("Critical_Gene_Affected"))
                .when((col("genes_overlapped") >= 10) & (col("sv_size") > 100000),
                     lit("Large_MultiGene_SV"))
                .when(col("genes_overlapped") >= 10,
                     lit("MultiGene_SV"))
                .when(col("genes_overlapped") == 1,
                     lit("Single_Gene_SV"))
                .when(col("genes_overlapped") == 0,
                     lit("Intergenic_SV"))
                .otherwise(lit("Other_SV")))
)

print("Combined SV impact score created")

# COMMAND ----------

# DBTITLE 1,Step 7: Deduplicate by SV ID
print("\nSTEP 7: DEDUPLICATE BY SV_ID")
print("="*80)

before_count = df_sv.count()
df_sv        = df_sv.dropDuplicates(["sv_id"])
after_count  = df_sv.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication:  {after_count:,}")
print(f"Duplicates removed:   {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Select Final Columns
print("\nSELECTING FINAL COLUMNS")
print("="*80)

df_final = df_sv.select(
    col("sv_id"),
    col("study_id"),
    col("variant_name"),
    col("chromosome"),
    col("start_pos"),
    col("end_pos"),
    col("assembly"),
    col("variant_type"),
    col("sv_type_class"),
    col("sv_size"),
    col("sv_size_category"),
    col("sv_pathogenicity_risk"),
    col("genes_overlapped"),
    col("gene_list"),
    col("gene_count_category"),
    col("pharmacogenes_affected"),
    col("omim_genes_affected"),
    col("kinase_genes_affected"),
    col("receptor_genes_affected"),
    col("max_gene_disruption_fraction"),
    col("avg_druggability_affected_genes"),
    col("has_critical_gene_disruption"),
    col("total_disease_associations"),
    col("cancer_genes_affected"),
    col("neuro_genes_affected"),
    col("has_disease_associated_genes"),
    col("disease_sv_priority"),
    col("broadly_expressed_genes_affected"),
    col("affects_essential_genes"),
    col("sv_clinical_priority"),
    col("sv_combined_impact_score"),
    col("sv_impact_tier"),
    col("sv_classification")
)

print(f"Final columns: {len(df_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Write gold.structural_variant_ml_features
print("\nWRITING gold.structural_variant_ml_features")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.structural_variant_ml_features")

print(f"Saved: {catalog_name}.gold.structural_variant_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Verification
print("\nFINAL VERIFICATION")
print("="*80)

df_check = spark.table(f"{catalog_name}.gold.structural_variant_ml_features")
rows     = df_check.count()
cols     = len(df_check.columns)

print(f"Rows:    {rows:,}")
print(f"Columns: {cols}")

print("\nSV impact tier breakdown:")
df_check.groupBy("sv_impact_tier").count().orderBy("sv_impact_tier").show()

print("\nProcessing complete")
