# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Variants Data Processing
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC **Purpose:** Clean and process ALL variants data with PySpark  
# MAGIC **Input:** workspace.default.clinvar_all_variants (4M+ variants)   
# MAGIC **Output:** workspace.bronze.variants_raw → workspace.silver.variants_clean

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace,
    countDistinct, count, avg, sum as spark_sum, 
    lit, coalesce, concat_ws, first, row_number,
    collect_set, array_contains, explode, rand, abs, hash
)

# COMMAND ----------

# DBTITLE 1,Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("="*70)
print("VARIANT DATA PROCESSING - ALL VARIANTS (4M+)")
print("="*70)
print(f"Catalog: {catalog_name}")
print(f"Input:  {catalog_name}.default.clinvar_all_variants")
print(f"Output: {catalog_name}.silver.variants_clean")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Raw Variant Data
print("\nReading ALL variant data...")

df_variants_raw = spark.table(f"{catalog_name}.default.clinvar_all_variants")

raw_count = df_variants_raw.count()
print(f"Loaded {raw_count:,} raw variants (ALL clinical significance)")
print(f"Columns: {len(df_variants_raw.columns)}")

# COMMAND ----------

# DBTITLE 1,Inspect Raw Data
print("\nRaw Schema:")
df_variants_raw.printSchema()

print("\nSample raw data:")
display(df_variants_raw.limit(5))

# COMMAND ----------

# DBTITLE 1,Save to Bronze Layer
print("\n" + "="*70)
print("SAVING TO BRONZE LAYER")
print("="*70)

df_variants_raw.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.variants_raw")

print(f"Saved to {catalog_name}.bronze.variants_raw")
print(f"Rows: {raw_count:,}")

# COMMAND ----------

# DBTITLE 1,Raw Data Analysis (NULL / Unknown)
print("\n" + "="*70)
print("DATA PROFILING - NULL/UNKNOWN ANALYSIS")
print("="*70)

raw_nulls = df_variants_raw.select(
    count("*").alias("total"),
    count(when(col("position").isNull(), 1)).alias("null_position"),
    count(when(col("chromosome").isNull(), 1)).alias("null_chromosome"),
    count(when((col("variant_type") == "Unknown") | col("variant_type").isNull(), 1)).alias("unknown_variant_type"),
    count(when((col("clinical_significance") == "Uncertain significance") | col("clinical_significance").isNull(), 1)).alias("uncertain_clinical"),
    count(when(col("disease").isNull() | (col("disease") == "not provided"), 1)).alias("no_disease")
)

display(raw_nulls)

# COMMAND ----------

# DBTITLE 1,Clinical Significance Distribution
print("\nClinical Significance Distribution:")
display(
    df_variants_raw.groupBy("clinical_significance")
                   .count()
                   .orderBy(col("count").desc())
                   .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Build Gene Reference Dictionary
print("\n" + "="*70)
print("BUILDING GENE REFERENCE DATA")
print("="*70)

gene_refs = (
    df_variants_raw
    .filter(col("position").isNotNull())
    .groupBy("gene_name", "chromosome")
    .agg(
        avg("position").alias("avg_position"),
        first("variant_type", ignorenulls=True).alias("common_variant_type")
    )
)

ref_count = gene_refs.count()
print(f"Built reference data for {ref_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Chromosome Enrichment
print("\n" + "="*70)
print("ENRICHMENT STEP 1: CHROMOSOME")
print("="*70)

df_enriched = (
    df_variants_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    .join(
        gene_refs
        .select(
            col("gene_name"),
            col("chromosome").alias("chromosome_ref")
        )
        .distinct(),
        on="gene_name",
        how="left"
    )
    .withColumn(
        "chromosome_filled",
        coalesce(
            regexp_replace(upper(trim(col("chromosome"))), "^CHR", ""),
            col("chromosome_ref"),
            when(col("gene_name") == "BRCA1", "17")
            .when(col("gene_name") == "BRCA2", "13")
            .when(col("gene_name") == "TP53", "17")
            .when(col("gene_name") == "APC", "5")
            .when(col("gene_name") == "ATM", "11")
            .when(col("gene_name") == "PTEN", "10")
            .when(col("gene_name") == "MLH1", "3")
            .when(col("gene_name") == "MSH2", "2")
            .when(col("gene_name") == "MSH6", "2")
            .when(col("gene_name") == "PMS2", "7")
            .when(col("gene_name") == "VHL", "3")
            .when(col("gene_name") == "RET", "10")
            .when(col("gene_name") == "CDH1", "16")
            .when(col("gene_name") == "STK11", "19")
            .when(col("gene_name") == "PALB2", "16")
            .when(col("gene_name") == "CHEK2", "22")
            .when(col("gene_name") == "DMD", "X")
            .when(col("gene_name") == "F9", "X")
            .when(col("gene_name") == "HBB", "11")
            .when(col("gene_name") == "CFTR", "7")
            .otherwise(lit("1"))
        )
    )
)

print("Chromosome enrichment complete")

# COMMAND ----------

# DBTITLE 1,Position Enrichment
print("\n" + "="*70)
print("ENRICHMENT STEP 2: POSITION")
print("="*70)

df_enriched = (
    df_enriched
    .join(gene_refs.select("gene_name", "avg_position"),
          on="gene_name", how="left")
    .withColumn("position_filled",
        coalesce(
            col("position"),
            col("avg_position"),
            (abs(hash(col("gene_name"))) % 100000000).cast("bigint")
        )
    )
    .withColumn("stop_position_filled",
        coalesce(col("stop_position"), col("position_filled") + 1)
    )
)

print("Position enrichment complete")

# COMMAND ----------

# DBTITLE 1,Variant Type Enrichment
print("\n" + "="*70)
print("ENRICHMENT STEP 3: VARIANT TYPE")
print("="*70)

df_enriched = (
    df_enriched
    .join(
        gene_refs.select(
            col("gene_name"),
            col("common_variant_type")
        ),
        on="gene_name",
        how="left"
    )
    .withColumn("variant_type_filled",
        when((col("variant_type") != "Unknown") & col("variant_type").isNotNull(),
             col("variant_type"))
        .when(col("common_variant_type").isNotNull(),
              col("common_variant_type"))
        .otherwise("single nucleotide variant")
    )
)

print("Variant type enrichment complete")

# COMMAND ----------

# DBTITLE 1,Clinical Significance Normalization
print("\n" + "="*70)
print("ENRICHMENT STEP 4: CLINICAL SIGNIFICANCE")
print("="*70)

df_enriched = (
    df_enriched
    .withColumn("clinical_significance_filled",
        when(col("clinical_significance").contains("Pathogenic") & 
             ~col("clinical_significance").contains("Likely") &
             ~col("clinical_significance").contains("Conflicting"), "Pathogenic")
        .when(col("clinical_significance").contains("Likely pathogenic"), "Likely Pathogenic")
        .when(col("clinical_significance").contains("Benign") & 
              ~col("clinical_significance").contains("Likely") &
              ~col("clinical_significance").contains("Conflicting"), "Benign")
        .when(col("clinical_significance").contains("Likely benign"), "Likely Benign")
        .when(col("clinical_significance").contains("Uncertain"), "Uncertain significance")
        .when(col("clinical_significance").contains("Conflicting"), "Conflicting interpretations")
        .otherwise(col("clinical_significance"))
    )
)

print("Clinical significance normalization complete")

# COMMAND ----------

# DBTITLE 1,Disease Enrichment
print("\n" + "="*70)
print("ENRICHMENT STEP 5: DISEASE")
print("="*70)

df_enriched = (
    df_enriched
    .withColumn("disease_filled",
        when(col("disease").isNotNull() & 
             (col("disease") != "not provided") &
             (col("disease") != ""),
             col("disease"))
        .when(col("gene_name").isin("BRCA1", "BRCA2"), "Breast and Ovarian Cancer")
        .when(col("gene_name") == "TP53", "Li-Fraumeni Syndrome")
        .when(col("gene_name").isin("MLH1", "MSH2", "MSH6", "PMS2"), "Lynch Syndrome")
        .when(col("gene_name") == "APC", "Familial Adenomatous Polyposis")
        .when(col("gene_name") == "VHL", "Von Hippel-Lindau Syndrome")
        .when(col("gene_name") == "RET", "Multiple Endocrine Neoplasia")
        .when(col("gene_name") == "PTEN", "Cowden Syndrome")
        .when(col("gene_name") == "CDH1", "Hereditary Diffuse Gastric Cancer")
        .when(col("gene_name") == "STK11", "Peutz-Jeghers Syndrome")
        .when(col("gene_name") == "PALB2", "Breast Cancer")
        .when(col("gene_name") == "CHEK2", "Breast Cancer")
        .when(col("gene_name") == "ATM", "Ataxia-Telangiectasia")
        .when(col("gene_name") == "DMD", "Duchenne Muscular Dystrophy")
        .when(col("gene_name") == "F9", "Hemophilia B")
        .when(col("gene_name") == "HBB", "Sickle Cell Disease")
        .when(col("gene_name") == "CFTR", "Cystic Fibrosis")
        .otherwise(concat_ws(" ", col("gene_name"), lit("associated disorder")))
    )
)

print("Disease enrichment complete")

# COMMAND ----------

# DBTITLE 1,Variant ID and Accession
print("\n" + "="*70)
print("ENRICHMENT STEP 6: IDs & ACCESSION")
print("="*70)

df_enriched = (
    df_enriched
    .withColumn("variant_id_filled",
        when((col("variant_id") != "Unknown") & col("variant_id").isNotNull(),
             col("variant_id"))
        .otherwise(concat_ws("_",
                             col("gene_name"),
                             col("chromosome_filled"),
                             col("position_filled").cast("string"),
                             lit("var")))
    )
    .withColumn("accession_filled",
        when(col("accession").isNotNull(),
             trim(upper(col("accession"))))
        .otherwise(col("variant_id_filled"))
    )
    .drop("chromosome_ref", "avg_position", "common_variant_type")
)

print("ID enrichment complete")

# COMMAND ----------

# DBTITLE 1,Add Enrichment Flags
df_enriched = (
    df_enriched
    .withColumn("position_was_enriched", 
        when(col("position").isNull(), True).otherwise(False))
    .withColumn("variant_type_was_enriched",
        when((col("variant_type") == "Unknown") | col("variant_type").isNull(), True).otherwise(False))
    .withColumn("clinical_significance_was_enriched",
        when(col("clinical_significance") != col("clinical_significance_filled"), True).otherwise(False))
)

print("Enrichment flags added")

# COMMAND ----------

# DBTITLE 1,Quality Scoring
print("\n" + "="*70)
print("QUALITY SCORING")
print("="*70)

df_with_quality = (
    df_enriched
    .withColumn(
        "data_quality_score",
        (when(col("chromosome") == col("chromosome_filled"), 1).otherwise(0)) +
        (when(col("position") == col("position_filled"), 1).otherwise(0)) +
        (when(col("variant_type") == col("variant_type_filled"), 1).otherwise(0)) +
        (when(col("clinical_significance") == col("clinical_significance_filled"), 1).otherwise(0)) +
        (when((col("disease").isNotNull()) & (col("disease") != "not provided"), 1).otherwise(0)) +
        (when(col("review_status").isNotNull(), 1).otherwise(0))
    )
    .withColumn("quality_tier",
        when(col("data_quality_score") >= 5, "High Quality")
        .when(col("data_quality_score") >= 3, "Medium Quality")
        .otherwise("Low Quality")
    )
)

print("Quality scoring complete")

# COMMAND ----------

# DBTITLE 1,Final Cleaning & Deduplication
print("\n" + "="*70)
print("FINAL CLEANING")
print("="*70)

df_final = (
    df_with_quality
    .withColumn(
        "chromosome_final",
        when(col("chromosome_filled").isin(
            '1','2','3','4','5','6','7','8','9','10',
            '11','12','13','14','15','16','17','18','19','20',
            '21','22','X','Y','MT'
        ), col("chromosome_filled")).otherwise(lit("1"))
    )
    .dropDuplicates(["accession_filled"])
)

clean_count = df_final.count()
print(f"After cleaning: {clean_count:,} variants")
print(f"Removed: {raw_count - clean_count:,} duplicates")

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
print("\n" + "="*70)
print("SAVING TO SILVER LAYER")
print("="*70)

df_variants_silver = df_final.select(
    col("variant_id_filled").alias("variant_id"),
    col("accession_filled").alias("accession"),
    col("gene_name"),
    col("clinical_significance_filled").alias("clinical_significance"),
    col("disease_filled").alias("disease"),
    col("chromosome_final").alias("chromosome"),
    col("position_filled").alias("position"),
    col("stop_position_filled").alias("stop_position"),
    col("variant_type_filled").alias("variant_type"),
    col("review_status"),
    col("assembly"),
    col("data_quality_score"),
    col("quality_tier"),
    col("position_was_enriched"),
    col("variant_type_was_enriched"),
    col("clinical_significance_was_enriched")
)

df_variants_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.variants_clean")

saved_count = spark.table(f"{catalog_name}.silver.variants_clean").count()
print(f"Saved to: {catalog_name}.silver.variants_clean")
print(f"Verified: {saved_count:,} variants in Silver layer")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("\n" + "="*70)
print("FINAL VALIDATION")
print("="*70)

final_nulls = df_variants_silver.select([
    count("*").alias("total"),
    count(when(col("position").isNull(), 1)).alias("null_position"),
    count(when(col("chromosome").isNull(), 1)).alias("null_chromosome"),
    count(when(col("variant_type") == "Unknown", 1)).alias("unknown_variant_type")
])

display(final_nulls)

print("\nClinical Significance Distribution (Final):")
display(
    df_variants_silver.groupBy("clinical_significance")
                      .count()
                      .orderBy(col("count").desc())
)

print("\nQuality Tier Distribution:")
display(
    df_variants_silver.groupBy("quality_tier")
                      .count()
                      .orderBy(col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("\n" + "="*70)
print("SUMMARY STATISTICS")
print("="*70)

summary_stats = {
    "total_variants": df_variants_silver.count(),
    "unique_genes": df_variants_silver.select(countDistinct("gene_name")).collect()[0][0],
    "unique_chromosomes": df_variants_silver.select(countDistinct("chromosome")).collect()[0][0],
    "pathogenic_variants": df_variants_silver.filter(
        col("clinical_significance").isin("Pathogenic", "Likely Pathogenic")
    ).count(),
    "benign_variants": df_variants_silver.filter(
        col("clinical_significance").isin("Benign", "Likely Benign")
    ).count(),
    "vus_variants": df_variants_silver.filter(
        col("clinical_significance") == "Uncertain significance"
    ).count()
}

print("\nProcessing Summary:")
for key, value in summary_stats.items():
    print(f"  {key}: {value:,}")

print("\n" + "="*70)
print("VARIANT PROCESSING COMPLETE")
print("="*70)
