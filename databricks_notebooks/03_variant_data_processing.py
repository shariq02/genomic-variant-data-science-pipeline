# Databricks notebook source
# MAGIC %md
# MAGIC #### WEEK 3 - PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Variants Data Processing
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:**  10 January 2026  
# MAGIC **Purpose:** Clean and process variants data with PySpark  
# MAGIC **Input:** workspace.default.clinvar_pathogenic   
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

print("VARIANT DATA ENRICHMENT")
print(f"Catalog: {catalog_name}")
print(f"Input:  {catalog_name}.default.clinvar_pathogenic")
print(f"Output: {catalog_name}.silver.variants_clean")

# COMMAND ----------

# DBTITLE 1,Read Raw Variant Data
print("Reading variant data...")

df_variants_raw = spark.table(f"{catalog_name}.default.clinvar_pathogenic")

print(f"Loaded {df_variants_raw.count()} raw variants")
print(f"Columns: {len(df_variants_raw.columns)}")


# COMMAND ----------

# DBTITLE 1,Save to Bronze Layer
df_variants_raw.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.variants_raw")

print(f"Saved to {catalog_name}.bronze.variants_raw")

# COMMAND ----------

# DBTITLE 1,Raw Data Analysis (NULL / Unknown)
raw_nulls = df_variants_raw.select(
    count("*").alias("total"),
    count(when(col("position").isNull(), 1)).alias("null_position"),
    count(when(col("chromosome").isNull(), 1)).alias("null_chromosome"),
    count(when((col("variant_type") == "Unknown") | col("variant_type").isNull(), 1)).alias("unknown_variant_type"),
    count(when((col("clinical_significance") == "Uncertain") | col("clinical_significance").isNull(), 1)).alias("uncertain_clinical"),
    count(when((col("molecular_consequence") == "Unknown") | col("molecular_consequence").isNull(), 1)).alias("unknown_consequence"),
    count(when((col("protein_change") == "Unknown") | col("protein_change").isNull(), 1)).alias("unknown_protein")
)

raw_nulls.show(vertical=True)


# COMMAND ----------

# DBTITLE 1,Build Gene Reference Dictionary
gene_refs = (
    df_variants_raw
    .filter(col("position").isNotNull())
    .groupBy("gene_name", "chromosome")
    .agg(
        avg("position").alias("avg_position"),
        first("variant_type", ignorenulls=True).alias("common_variant_type"),
        first("molecular_consequence", ignorenulls=True).alias("common_consequence")
    )
)

print(f"Built reference data for {gene_refs.count()} genes")


# COMMAND ----------

# DBTITLE 1,Aggressive Enrichment
df_enriched = (
    df_variants_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))

    # ---- CHROMOSOME REF (ALIAS FIX ONLY) ----
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

            # ---- YOUR FULL MANUAL FALLBACK LIST (UNCHANGED) ----
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


# COMMAND ----------

# DBTITLE 1,Position, Variant Type, Clinical Enrichment
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


# COMMAND ----------

# DBTITLE 1,Quality Scoring
df_with_quality = (
    df_enriched
    .withColumn(
        "data_quality_score",
        (when(col("chromosome") == col("chromosome_filled"), 1).otherwise(0)) +
        (when(col("position") == col("position_filled"), 1).otherwise(0)) +
        (when(col("variant_type") == col("variant_type_filled"), 1).otherwise(0)) +
        (when(col("molecular_consequence") == col("molecular_consequence_filled"), 1).otherwise(0)) +
        (when(col("clinical_significance") == col("clinical_significance_filled"), 1).otherwise(0)) +
        (when(col("protein_change") == col("protein_change_filled"), 1).otherwise(0))
    )
)


# COMMAND ----------

# DBTITLE 1,Final Cleaning
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


# COMMAND ----------

# DBTITLE 1,Save to Silver
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
    col("molecular_consequence_filled").alias("molecular_consequence"),
    col("protein_change_filled").alias("protein_change"),
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

print("Saved variants to silver layer")


# COMMAND ----------

# DBTITLE 1,Final Validation
final_nulls = df_variants_silver.select([
    count("*").alias("total"),
    count(when(col("position").isNull(), 1)).alias("null_position"),
    count(when(col("chromosome").isNull(), 1)).alias("null_chromosome"),
    count(when(col("variant_type") == "Unknown", 1)).alias("unknown_variant_type"),
    count(when(col("clinical_significance") == "Uncertain", 1)).alias("uncertain_clinical")
])

final_nulls.show(vertical=True)
