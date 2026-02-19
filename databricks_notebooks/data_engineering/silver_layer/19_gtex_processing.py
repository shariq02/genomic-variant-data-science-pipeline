# Databricks notebook source
# MAGIC %md
# MAGIC #### GTEx Expression Data Processing
# MAGIC ###### Transform GTEx tissue expression data to silver layer
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Input:** default.gtex_tissue_expression  
# MAGIC **Output:** silver.gtex_tissue_expression

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, count as spark_count, max
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITILIZED FOR GTEX EXPRESSION PROCESSING")

# COMMAND ----------

# DBTITLE 1,Load GTEx Data
print("\nLOADING GTEX EXPRESSION DATA")
print("="*80)

df_gtex = spark.table(f"{catalog_name}.default.gtex_tissue_expression")

gtex_count = df_gtex.count()
print(f"GTEx records: {gtex_count:,}")

print("\nSchema:")
df_gtex.printSchema()

print("\nSample data:")
df_gtex.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Check Data Quality
print("\nDATA QUALITY CHECKS")
print("="*80)

print("\nUnique genes:")
unique_genes = df_gtex.select("gene_id").distinct().count()
print(f"  Gene IDs: {unique_genes:,}")

print("\nUnique tissues:")
df_gtex.select("tissue_type").distinct().show(30, truncate=False)

print("\nExpression category distribution:")
df_gtex.groupBy("expression_category").count().orderBy("count", ascending=False).show()

print("\nExpression TPM statistics:")
df_gtex.select("expression_tpm").summary().show()

# COMMAND ----------

# DBTITLE 1,Clean GTEx Data
print("\nCLEANING GTEX DATA")
print("="*80)

df_gtex_clean = (
    df_gtex
    .withColumn("gene_id", trim(col("gene_id")))
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    .withColumn("tissue_type", trim(col("tissue_type")))
    .withColumn("expression_tpm", col("expression_tpm").cast("double"))
    .withColumn("expression_category", lower(trim(col("expression_category"))))
    
    .filter(col("gene_id").isNotNull())
    .filter(col("gene_id") != "")
    .filter(col("gene_name").isNotNull())
    .filter(col("gene_name") != "")
    .filter(col("tissue_type").isNotNull())
    .filter(col("tissue_type") != "")
    .filter(col("expression_tpm").isNotNull())
    .filter(col("expression_tpm") > 0)
    
    .dropDuplicates(["gene_id", "tissue_type"])
    
    .select(
        "gene_id",
        "gene_name",
        "tissue_type",
        "expression_tpm",
        "expression_category"
    )
)

gtex_clean_count = df_gtex_clean.count()
print(f"Clean GTEx records: {gtex_clean_count:,}")

df_gtex_clean.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Tissue Specificity Metrics
print("\nADDING TISSUE SPECIFICITY METRICS")
print("="*80)



gene_window = Window.partitionBy("gene_id")

df_gtex_enriched = (
    df_gtex_clean
    .withColumn("max_tpm", max(col("expression_tpm")).over(gene_window))
    .withColumn("tissues_expressed", spark_count("tissue_type").over(gene_window))
    
    .withColumn("is_primary_tissue", 
                when(col("expression_tpm") == col("max_tpm"), True).otherwise(False))
    
    .withColumn("tissue_specificity_category",
                when(col("tissues_expressed") <= 3, lit("highly_specific"))
                .when(col("tissues_expressed") <= 10, lit("moderately_specific"))
                .otherwise(lit("broadly_expressed")))
)

print("Added tissue specificity metrics")
df_gtex_enriched.show(5, truncate=60)


# COMMAND ----------

# DBTITLE 1,Validate Enriched Data
print("\nVALIDATING ENRICHED DATA")
print("="*80)

print("\nTissue specificity distribution:")
df_gtex_enriched.groupBy("tissue_specificity_category").count().show()

print("\nSample highly specific genes:")
df_gtex_enriched.filter(
    col("tissue_specificity_category") == "highly_specific"
).select(
    "gene_name",
    "tissue_type",
    "expression_tpm",
    "tissues_expressed"
).show(5, truncate=60)

print("\nSample broadly expressed genes:")
df_gtex_enriched.filter(
    col("tissue_specificity_category") == "broadly_expressed"
).select(
    "gene_name",
    "tissue_type",
    "expression_tpm",
    "tissues_expressed"
).distinct().show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver GTEx
print("\nSAVING SILVER GTEX")
print("="*80)

df_gtex_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.gtex_tissue_expression")

print(f"Saved: {catalog_name}.silver.gtex_tissue_expression")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("GTEX SILVER PROCESSING COMPLETE")
print("="*80)

final_count = spark.table(f"{catalog_name}.silver.gtex_tissue_expression").count()
print(f"\nTable created:")
print(f"  silver.gtex_tissue_expression: {final_count:,} records")

print("\nSample tissue expression:")
spark.table(f"{catalog_name}.silver.gtex_tissue_expression") \
    .select("gene_name", "tissue_type", "expression_tpm", "expression_category", "tissue_specificity_category") \
    .show(10, truncate=60)

print("\nProcessing complete")
