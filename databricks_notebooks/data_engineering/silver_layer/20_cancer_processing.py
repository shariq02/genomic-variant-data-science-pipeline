# Databricks notebook source
# MAGIC %md
# MAGIC #### TCGA Cancer Data Processing
# MAGIC ######Transform TCGA cancer mutation data to silver layer
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Input:** default.cancer_mutations  
# MAGIC **Output:** silver.cancer_mutations

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, count as spark_count
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR TCGA CANCER PROCESSING")

# COMMAND ----------

# DBTITLE 1,Load TCGA Cancer Data
print("\nLOADING TCGA CANCER DATA")
print("="*80)

df_cancer = spark.table(f"{catalog_name}.default.cancer_mutations")

cancer_count = df_cancer.count()
print(f"Cancer mutations: {cancer_count:,}")

print("\nSchema:")
df_cancer.printSchema()

print("\nSample data:")
df_cancer.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Check Data Quality
print("\nDATA QUALITY CHECKS")
print("="*80)

print("\nUnique genes:")
unique_genes = df_cancer.select("gene_symbol").distinct().count()
print(f"  Genes: {unique_genes:,}")

print("\nTop 10 mutated genes:")
df_cancer.groupBy("gene_symbol").count().orderBy(col("count").desc()).show(10, truncate=False)

print("\nVariant classification distribution:")
df_cancer.groupBy("variant_class").count().orderBy(col("count").desc()).show(truncate=False)

print("\nVariant type distribution:")
df_cancer.groupBy("variant_type").count().orderBy(col("count").desc()).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Clean Cancer Data
print("\nCLEANING CANCER DATA")
print("="*80)

df_cancer_clean = (
    df_cancer
    .withColumn("gene_symbol", upper(trim(col("gene_symbol"))))
    .withColumn("chromosome", trim(col("chromosome")))
    .withColumn("position", col("position").cast("long"))
    .withColumn("variant_class", trim(col("variant_class")))
    .withColumn("variant_type", trim(col("variant_type")))
    .withColumn("reference_allele", upper(trim(col("reference_allele"))))
    .withColumn("alternate_allele", upper(trim(col("alternate_allele"))))
    .withColumn("tumor_sample", trim(col("tumor_sample")))
    
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    .filter(col("chromosome").isNotNull())
    .filter(col("position").isNotNull())
    .filter(col("variant_class").isNotNull())
    
    .select(
        "gene_symbol",
        "chromosome",
        "position",
        "variant_class",
        "variant_type",
        "reference_allele",
        "alternate_allele",
        "tumor_sample"
    )
)

cancer_clean_count = df_cancer_clean.count()
print(f"Clean cancer mutations: {cancer_clean_count:,}")

df_cancer_clean.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Cancer Classification Flags
print("\nADDING CANCER CLASSIFICATION FLAGS")
print("="*80)

df_cancer_enriched = (
    df_cancer_clean
    .withColumn("is_missense",
                when(col("variant_class") == "Missense_Mutation", True).otherwise(False))
    
    .withColumn("is_truncating",
                when(col("variant_class").isin("Nonsense_Mutation", "Frame_Shift_Del", 
                                               "Frame_Shift_Ins", "Splice_Site"), True).otherwise(False))
    
    .withColumn("is_silent",
                when(col("variant_class") == "Silent", True).otherwise(False))
    
    .withColumn("is_snv",
                when(col("variant_type") == "SNP", True).otherwise(False))
    
    .withColumn("is_indel",
                when(col("variant_type").isin("INS", "DEL"), True).otherwise(False))
    
    .withColumn("impact_category",
                when(col("variant_class").isin("Nonsense_Mutation", "Frame_Shift_Del", 
                                               "Frame_Shift_Ins"), lit("high"))
                .when(col("variant_class").isin("Missense_Mutation", "In_Frame_Del", 
                                               "In_Frame_Ins"), lit("moderate"))
                .when(col("variant_class").isin("Splice_Site", "Splice_Region"), lit("moderate"))
                .otherwise(lit("low")))
)

print("Added cancer classification flags")
df_cancer_enriched.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Calculate Gene Mutation Frequencies
print("\nCALCULATING GENE MUTATION FREQUENCIES")
print("="*80)

from pyspark.sql.window import Window

gene_window = Window.partitionBy("gene_symbol")

df_cancer_enriched = (
    df_cancer_enriched
    .withColumn("mutation_count", spark_count("gene_symbol").over(gene_window))
    
    .withColumn("is_frequently_mutated",
                when(col("mutation_count") >= 100, True).otherwise(False))
)

print("\nFrequently mutated genes:")
df_cancer_enriched.filter(col("is_frequently_mutated")) \
    .select("gene_symbol", "mutation_count") \
    .distinct() \
    .orderBy(col("mutation_count").desc()) \
    .show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validate Enriched Data
print("\nVALIDATING ENRICHED DATA")
print("="*80)

print("\nImpact category distribution:")
df_cancer_enriched.groupBy("impact_category").count().orderBy(col("count").desc()).show()

print("\nVariant type flags:")
print(f"  Missense: {df_cancer_enriched.filter(col('is_missense')).count():,}")
print(f"  Truncating: {df_cancer_enriched.filter(col('is_truncating')).count():,}")
print(f"  Silent: {df_cancer_enriched.filter(col('is_silent')).count():,}")
print(f"  SNV: {df_cancer_enriched.filter(col('is_snv')).count():,}")
print(f"  Indel: {df_cancer_enriched.filter(col('is_indel')).count():,}")

print("\nSample high-impact mutations:")
df_cancer_enriched.filter(
    col("impact_category") == "high"
).select(
    "gene_symbol",
    "chromosome",
    "position",
    "variant_class",
    "mutation_count"
).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver Cancer
print("\nSAVING SILVER CANCER")
print("="*80)

df_cancer_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.cancer_mutations")

print(f"Saved: {catalog_name}.silver.cancer_mutations")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("TCGA CANCER SILVER PROCESSING COMPLETE")
print("="*80)

final_count = spark.table(f"{catalog_name}.silver.cancer_mutations").count()
print(f"\nTable created:")
print(f"  silver.cancer_mutations: {final_count:,} records")

print("\nTop cancer genes:")
spark.table(f"{catalog_name}.silver.cancer_mutations") \
    .groupBy("gene_symbol") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10, truncate=False)

print("\nSample mutations:")
spark.table(f"{catalog_name}.silver.cancer_mutations") \
    .select("gene_symbol", "chromosome", "position", "variant_class", "impact_category") \
    .show(5, truncate=60)

print("\nProcessing complete")
