# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lit, when, expr
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
print("Spark Initialized")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print(f"Catalog: {catalog_name}")
print("Processing dbVar structural variants")

# COMMAND ----------

# DBTITLE 1,Read Raw dbVar Tables
df_estd214_raw = spark.table(f"{catalog_name}.default.estd_214_gr_ch_38_variant_region_vcf_parsed_full")
df_nstd102_calls_raw = spark.table(f"{catalog_name}.default.nstd_102_gr_ch_38_variant_call_vcf_parsed_full")
df_nstd102_regions_raw = spark.table(f"{catalog_name}.default.nstd_102_gr_ch_38_variant_region_vcf_parsed_full")

print(f"Loaded estd214 regions: {df_estd214_raw.count():,}")
print(f"Loaded nstd102 calls: {df_nstd102_calls_raw.count():,}")
print(f"Loaded nstd102 regions: {df_nstd102_regions_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Inspect Schemas
print("estd214 Schema:")
df_estd214_raw.printSchema()

print("\nnstd102 calls Schema:")
df_nstd102_calls_raw.printSchema()

print("\nnstd102 regions Schema:")
df_nstd102_regions_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Sample Raw Data
print("estd214 Sample:")
df_estd214_raw.show(3, truncate=60)

print("\nnstd102 calls Sample:")
df_nstd102_calls_raw.show(3, truncate=60)

print("\nnstd102 regions Sample:")
df_nstd102_regions_raw.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 1: Clean estd214 Regions
df_estd214_clean = (
    df_estd214_raw
    .withColumn("variant_id", trim(col("variant_id")))
    .withColumn("variant_name", trim(col("variant_name")))
    .withColumn("variant_type", trim(col("variant_type")))
    .withColumn("chromosome", 
                when(col("chromosome").startswith("chr"), col("chromosome").substr(4, 10))
                .otherwise(trim(col("chromosome"))))
    .withColumn("start_position", expr("try_cast(start_position as long)"))
    .withColumn("end_position", expr("try_cast(end_position as long)"))
    .withColumn("assembly", trim(col("assembly")))
    .withColumn("study_id", lit("estd214"))
    
    # Filter valid variants
    .filter(col("variant_id").isNotNull())
    .filter(col("variant_id") != "")
    .filter(col("chromosome").isNotNull())
    .filter(col("chromosome") != "")
    
    # Valid chromosomes only
    .filter(
        col("chromosome").isin(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "X", "Y", "M", "MT"
        )
    )
)

print(f"Clean estd214 variants: {df_estd214_clean.count():,}")
df_estd214_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean nstd102 Calls
df_nstd102_calls_clean = (
    df_nstd102_calls_raw
    .withColumn("variant_id", trim(col("variant_id")))
    .withColumn("variant_name", trim(col("variant_name")))
    .withColumn("variant_type", trim(col("variant_type")))
    .withColumn("chromosome", 
                when(col("chromosome").startswith("chr"), col("chromosome").substr(4, 10))
                .otherwise(trim(col("chromosome"))))
    .withColumn("start_position", expr("try_cast(start_position as long)"))
    .withColumn("end_position", expr("try_cast(end_position as long)"))
    .withColumn("assembly", trim(col("assembly")))
    .withColumn("study_id", lit("nstd102_calls"))
    
    # Filter valid variants
    .filter(col("variant_id").isNotNull())
    .filter(col("variant_id") != "")
    .filter(col("chromosome").isNotNull())
    .filter(col("chromosome") != "")
    
    # Valid chromosomes only
    .filter(
        col("chromosome").isin(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "X", "Y", "M", "MT"
        )
    )
)

print(f"Clean nstd102 calls: {df_nstd102_calls_clean.count():,}")
df_nstd102_calls_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 3: Clean nstd102 Regions
df_nstd102_regions_clean = (
    df_nstd102_regions_raw
    .withColumn("variant_id", trim(col("variant_id")))
    .withColumn("variant_name", trim(col("variant_name")))
    .withColumn("variant_type", trim(col("variant_type")))
    .withColumn("chromosome", 
                when(col("chromosome").startswith("chr"), col("chromosome").substr(4, 10))
                .otherwise(trim(col("chromosome"))))
    .withColumn("start_position", expr("try_cast(start_position as long)"))
    .withColumn("end_position", expr("try_cast(end_position as long)"))
    .withColumn("assembly", trim(col("assembly")))
    .withColumn("study_id", lit("nstd102_regions"))
    
    # Filter valid variants
    .filter(col("variant_id").isNotNull())
    .filter(col("variant_id") != "")
    .filter(col("chromosome").isNotNull())
    .filter(col("chromosome") != "")
    
    # Valid chromosomes only
    .filter(
        col("chromosome").isin(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "X", "Y", "M", "MT"
        )
    )
)

print(f"Clean nstd102 regions: {df_nstd102_regions_clean.count():,}")
df_nstd102_regions_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 4: Combine All Structural Variants
df_structural_variants = (
    df_estd214_clean
    .select(
        "variant_id",
        "study_id",
        "variant_name",
        "variant_type",
        "chromosome",
        "start_position",
        "end_position",
        "assembly"
    )
    .unionByName(
        df_nstd102_calls_clean.select(
            "variant_id",
            "study_id",
            "variant_name",
            "variant_type",
            "chromosome",
            "start_position",
            "end_position",
            "assembly"
        )
    )
    .unionByName(
        df_nstd102_regions_clean.select(
            "variant_id",
            "study_id",
            "variant_name",
            "variant_type",
            "chromosome",
            "start_position",
            "end_position",
            "assembly"
        )
    )
    .dropDuplicates(["variant_id", "study_id"])
)

print(f"Combined structural variants: {df_structural_variants.count():,}")
df_structural_variants.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Structural Variants to Silver
df_structural_variants.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.structural_variants")

print(f"Saved table: {catalog_name}.silver.structural_variants")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("VALIDATION SUMMARY")
print("=" * 70)

sv_count = spark.table(f"{catalog_name}.silver.structural_variants").count()

print(f"\nsilver.structural_variants: {sv_count:,} rows")

# Study distribution
print("\nVariants by Study:")
spark.table(f"{catalog_name}.silver.structural_variants") \
    .groupBy("study_id") \
    .count() \
    .orderBy("study_id") \
    .show()

# Variant type distribution
print("\nVariants by Type:")
spark.table(f"{catalog_name}.silver.structural_variants") \
    .groupBy("variant_type") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

# Chromosome distribution
print("\nVariants by Chromosome:")
spark.table(f"{catalog_name}.silver.structural_variants") \
    .groupBy("chromosome") \
    .count() \
    .orderBy("chromosome") \
    .show(30)

# Sample variants
print("\nSample Structural Variants:")
spark.table(f"{catalog_name}.silver.structural_variants").show(10, truncate=60)

print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
