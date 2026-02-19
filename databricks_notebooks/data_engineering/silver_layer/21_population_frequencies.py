# Databricks notebook source
# MAGIC %md
# MAGIC #### Population Frequency Processing
# MAGIC ######Extract population frequencies from existing conservation scores
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Input:** silver.conservation_scores  
# MAGIC **Output:** silver.population_frequencies

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR POPULATION FREQUENCY PROCESSING")

# COMMAND ----------

# DBTITLE 1,Load Conservation Scores
print("\nLOADING CONSERVATION SCORES")
print("="*80)

df_conservation = spark.table(f"{catalog_name}.silver.conservation_scores")

conservation_count = df_conservation.count()
print(f"Conservation scores: {conservation_count:,}")

print("\nSchema:")
df_conservation.printSchema()

print("\nSample data:")
df_conservation.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Check gnomAD Coverage
print("\nCHECKING GNOMAD COVERAGE")
print("="*80)

gnomad_count = df_conservation.filter(col("gnomad_af").isNotNull()).count()
gnomad_pct = gnomad_count / conservation_count * 100

print(f"Variants with gnomAD frequency: {gnomad_count:,} ({gnomad_pct:.2f}%)")

print("\ngnomAD frequency distribution:")
df_conservation.filter(col("gnomad_af").isNotNull()) \
    .select("gnomad_af") \
    .summary() \
    .show()

print("\nRarity distribution:")
df_conservation.groupBy("is_rare_variant", "is_common_variant").count().show()

# COMMAND ----------

# DBTITLE 1,Extract Population Frequencies
print("\nEXTRACTING POPULATION FREQUENCIES")
print("="*80)

df_population = (
    df_conservation
    .select(
        "variant_id",
        "chromosome",
        "position",
        "reference_allele",
        "alternate_allele",
        "gene_name",
        col("gnomad_af").alias("allele_frequency_global")
    )
    .filter(col("allele_frequency_global").isNotNull())
)

population_count = df_population.count()
print(f"Population frequency records: {population_count:,}")

df_population.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Rarity Classifications
print("\nADDING RARITY CLASSIFICATIONS")
print("="*80)

df_population_enriched = (
    df_population
    .withColumn("is_rare",
                when(col("allele_frequency_global") < 0.01, True).otherwise(False))
    
    .withColumn("is_ultra_rare",
                when(col("allele_frequency_global") < 0.001, True).otherwise(False))
    
    .withColumn("is_common",
                when(col("allele_frequency_global") >= 0.05, True).otherwise(False))
    
    .withColumn("frequency_category",
                when(col("allele_frequency_global") < 0.001, lit("ultra_rare"))
                .when(col("allele_frequency_global") < 0.01, lit("rare"))
                .when(col("allele_frequency_global") < 0.05, lit("low_frequency"))
                .otherwise(lit("common")))
)

print("Added rarity classifications")
df_population_enriched.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Validate Classifications
print("\nVALIDATING CLASSIFICATIONS")
print("="*80)

print("\nFrequency category distribution:")
df_population_enriched.groupBy("frequency_category").count().orderBy(col("count").desc()).show()

print("\nRarity flags:")
print(f"  Ultra-rare (AF < 0.001): {df_population_enriched.filter(col('is_ultra_rare')).count():,}")
print(f"  Rare (AF < 0.01): {df_population_enriched.filter(col('is_rare')).count():,}")
print(f"  Common (AF >= 0.05): {df_population_enriched.filter(col('is_common')).count():,}")

print("\nSample ultra-rare variants:")
df_population_enriched.filter(
    col("is_ultra_rare")
).select(
    "gene_name",
    "chromosome",
    "position",
    "allele_frequency_global",
    "frequency_category"
).show(5, truncate=60)

print("\nSample common variants:")
df_population_enriched.filter(
    col("is_common")
).select(
    "gene_name",
    "chromosome",
    "position",
    "allele_frequency_global",
    "frequency_category"
).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver Population Frequencies
print("\nSAVING SILVER POPULATION FREQUENCIES")
print("="*80)

df_population_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.population_frequencies")

print(f"Saved: {catalog_name}.silver.population_frequencies")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("POPULATION FREQUENCY SILVER PROCESSING COMPLETE")
print("="*80)

final_count = spark.table(f"{catalog_name}.silver.population_frequencies").count()
print(f"\nTable created:")
print(f"  silver.population_frequencies: {final_count:,} records")

print("\nFrequency statistics:")
spark.table(f"{catalog_name}.silver.population_frequencies") \
    .select("allele_frequency_global") \
    .summary() \
    .show()

print("\nCategory breakdown:")
spark.table(f"{catalog_name}.silver.population_frequencies") \
    .groupBy("frequency_category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("\nProcessing complete")
