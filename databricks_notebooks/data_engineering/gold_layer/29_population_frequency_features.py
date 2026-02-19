# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - POPULATION FREQUENCY ANALYSIS
# MAGIC ##### Module: Variant-Level Population Frequency Features
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 10: Population Carrier Screening
# MAGIC - Use Case 19: Ancestry-Specific Risk
# MAGIC 
# MAGIC **Input:**
# MAGIC - silver.population_frequencies
# MAGIC - silver.variants_ultra_enriched
# MAGIC 
# MAGIC **Output:** gold.population_frequency_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, lit, trim, upper, coalesce, concat_ws
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD POPULATION FREQUENCY FEATURES")
print("Variant-level population frequency feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_pop_freq = spark.table(f"{catalog_name}.silver.population_frequencies")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

print(f"Population frequencies: {df_pop_freq.count():,}")
print(f"Variants: {df_variants.count():,}")

print("\nPopulation frequencies schema:")
df_pop_freq.printSchema()

print("\nSample population frequency data:")
df_pop_freq.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Frequency Classification
print("\nADDING FREQUENCY CLASSIFICATION")
print("="*80)

df_classified = (
    df_pop_freq
    .withColumn("allele_frequency",
                coalesce(col("allele_frequency_global"), lit(0.0)))
    
    .withColumn("is_ultra_rare_variant",
                when(col("allele_frequency") < 0.0001, True).otherwise(False))
    
    .withColumn("is_very_rare_variant",
                when((col("allele_frequency") >= 0.0001) & 
                     (col("allele_frequency") < 0.001), True).otherwise(False))
    
    .withColumn("is_rare_variant",
                when((col("allele_frequency") >= 0.001) & 
                     (col("allele_frequency") < 0.01), True).otherwise(False))
    
    .withColumn("is_low_frequency_variant",
                when((col("allele_frequency") >= 0.01) & 
                     (col("allele_frequency") < 0.05), True).otherwise(False))
    
    .withColumn("is_common_variant",
                when(col("allele_frequency") >= 0.05, True).otherwise(False))
    
    .withColumn("frequency_tier",
                when(col("allele_frequency") < 0.0001, "ultra_rare")
                .when(col("allele_frequency") < 0.001, "very_rare")
                .when(col("allele_frequency") < 0.01, "rare")
                .when(col("allele_frequency") < 0.05, "low_frequency")
                .otherwise("common"))
)

print("Added frequency classification")
print("\nFrequency tier distribution:")
df_classified.groupBy("frequency_tier").count().orderBy("frequency_tier").show()

# COMMAND ----------

# DBTITLE 1,Calculate Rarity Scores
print("\nCALCULATING RARITY SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("rarity_score",
                when(col("allele_frequency") < 0.0001, 10)
                .when(col("allele_frequency") < 0.001, 8)
                .when(col("allele_frequency") < 0.01, 6)
                .when(col("allele_frequency") < 0.05, 4)
                .otherwise(2))
    
    .withColumn("carrier_risk_score",
                when((col("is_ultra_rare_variant")) | (col("is_very_rare_variant")), 10)
                .when(col("is_rare_variant"), 7)
                .when(col("is_low_frequency_variant"), 5)
                .otherwise(2))
    
    .withColumn("pathogenicity_likelihood_score",
                when(col("is_ultra_rare_variant"), 9)
                .when(col("is_very_rare_variant"), 7)
                .when(col("is_rare_variant"), 5)
                .when(col("is_low_frequency_variant"), 3)
                .otherwise(1))
)

print("Added scores")
print("\nScore distribution:")
df_scored.select("rarity_score", "carrier_risk_score", "pathogenicity_likelihood_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Join with Variant Clinical Data
print("\nJOINING WITH VARIANT CLINICAL DATA")
print("="*80)

df_with_variants = (
    df_scored
    .join(
        df_variants.select(
            col("variant_id"),
            upper(trim(col("gene_name"))).alias("gene_symbol"),
            col("clinical_significance_simple"),
            col("is_pathogenic"),
            col("is_benign"),
            col("is_vus"),
            col("is_germline"),
            col("is_somatic")
        ),
        on="variant_id",
        how="left"
    )
)

print(f"Variants with population frequency: {df_with_variants.count():,}")
print("\nSample with variants:")
df_with_variants.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Clinical Actionability Classification
print("\nADDING CLINICAL ACTIONABILITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_with_variants
    .withColumn("is_clinically_actionable_rare_variant",
                when((col("is_rare_variant") | col("is_ultra_rare_variant")) & 
                     (col("is_pathogenic")), True).otherwise(False))
    
    .withColumn("is_carrier_screening_candidate",
                when((col("is_rare_variant")) & 
                     (col("is_germline")), True).otherwise(False))
    
    .withColumn("population_priority",
                when(col("is_clinically_actionable_rare_variant"), "high")
                .when((col("is_rare_variant")) & (col("is_vus")), "medium")
                .otherwise("low"))
    
    .withColumn("screening_recommendation",
                when(col("is_clinically_actionable_rare_variant"), "recommended")
                .when(col("is_carrier_screening_candidate"), "consider")
                .otherwise("not_indicated"))
)

print("Added actionability classification")
print("\nPopulation priority distribution:")
df_priority.groupBy("population_priority").count().orderBy("population_priority").show()

print("\nScreening recommendation distribution:")
df_priority.groupBy("screening_recommendation").count().orderBy("screening_recommendation").show()

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        col("variant_id"),
        col("gene_symbol"),
        col("gene_name"),
        col("chromosome"),
        col("position"),
        col("reference_allele"),
        col("alternate_allele"),
        
        col("allele_frequency"),
        col("frequency_category"),
        
        col("is_ultra_rare_variant"),
        col("is_very_rare_variant"),
        col("is_rare_variant"),
        col("is_low_frequency_variant"),
        col("is_common_variant"),
        
        col("frequency_tier"),
        
        coalesce(col("clinical_significance_simple"), lit("Unknown")).alias("clinical_significance"),
        coalesce(col("is_pathogenic"), lit(False)).alias("is_pathogenic"),
        coalesce(col("is_benign"), lit(False)).alias("is_benign"),
        coalesce(col("is_vus"), lit(False)).alias("is_vus"),
        coalesce(col("is_germline"), lit(False)).alias("is_germline"),
        coalesce(col("is_somatic"), lit(False)).alias("is_somatic"),
        
        col("rarity_score"),
        col("carrier_risk_score"),
        col("pathogenicity_likelihood_score"),
        
        col("is_clinically_actionable_rare_variant"),
        col("is_carrier_screening_candidate"),
        col("population_priority"),
        col("screening_recommendation")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} variants")

print("\nFeature columns:")
for col_name in df_final.columns:
    print(f"  - {col_name}")

print("\nSample final features:")
df_final.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant ID
print("\nDEDUPLICATING BY VARIANT_ID")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["variant_id"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Population Frequency Features
print("\nSAVING GOLD POPULATION FREQUENCY FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.population_frequency_ml_features")

print(f"Saved: {catalog_name}.gold.population_frequency_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nPOPULATION FREQUENCY FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.population_frequency_ml_features").count()
print(f"\nTable created:")
print(f"  gold.population_frequency_ml_features: {result_count:,} variants")

print("\nFrequency tier breakdown:")
spark.table(f"{catalog_name}.gold.population_frequency_ml_features") \
    .groupBy("frequency_tier") \
    .count() \
    .orderBy("frequency_tier") \
    .show()

print("\nPopulation priority breakdown:")
spark.table(f"{catalog_name}.gold.population_frequency_ml_features") \
    .groupBy("population_priority") \
    .count() \
    .orderBy("population_priority") \
    .show()

print("\nClinically actionable rare variants:")
actionable = spark.table(f"{catalog_name}.gold.population_frequency_ml_features") \
    .filter(col("is_clinically_actionable_rare_variant")).count()
print(f"  Count: {actionable:,}")

print("\nProcessing complete")
