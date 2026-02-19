# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - CANCER VARIANT ANALYSIS
# MAGIC ##### Module: Variant and Gene-Level Cancer Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 12: Cancer Variant Classification
# MAGIC
# MAGIC **Input:**
# MAGIC - silver.cancer_mutations
# MAGIC - silver.genes_ultra_enriched
# MAGIC
# MAGIC **Output:** gold.cancer_variant_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    when, lit, trim, upper, lower, coalesce, concat_ws
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD CANCER VARIANT FEATURES")
print("Variant and gene-level cancer feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_cancer = spark.table(f"{catalog_name}.silver.cancer_mutations")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"Cancer mutations: {df_cancer.count():,}")
print(f"Genes: {df_genes.count():,}")

print("\nCancer mutations schema:")
df_cancer.printSchema()

print("\nSample cancer mutations:")
df_cancer.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Create Variant-Level Features
print("\nCREATING VARIANT-LEVEL FEATURES")
print("="*80)

df_variant_cancer = (
    df_cancer
    .withColumn("variant_key",
                concat_ws(":", col("chromosome"), col("position"), 
                         col("reference_allele"), col("alternate_allele")))
    
    .groupBy("gene_symbol", "variant_key", "chromosome", "position", 
             "reference_allele", "alternate_allele")
    .agg(
        count("tumor_sample").alias("sample_count"),
        spark_sum("mutation_count").alias("total_mutation_count"),
        countDistinct("variant_class").alias("variant_class_count"),
        countDistinct("variant_type").alias("variant_type_count"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_sample_count"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("truncating_sample_count"),
        spark_sum(when(col("is_silent"), 1).otherwise(0)).alias("silent_sample_count"),
        spark_sum(when(col("is_snv"), 1).otherwise(0)).alias("snv_sample_count"),
        spark_sum(when(col("is_indel"), 1).otherwise(0)).alias("indel_sample_count"),
        spark_sum(when(col("is_frequently_mutated"), 1).otherwise(0)).alias("frequent_mutation_samples")
    )
)

print(f"Unique cancer variants: {df_variant_cancer.count():,}")
print("\nSample variant features:")
df_variant_cancer.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Variant Classification Flags
print("\nADDING VARIANT CLASSIFICATION FLAGS")
print("="*80)

df_variant_classified = (
    df_variant_cancer
    .withColumn("is_recurrent_mutation",
                when(col("sample_count") >= 3, True).otherwise(False))
    
    .withColumn("is_hotspot_mutation",
                when(col("sample_count") >= 10, True).otherwise(False))
    
    .withColumn("is_high_impact_cancer_variant",
                when((col("truncating_sample_count") > 0) & 
                     (col("sample_count") >= 2), True).otherwise(False))
    
    .withColumn("is_driver_candidate",
                when((col("is_hotspot_mutation")) | 
                     (col("is_high_impact_cancer_variant")), True).otherwise(False))
    
    .withColumn("mutation_frequency_category",
                when(col("sample_count") >= 10, "hotspot")
                .when(col("sample_count") >= 3, "recurrent")
                .when(col("sample_count") >= 2, "multiple")
                .otherwise("rare"))
)

print("Added variant classification")
print("\nMutation frequency distribution:")
df_variant_classified.groupBy("mutation_frequency_category").count().orderBy("mutation_frequency_category").show()

print("\nDriver candidates:")
driver_count = df_variant_classified.filter(col("is_driver_candidate")).count()
print(f"  Count: {driver_count:,}")

# COMMAND ----------

# DBTITLE 1,Calculate Gene-Level Cancer Statistics
print("\nCALCULATING GENE-LEVEL CANCER STATISTICS")
print("="*80)

df_gene_cancer = (
    df_cancer
    .groupBy("gene_symbol")
    .agg(
        count("tumor_sample").alias("total_samples_affected"),
        countDistinct("tumor_sample").alias("unique_samples_affected"),
        countDistinct(concat_ws(":", col("chromosome"), col("position"))).alias("unique_mutation_sites"),
        spark_sum("mutation_count").alias("total_mutations"),
        avg("mutation_count").alias("avg_mutations_per_sample"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_mutations"),
        spark_sum(when(col("is_truncating"), 1).otherwise(0)).alias("truncating_mutations"),
        spark_sum(when(col("is_silent"), 1).otherwise(0)).alias("silent_mutations"),
        spark_sum(when(col("is_frequently_mutated"), 1).otherwise(0)).alias("frequent_mutations")
    )
)

print(f"Genes with cancer mutations: {df_gene_cancer.count():,}")
print("\nGene cancer statistics:")
df_gene_cancer.describe().show()

print("\nTop 10 genes by sample count:")
df_gene_cancer.orderBy(col("unique_samples_affected").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Add Gene-Level Classification
print("\nADDING GENE-LEVEL CLASSIFICATION")
print("="*80)

df_gene_classified = (
    df_gene_cancer
    .withColumn("is_cancer_gene",
                when(col("unique_samples_affected") >= 5, True).otherwise(False))
    
    .withColumn("is_frequently_mutated_gene",
                when(col("unique_mutation_sites") >= 10, True).otherwise(False))
    
    .withColumn("is_tumor_suppressor_candidate",
                when((col("truncating_mutations") > col("missense_mutations")) & 
                     (col("unique_samples_affected") >= 3), True).otherwise(False))
    
    .withColumn("is_oncogene_candidate",
                when((col("missense_mutations") > col("truncating_mutations")) & 
                     (col("unique_samples_affected") >= 5), True).otherwise(False))
    
    .withColumn("gene_cancer_role",
                when(col("is_tumor_suppressor_candidate"), "tumor_suppressor")
                .when(col("is_oncogene_candidate"), "oncogene")
                .when(col("is_cancer_gene"), "cancer_associated")
                .otherwise("other"))
)

print("Added gene classification")
print("\nGene cancer role distribution:")
df_gene_classified.groupBy("gene_cancer_role").count().orderBy("gene_cancer_role").show()

# COMMAND ----------

# DBTITLE 1,Calculate Cancer Scores
print("\nCALCULATING CANCER SCORES")
print("="*80)

df_gene_scored = (
    df_gene_classified
    .withColumn("cancer_mutation_burden_score",
                (col("unique_samples_affected") * 2) +
                (col("unique_mutation_sites") * 1))
    
    .withColumn("functional_impact_score",
                (col("truncating_mutations") * 3) +
                (col("missense_mutations") * 1) -
                (col("silent_mutations") * 0.5))
    
    .withColumn("cancer_priority_score",
                when(col("is_tumor_suppressor_candidate"), 10).otherwise(0) +
                when(col("is_oncogene_candidate"), 10).otherwise(0) +
                (col("unique_samples_affected") * 0.5))
)

print("Added scores")
print("\nScore distribution:")
df_gene_scored.select("cancer_mutation_burden_score", "functional_impact_score", "cancer_priority_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Join Variant and Gene Features
print("\nJOINING VARIANT AND GENE FEATURES")
print("="*80)

df_combined = (
    df_variant_classified
    .withColumn("variant_gene_symbol", upper(trim(col("gene_symbol"))))  
    .drop("gene_symbol")  # Drop original
    .join(
        df_gene_scored.select(
            upper(trim(col("gene_symbol"))).alias("gene_symbol"),
            col("total_samples_affected").alias("gene_total_samples"),
            col("unique_mutation_sites").alias("gene_unique_sites"),
            col("is_cancer_gene"),
            col("is_tumor_suppressor_candidate"),
            col("is_oncogene_candidate"),
            col("gene_cancer_role"),
            col("cancer_mutation_burden_score"),
            col("cancer_priority_score")
        ),
        on=col("variant_gene_symbol") == col("gene_symbol"),
        how="left"
    )
    .withColumn("gene_symbol", col("variant_gene_symbol"))  # Restore column name
    .drop("variant_gene_symbol")
)

print(f"Combined variant-gene features: {df_combined.count():,}")
print("\nSample combined features:")
df_combined.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data
print("\nJOINING WITH GENE MASTER DATA")
print("="*80)

df_with_genes = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome").alias("gene_chromosome"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme")
    )
    .join(
        df_combined.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
        on="gene_symbol",
        how="right"
    )
)

print(f"Final features with gene data: {df_with_genes.count():,}")
print("\nSample with genes:")
df_with_genes.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_with_genes
    .select(
        col("gene_symbol"),
        col("gene_name"),
        col("variant_key"),
        col("chromosome"),
        col("position"),
        col("reference_allele"),
        col("alternate_allele"),
        
        col("sample_count"),
        col("total_mutation_count"),
        coalesce(col("missense_sample_count"), lit(0)).alias("missense_sample_count"),
        coalesce(col("truncating_sample_count"), lit(0)).alias("truncating_sample_count"),
        coalesce(col("silent_sample_count"), lit(0)).alias("silent_sample_count"),
        coalesce(col("snv_sample_count"), lit(0)).alias("snv_sample_count"),
        coalesce(col("indel_sample_count"), lit(0)).alias("indel_sample_count"),
        
        col("is_recurrent_mutation"),
        col("is_hotspot_mutation"),
        col("is_high_impact_cancer_variant"),
        col("is_driver_candidate"),
        col("mutation_frequency_category"),
        
        coalesce(col("gene_total_samples"), lit(0)).alias("gene_total_samples"),
        coalesce(col("gene_unique_sites"), lit(0)).alias("gene_unique_sites"),
        coalesce(col("is_cancer_gene"), lit(False)).alias("is_cancer_gene"),
        coalesce(col("is_tumor_suppressor_candidate"), lit(False)).alias("is_tumor_suppressor_candidate"),
        coalesce(col("is_oncogene_candidate"), lit(False)).alias("is_oncogene_candidate"),
        coalesce(col("gene_cancer_role"), lit("other")).alias("gene_cancer_role"),
        
        coalesce(col("cancer_mutation_burden_score"), lit(0)).alias("cancer_mutation_burden_score"),
        coalesce(col("cancer_priority_score"), lit(0)).alias("cancer_priority_score")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} cancer variants")

print("\nFeature columns:")
for col_name in df_final.columns:
    print(f"  - {col_name}")

print("\nSample final features:")
df_final.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant Key
print("\nDEDUPLICATING BY VARIANT_KEY")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["variant_key"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Cancer Variant Features
print("\nSAVING GOLD CANCER VARIANT FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.cancer_variant_ml_features")

print(f"Saved: {catalog_name}.gold.cancer_variant_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nCANCER VARIANT FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.cancer_variant_ml_features").count()
print(f"\nTable created:")
print(f"  gold.cancer_variant_ml_features: {result_count:,} variants")

print("\nMutation frequency breakdown:")
spark.table(f"{catalog_name}.gold.cancer_variant_ml_features") \
    .groupBy("mutation_frequency_category") \
    .count() \
    .orderBy("mutation_frequency_category") \
    .show()

print("\nGene cancer role breakdown:")
spark.table(f"{catalog_name}.gold.cancer_variant_ml_features") \
    .groupBy("gene_cancer_role") \
    .count() \
    .orderBy("gene_cancer_role") \
    .show()

print("\nDriver candidates:")
driver_final = spark.table(f"{catalog_name}.gold.cancer_variant_ml_features") \
    .filter(col("is_driver_candidate")).count()
print(f"  Driver candidates: {driver_final:,}")

print("\nProcessing complete")
