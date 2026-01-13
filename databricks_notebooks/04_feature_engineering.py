# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Feature Engineering  
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC
# MAGIC **Purpose:** Create analytical and machine-learning features from cleaned gene and variant data (ALL variants - 4M+).
# MAGIC
# MAGIC **Input Tables (Silver Layer):**  
# MAGIC - `workspace.silver.genes_clean` (190K genes)  
# MAGIC - `workspace.silver.variants_clean` (4M+ variants)
# MAGIC
# MAGIC **Output Tables (Gold Layer):**  
# MAGIC - `workspace.gold.gene_features`  
# MAGIC - `workspace.gold.chromosome_features`  
# MAGIC - `workspace.gold.gene_disease_association`  
# MAGIC - `workspace.gold.ml_features`

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    when, round as spark_round, countDistinct
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("\n" + "="*70)
print("FEATURE ENGINEERING - ALL DATA (190K GENES, 4M+ VARIANTS)")
print("="*70)
print(f"Catalog: {catalog_name}")
print("Creating ML features from Silver layer")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Silver Layer Tables
print("\nReading cleaned data...")

df_genes = spark.table(f"{catalog_name}.silver.genes_clean")
df_variants = spark.table(f"{catalog_name}.silver.variants_clean")

gene_count = df_genes.count()
variant_count = df_variants.count()

print(f"Loaded {gene_count:,} genes")
print(f"Loaded {variant_count:,} variants")

# COMMAND ----------

# DBTITLE 1,Inspect Data Distribution
print("\nData Distribution:")

print("\n1. Variants by Clinical Significance:")
display(
    df_variants.groupBy("clinical_significance")
               .count()
               .orderBy(col("count").desc())
)

print("\n2. Variants by Quality Tier:")
display(
    df_variants.groupBy("quality_tier")
               .count()
               .orderBy(col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Create Gene-Level Aggregations
print("\n" + "="*70)
print("CREATING GENE-LEVEL FEATURES")
print("="*70)

df_gene_features = (
    df_variants
    .groupBy("gene_name")
    .agg(
        count("*").alias("mutation_count"),
        
        # Pathogenic variants
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        
        # Benign variants
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        
        # VUS and Conflicting
        spark_sum(when(col("clinical_significance") == "Uncertain significance", 1).otherwise(0)).alias("vus_count"),
        spark_sum(when(col("clinical_significance") == "Conflicting interpretations", 1).otherwise(0)).alias("conflicting_count"),
        
        # Other metrics
        countDistinct("disease").alias("disease_count"),
        countDistinct("variant_type").alias("variant_type_count"),
        avg(when(col("position").isNotNull(), col("position"))).alias("avg_position")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_pathogenic") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("benign_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_benign") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("vus_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("vus_count") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("risk_level",
                when(col("pathogenic_ratio") >= 0.7, "High")
                .when(col("pathogenic_ratio") >= 0.3, "Medium")
                .otherwise("Low"))
    .withColumn("risk_score",
                spark_round(
                    (col("pathogenic_ratio") * 50) +
                    (col("mutation_count") / 100) +
                    (col("disease_count") * 2),
                    2
                ))
)

feature_count = df_gene_features.count()
print(f"Created features for {feature_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Join Gene Metadata
df_gene_features = df_gene_features.join(
    df_genes.select("gene_name", "gene_id", "chromosome", "gene_type", "gene_length"),
    on="gene_name",
    how="left"
)

print("Joined with gene metadata")

# COMMAND ----------

# DBTITLE 1,Mutation Density Calculation
df_gene_features = df_gene_features.withColumn("mutation_density",
    when(col("gene_length").isNotNull() & (col("gene_length") > 0),
         spark_round(col("mutation_count") / col("gene_length") * 1000000, 6))
    .otherwise(None)
)

print("Mutation density calculated")

# COMMAND ----------

# DBTITLE 1,Sample Gene Features
print("\nSample Gene Features (Top 10 by mutation count):")
display(
    df_gene_features.orderBy(col("mutation_count").desc())
                    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Chromosome-Level Features
print("\n" + "="*70)
print("CREATING CHROMOSOME-LEVEL FEATURES")
print("="*70)

df_chromosome_features = (
    df_variants
    .filter(col("chromosome").isNotNull())
    .groupBy("chromosome")
    .agg(
        countDistinct("gene_name").alias("gene_count"),
        count("*").alias("variant_count"),
        
        # Pathogenic
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        
        # Benign
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        
        # VUS
        spark_sum(when(col("clinical_significance") == "Uncertain significance", 1).otherwise(0)).alias("vus_count")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_percentage",
                spark_round(
                    when(col("variant_count") > 0,
                         (col("total_pathogenic") / col("variant_count")) * 100)
                    .otherwise(0),
                    2
                ))
    .withColumn("avg_mutations_per_gene",
                spark_round(
                    when(col("gene_count") > 0,
                         col("variant_count") / col("gene_count"))
                    .otherwise(0),
                    2
                ))
    .orderBy("chromosome")
)

chrom_count = df_chromosome_features.count()
print(f"Created chromosome features for {chrom_count} chromosomes")

# COMMAND ----------

# DBTITLE 1,Sample Chromosome Features
print("\nChromosome Features:")
display(df_chromosome_features)

# COMMAND ----------

# DBTITLE 1,Gene-Disease Associations
print("\n" + "="*70)
print("CREATING GENE-DISEASE ASSOCIATIONS")
print("="*70)

df_gene_disease = (
    df_variants
    .filter(col("disease").isNotNull())
    .filter(~col("disease").contains("associated disorder"))  # Filter out generic enriched diseases
    .groupBy("gene_name", "disease")
    .agg(
        count("*").alias("mutation_count"),
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_pathogenic") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("association_strength",
                when(col("pathogenic_ratio") >= 0.8, "Strong")
                .when(col("pathogenic_ratio") >= 0.5, "Moderate")
                .otherwise("Weak"))
    .filter(col("mutation_count") >= 3)  # Only associations with 3+ variants
    .orderBy(col("pathogenic_ratio").desc())
)

assoc_count = df_gene_disease.count()
print(f"Created {assoc_count:,} gene-disease associations")

# COMMAND ----------

# DBTITLE 1,Sample Gene-Disease Associations
print("\nTop Gene-Disease Associations (by pathogenic ratio):")
display(
    df_gene_disease.orderBy(col("pathogenic_ratio").desc())
                   .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Save Gene Features
print("\n" + "="*70)
print("SAVING TO GOLD LAYER")
print("="*70)

df_gene_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_features")

print(f"✓ Saved: {catalog_name}.gold.gene_features")

# COMMAND ----------

# DBTITLE 1,Save Chromosome Features
df_chromosome_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.chromosome_features")

print(f"✓ Saved: {catalog_name}.gold.chromosome_features")

# COMMAND ----------

# DBTITLE 1,Save Gene-Disease Associations
df_gene_disease.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_disease_association")

print(f"✓ Saved: {catalog_name}.gold.gene_disease_association")

# COMMAND ----------

# DBTITLE 1,Prepare ML Features
print("\n" + "="*70)
print("CREATING ML FEATURES")
print("="*70)

df_ml_features = df_gene_features.select(
    "gene_name", 
    "chromosome", 
    "mutation_count", 
    "pathogenic_count",
    "likely_pathogenic_count",
    "total_pathogenic",
    "benign_count",
    "likely_benign_count",
    "total_benign",
    "vus_count",
    "pathogenic_ratio",
    "benign_ratio",
    "vus_ratio",
    "disease_count",
    "variant_type_count", 
    "mutation_density", 
    "risk_level"
)

ml_count = df_ml_features.count()
print(f"Created ML features for {ml_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Save ML Features
df_ml_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_features")

print(f"✓ Saved: {catalog_name}.gold.ml_features")

# COMMAND ----------

# DBTITLE 1,Verification & Summary
print("\n" + "="*70)
print("FEATURE ENGINEERING COMPLETE!")
print("="*70)
print("\nGold Layer Tables Created:")
print(f"  ✓ gene_features: {feature_count:,} rows")
print(f"  ✓ chromosome_features: {chrom_count:,} rows")
print(f"  ✓ gene_disease_association: {assoc_count:,} rows")
print(f"  ✓ ml_features: {ml_count:,} rows")

print("\n" + "="*70)
print("READY FOR ML MODEL TRAINING")
print("="*70)
print("Next steps:")
print("  1. Export gold tables to PostgreSQL")
print("  2. Run ML model training (05_ml_model_training.py)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Quick Stats Check
print("\nQuick Statistics:")

print("\n1. Genes by Risk Level:")
display(
    df_gene_features.groupBy("risk_level")
                    .count()
                    .orderBy(col("count").desc())
)

print("\n2. Top 10 Genes by Pathogenic Ratio:")
display(
    df_gene_features.orderBy(col("pathogenic_ratio").desc())
                    .select("gene_name", "mutation_count", "pathogenic_ratio", "risk_level")
                    .limit(10)
)

print("\n3. Top 10 Genes by Mutation Count:")
display(
    df_gene_features.orderBy(col("mutation_count").desc())
                    .select("gene_name", "mutation_count", "pathogenic_ratio", "risk_level")
                    .limit(10)
)
