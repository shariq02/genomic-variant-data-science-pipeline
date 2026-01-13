# Databricks notebook source
# MAGIC %md
# MAGIC #### ENHANCED FEATURE ENGINEERING WITH ENRICHED DATA
# MAGIC ##### Using Parsed Text Data from Genes and Variants
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 13, 2026  
# MAGIC
# MAGIC **Purpose:** Create enriched analytical features using parsed gene functions and variant disease data
# MAGIC
# MAGIC **Input Tables (Silver Layer - ENRICHED):**  
# MAGIC - workspace.silver.genes_enriched (193K genes with functional categories)  
# MAGIC - workspace.silver.variants_enriched (4M+ variants with parsed diseases)
# MAGIC
# MAGIC **Output Tables (Gold Layer - ENRICHED):**  
# MAGIC - workspace.gold.gene_features (with functional categories)
# MAGIC - workspace.gold.chromosome_features
# MAGIC - workspace.gold.gene_disease_association (with disease arrays)
# MAGIC - workspace.gold.ml_features (with gene functions)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    when, round as spark_round, countDistinct, explode, size
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
print("ENHANCED FEATURE ENGINEERING WITH ENRICHED DATA")
print("="*70)
print(f"Catalog: {catalog_name}")
print("Using: silver.genes_enriched + silver.variants_enriched")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Enriched Silver Layer Tables
print("\nReading ENRICHED data...")

df_genes = spark.table(f"{catalog_name}.silver.genes_enriched")
df_variants = spark.table(f"{catalog_name}.silver.variants_enriched")

gene_count = df_genes.count()
variant_count = df_variants.count()

print(f"Loaded {gene_count:,} enriched genes")
print(f"Loaded {variant_count:,} enriched variants")

# COMMAND ----------

# DBTITLE 1,Inspect Enriched Data
print("\nEnriched Data Preview:")

print("\n1. Gene Functional Categories:")
display(
    df_genes.groupBy("primary_function")
            .count()
            .orderBy(col("count").desc())
)

print("\n2. Variants by Quality Tier:")
display(
    df_variants.groupBy("quality_tier")
               .count()
               .orderBy(col("count").desc())
)

print("\n3. Variant Types:")
display(
    df_variants.groupBy("variant_type")
               .count()
               .orderBy(col("count").desc())
               .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Create Enhanced Gene-Level Aggregations
print("\n" + "="*70)
print("CREATING ENHANCED GENE-LEVEL FEATURES")
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
        
        # NEW: Variant type counts
        spark_sum(when(col("is_frameshift"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense"), 1).otherwise(0)).alias("nonsense_count"),
        spark_sum(when(col("is_splice"), 1).otherwise(0)).alias("splice_count"),
        
        # NEW: Origin counts
        spark_sum(when(col("is_germline"), 1).otherwise(0)).alias("germline_count"),
        spark_sum(when(col("is_somatic"), 1).otherwise(0)).alias("somatic_count"),
        
        # NEW: Disease flags
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0)).alias("cancer_variant_count"),
        spark_sum(when(col("has_syndrome"), 1).otherwise(0)).alias("syndrome_variant_count"),
        
        # Quality metrics
        avg(col("review_quality_score")).alias("avg_review_quality"),
        spark_sum(when(col("quality_tier") == "High Quality", 1).otherwise(0)).alias("high_quality_count"),
        
        # Disease and phenotype counts
        countDistinct("disease").alias("disease_count"),
        spark_sum(when(col("has_omim"), 1).otherwise(0)).alias("omim_linked_count"),
        countDistinct("variant_type").alias("variant_type_count"),
        avg(when(col("position").cast("long").isNotNull(), col("position").cast("long"))).alias("avg_position")
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
    .withColumn("frameshift_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("frameshift_count") / col("mutation_count"))
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
                    (col("disease_count") * 2) +
                    (col("frameshift_count") * 0.5),
                    2
                ))
)

feature_count = df_gene_features.count()
print(f"Created features for {feature_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Join Enriched Gene Metadata
df_gene_features = df_gene_features.join(
    df_genes.select(
        "gene_name", 
        "gene_id", 
        "chromosome", 
        "gene_type", 
        "gene_length",
        # NEW ENRICHED FIELDS
        "primary_function",
        "biological_process",
        "cellular_location",
        "is_kinase",
        "is_receptor",
        "is_transcription_factor",
        "is_enzyme",
        "cancer_related",
        "immune_related",
        "neurological_related"
    ),
    on="gene_name",
    how="left"
)

print("Joined with enriched gene metadata")

# COMMAND ----------

# DBTITLE 1,Mutation Density Calculation
df_gene_features = df_gene_features.withColumn("mutation_density",
    when(col("gene_length").isNotNull() & (col("gene_length") > 0),
         spark_round(col("mutation_count") / col("gene_length") * 1000000, 6))
    .otherwise(None)
)

print("Mutation density calculated")

# COMMAND ----------

# DBTITLE 1,Sample Enhanced Gene Features
print("\nSample Enhanced Gene Features (Top 10 cancer-related kinases):")
display(
    df_gene_features.filter(col("is_kinase") & col("cancer_related"))
                    .orderBy(col("mutation_count").desc())
                    .select(
                        "gene_name",
                        "primary_function",
                        "mutation_count",
                        "pathogenic_ratio",
                        "frameshift_count",
                        "risk_level"
                    )
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
        spark_sum(when(col("clinical_significance") == "Uncertain significance", 1).otherwise(0)).alias("vus_count"),
        
        # NEW: Variant types
        spark_sum(when(col("is_frameshift"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense"), 1).otherwise(0)).alias("nonsense_count"),
        spark_sum(when(col("is_splice"), 1).otherwise(0)).alias("splice_count")
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

# DBTITLE 1,Enhanced Gene-Disease Associations
print("\n" + "="*70)
print("CREATING ENHANCED GENE-DISEASE ASSOCIATIONS")
print("="*70)

# Explode disease arrays to get individual diseases
df_variants_exploded = (
    df_variants
    .filter(col("disease_array").isNotNull())
    .filter(size(col("disease_array")) > 0)
    .select(
        "gene_name",
        explode(col("disease_array")).alias("disease"),
        "clinical_significance",
        "has_cancer_disease",
        "has_syndrome",
        "has_hereditary",
        "is_frameshift",
        "is_nonsense",
        "quality_tier",
        "review_quality_score"
    )
    .filter(col("disease").isNotNull())
    .filter(col("disease") != "not provided")
)

df_gene_disease = (
    df_variants_exploded
    .groupBy("gene_name", "disease")
    .agg(
        count("*").alias("mutation_count"),
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        
        # NEW: Disease and variant type flags
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0)).alias("cancer_disease_flag"),
        spark_sum(when(col("has_syndrome"), 1).otherwise(0)).alias("syndrome_flag"),
        spark_sum(when(col("is_frameshift"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense"), 1).otherwise(0)).alias("nonsense_count"),
        avg(col("review_quality_score")).alias("avg_quality")
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
    .filter(col("mutation_count") >= 1)
    .orderBy(col("pathogenic_ratio").desc())
)

assoc_count = df_gene_disease.count()
print(f"Created {assoc_count:,} gene-disease associations")

# COMMAND ----------

# DBTITLE 1,Sample Gene-Disease Associations
print("\nTop Gene-Disease Associations (cancer-related):")
display(
    df_gene_disease.filter(col("cancer_disease_flag") > 0)
                   .orderBy(col("pathogenic_ratio").desc())
                   .select(
                       "gene_name",
                       "disease",
                       "mutation_count",
                       "pathogenic_ratio",
                       "frameshift_count",
                       "association_strength"
                   )
                   .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Save Enhanced Gene Features
print("\n" + "="*70)
print("SAVING TO GOLD LAYER")
print("="*70)

df_gene_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_features")

print(f"Saved: {catalog_name}.gold.gene_features")

# COMMAND ----------

# DBTITLE 1,Save Chromosome Features
df_chromosome_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.chromosome_features")

print(f"Saved: {catalog_name}.gold.chromosome_features")

# COMMAND ----------

# DBTITLE 1,Save Gene-Disease Associations
df_gene_disease.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_disease_association")

print(f"Saved: {catalog_name}.gold.gene_disease_association")

# COMMAND ----------

# DBTITLE 1,Prepare Enhanced ML Features
print("\n" + "="*70)
print("CREATING ENHANCED ML FEATURES")
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
    "risk_level",
    
    # NEW ENRICHED FEATURES
    "frameshift_count",
    "nonsense_count",
    "splice_count",
    "frameshift_ratio",
    "germline_count",
    "somatic_count",
    "cancer_variant_count",
    "syndrome_variant_count",
    "avg_review_quality",
    "high_quality_count",
    "omim_linked_count",
    
    # Gene functional categories
    "primary_function",
    "biological_process",
    "cellular_location",
    "is_kinase",
    "is_receptor",
    "is_transcription_factor",
    "is_enzyme",
    "cancer_related",
    "immune_related",
    "neurological_related"
)

ml_count = df_ml_features.count()
print(f"Created ML features for {ml_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Save ML Features
df_ml_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_features")

print(f"Saved: {catalog_name}.gold.ml_features")

# COMMAND ----------

# DBTITLE 1,Verification & Summary
print("\n" + "="*70)
print("ENHANCED FEATURE ENGINEERING COMPLETE!")
print("="*70)
print("\nGold Layer Tables Created:")
print(f"  gene_features: {feature_count:,} rows")
print(f"  chromosome_features: {chrom_count:,} rows")
print(f"  gene_disease_association: {assoc_count:,} rows")
print(f"  ml_features: {ml_count:,} rows")

print("\n" + "="*70)
print("ENRICHMENT IMPROVEMENTS")
print("="*70)
print("Gene features now include:")
print("  - Functional categories (kinase, receptor, etc.)")
print("  - Variant type counts (frameshift, nonsense, splice)")
print("  - Disease associations (cancer, immune, neurological)")
print("  - Quality metrics (review scores, OMIM links)")
print("\n" + "="*70)
print("READY FOR EXPORT AND ML MODEL TRAINING")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Enhanced Statistics
print("\nEnhanced Statistics:")

print("\n1. Genes by Function and Risk:")
display(
    df_gene_features.groupBy("primary_function", "risk_level")
                    .count()
                    .orderBy("primary_function", col("count").desc())
)

print("\n2. Top 10 Cancer-Related Genes:")
display(
    df_gene_features.filter(col("cancer_related"))
                    .orderBy(col("mutation_count").desc())
                    .select(
                        "gene_name",
                        "primary_function",
                        "mutation_count",
                        "cancer_variant_count",
                        "pathogenic_ratio",
                        "risk_level"
                    )
                    .limit(10)
)

print("\n3. Top 10 Kinases by Pathogenic Ratio:")
display(
    df_gene_features.filter(col("is_kinase"))
                    .orderBy(col("pathogenic_ratio").desc())
                    .select(
                        "gene_name",
                        "mutation_count",
                        "pathogenic_ratio",
                        "frameshift_count",
                        "risk_level"
                    )
                    .limit(10)
)
