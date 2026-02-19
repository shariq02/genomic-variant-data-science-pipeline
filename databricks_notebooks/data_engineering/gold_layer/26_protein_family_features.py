# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PROTEIN FAMILY ANALYSIS
# MAGIC ##### Module: Gene-Level Protein Family Features
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 4: Protein Domain Analysis
# MAGIC - Use Case 7: Protein Family Conservation
# MAGIC 
# MAGIC **Input:**
# MAGIC - silver.protein_domains
# MAGIC - silver.proteins_uniprot
# MAGIC - silver.genes_ultra_enriched
# MAGIC 
# MAGIC **Output:** gold.protein_family_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max,
    when, lit, trim, upper, coalesce
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD PROTEIN FAMILY FEATURES")
print("Gene-level protein family feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_protein_domains = spark.table(f"{catalog_name}.silver.protein_domains")
df_proteins_uniprot = spark.table(f"{catalog_name}.silver.proteins_uniprot")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"Protein domains: {df_protein_domains.count():,}")
print(f"Proteins uniprot: {df_proteins_uniprot.count():,}")
print(f"Genes: {df_genes.count():,}")

print("\nProtein domains schema:")
df_protein_domains.printSchema()

print("\nSample protein domains:")
df_protein_domains.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Calculate Gene Domain Statistics
print("\nCALCULATING GENE DOMAIN STATISTICS")
print("="*80)

df_gene_domains = (
    df_protein_domains
    .groupBy("gene_symbol")
    .agg(
        countDistinct("uniprot_accession").alias("protein_count"),
        spark_max("domain_count").alias("max_domain_count"),
        spark_sum(when(col("has_kinase_domain"), 1).otherwise(0)).alias("proteins_with_kinase"),
        spark_sum(when(col("has_receptor_domain"), 1).otherwise(0)).alias("proteins_with_receptor"),
        spark_sum(when(col("has_zinc_finger"), 1).otherwise(0)).alias("proteins_with_zinc_finger"),
        spark_sum(when(col("has_sh2_domain"), 1).otherwise(0)).alias("proteins_with_sh2"),
        spark_sum(when(col("has_sh3_domain"), 1).otherwise(0)).alias("proteins_with_sh3"),
        spark_sum(when(col("has_ph_domain"), 1).otherwise(0)).alias("proteins_with_ph"),
        spark_sum(when(col("has_death_domain"), 1).otherwise(0)).alias("proteins_with_death"),
        spark_sum(when(col("has_leucine_zipper"), 1).otherwise(0)).alias("proteins_with_leucine_zipper"),
        spark_sum(when(col("has_helix_loop_helix"), 1).otherwise(0)).alias("proteins_with_helix_loop"),
        spark_sum(when(col("has_immunoglobulin"), 1).otherwise(0)).alias("proteins_with_ig"),
        spark_sum(when(col("has_functional_domain"), 1).otherwise(0)).alias("proteins_with_functional_domain")
    )
)

print(f"Genes with domain data: {df_gene_domains.count():,}")
print("\nDomain statistics:")
df_gene_domains.describe().show()

print("\nTop 10 genes by domain count:")
df_gene_domains.orderBy(col("max_domain_count").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Add Domain Classification Flags
print("\nADDING DOMAIN CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_gene_domains
    .withColumn("has_signaling_domain",
                when((col("proteins_with_kinase") > 0) |
                     (col("proteins_with_sh2") > 0) |
                     (col("proteins_with_sh3") > 0), True).otherwise(False))
    
    .withColumn("has_dna_binding_domain",
                when((col("proteins_with_zinc_finger") > 0) |
                     (col("proteins_with_helix_loop") > 0) |
                     (col("proteins_with_leucine_zipper") > 0), True).otherwise(False))
    
    .withColumn("has_membrane_domain",
                when((col("proteins_with_receptor") > 0) |
                     (col("proteins_with_ph") > 0), True).otherwise(False))
    
    .withColumn("has_apoptosis_domain",
                when(col("proteins_with_death") > 0, True).otherwise(False))
    
    .withColumn("has_immune_domain",
                when(col("proteins_with_ig") > 0, True).otherwise(False))
    
    .withColumn("is_multi_domain_protein",
                when(col("max_domain_count") >= 5, True).otherwise(False))
)

print("Added classification flags")
print("\nSignaling domain distribution:")
df_classified.groupBy("has_signaling_domain").count().show()

print("\nDNA binding domain distribution:")
df_classified.groupBy("has_dna_binding_domain").count().show()

# COMMAND ----------

# DBTITLE 1,Calculate Protein Family Scores
print("\nCALCULATING PROTEIN FAMILY SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("domain_diversity_score",
                col("max_domain_count") * 2 +
                when(col("has_signaling_domain"), 3).otherwise(0) +
                when(col("has_dna_binding_domain"), 3).otherwise(0) +
                when(col("has_membrane_domain"), 2).otherwise(0))
    
    .withColumn("functional_complexity_score",
                (col("proteins_with_functional_domain") * 2) +
                when(col("is_multi_domain_protein"), 5).otherwise(0))
    
    .withColumn("druggability_potential_score",
                when(col("proteins_with_kinase") > 0, 10).otherwise(0) +
                when(col("proteins_with_receptor") > 0, 8).otherwise(0) +
                when(col("has_signaling_domain"), 5).otherwise(0))
)

print("Added scores")
print("\nScore distribution:")
df_scored.select("domain_diversity_score", "functional_complexity_score", "druggability_potential_score").describe().show()

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
        col("chromosome"),
        col("protein_family"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("druggability_score")
    )
    .join(
        df_scored.withColumn("gene_symbol", upper(trim(col("gene_symbol")))),
        on="gene_symbol",
        how="left"
    )
)

print(f"Genes with protein family features: {df_with_genes.count():,}")
print("\nSample with genes:")
df_with_genes.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Protein Family Priority Classification
print("\nADDING PROTEIN FAMILY PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_with_genes
    .withColumn("protein_family_priority",
                when(col("druggability_potential_score") >= 15, "high")
                .when(col("druggability_potential_score") >= 8, "medium")
                .otherwise("low"))
    
    .withColumn("is_high_value_protein_family",
                when((col("has_signaling_domain")) & 
                     (col("is_multi_domain_protein")), True).otherwise(False))
    
    .withColumn("protein_functional_category",
                when(col("has_signaling_domain"), "signaling")
                .when(col("has_dna_binding_domain"), "transcription")
                .when(col("has_membrane_domain"), "membrane")
                .when(col("has_immune_domain"), "immune")
                .otherwise("other"))
)

print("Added priority classification")
print("\nPriority distribution:")
df_priority.groupBy("protein_family_priority").count().orderBy("protein_family_priority").show()

print("\nFunctional category distribution:")
df_priority.groupBy("protein_functional_category").count().orderBy("protein_functional_category").show()

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        col("gene_symbol"),
        col("gene_name"),
        col("description"),
        col("chromosome"),
        col("protein_family"),
        
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        
        coalesce(col("protein_count"), lit(0)).alias("protein_count"),
        coalesce(col("max_domain_count"), lit(0)).alias("max_domain_count"),
        coalesce(col("proteins_with_kinase"), lit(0)).alias("proteins_with_kinase"),
        coalesce(col("proteins_with_receptor"), lit(0)).alias("proteins_with_receptor"),
        coalesce(col("proteins_with_zinc_finger"), lit(0)).alias("proteins_with_zinc_finger"),
        coalesce(col("proteins_with_sh2"), lit(0)).alias("proteins_with_sh2"),
        coalesce(col("proteins_with_sh3"), lit(0)).alias("proteins_with_sh3"),
        coalesce(col("proteins_with_ph"), lit(0)).alias("proteins_with_ph"),
        coalesce(col("proteins_with_death"), lit(0)).alias("proteins_with_death"),
        coalesce(col("proteins_with_leucine_zipper"), lit(0)).alias("proteins_with_leucine_zipper"),
        coalesce(col("proteins_with_helix_loop"), lit(0)).alias("proteins_with_helix_loop"),
        coalesce(col("proteins_with_ig"), lit(0)).alias("proteins_with_ig"),
        coalesce(col("proteins_with_functional_domain"), lit(0)).alias("proteins_with_functional_domain"),
        
        coalesce(col("has_signaling_domain"), lit(False)).alias("has_signaling_domain"),
        coalesce(col("has_dna_binding_domain"), lit(False)).alias("has_dna_binding_domain"),
        coalesce(col("has_membrane_domain"), lit(False)).alias("has_membrane_domain"),
        coalesce(col("has_apoptosis_domain"), lit(False)).alias("has_apoptosis_domain"),
        coalesce(col("has_immune_domain"), lit(False)).alias("has_immune_domain"),
        coalesce(col("is_multi_domain_protein"), lit(False)).alias("is_multi_domain_protein"),
        
        coalesce(col("domain_diversity_score"), lit(0)).alias("domain_diversity_score"),
        coalesce(col("functional_complexity_score"), lit(0)).alias("functional_complexity_score"),
        coalesce(col("druggability_potential_score"), lit(0)).alias("druggability_potential_score"),
        coalesce(col("druggability_score"), lit(0.0)).alias("gene_druggability_score"),
        
        coalesce(col("protein_family_priority"), lit("low")).alias("protein_family_priority"),
        coalesce(col("is_high_value_protein_family"), lit(False)).alias("is_high_value_protein_family"),
        coalesce(col("protein_functional_category"), lit("other")).alias("protein_functional_category")
    )
)

final_count = df_final.count()
print(f"Final features: {final_count:,} genes")

print("\nFeature columns:")
for col_name in df_final.columns:
    print(f"  - {col_name}")

print("\nSample final features:")
df_final.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Deduplicate by Gene Symbol
print("\nDEDUPLICATING BY GENE_SYMBOL")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["gene_symbol"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Protein Family Features
print("\nSAVING GOLD PROTEIN FAMILY FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.protein_family_ml_features")

print(f"Saved: {catalog_name}.gold.protein_family_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nPROTEIN FAMILY FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.protein_family_ml_features").count()
print(f"\nTable created:")
print(f"  gold.protein_family_ml_features: {result_count:,} genes")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.protein_family_ml_features") \
    .groupBy("protein_family_priority") \
    .count() \
    .orderBy("protein_family_priority") \
    .show()

print("\nFunctional category breakdown:")
spark.table(f"{catalog_name}.gold.protein_family_ml_features") \
    .groupBy("protein_functional_category") \
    .count() \
    .orderBy("protein_functional_category") \
    .show()

print("\nProcessing complete")
