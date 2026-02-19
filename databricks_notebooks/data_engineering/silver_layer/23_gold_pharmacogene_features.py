# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - PHARMACOGENE ANALYSIS
# MAGIC ##### Module: Pharmacogene Gene-Level Features
# MAGIC 
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC 
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 14: Drug Target Identification
# MAGIC 
# MAGIC **Input:**
# MAGIC - silver.pharmgkb_genes
# MAGIC - silver.pharmgkb_relationships
# MAGIC - silver.genes_ultra_enriched
# MAGIC 
# MAGIC **Output:** gold.pharmacogene_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max,
    when, lit, trim, upper, lower, coalesce, array_contains, split, size
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD PHARMACOGENE FEATURES")
print("Gene-level pharmacogene feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_pharmgkb_genes = spark.table(f"{catalog_name}.silver.pharmgkb_genes")
df_relationships = spark.table(f"{catalog_name}.silver.pharmgkb_relationships")
df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

print(f"PharmGKB genes: {df_pharmgkb_genes.count():,}")
print(f"PharmGKB relationships: {df_relationships.count():,}")
print(f"Genes ultra enriched: {df_genes.count():,}")

print("\nPharmGKB genes schema:")
df_pharmgkb_genes.printSchema()

print("\nSample PharmGKB genes:")
df_pharmgkb_genes.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Extract Gene Relationships
print("\nEXTRACTING GENE RELATIONSHIPS")
print("="*80)

df_gene_relationships = (
    df_relationships
    .filter(col("Entity1_type") == "Gene")
    .select(
        col("Entity1_name").alias("gene_symbol"),
        col("Entity2_type").alias("related_entity_type"),
        col("Evidence"),
        col("Association"),
        col("PK"),
        col("PD")
    )
)

print(f"Gene relationships: {df_gene_relationships.count():,}")
print("\nSample relationships:")
df_gene_relationships.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Calculate Relationship Counts
print("\nCALCULATING RELATIONSHIP COUNTS")
print("="*80)

df_relationship_counts = (
    df_gene_relationships
    .groupBy("gene_symbol")
    .agg(
        count("*").alias("total_relationships"),
        countDistinct("related_entity_type").alias("entity_type_count"),
        spark_sum(when(col("related_entity_type") == "Drug", 1).otherwise(0)).alias("drug_relationships"),
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_relationships"),
        spark_sum(when(col("related_entity_type") == "Variant", 1).otherwise(0)).alias("variant_relationships"),
        spark_sum(when(col("Evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count"),
        spark_sum(when(col("PK") == "yes", 1).otherwise(0)).alias("pk_relationships"),
        spark_sum(when(col("PD") == "yes", 1).otherwise(0)).alias("pd_relationships")
    )
)

print(f"Genes with relationships: {df_relationship_counts.count():,}")
print("\nTop genes by relationship count:")
df_relationship_counts.orderBy(col("total_relationships").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Join PharmGKB Gene Data
print("\nJOINING PHARMGKB GENE DATA")
print("="*80)

df_pharmgkb_features = (
    df_pharmgkb_genes
    .select(
        upper(trim(col("Symbol"))).alias("gene_symbol"),
        col("Name").alias("gene_name"),
        col("Is VIP").alias("is_vip"),
        col("Has Variant Annotation").alias("has_variant_annotation"),
        col("Has CPIC Dosing Guideline").alias("has_cpic_guideline"),
        col("Chromosome").alias("chromosome")
    )
    .join(
        df_relationship_counts,
        on="gene_symbol",
        how="left"
    )
)

print(f"PharmGKB features: {df_pharmgkb_features.count():,}")
print("\nSample features:")
df_pharmgkb_features.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Join with Gene Master Data
print("\nJOINING WITH GENE MASTER DATA")
print("="*80)

df_gene_pharmacogene = (
    df_genes
    .select(
        upper(trim(col("official_symbol"))).alias("gene_symbol"),
        col("gene_name").alias("gene_full_name"),
        col("description"),
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transporter"),
        col("is_metabolic")
    )
    .join(
        df_pharmgkb_features,
        on="gene_symbol",
        how="inner"
    )
)

print(f"Gene pharmacogene joined: {df_gene_pharmacogene.count():,}")
print("\nSample joined data:")
df_gene_pharmacogene.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Pharmacogene Classification Flags
print("\nADDING PHARMACOGENE CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_gene_pharmacogene
    .withColumn("is_vip_gene",
                when(col("is_vip") == "yes", True).otherwise(False))
    
    .withColumn("has_clinical_annotation",
                when(col("has_variant_annotation") == "yes", True).otherwise(False))
    
    .withColumn("has_dosing_guideline",
                when(col("has_cpic_guideline") == "yes", True).otherwise(False))
    
    .withColumn("is_drug_metabolizer",
                when(col("is_metabolic") & (col("drug_relationships") > 0), True).otherwise(False))
    
    .withColumn("is_drug_transporter_gene",
                when(col("is_transporter") & (col("drug_relationships") > 0), True).otherwise(False))
    
    .withColumn("is_drug_target_gene",
                when((col("is_kinase") | col("is_receptor") | col("is_enzyme")) & 
                     (col("drug_relationships") > 0), True).otherwise(False))
)

print("Added classification flags")
print("\nVIP gene distribution:")
df_classified.groupBy("is_vip_gene").count().show()

print("\nDrug metabolizer distribution:")
df_classified.groupBy("is_drug_metabolizer").count().show()

# COMMAND ----------

# DBTITLE 1,Calculate Evidence Scores
print("\nCALCULATING EVIDENCE SCORES")
print("="*80)

df_evidence = (
    df_classified
    .withColumn("pharmacogene_evidence_score",
                coalesce(col("evidence_count"), lit(0)) +
                when(col("is_vip_gene"), 5).otherwise(0) +
                when(col("has_clinical_annotation"), 3).otherwise(0) +
                when(col("has_dosing_guideline"), 4).otherwise(0))
    
    .withColumn("drug_interaction_score",
                coalesce(col("drug_relationships"), lit(0)) * 2 +
                coalesce(col("pk_relationships"), lit(0)) * 3 +
                coalesce(col("pd_relationships"), lit(0)) * 3)
    
    .withColumn("clinical_utility_score",
                when(col("has_dosing_guideline"), 10).otherwise(0) +
                when(col("is_vip_gene"), 8).otherwise(0) +
                when(col("has_clinical_annotation"), 5).otherwise(0) +
                (coalesce(col("drug_relationships"), lit(0)) * 0.5))
)

print("Added evidence scores")
print("\nEvidence score distribution:")
df_evidence.select("pharmacogene_evidence_score", "drug_interaction_score", "clinical_utility_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Add Pharmacogene Priority Classification
print("\nADDING PHARMACOGENE PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_evidence
    .withColumn("pharmacogene_priority",
                when(col("clinical_utility_score") >= 15, "high")
                .when(col("clinical_utility_score") >= 8, "medium")
                .otherwise("low"))
    
    .withColumn("is_high_priority_pharmacogene",
                when(col("pharmacogene_priority") == "high", True).otherwise(False))
    
    .withColumn("pharmacogene_category",
                when(col("is_drug_metabolizer"), "metabolizer")
                .when(col("is_drug_transporter_gene"), "transporter")
                .when(col("is_drug_target_gene"), "target")
                .otherwise("other"))
)

print("Added priority classification")
print("\nPriority distribution:")
df_priority.groupBy("pharmacogene_priority").count().orderBy("pharmacogene_priority").show()

print("\nCategory distribution:")
df_priority.groupBy("pharmacogene_category").count().orderBy("pharmacogene_category").show()

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        col("gene_symbol"),
        col("gene_name"),
        col("gene_full_name"),
        col("description"),
        col("chromosome"),
        
        col("is_vip_gene"),
        col("has_clinical_annotation"),
        col("has_dosing_guideline"),
        col("is_drug_metabolizer"),
        col("is_drug_transporter_gene"),
        col("is_drug_target_gene"),
        
        col("is_kinase"),
        col("is_receptor"),
        col("is_enzyme"),
        col("is_transporter"),
        col("is_metabolic"),
        
        coalesce(col("total_relationships"), lit(0)).alias("total_relationships"),
        coalesce(col("entity_type_count"), lit(0)).alias("entity_type_count"),
        coalesce(col("drug_relationships"), lit(0)).alias("drug_relationships"),
        coalesce(col("disease_relationships"), lit(0)).alias("disease_relationships"),
        coalesce(col("variant_relationships"), lit(0)).alias("variant_relationships"),
        coalesce(col("evidence_count"), lit(0)).alias("evidence_count"),
        coalesce(col("pk_relationships"), lit(0)).alias("pk_relationships"),
        coalesce(col("pd_relationships"), lit(0)).alias("pd_relationships"),
        
        col("pharmacogene_evidence_score"),
        col("drug_interaction_score"),
        col("clinical_utility_score"),
        
        col("pharmacogene_priority"),
        col("is_high_priority_pharmacogene"),
        col("pharmacogene_category")
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

# DBTITLE 1,Validate Feature Quality
print("\nVALIDATING FEATURE QUALITY")
print("="*80)

print("\nNull counts:")
df_final.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_final.columns
]).show(vertical=True)

print("\nHigh priority pharmacogenes:")
high_priority = df_final.filter(col("is_high_priority_pharmacogene")).count()
print(f"  Count: {high_priority:,}")

print("\nTop 10 genes by clinical utility:")
df_final.orderBy(col("clinical_utility_score").desc()).show(10, truncate=False)

print("\nTop 10 genes by drug relationships:")
df_final.orderBy(col("drug_relationships").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Save Gold Pharmacogene Features
print("\nSAVING GOLD PHARMACOGENE FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.pharmacogene_ml_features")

print(f"Saved: {catalog_name}.gold.pharmacogene_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nPHARMACOGENE FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features").count()
print(f"\nTable created:")
print(f"  gold.pharmacogene_ml_features: {result_count:,} genes")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.pharmacogene_ml_features") \
    .groupBy("pharmacogene_priority") \
    .count() \
    .orderBy("pharmacogene_priority") \
    .show()

print("\nCategory breakdown:")
spark.table(f"{catalog_name}.gold.pharmacogene_ml_features") \
    .groupBy("pharmacogene_category") \
    .count() \
    .orderBy("pharmacogene_category") \
    .show()

print("\nVIP genes:")
vip_count = spark.table(f"{catalog_name}.gold.pharmacogene_ml_features") \
    .filter(col("is_vip_gene")).count()
print(f"  VIP genes: {vip_count:,}")

print("\nProcessing complete")
