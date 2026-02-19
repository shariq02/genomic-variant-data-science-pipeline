# Databricks notebook source
# MAGIC %md
# MAGIC #### FEATURE ENGINEERING - DRUG RESPONSE ANALYSIS
# MAGIC ##### Module: Drug Response Variant-Level Features
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Use Case 9: Pharmacogenomic Guidance
# MAGIC - Use Case 11: Treatment Response Prediction
# MAGIC
# MAGIC **Input:**
# MAGIC - silver.pharmgkb_variants
# MAGIC - silver.pharmgkb_relationships
# MAGIC - silver.variant_protein_impact
# MAGIC - silver.variants_ultra_enriched
# MAGIC
# MAGIC **Output:** gold.drug_response_ml_features

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, max as spark_max,
    when, lit, trim, upper, lower, coalesce, split, size, array_contains, row_number
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GOLD DRUG RESPONSE FEATURES")
print("Variant-level drug response feature engineering")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Load Source Tables
print("\nLOADING SOURCE TABLES")
print("="*80)

df_pharmgkb_variants = spark.table(f"{catalog_name}.silver.pharmgkb_variants")
df_relationships = spark.table(f"{catalog_name}.silver.pharmgkb_relationships")
df_variant_impact = spark.table(f"{catalog_name}.silver.variant_protein_impact")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

print(f"PharmGKB variants: {df_pharmgkb_variants.count():,}")
print(f"PharmGKB relationships: {df_relationships.count():,}")
print(f"Variant protein impact: {df_variant_impact.count():,}")
print(f"Variants ultra enriched: {df_variants.count():,}")

print("\nPharmGKB variants schema:")
df_pharmgkb_variants.printSchema()

print("\nSample PharmGKB variants:")
df_pharmgkb_variants.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Extract Variant-Drug Relationships
print("\nEXTRACTING VARIANT-DRUG RELATIONSHIPS")
print("="*80)

df_variant_relationships = (
    df_relationships
    .filter(col("entity1_type") == "Variant")
    .select(
        col("entity1_id").alias("variant_pharmgkb_id"),
        col("entity1_name").alias("variant_name_rel"),
        col("entity2_type").alias("related_entity_type"),
        col("entity2_name").alias("related_entity_name"),
        col("evidence")
    )
)

print(f"Variant relationships: {df_variant_relationships.count():,}")
print("\nRelationship type distribution:")
df_variant_relationships.groupBy("related_entity_type").count().orderBy("related_entity_type").show()

print("\nSample relationships:")
df_variant_relationships.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Calculate Variant Drug Interaction Counts
print("\nCALCULATING VARIANT DRUG INTERACTION COUNTS")
print("="*80)

df_variant_drug_counts = (
    df_variant_relationships
    .groupBy("variant_pharmgkb_id")
    .agg(
        count("*").alias("total_interactions"),
        countDistinct("related_entity_type").alias("interaction_type_count"),
        spark_sum(when(col("related_entity_type") == "Chemical", 1).otherwise(0)).alias("drug_interaction_count"),  
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_interaction_count"),
        spark_sum(when(col("evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count")
    )
)

print(f"Variants with interactions: {df_variant_drug_counts.count():,}")
print("\nTop variants by interaction count:")
df_variant_drug_counts.orderBy(col("total_interactions").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Process PharmGKB Variant Annotations
print("\nPROCESSING PHARMGKB VARIANT ANNOTATIONS")
print("="*80)

# PharmGKB variants already have annotation counts - use them directly
df_pharmgkb_features = (
    df_pharmgkb_variants
    .select(
        col("variant_id").alias("variant_pharmgkb_id"),
        col("variant_name"),
        upper(trim(col("gene_symbols"))).alias("gene_symbol"),
        col("location").alias("variant_location")
    )
    .withColumn("has_annotation", lit(True))
)

print(f"PharmGKB variant features: {df_pharmgkb_features.count():,}")
print("\nSample features:")
df_pharmgkb_features.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Join with Variant Protein Impact
print("\nJOINING WITH VARIANT PROTEIN IMPACT")
print("="*80)

# Prepare variant impact data
df_variant_impact_prep = (
    df_variant_impact
    .select(
        col("variant_id"),
        col("variant_name").alias("clinvar_variant_name"),
        upper(trim(col("gene_name"))).alias("gene_symbol"),
        col("clinical_significance_simple"),
        col("is_pathogenic"),
        col("is_benign"),
        col("is_vus"),
        col("is_missense_variant"),
        col("is_frameshift_variant"),
        col("is_nonsense_variant"),
        col("is_splice_variant"),
        col("has_functional_domain"),
        col("affects_functional_domain"),
        col("phylop_score"),
        col("cadd_phred"),
        col("conservation_level")
    )
)

# RIGHT JOIN to keep all ClinVar variants + add PharmGKB annotations where available
df_with_impact = (
    df_pharmgkb_features
    .join(
        df_variant_impact_prep,
        on="gene_symbol",
        how="right"
    )
)

print(f"Variants with impact (before dedup): {df_with_impact.count():,}")

# Deduplicate by variant_id (ClinVar), keeping best PharmGKB annotation
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("variant_id").orderBy(
    col("variant_pharmgkb_id").desc_nulls_last()
)

df_with_impact = (
    df_with_impact
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print(f"Variants with impact (after dedup): {df_with_impact.count():,}")
print("\nSample with impact:")
df_with_impact.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Drug Response Classification Flags
print("\nADDING DRUG RESPONSE CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_with_impact
    .withColumn("has_pharmgkb_annotation",
                when(col("variant_pharmgkb_id").isNotNull(), True).otherwise(False))
    
    .withColumn("has_high_conservation",
                when(col("conservation_level") >= 1, True).otherwise(False))
    
    .withColumn("affects_drug_metabolism",
                when((col("has_pharmgkb_annotation")) & 
                     (col("has_functional_domain")), True).otherwise(False))
    
    .withColumn("affects_drug_efficacy",
                when((col("has_pharmgkb_annotation")) & 
                     (col("is_missense_variant") | col("affects_functional_domain")), True).otherwise(False))
    
    .withColumn("is_high_impact_variant",
                when((col("is_pathogenic")) & (col("has_pharmgkb_annotation")), True).otherwise(False))
)

print("Added classification flags")
print("\nPharmGKB annotation distribution:")
df_classified.groupBy("has_pharmgkb_annotation").count().show()

print("\nHigh impact distribution:")
df_classified.groupBy("is_high_impact_variant").count().show()

# COMMAND ----------

# DBTITLE 1,Calculate Drug Response Scores
print("\nCALCULATING DRUG RESPONSE SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("pharmacogene_annotation_score",
                when(col("has_pharmgkb_annotation"), 10).otherwise(0))
    
    .withColumn("functional_impact_score",
                when(col("affects_functional_domain"), 5).otherwise(0) +
                when(col("is_missense_variant"), 3).otherwise(0) +
                when(col("is_nonsense_variant"), 5).otherwise(0) +
                when(col("is_frameshift_variant"), 5).otherwise(0) +
                (coalesce(col("conservation_level"), lit(0))))
    
    .withColumn("pathogenicity_score",
                when(col("is_pathogenic"), 10)
                .when(col("is_benign"), -5)
                .when(col("is_vus"), 0)
                .otherwise(0))
    
    .withColumn("drug_response_priority_score",
                col("pharmacogene_annotation_score") * 0.5 +
                col("functional_impact_score") * 0.3 +
                col("pathogenicity_score") * 0.2)
)

print("Added response scores")
print("\nScore distribution:")
df_scored.select("pharmacogene_annotation_score", "functional_impact_score", "pathogenicity_score", "drug_response_priority_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Add Drug Response Priority Classification
print("\nADDING DRUG RESPONSE PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_scored
    .withColumn("drug_response_priority",
                when(col("drug_response_priority_score") >= 15, "high")
                .when(col("drug_response_priority_score") >= 8, "medium")
                .otherwise("low"))
    
    .withColumn("is_actionable_pharmacogene_variant",
                when(col("has_pharmgkb_annotation"), True).otherwise(False))
    
    .withColumn("drug_response_category",
                when(col("affects_drug_metabolism"), "metabolism")
                .when(col("affects_drug_efficacy"), "efficacy")
                .when(col("has_pharmgkb_annotation"), "pharmacogene_variant")
                .otherwise("unknown"))
    
    .withColumn("clinical_actionability",
                when((col("is_pathogenic")) & (col("has_pharmgkb_annotation")), "high_evidence")
                .when(col("has_pharmgkb_annotation"), "pharmgkb_annotated")
                .otherwise("research_only"))
)

print("Added priority classification")
print("\nPriority distribution:")
df_priority.groupBy("drug_response_priority").count().orderBy("drug_response_priority").show()

print("\nActionability distribution:")
df_priority.groupBy("clinical_actionability").count().orderBy("clinical_actionability").show()

print("\nCategory distribution:")
df_priority.groupBy("drug_response_category").count().orderBy("drug_response_category").show()

# COMMAND ----------

# DBTITLE 1,Select Final Features
print("\nSELECTING FINAL FEATURES")
print("="*80)

df_final = (
    df_priority
    .select(
        col("variant_pharmgkb_id"),
        coalesce(col("variant_name"), col("clinvar_variant_name")).alias("variant_name"),
        col("variant_id"),
        col("gene_symbol"),
        col("variant_location"),
        
        col("clinical_significance_simple"),
        col("is_pathogenic"),
        col("is_benign"),
        col("is_vus"),
        
        col("is_missense_variant"),
        col("is_frameshift_variant"),
        col("is_nonsense_variant"),
        col("is_splice_variant"),
        
        col("has_functional_domain"),
        col("affects_functional_domain"),
        
        coalesce(col("phylop_score"), lit(0.0)).alias("phylop_score"),
        coalesce(col("cadd_phred"), lit(0.0)).alias("cadd_phred"),
        coalesce(col("conservation_level"), lit(0)).alias("conservation_level"),
        
        col("has_pharmgkb_annotation"),
        col("has_high_conservation"),
        col("affects_drug_metabolism"),
        col("affects_drug_efficacy"),
        col("is_high_impact_variant"),
        
        col("pharmacogene_annotation_score"),
        col("functional_impact_score"),
        col("pathogenicity_score"),
        col("drug_response_priority_score"),
        
        col("drug_response_priority"),
        col("is_actionable_pharmacogene_variant"),
        col("drug_response_category"),
        col("clinical_actionability")
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

# DBTITLE 1,Validate Feature Quality
print("\nVALIDATING FEATURE QUALITY")
print("="*80)

print("\nNull counts:")
null_counts = df_final.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in ["variant_pharmgkb_id", "gene_symbol", "drug_response_priority"]
])
null_counts.show(vertical=True)

print("\nActionable variants:")
actionable = df_final.filter(col("is_actionable_pharmacogene_variant")).count()
print(f"  Count: {actionable:,}")

print("\nTop 10 variants by priority score:")
df_final.orderBy(col("drug_response_priority_score").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Deduplicate by Variant PharmGKB ID
# DBTITLE 1,Deduplicate by Variant ID
print("\nFINAL DEDUPLICATION BY VARIANT_ID")
print("="*80)

before_count = df_final.count()
df_final = df_final.dropDuplicates(["variant_id"])
after_count = df_final.count()

print(f"Before deduplication: {before_count:,}")
print(f"After deduplication: {after_count:,}")
print(f"Duplicates removed: {before_count - after_count:,}")

# COMMAND ----------

# DBTITLE 1,Save Gold Drug Response Features
print("\nSAVING GOLD DRUG RESPONSE FEATURES")
print("="*80)

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.drug_response_ml_features")

print(f"Saved: {catalog_name}.gold.drug_response_ml_features")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nDRUG RESPONSE FEATURES COMPLETE")
print("="*80)

result_count = spark.table(f"{catalog_name}.gold.drug_response_ml_features").count()
print(f"\nTable created:")
print(f"  gold.drug_response_ml_features: {result_count:,} variants")

print("\nPriority breakdown:")
spark.table(f"{catalog_name}.gold.drug_response_ml_features") \
    .groupBy("drug_response_priority") \
    .count() \
    .orderBy("drug_response_priority") \
    .show()

print("\nActionability breakdown:")
spark.table(f"{catalog_name}.gold.drug_response_ml_features") \
    .groupBy("clinical_actionability") \
    .count() \
    .orderBy("clinical_actionability") \
    .show()

print("\nActionable variants:")
actionable_final = spark.table(f"{catalog_name}.gold.drug_response_ml_features") \
    .filter(col("is_actionable_pharmacogene_variant")).count()
print(f"  Actionable: {actionable_final:,}")

print("\nProcessing complete")
