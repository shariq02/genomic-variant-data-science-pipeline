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
    when, lit, trim, upper, lower, coalesce, split, size, array_contains
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
        col("entity1_name").alias("variant_name"),
        col("entity2_type").alias("related_entity_type"),
        col("entity2_name").alias("related_entity_name"),
        col("evidence"),
        col("pk"),
        col("pd")
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
        spark_sum(when(col("related_entity_type") == "Drug", 1).otherwise(0)).alias("drug_interaction_count"),
        spark_sum(when(col("related_entity_type") == "Disease", 1).otherwise(0)).alias("disease_interaction_count"),
        spark_sum(when(col("evidence").isNotNull(), 1).otherwise(0)).alias("evidence_count"),
        spark_sum(when(col("pk") == "yes", 1).otherwise(0)).alias("pk_interaction_count"),
        spark_sum(when(col("pd") == "yes", 1).otherwise(0)).alias("pd_interaction_count")
    )
)

print(f"Variants with interactions: {df_variant_drug_counts.count():,}")
print("\nTop variants by interaction count:")
df_variant_drug_counts.orderBy(col("total_interactions").desc()).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Process PharmGKB Variant Annotations
print("\nPROCESSING PHARMGKB VARIANT ANNOTATIONS")
print("="*80)

df_pharmgkb_features = (
    df_pharmgkb_variants
    .select(
        col("`Variant ID`").alias("variant_pharmgkb_id"),
        col("`Variant Name`").alias("variant_name"),
        upper(trim(col("`Gene Symbols`"))).alias("gene_symbol"),
        col("Location").alias("variant_location"),
        coalesce(col("`Variant Annotation count`"), lit(0)).alias("variant_annotation_count"),
        coalesce(col("`Clinical Annotation count`"), lit(0)).alias("clinical_annotation_count"),
        coalesce(col("`Level 1/2 Clinical Annotation count`"), lit(0)).alias("high_level_annotation_count"),
        coalesce(col("`Guideline Annotation count`"), lit(0)).alias("guideline_annotation_count"),
        coalesce(col("`Label Annotation count`"), lit(0)).alias("label_annotation_count")
    )
    .join(
        df_variant_drug_counts,
        on="variant_pharmgkb_id",
        how="left"
    )
)

print(f"PharmGKB variant features: {df_pharmgkb_features.count():,}")
print("\nSample features:")
df_pharmgkb_features.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Join with Variant Protein Impact
print("\nJOINING WITH VARIANT PROTEIN IMPACT")
print("="*80)

df_with_impact = (
    df_pharmgkb_features
    .join(
        df_variant_impact.select(
            col("variant_id"),
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
        ),
        on="gene_symbol",
        how="left"
    )
)

print(f"Variants with impact: {df_with_impact.count():,}")
print("\nSample with impact:")
df_with_impact.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Add Drug Response Classification Flags
print("\nADDING DRUG RESPONSE CLASSIFICATION FLAGS")
print("="*80)

df_classified = (
    df_with_impact
    .withColumn("has_clinical_annotation",
                when(col("clinical_annotation_count") > 0, True).otherwise(False))
    
    .withColumn("has_high_level_evidence",
                when(col("high_level_annotation_count") > 0, True).otherwise(False))
    
    .withColumn("has_guideline",
                when(col("guideline_annotation_count") > 0, True).otherwise(False))
    
    .withColumn("has_fda_label",
                when(col("label_annotation_count") > 0, True).otherwise(False))
    
    .withColumn("has_drug_interaction",
                when(col("drug_interaction_count") > 0, True).otherwise(False))
    
    .withColumn("is_pharmacokinetic",
                when(col("pk_interaction_count") > 0, True).otherwise(False))
    
    .withColumn("is_pharmacodynamic",
                when(col("pd_interaction_count") > 0, True).otherwise(False))
    
    .withColumn("affects_drug_metabolism",
                when((col("is_pharmacokinetic")) & 
                     (col("has_functional_domain")), True).otherwise(False))
    
    .withColumn("affects_drug_efficacy",
                when((col("is_pharmacodynamic")) & 
                     (col("is_missense_variant") | col("affects_functional_domain")), True).otherwise(False))
)

print("Added classification flags")
print("\nClinical annotation distribution:")
df_classified.groupBy("has_clinical_annotation").count().show()

print("\nHigh level evidence distribution:")
df_classified.groupBy("has_high_level_evidence").count().show()

# COMMAND ----------

# DBTITLE 1,Calculate Drug Response Scores
print("\nCALCULATING DRUG RESPONSE SCORES")
print("="*80)

df_scored = (
    df_classified
    .withColumn("clinical_evidence_score",
                coalesce(col("clinical_annotation_count"), lit(0)) +
                (coalesce(col("high_level_annotation_count"), lit(0)) * 3) +
                (coalesce(col("guideline_annotation_count"), lit(0)) * 4) +
                (coalesce(col("label_annotation_count"), lit(0)) * 2))
    
    .withColumn("drug_interaction_score",
                (coalesce(col("drug_interaction_count"), lit(0)) * 2) +
                (coalesce(col("pk_interaction_count"), lit(0)) * 3) +
                (coalesce(col("pd_interaction_count"), lit(0)) * 3))
    
    .withColumn("functional_impact_score",
                when(col("affects_functional_domain"), 5).otherwise(0) +
                when(col("is_missense_variant"), 3).otherwise(0) +
                when(col("is_nonsense_variant"), 5).otherwise(0) +
                when(col("is_frameshift_variant"), 5).otherwise(0) +
                (coalesce(col("conservation_level"), lit(0))))
    
    .withColumn("drug_response_priority_score",
                col("clinical_evidence_score") * 0.4 +
                col("drug_interaction_score") * 0.3 +
                col("functional_impact_score") * 0.3)
)

print("Added response scores")
print("\nScore distribution:")
df_scored.select("clinical_evidence_score", "drug_interaction_score", "functional_impact_score", "drug_response_priority_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Add Drug Response Priority Classification
print("\nADDING DRUG RESPONSE PRIORITY CLASSIFICATION")
print("="*80)

df_priority = (
    df_scored
    .withColumn("drug_response_priority",
                when(col("drug_response_priority_score") >= 20, "high")
                .when(col("drug_response_priority_score") >= 10, "medium")
                .otherwise("low"))
    
    .withColumn("is_actionable_pharmacogene_variant",
                when((col("has_guideline")) | 
                     (col("has_fda_label")) |
                     (col("has_high_level_evidence")), True).otherwise(False))
    
    .withColumn("drug_response_category",
                when(col("affects_drug_metabolism"), "metabolism")
                .when(col("affects_drug_efficacy"), "efficacy")
                .when(col("has_drug_interaction"), "interaction")
                .otherwise("unknown"))
    
    .withColumn("clinical_actionability",
                when(col("has_guideline"), "guideline_available")
                .when(col("has_fda_label"), "fda_label")
                .when(col("has_high_level_evidence"), "high_evidence")
                .when(col("has_clinical_annotation"), "clinical_annotation")
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
        col("variant_name"),
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
        
        coalesce(col("variant_annotation_count"), lit(0)).alias("variant_annotation_count"),
        coalesce(col("clinical_annotation_count"), lit(0)).alias("clinical_annotation_count"),
        coalesce(col("high_level_annotation_count"), lit(0)).alias("high_level_annotation_count"),
        coalesce(col("guideline_annotation_count"), lit(0)).alias("guideline_annotation_count"),
        coalesce(col("label_annotation_count"), lit(0)).alias("label_annotation_count"),
        
        coalesce(col("total_interactions"), lit(0)).alias("total_interactions"),
        coalesce(col("interaction_type_count"), lit(0)).alias("interaction_type_count"),
        coalesce(col("drug_interaction_count"), lit(0)).alias("drug_interaction_count"),
        coalesce(col("disease_interaction_count"), lit(0)).alias("disease_interaction_count"),
        coalesce(col("evidence_count"), lit(0)).alias("evidence_count"),
        coalesce(col("pk_interaction_count"), lit(0)).alias("pk_interaction_count"),
        coalesce(col("pd_interaction_count"), lit(0)).alias("pd_interaction_count"),
        
        col("has_clinical_annotation"),
        col("has_high_level_evidence"),
        col("has_guideline"),
        col("has_fda_label"),
        col("has_drug_interaction"),
        col("is_pharmacokinetic"),
        col("is_pharmacodynamic"),
        col("affects_drug_metabolism"),
        col("affects_drug_efficacy"),
        
        col("clinical_evidence_score"),
        col("drug_interaction_score"),
        col("functional_impact_score"),
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

print("\nTop 10 variants by clinical evidence:")
df_final.orderBy(col("clinical_evidence_score").desc()).show(10, truncate=False)

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
