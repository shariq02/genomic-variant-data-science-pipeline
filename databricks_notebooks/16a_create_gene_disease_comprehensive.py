# Databricks notebook source
# MAGIC %md
# MAGIC ##### Module 16a: Create Comprehensive Gene-Disease Association Table
# MAGIC
# MAGIC This module combines disease associations from multiple sources:
# MAGIC - OMIM (via gene_disease_links): ~5K genes
# MAGIC - ClinVar (via variant_protein_impact): ~28K genes
# MAGIC
# MAGIC **Run this ONCE to create the comprehensive disease table**

# COMMAND ----------

from pyspark.sql.functions import *

catalog_name = "workspace"

# COMMAND ----------

# DBTITLE 1,Load OMIM Disease Associations
print("LOADING DISEASE DATA FROM MULTIPLE SOURCES")
print("="*80)

# Source 1: OMIM disease links (5,086 genes)
df_omim_diseases = (
    spark.table(f"{catalog_name}.silver.gene_disease_links")
    .select(
        col("gene_symbol").alias("gene_name"),
        col("disease_name").alias("omim_disease"),
        col("omim_id")
    )
    .groupBy("gene_name")
    .agg(
        collect_set("omim_disease").alias("omim_diseases_list"),
        countDistinct("omim_disease").alias("omim_disease_count"),
        collect_set("omim_id").alias("omim_ids")
    )
    .withColumn("omim_diseases", concat_ws(" | ", col("omim_diseases_list")))
    .drop("omim_diseases_list")
)

print(f"OMIM diseases: {df_omim_diseases.count():,} genes")

# COMMAND ----------

# DBTITLE 1,Load ClinVar Disease Associations
# Source 2: ClinVar variant data (28,166 genes)
df_clinvar_diseases = (
    spark.table(f"{catalog_name}.silver.variant_protein_impact")
    .filter(
        (col("primary_disease").isNotNull()) &
        (col("primary_disease") != "not provided") &
        (col("primary_disease") != "not specified")
    )
    .select(
        col("gene_name"),
        col("primary_disease").alias("clinvar_disease"),
        col("omim_id").alias("clinvar_omim_id"),
        col("mondo_id")
    )
    .groupBy("gene_name")
    .agg(
        collect_set("clinvar_disease").alias("clinvar_diseases_list"),
        countDistinct("clinvar_disease").alias("clinvar_disease_count"),
        collect_set("clinvar_omim_id").alias("clinvar_omim_ids"),
        collect_set("mondo_id").alias("mondo_ids")
    )
    .withColumn("clinvar_diseases", concat_ws(" | ", col("clinvar_diseases_list")))
    .drop("clinvar_diseases_list")
)

print(f"ClinVar diseases: {df_clinvar_diseases.count():,} genes")

# COMMAND ----------

# DBTITLE 1,Merge and Extract Disease Keywords
df_all_diseases = (
    df_omim_diseases
    .join(df_clinvar_diseases, "gene_name", "full_outer")
    
    # Combine disease text from both sources
    .withColumn("all_disease_text",
                concat_ws(" | ",
                         coalesce(col("omim_diseases"), lit("")),
                         coalesce(col("clinvar_diseases"), lit(""))))
    
    .withColumn("total_disease_count",
                coalesce(col("omim_disease_count"), lit(0)) +
                coalesce(col("clinvar_disease_count"), lit(0)))
    
    # Extract disease keywords
    .withColumn("diseases_lower", lower(col("all_disease_text")))
    
    .withColumn("has_cancer_disease",
                col("diseases_lower").rlike("cancer|carcinoma|tumor|leukemia|lymphoma|melanoma|sarcoma|oncolog|neoplasm|malignancy"))
    
    .withColumn("has_neurological_disease",
                col("diseases_lower").rlike("ataxia|dystonia|neuropathy|encephalopathy|dementia|parkinson|alzheimer|epilepsy|seizure|spastic|cerebral|neural|neurodegenerative|motor neuron"))
    
    .withColumn("has_metabolic_disease",
                col("diseases_lower").rlike("diabetes|glycogen|lipid|metabolic|aciduria|deficiency|storage disease|mitochondrial"))
    
    .withColumn("has_cardiovascular_disease",
                col("diseases_lower").rlike("cardiomyopathy|arrhythmia|hypertension|heart|cardiac|vascular|qt syndrome"))
    
    .withColumn("has_developmental_disease",
                col("diseases_lower").rlike("syndrome|developmental|congenital|dysmorphia|malformation|anomalies|intellectual disability"))
    
    .withColumn("has_immune_disease",
                col("diseases_lower").rlike("immunodeficiency|autoimmune|immune|inflammatory|lupus|arthritis"))
    
    .withColumn("has_muscular_disease",
                col("diseases_lower").rlike("muscular dystrophy|myopathy|myasthenia|skeletal|bone"))
    
    .withColumn("is_hereditary_disease",
                col("diseases_lower").rlike("hereditary|familial|inherited|genetic|autosomal"))
    
    .drop("diseases_lower")
)

print(f"\nTotal genes with disease associations: {df_all_diseases.count():,}")

# COMMAND ----------

# DBTITLE 1,Show Statistics
print("\nDisease category statistics:")
print(f"  Cancer: {df_all_diseases.filter(col('has_cancer_disease') == True).count():,}")
print(f"  Neurological: {df_all_diseases.filter(col('has_neurological_disease') == True).count():,}")
print(f"  Metabolic: {df_all_diseases.filter(col('has_metabolic_disease') == True).count():,}")
print(f"  Cardiovascular: {df_all_diseases.filter(col('has_cardiovascular_disease') == True).count():,}")
print(f"  Developmental: {df_all_diseases.filter(col('has_developmental_disease') == True).count():,}")
print(f"  Immune: {df_all_diseases.filter(col('has_immune_disease') == True).count():,}")
print(f"  Muscular: {df_all_diseases.filter(col('has_muscular_disease') == True).count():,}")
print(f"  Hereditary: {df_all_diseases.filter(col('is_hereditary_disease') == True).count():,}")

# COMMAND ----------

# DBTITLE 1,Save Comprehensive Disease Table
df_all_diseases.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.gene_disease_comprehensive")

print("\nSaved: workspace.silver.gene_disease_comprehensive")
