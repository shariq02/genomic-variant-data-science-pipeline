# Databricks notebook source

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace,
    first, lit
)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("MEDGEN DISEASE PROCESSING → SILVER")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Read MedGen Concepts
df_concepts_raw = spark.table(f"{catalog_name}.default.medgen_concepts")

print(f"Loaded MedGen concepts: {df_concepts_raw.count():,}")

df_concepts_raw.select(
    "medgen_id", "concept_name", "semantic_type", "source"
).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 1: CLEAN & NORMALIZE DISEASE CONCEPTS
df_diseases = (
    df_concepts_raw
    .withColumn("medgen_id", trim(col("medgen_id")))
    .withColumn("disease_name", trim(col("concept_name")))
    .withColumn("term_type", trim(col("semantic_type")))
    .withColumn("source_db", trim(col("source")))
    
    .filter(col("medgen_id").isNotNull())
    .filter(col("disease_name").isNotNull())
    .filter(trim(col("medgen_id")) != "")
    .filter(trim(col("disease_name")) != "")
)

# Deduplicate MedGen IDs
df_diseases = (
    df_diseases
    .groupBy("medgen_id")
    .agg(
        first("disease_name").alias("disease_name"),
        first("term_type").alias("term_type"),
        first("source_db").alias("source_db")
    )
)

print(f"Normalized diseases: {df_diseases.count():,}")

# COMMAND ----------

# DBTITLE 1,Save diseases → Silver
df_diseases.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.diseases")

print(f"Saved: {catalog_name}.silver.diseases")

# COMMAND ----------

# DBTITLE 1,Read MedGen Relations
df_relations_raw = spark.table(f"{catalog_name}.default.medgen_relations")

print(f"Loaded MedGen relations: {df_relations_raw.count():,}")

df_relations_raw.select(
    "source_medgen_id", "target_medgen_id", "relationship_type"
).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: NORMALIZE DISEASE HIERARCHY
df_hierarchy = (
    df_relations_raw
    .withColumn("source_medgen_id", trim(col("source_medgen_id")))
    .withColumn("target_medgen_id", trim(col("target_medgen_id")))
    .withColumn("relationship", upper(trim(col("relationship_type"))))
    
    .withColumn(
        "relationship_detail",
        when(lower(col("relationship_type")).contains("parent"), "IS_A")
        .when(lower(col("relationship_type")).contains("broader"), "BROADER_THAN")
        .when(lower(col("relationship_type")).contains("narrower"), "NARROWER_THAN")
        .otherwise("RELATED_TO")
    )
    
    .filter(col("source_medgen_id").isNotNull())
    .filter(col("target_medgen_id").isNotNull())
    .filter(trim(col("source_medgen_id")) != "")
    .filter(trim(col("target_medgen_id")) != "")
)

# Remove self-loops
df_hierarchy = df_hierarchy.filter(
    col("source_medgen_id") != col("target_medgen_id")
)

print(f"Normalized disease relationships: {df_hierarchy.count():,}")

# COMMAND ----------

# DBTITLE 1,Save hierarchy → Silver
df_hierarchy.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.disease_hierarchy")

print(f"Saved: {catalog_name}.silver.disease_hierarchy")

# COMMAND ----------

# DBTITLE 1,Validation
print("VALIDATION SUMMARY")
print("=" * 70)

print("Disease term types:")
spark.table(f"{catalog_name}.silver.diseases") \
    .groupBy("term_type").count().orderBy(col("count").desc()).show()

print("Relationship types:")
spark.table(f"{catalog_name}.silver.disease_hierarchy") \
    .groupBy("relationship_detail").count().show()

# COMMAND ----------

print("MEDGEN SILVER PROCESSING COMPLETE")
print("=" * 70)
print("Tables created:")
print(f" - {catalog_name}.silver.diseases")
print(f" - {catalog_name}.silver.disease_hierarchy")
