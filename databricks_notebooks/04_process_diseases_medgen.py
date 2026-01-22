# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, lit, first, initcap, upper
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
print("Spark Initialized!!!")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"

spark.sql(f"USE CATALOG {catalog_name}")

print(f"Catalog: {catalog_name}")
print("Reading MedGen from workspace.default tables")

# COMMAND ----------

# DBTITLE 1,Read Raw MedGen Tables
df_concepts_raw = spark.table(f"{catalog_name}.default.medgen_concepts_raw")
df_relations_raw = spark.table(f"{catalog_name}.default.medgen_relations_raw")

print(f"Loaded MedGen concepts: {df_concepts_raw.count():,}")
print(f"Loaded MedGen relations: {df_relations_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Inspect Schema
print("MedGen Concepts Schema:")
df_concepts_raw.printSchema()

print("\nMedGen Relations Schema:")
df_relations_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Sample Raw MedGen Concepts
df_concepts_raw.select(
    "medgen_id",
    "disease_name",
    "is_preferred",
    "source_code"
).show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 1: Clean Preferred MedGen Diseases
df_diseases_clean = (
    df_concepts_raw
    .withColumn("medgen_id", trim(col("medgen_id")))
    .withColumn("disease_name", initcap(trim(col("disease_name"))))
    .withColumn("source_db", trim(col("source_code")))
    .withColumn("term_type", lit("Disease"))
    
    # NORMALIZE is_preferred (STRING SAFE)
    .withColumn(
        "is_preferred_norm",
        upper(trim(col("is_preferred")))
    )
    
    # Keep preferred concepts only
    .filter(col("is_preferred_norm").isin("Y", "YES", "TRUE", "1"))
    
    # Remove junk rows
    .filter(col("medgen_id").isNotNull())
    .filter(trim(col("medgen_id")) != "")
    .filter(col("disease_name").isNotNull())
    .filter(trim(col("disease_name")) != "")
    
    # Cleanup
    .drop("is_preferred_norm")
)

print(f"Preferred diseases retained: {df_diseases_clean.count():,}")
df_diseases_clean.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: Create Diseases Dimension Table
df_diseases = (
    df_diseases_clean
    .select(
        "medgen_id",
        "disease_name",
        "term_type",
        "source_db"
    )
    .groupBy("medgen_id")
    .agg(
        first("disease_name").alias("disease_name"),
        first("term_type").alias("term_type"),
        first("source_db").alias("source_db")
    )
)

print(f"Final diseases count: {df_diseases.count():,}")
df_diseases.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Silver.Diseases
# DBTITLE 1, Save Diseases to Silver Layer

df_diseases.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.diseases")

print(f"Saved table: {catalog_name}.silver.diseases")

# COMMAND ----------

# DBTITLE 1,STEP 3: Clean MedGen Disease Relationships
df_disease_relations = (
    df_relations_raw
    .withColumn("source_medgen_id", trim(col("source_medgen_id")))
    .withColumn("target_medgen_id", trim(col("target_medgen_id")))
    .withColumn("relationship", lower(trim(col("relationship"))))
    .withColumn("relationship_detail", trim(col("relationship_detail")))
    
    # Valid IDs only
    .filter(col("source_medgen_id").isNotNull())
    .filter(col("target_medgen_id").isNotNull())
    .filter(trim(col("source_medgen_id")) != "")
    .filter(trim(col("target_medgen_id")) != "")
)

print(f"Clean disease relations: {df_disease_relations.count():,}")
df_disease_relations.show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,silver.disease_hierarchy
df_disease_relations.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.disease_hierarchy")

print(f"Saved table: {catalog_name}.silver.disease_hierarchy")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("VALIDATION SUMMARY")
print("=" * 60)

print("Diseases table:")
spark.table(f"{catalog_name}.silver.diseases").select(
    "medgen_id", "disease_name", "source_db"
).show(10, truncate=60)

print("\nDisease hierarchy table:")
spark.table(f"{catalog_name}.silver.disease_hierarchy").show(5, truncate=60)
