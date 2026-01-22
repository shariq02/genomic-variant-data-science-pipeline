# COMMAND ----------
# DBTITLE 1, Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace,
    lit, coalesce, first
)

# COMMAND ----------
# DBTITLE 1, Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("PROCESS MEDGEN DISEASE DATA (CSV → SILVER)")
print("=" * 80)

# COMMAND ----------
# DBTITLE 1, Configuration
catalog_name = "workspace"

RAW_BASE_PATH = "/mnt/data/raw/medgen"

MEDGEN_CONCEPTS_CSV = f"{RAW_BASE_PATH}/medgen_concepts_raw.csv"
MEDGEN_RELATIONS_CSV = f"{RAW_BASE_PATH}/medgen_relations_raw.csv"

spark.sql(f"USE CATALOG {catalog_name}")

print(f"Catalog: {catalog_name}")
print("Reading MedGen CSVs directly (no bronze layer)")

# COMMAND ----------
# DBTITLE 1, Read MedGen Concepts (RAW CSV)
df_concepts_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(MEDGEN_CONCEPTS_CSV)
)

print(f"Loaded MedGen concepts: {df_concepts_raw.count():,}")

df_concepts_raw.select(
    "medgen_id", "preferred_name", "definition", "semantic_type"
).show(5, truncate=50)

# COMMAND ----------
# DBTITLE 1, Read MedGen Relations (RAW CSV)
df_relations_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(MEDGEN_RELATIONS_CSV)
)

print(f"Loaded MedGen relations: {df_relations_raw.count():,}")

df_relations_raw.select(
    "source_medgen_id", "target_medgen_id", "relationship_type"
).show(5)

# COMMAND ----------
# DBTITLE 1, STEP 1: CLEAN & NORMALIZE MEDGEN CONCEPTS
print("STEP 1: CLEAN MEDGEN CONCEPTS")
print("=" * 80)

df_diseases_clean = (
    df_concepts_raw
    .withColumn("medgen_id", trim(col("medgen_id")))
    .withColumn("disease_name", initcap(trim(col("preferred_name"))))
    .withColumn("term_type", trim(col("semantic_type")))
    .withColumn("definition", trim(col("definition")))
    
    # Normalize source DB
    .withColumn(
        "source_db",
        when(lower(col("medgen_id")).startswith("c"), lit("MedGen"))
        .otherwise(lit("Unknown"))
    )
    
    .filter(col("medgen_id").isNotNull())
    .filter(trim(col("medgen_id")) != "")
)

print(f"Clean diseases: {df_diseases_clean.count():,}")

# COMMAND ----------
# DBTITLE 1, STEP 2: CREATE DISEASE LOOKUP (PRIMARY TABLE)
print("STEP 2: CREATE DISEASE LOOKUP TABLE")
print("=" * 80)

df_diseases = (
    df_diseases_clean
    .select(
        "medgen_id",
        "disease_name",
        col("term_type"),
        "source_db"
    )
    .groupBy("medgen_id")
    .agg(
        first("disease_name").alias("disease_name"),
        first("term_type").alias("term_type"),
        first("source_db").alias("source_db")
    )
)

print(f"Diseases table rows: {df_diseases.count():,}")

df_diseases.show(10, truncate=50)

# COMMAND ----------
# DBTITLE 1, SAVE: SILVER.DISEASES
print("SAVING TO SILVER: diseases")
print("=" * 80)

df_diseases.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.diseases")

print(f"Saved: {catalog_name}.silver.diseases")

# COMMAND ----------
# DBTITLE 1, STEP 3: CLEAN MEDGEN RELATIONS (HIERARCHY)
print("STEP 3: CLEAN MEDGEN RELATIONS")
print("=" * 80)

df_disease_hierarchy = (
    df_relations_raw
    .withColumn("source_medgen_id", trim(col("source_medgen_id")))
    .withColumn("target_medgen_id", trim(col("target_medgen_id")))
    .withColumn("relationship", trim(col("relationship_type")))
    .withColumn(
        "relationship_detail",
        when(col("relationship_detail").isNotNull(),
             trim(col("relationship_detail")))
        .otherwise(col("relationship"))
    )
    
    .filter(col("source_medgen_id").isNotNull())
    .filter(col("target_medgen_id").isNotNull())
)

print(f"Hierarchy rows: {df_disease_hierarchy.count():,}")

df_disease_hierarchy.show(10)

# COMMAND ----------
# DBTITLE 1, SAVE: SILVER.DISEASE_HIERARCHY
print("SAVING TO SILVER: disease_hierarchy")
print("=" * 80)

df_disease_hierarchy.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.disease_hierarchy")

print(f"Saved: {catalog_name}.silver.disease_hierarchy")

# COMMAND ----------
# DBTITLE 1, STEP 4: OPTIONAL – CREATE REFERENCE VIEW (LOOKUP USE)
print("STEP 4: CREATE REFERENCE DISEASE LOOKUP")
print("=" * 80)

df_diseases.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.medgen_disease_lookup")

print(f"Saved: {catalog_name}.reference.medgen_disease_lookup")

# COMMAND ----------
# DBTITLE 1, FINAL SUMMARY
print("\nMEDGEN PROCESSING COMPLETE")
print("=" * 80)

print("Tables created:")
print(f"  ✔ {catalog_name}.silver.diseases")
print(f"  ✔ {catalog_name}.silver.disease_hierarchy")
print(f"  ✔ {catalog_name}.reference.medgen_disease_lookup")

print("\nUsage:")
print("  - Join ClinVar variants on medgen_id")
print("  - Traverse disease_hierarchy for parent/child disease analysis")
print("  - Use diseases as canonical disease dimension")

print("\nNEXT STEPS:")
print("  1. Join gene_disease_ncbi.csv → diseases")
print("  2. Enrich variants_ultra_enriched with disease hierarchy depth")
print("  3. Build GOLD: gene → disease → variant cube")
