# Databricks notebook source
# MAGIC %md
# MAGIC ## DROP OLD VIEWS AND TABLES - DATABRICKS CLEANUP
# MAGIC ### Prepare for Enhanced Pipeline Execution
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026
# MAGIC
# MAGIC **PURPOSE:** Clean up old tables and views in Databricks before running enhanced pipeline
# MAGIC
# MAGIC **RUN THIS FIRST!** Before any enhanced processing scripts
# MAGIC
# MAGIC **Execution Order:**
# MAGIC ```
# MAGIC 0. ▶️ 00_drop_old_views_databricks.py (THIS SCRIPT - RUN FIRST!)
# MAGIC    00_drop_old_views_postgres.sql (Run in PostgreSQL)
# MAGIC 1.    02_gene_data_processing_ENHANCED.py
# MAGIC 2.    03_variant_data_processing_ENHANCED.py
# MAGIC 3.    06_create_gene_alias_mapper_COMPLETE.py
# MAGIC 4.    04_feature_engineering.py
# MAGIC 5.    01_statistical_analysis.ipynb
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("="*80)
print("DROP OLD VIEWS AND TABLES - DATABRICKS CLEANUP")
print("="*80)
print(f"Spark version: {spark.version}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print(f"\n📁 Catalog: {catalog_name}")
print(f"🗑️  Purpose: Drop old tables and views to prevent conflicts")
print(f"⚠️  Warning: This will delete existing data!")
print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,STEP 1: Drop Old Silver Layer Tables
print("\nSTEP 1: DROPPING OLD SILVER LAYER TABLES")
print("="*80)

silver_tables = [
    "genes_ultra_enriched",
    "variants_ultra_enriched",
    "genes_enriched",
    "variants_enriched",
    "genes_processed",
    "variants_processed",
    "genes",
    "variants"
]

dropped_silver = []
skipped_silver = []

for table in silver_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.silver.{table}")
        dropped_silver.append(table)
        print(f"✅ Dropped silver.{table}")
    except Exception as e:
        skipped_silver.append(table)
        print(f"⚠️  Skipped silver.{table} (may not exist)")

print(f"\n📊 Silver tables dropped: {len(dropped_silver)}")
print(f"📊 Silver tables skipped: {len(skipped_silver)}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Drop Old Gold Layer Tables
print("\nSTEP 2: DROPPING OLD GOLD LAYER TABLES")
print("="*80)

gold_tables = [
    "gene_features",
    "chromosome_features",
    "gene_disease_association",
    "ml_features",
    "variant_features",
    "clinical_features",
    "gene_annotations"
]

dropped_gold = []
skipped_gold = []

for table in gold_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.gold.{table}")
        dropped_gold.append(table)
        print(f"✅ Dropped gold.{table}")
    except Exception as e:
        skipped_gold.append(table)
        print(f"⚠️  Skipped gold.{table} (may not exist)")

print(f"\n📊 Gold tables dropped: {len(dropped_gold)}")
print(f"📊 Gold tables skipped: {len(skipped_gold)}")

# COMMAND ----------

# DBTITLE 1,STEP 3: Drop Old Bronze Layer Tables
print("\nSTEP 3: DROPPING OLD BRONZE LAYER TABLES")
print("="*80)

bronze_tables = [
    "variants_raw",
    "genes_raw"
]

dropped_bronze = []
skipped_bronze = []

for table in bronze_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.bronze.{table}")
        dropped_bronze.append(table)
        print(f"✅ Dropped bronze.{table}")
    except Exception as e:
        skipped_bronze.append(table)
        print(f"⚠️  Skipped bronze.{table} (may not exist)")

print(f"\n📊 Bronze tables dropped: {len(dropped_bronze)}")
print(f"📊 Bronze tables skipped: {len(skipped_bronze)}")

# COMMAND ----------

# DBTITLE 1,STEP 4: Drop Old Reference/Lookup Tables
print("\nSTEP 4: DROPPING OLD REFERENCE/LOOKUP TABLES")
print("="*80)

reference_tables = [
    "omim_disease_lookup",
    "orphanet_disease_lookup",
    "mondo_disease_lookup",
    "gene_alias_lookup",
    "gene_designation_lookup",
    "gene_universal_search",
    "gene_xrefs",
    "disease_ontology"
]

dropped_reference = []
skipped_reference = []

for table in reference_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.reference.{table}")
        dropped_reference.append(table)
        print(f"✅ Dropped reference.{table}")
    except Exception as e:
        skipped_reference.append(table)
        print(f"⚠️  Skipped reference.{table} (may not exist)")

print(f"\n📊 Reference tables dropped: {len(dropped_reference)}")
print(f"📊 Reference tables skipped: {len(skipped_reference)}")

# COMMAND ----------

# DBTITLE 1,STEP 5: Drop Old Views
print("\nSTEP 5: DROPPING OLD VIEWS")
print("="*80)

views = [
    "silver.v_genes",
    "silver.v_variants",
    "silver.v_gene_variants",
    "gold.v_high_risk_genes",
    "gold.v_top_genes_by_mutations",
    "gold.v_druggable_targets",
    "gold.v_cancer_kinases",
    "gold.v_functional_gene_summary",
    "gold.v_chromosome_risk_profile",
    "gold.v_gene_disease_matrix",
    "gold.v_disease_complexity",
    "gold.v_ml_features_summary",
    "gold.v_therapeutic_targets",
    "reference.gene_search_view"
]

dropped_views = []
skipped_views = []

for view in views:
    try:
        spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{view}")
        dropped_views.append(view)
        print(f"✅ Dropped {view}")
    except Exception as e:
        skipped_views.append(view)
        print(f"⚠️  Skipped {view} (may not exist)")

print(f"\n📊 Views dropped: {len(dropped_views)}")
print(f"📊 Views skipped: {len(skipped_views)}")

# COMMAND ----------

# DBTITLE 1,STEP 6: Verify Cleanup
print("\nSTEP 6: VERIFYING CLEANUP")
print("="*80)

# Check remaining tables in each schema
schemas = ['bronze', 'silver', 'gold', 'reference']

for schema in schemas:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}").collect()
        print(f"\n📁 {schema.upper()} schema:")
        if len(tables) == 0:
            print(f"   ✅ Clean (0 tables/views)")
        else:
            print(f"   ⚠️  {len(tables)} tables/views remaining:")
            for table in tables:
                print(f"      - {table.tableName} ({table.isTemporary})")
    except Exception as e:
        print(f"\n📁 {schema.upper()} schema:")
        print(f"   ⚠️  Schema may not exist or is empty")

# COMMAND ----------

# DBTITLE 1,STEP 7: List All Remaining Objects
print("\nSTEP 7: LISTING ALL REMAINING OBJECTS")
print("="*80)

all_remaining = []

for schema in schemas:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}").collect()
        for table in tables:
            all_remaining.append({
                'schema': schema,
                'name': table.tableName,
                'type': 'VIEW' if table.isTemporary else 'TABLE'
            })
    except:
        pass

if len(all_remaining) == 0:
    print("\n✅ All schemas are clean! No remaining tables or views.")
else:
    print(f"\n⚠️  Found {len(all_remaining)} remaining objects:")
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql import Row
    
    schema = StructType([
        StructField("schema", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True)
    ])
    
    remaining_df = spark.createDataFrame([Row(**item) for item in all_remaining], schema)
    remaining_df.show(100, truncate=False)

# COMMAND ----------

# DBTITLE 1,STEP 8: Summary Report
print("\nSTEP 8: CLEANUP SUMMARY")
print("="*80)

total_dropped = len(dropped_bronze) + len(dropped_silver) + len(dropped_gold) + len(dropped_reference) + len(dropped_views)
total_skipped = len(skipped_bronze) + len(skipped_silver) + len(skipped_gold) + len(skipped_reference) + len(skipped_views)

print("\n📊 CLEANUP STATISTICS:")
print(f"   Bronze tables dropped:    {len(dropped_bronze)}")
print(f"   Silver tables dropped:    {len(dropped_silver)}")
print(f"   Gold tables dropped:      {len(dropped_gold)}")
print(f"   Reference tables dropped: {len(dropped_reference)}")
print(f"   Views dropped:            {len(dropped_views)}")
print(f"   {'─'*40}")
print(f"   Total objects dropped:    {total_dropped}")
print(f"   Total objects skipped:    {total_skipped}")

print("\n✅ DATABRICKS CLEANUP COMPLETE!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Next Steps
print("\n📋 NEXT STEPS:")
print("="*80)
print("\n1. ✅ Databricks cleanup complete")
print("\n2. 🔄 Run PostgreSQL cleanup:")
print("   - Execute 00_drop_old_views_postgres.sql in your PostgreSQL database")
print("\n3. ▶️ Run enhanced processing pipeline in order:")
print("   Step 1: 02_gene_data_processing_ENHANCED.py")
print("   Step 2: 03_variant_data_processing_ENHANCED.py")
print("   Step 3: 06_create_gene_alias_mapper_COMPLETE.py ⭐")
print("   Step 4: 04_feature_engineering.py")
print("   Step 5: 01_statistical_analysis.ipynb")
print("\n4. 📤 Load to PostgreSQL:")
print("   - Use load_gold_to_postgres.py")
print("\n5. 📊 Create views in PostgreSQL:")
print("   - Run 03_create_views.sql (updated version)")

print("\n" + "="*80)
print("🎉 READY TO START ENHANCED PIPELINE!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Optional: Vacuum Delta Tables
print("\n⚙️  OPTIONAL: VACUUM DELTA TABLES")
print("="*80)
print("Uncomment the code below to vacuum delta tables and free up storage")
print("WARNING: This is permanent and cannot be undone!")

# Uncomment to run vacuum
# for schema in schemas:
#     try:
#         tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}").collect()
#         for table in tables:
#             if not table.isTemporary:
#                 try:
#                     spark.sql(f"VACUUM {catalog_name}.{schema}.{table.tableName} RETAIN 0 HOURS")
#                     print(f"✅ Vacuumed {schema}.{table.tableName}")
#                 except:
#                     print(f"⚠️  Could not vacuum {schema}.{table.tableName}")
#     except:
#         pass

print("\n✅ Cleanup script complete!")
