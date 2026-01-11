# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Export Gold Layer to CSV  
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** 11 January 2026  
# MAGIC
# MAGIC **Purpose:**  Export Gold layer analytical and ML feature tables from Unity Catalog for external use.
# MAGIC
# MAGIC **Input Tables (Gold Layer):**  
# MAGIC - workspace.gold.gene_features  
# MAGIC - workspace.gold.chromosome_features  
# MAGIC - workspace.gold.gene_disease_association  
# MAGIC - workspace.gold.ml_features  
# MAGIC
# MAGIC **Output:**  Manual CSV download from Databricks UI
# MAGIC

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,INITIALIZE SPARKSESSION
spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,CONFIGURATION
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# DBTITLE 1,READ GOLD LAYER TABLES
print("\nReading Gold layer tables...")

df_gene_features = spark.table(f"{catalog_name}.gold.gene_features")
df_chromosome_features = spark.table(f"{catalog_name}.gold.chromosome_features")
df_gene_disease = spark.table(f"{catalog_name}.gold.gene_disease_association")
df_ml_features = spark.table(f"{catalog_name}.gold.ml_features")

print(f"gene_features: {df_gene_features.count():,} rows")
print(f"chromosome_features: {df_chromosome_features.count():,} rows")
print(f"gene_disease_association: {df_gene_disease.count():,} rows")
print(f"ml_features: {df_ml_features.count():,} rows")

# COMMAND ----------

# DBTITLE 1,DISPLAY TABLES FOR CSV DOWNLOAD
print("\n" + "="*40)
print("EXPORT METHOD: DISPLAY AND DOWNLOAD")
print("="*40)

tables = {
    "gene_features": df_gene_features,
    "chromosome_features": df_chromosome_features,
    "gene_disease_association": df_gene_disease,
    "ml_features": df_ml_features
}

print("\nDISPLAYING TABLES FOR DOWNLOAD")
print("For each table below:")
print("1. Click the download icon in the table")
print("2. Select 'Download as CSV'")
print("3. Save to your local data/processed/ folder")
print("="*70)

for table_name, df in tables.items():
    print(f"\n\nTABLE: {table_name}")
    print("-"*40)
    display(df)


# COMMAND ----------

# DBTITLE 1,ALTERNATIVE SQL EXPORT METHOD
# print("\n" + "="*40)
# print("ALTERNATIVE METHOD: SQL QUERIES")
# print("="*40)

# print("\n-- Query 1: Gene Features")
# print(f"SELECT * FROM {catalog_name}.gold.gene_features;")

# print("\n-- Query 2: Chromosome Features")
# print(f"SELECT * FROM {catalog_name}.gold.chromosome_features;")

# print("\n-- Query 3: Gene-Disease Association")
# print(f"SELECT * FROM {catalog_name}.gold.gene_disease_association;")

# print("\n-- Query 4: ML Features")
# print(f"SELECT * FROM {catalog_name}.gold.ml_features;")

# COMMAND ----------

# DBTITLE 1,EXPORT VERIFICATION SUMMARY
print("EXPORT SUMMARY \n")

summary_data = []
for table_name, df in tables.items():
    row_count = df.count()
    col_count = len(df.columns)
    summary_data.append({
        "table": table_name,
        "rows": row_count,
        "columns": col_count
    })

summary_df = spark.createDataFrame(summary_data)
display(summary_df)


# COMMAND ----------

# DBTITLE 1,NEXT STEPS
print("\n" + "="*40)
print("NEXT STEPS")
print("="*40)

print("\n1. Download all 4 CSV files using method above")
print("2. Save files to: genomic-variant-data-science-pipeline/data/processed/")
print("3. Verify file names:")
print("   - gene_features.csv")
print("   - chromosome_features.csv")
print("   - gene_disease_association.csv")
print("   - ml_features.csv")

print("\n4. Run locally:")
print("   python scripts/transformation/load_gold_to_postgres.py")

print("\n5. Verify in PostgreSQL:")
print("   SELECT COUNT(*) FROM gold.gene_features;")
print("   SELECT COUNT(*) FROM gold.chromosome_features;")
print("   SELECT COUNT(*) FROM gold.gene_disease_association;")
print("   SELECT COUNT(*) FROM gold.ml_features;")

