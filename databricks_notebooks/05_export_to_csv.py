# Databricks notebook source
# MAGIC %md
# MAGIC #### PYSPARK DATA PROCESSING WITH UNITY CATALOG  
# MAGIC ##### Export Gold Layer to CSV  
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 12, 2026  
# MAGIC
# MAGIC **Purpose:** Export Gold layer analytical and ML feature tables from Unity Catalog for external use.
# MAGIC
# MAGIC **Input Tables (Gold Layer):**  
# MAGIC - workspace.gold.gene_features (190K genes)  
# MAGIC - workspace.gold.chromosome_features  
# MAGIC - workspace.gold.gene_disease_association  
# MAGIC - workspace.gold.ml_features  
# MAGIC
# MAGIC **Output:** Manual CSV download from Databricks UI or Volume export

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

print("\n" + "="*70)
print("EXPORT GOLD LAYER TO CSV - ALL DATA")
print("="*70)

# COMMAND ----------

# DBTITLE 1,READ GOLD LAYER TABLES
print("\nReading Gold layer tables...")

df_gene_features = spark.table(f"{catalog_name}.gold.gene_features")
df_chromosome_features = spark.table(f"{catalog_name}.gold.chromosome_features")
df_gene_disease = spark.table(f"{catalog_name}.gold.gene_disease_association")
df_ml_features = spark.table(f"{catalog_name}.gold.ml_features")

gene_feat_count = df_gene_features.count()
chrom_feat_count = df_chromosome_features.count()
gene_disease_count = df_gene_disease.count()
ml_feat_count = df_ml_features.count()

print(f"✓ gene_features: {gene_feat_count:,} rows")
print(f"✓ chromosome_features: {chrom_feat_count:,} rows")
print(f"✓ gene_disease_association: {gene_disease_count:,} rows")
print(f"✓ ml_features: {ml_feat_count:,} rows")

# COMMAND ----------

# DBTITLE 1,SAMPLE DATA PREVIEW
print("\n" + "="*70)
print("DATA PREVIEW")
print("="*70)

print("\nGene Features (Top 10):")
display(df_gene_features.orderBy(col("mutation_count").desc()).limit(10))

print("\nChromosome Features:")
display(df_chromosome_features.orderBy("chromosome"))

print("\nGene-Disease Associations (Top 10):")
display(df_gene_disease.orderBy(col("pathogenic_ratio").desc()).limit(10))

print("\nML Features (Top 10):")
display(df_ml_features.orderBy(col("mutation_count").desc()).limit(10))

# COMMAND ----------

# DBTITLE 1,METHOD 1: EXPORT TO UNITY CATALOG VOLUME (RECOMMENDED)
print("\n" + "="*70)
print("METHOD 1: EXPORT TO UNITY CATALOG VOLUME (RECOMMENDED)")
print("="*70)

# Create volume for exports
volume_name = "gold_exports"
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.gold.{volume_name}
""")

volume_path = f"/Volumes/{catalog_name}/gold/{volume_name}/"
print(f"Volume created: {volume_path}")

# Export each table
print("\nExporting tables to volume...")

tables_to_export = {
    "gene_features": df_gene_features,
    "chromosome_features": df_chromosome_features,
    "gene_disease_association": df_gene_disease,
    "ml_features": df_ml_features
}

for table_name, df in tables_to_export.items():
    output_path = f"{volume_path}{table_name}.csv"
    
    print(f"\nExporting {table_name}...")
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(output_path)
    
    print(f"✓ Exported: {output_path}")

print("\n" + "="*70)
print("EXPORT COMPLETE!")
print("="*70)
print("\nNext steps:")
print("1. Go to: Catalog → workspace → gold → gold_exports")
print("2. Download each CSV file")
print("3. Save to: genomic-variant-data-science-pipeline/data/processed/")

# COMMAND ----------

# DBTITLE 1,METHOD 2: DISPLAY TABLES FOR MANUAL DOWNLOAD
print("\n" + "="*70)
print("METHOD 2: DISPLAY AND DOWNLOAD")
print("="*70)

print("\nFor each table below:")
print("1. Click the download icon (⬇) in the table")
print("2. Select 'Download as CSV'")
print("3. Save to your local data/processed/ folder")
print("="*70)

for table_name, df in tables_to_export.items():
    print(f"\n\nTABLE: {table_name}")
    print("-"*70)
    display(df)

# COMMAND ----------

# DBTITLE 1,EXPORT VERIFICATION SUMMARY
print("\n" + "="*70)
print("EXPORT VERIFICATION SUMMARY")
print("="*70)

summary_data = []
for table_name, df in tables_to_export.items():
    row_count = df.count()
    col_count = len(df.columns)
    summary_data.append({
        "table": table_name,
        "rows": row_count,
        "columns": col_count,
        "estimated_size_mb": round((row_count * col_count * 50) / (1024 * 1024), 2)  # Rough estimate
    })

summary_df = spark.createDataFrame(summary_data)
display(summary_df)

print("\nTotal data to export:")
total_rows = sum([d["rows"] for d in summary_data])
print(f"  Total rows: {total_rows:,}")
print(f"  Total tables: {len(summary_data)}")

# COMMAND ----------

# DBTITLE 1,DATA QUALITY CHECK
print("\n" + "="*70)
print("DATA QUALITY CHECK")
print("="*70)

print("\n1. Gene Features - Risk Level Distribution:")
display(
    df_gene_features.groupBy("risk_level")
                    .count()
                    .orderBy(col("count").desc())
)

print("\n2. Gene Features - Genes with High Pathogenic Ratio:")
high_risk = df_gene_features.filter(col("pathogenic_ratio") >= 0.7).count()
print(f"   High-risk genes (pathogenic_ratio >= 0.7): {high_risk:,}")

print("\n3. Gene-Disease Associations - Association Strength:")
display(
    df_gene_disease.groupBy("association_strength")
                   .count()
                   .orderBy(col("count").desc())
)

print("\n4. ML Features - Data Completeness:")
ml_complete = df_ml_features.filter(
    col("mutation_count").isNotNull() &
    col("pathogenic_ratio").isNotNull() &
    col("chromosome").isNotNull()
).count()
print(f"   Complete ML features: {ml_complete:,} / {ml_feat_count:,} ({(ml_complete/ml_feat_count*100):.1f}%)")

# COMMAND ----------

# DBTITLE 1,NEXT STEPS INSTRUCTIONS
print("\n" + "="*70)
print("NEXT STEPS")
print("="*70)

print("\n1. Download all 4 CSV files using Method 1 or 2 above")
print("\n2. Save files to: genomic-variant-data-science-pipeline/data/processed/")
print("   Expected files:")
print("   - gene_features.csv")
print("   - chromosome_features.csv")
print("   - gene_disease_association.csv")
print("   - ml_features.csv")

print("\n3. Load to PostgreSQL:")
print("   python scripts/transformation/load_gold_to_postgres.py")

print("\n4. Verify in PostgreSQL:")
print("   SELECT COUNT(*) FROM gold.gene_features;")
print("   SELECT COUNT(*) FROM gold.chromosome_features;")
print("   SELECT COUNT(*) FROM gold.gene_disease_association;")
print("   SELECT COUNT(*) FROM gold.ml_features;")

print("\n5. Run Statistical Analysis:")
print("   jupyter notebook jupyter_notebooks/01_statistical_analysis.ipynb")

print("\n6. Run ML Model Training:")
print("   jupyter notebook jupyter_notebooks/02_ml_model_training.ipynb")

print("\n" + "="*70)
print("READY FOR LOCAL ANALYSIS AND ML!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,OPTIONAL: CHECK FILE SIZES IN VOLUME
try:
    files = dbutils.fs.ls(volume_path)
    print("\nExported files in volume:")
    print("-"*70)
    for file in files:
        if file.path.endswith('.csv') or file.path.endswith('/'):
            size_mb = file.size / (1024 * 1024) if hasattr(file, 'size') else 0
            print(f"{file.name:40s} {size_mb:10.2f} MB")
except Exception as e:
    print(f"Could not list files: {e}")
