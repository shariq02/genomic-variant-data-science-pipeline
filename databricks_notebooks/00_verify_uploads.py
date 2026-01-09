# Databricks notebook source
# MAGIC %md
# MAGIC ####  DNA Gene Mapping Project  
# MAGIC ##### Verify Uploaded Data in Databricks
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** 2 January 2026  
# MAGIC
# MAGIC This notebook verifies whether the uploaded **genes** and **variants** datasets  
# MAGIC are correctly available in Databricks tables and performs basic validation checks.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

# COMMAND ----------

print("=" * 40)
print("VERIFY UPLOADED DATA IN DATABRICKS")
print("=" * 40)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.appName("VerifyUploads").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC We will independently verify:
# MAGIC 1. `default.gene_metadata`
# MAGIC 2. `default.clinvar_pathogenic`
# MAGIC
# MAGIC Each section will:
# MAGIC - Read the table
# MAGIC - Count rows
# MAGIC - Print schema
# MAGIC - Show sample records
# MAGIC - Run basic aggregations

# COMMAND ----------

# DBTITLE 1,Initialize Success Flags
genes_success = False
variants_success = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Verify Genes Table  
# MAGIC Table: `default.gene_metadata`

# COMMAND ----------

# DBTITLE 1,Genes Table Validation
try:
    print("\n1. Checking genes table...")
    print("   Reading: default.gene_metadata")

    df_genes = spark.table("default.gene_metadata")

    gene_count = df_genes.count()

    print("   SUCCESS: Genes table found and readable!")
    print(f"   Total genes: {gene_count}")
    print(f"   Columns: {len(df_genes.columns)}")

    print("\n   Schema:")
    df_genes.printSchema()

    print("\n   Sample data:")
    display(df_genes.limit(5))

    print("\n   Genes by chromosome:")
    display(
    df_genes.groupBy("chromosome")
            .count()
            .orderBy("chromosome")
)
    genes_success = True

except Exception as e:
    print("   ERROR: Could not read genes table!")
    print(f"   Reason: {e}")
    print("\n   Troubleshooting:")
    print("   - Table not created yet?")
    print("   - Try: Catalog > default > Tables")
    print("   - Or upload via: Create > Table")


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Verify Variants Table  
# MAGIC Table: `default.clinvar_pathogenic`

# COMMAND ----------

# DBTITLE 1,Variants Table Validation
try:
    print("\n2. Checking variants table...")
    print("   Reading: default.clinvar_pathogenic")

    df_variants = spark.table("default.clinvar_pathogenic")

    variant_count = df_variants.count()

    print("   SUCCESS: Variants table found and readable!")
    print(f"   Total variants: {variant_count}")
    print(f"   Columns: {len(df_variants.columns)}")

    print("\n   Schema:")
    df_variants.printSchema()

    print("\n   Sample data:")
    df_variants.show(5, truncate=False)

    print("\n   Variants by gene (top 10):")
    df_variants.groupBy("gene_name") \
        .count() \
        .orderBy(col("count").desc()) \
        .show(10)

    print("\n   Variants by clinical significance:")
    df_variants.groupBy("clinical_significance") \
        .count() \
        .show()

    variants_success = True

except Exception as e:
    print("   ERROR: Could not read variants table!")
    print(f"   Reason: {e}")
    print("\n   Troubleshooting:")
    print("   - Table not created yet?")
    print("   - Try: Catalog > default > Tables")
    print("   - Or upload via: Create > Table")


# COMMAND ----------

# DBTITLE 1,Final Summary Output
print("\n" + "=" * 70)
print("VERIFICATION SUMMARY")
print("=" * 70)

if genes_success and variants_success:
    print("SUCCESS: All tables uploaded and verified!")
elif genes_success or variants_success:
    print("PARTIAL SUCCESS:")
    if not genes_success:
        print("- Genes table: NOT FOUND")
    if not variants_success:
        print("- Variants table: NOT FOUND")
else:
    print("FAILED: No tables found")
    print("ACTION: Upload files via Create > Table")

print("=" * 70)
