# Databricks notebook source
# MAGIC %md
# MAGIC #### Silver: PharmGKB Data Processing
# MAGIC ######Transform PharmGKB data to silver layer
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 19, 2026
# MAGIC
# MAGIC **Input:** default.pharmgkb_drugs, default.pharmgkb_genes, default.pharmgkb_relationships, default.pharmgkb_variants  
# MAGIC **Output:** silver.pharmgkb_drugs, silver.pharmgkb_genes, silver.pharmgkb_relationships, silver.pharmgkb_variants

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace, coalesce, lit
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("SPARK INITIALIZED FOR PHARMGKB PROCESSING")

# COMMAND ----------

# DBTITLE 1,Load pharmgkb Drugs
print("\nLOADING DRUGS")
print("="*80)

df_drugs = spark.table(f"{catalog_name}.default.pharmgkb_drugs")

drugs_count = df_drugs.count()
print(f" drugs: {drugs_count:,}")

print("\nSchema:")
df_drugs.printSchema()

print("\nSample data:")
df_drugs.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Clean Drugs Data
print("\nCLEANING DRUGS DATA")
print("="*80)

df_drugs_clean = (
    df_drugs
    .withColumn("drug_id", trim(col("PharmGKB Accession Id")))
    .withColumn("drug_name", trim(col("Name")))
    .withColumn("drug_name_clean", lower(trim(col("Name"))))
    
    .filter(col("drug_id").isNotNull())
    .filter(col("drug_id") != "")
    .filter(col("drug_name").isNotNull())
    .filter(col("drug_name") != "")
    
    .dropDuplicates(["drug_id"])
    
    .select(
        "drug_id",
        "drug_name",
        "drug_name_clean"
    )
)

drugs_clean_count = df_drugs_clean.count()
print(f"Clean drugs: {drugs_clean_count:,}")

df_drugs_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver Drugs
print("\nSAVING SILVER DRUGS")
print("="*80)

df_drugs_clean.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.pharmgkb_drugs")

print(f"Saved: {catalog_name}.silver.pharmgkb_drugs")

# COMMAND ----------

# DBTITLE 1,Load Genes
print("\nLOADING GENES")
print("="*80)

df_genes = spark.table(f"{catalog_name}.default.pharmgkb_genes")

genes_count = df_genes.count()
print(f" genes: {genes_count:,}")

df_genes.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Clean Genes Data
print("\nCLEANING GENES DATA")
print("="*80)

df_genes_clean = (
    df_genes
    .withColumn("pharmgkb_gene_id", trim(col("PharmGKB Accession Id")))
    .withColumn("gene_symbol", upper(trim(col("Symbol"))))
    .withColumn("gene_name", trim(col("Name")))
    
    .withColumn("gene_symbol", regexp_replace(col("gene_symbol"), "\\{.*?\\}", ""))
    .withColumn("gene_symbol", trim(col("gene_symbol")))
    
    .filter(col("pharmgkb_gene_id").isNotNull())
    .filter(col("pharmgkb_gene_id") != "")
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    
    .dropDuplicates(["pharmgkb_gene_id"])
    
    .select(
        "pharmgkb_gene_id",
        "gene_symbol",
        "gene_name"
    )
)

genes_clean_count = df_genes_clean.count()
print(f"Clean genes: {genes_clean_count:,}")

df_genes_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver Genes
print("\nSAVING SILVER GENES")
print("="*80)

df_genes_clean.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.pharmgkb_genes")

print(f"Saved: {catalog_name}.silver.pharmgkb_genes")

# COMMAND ----------

# DBTITLE 1,Load  Relationships
print("\nLOADING  RELATIONSHIPS")
print("="*80)

df_rel = spark.table(f"{catalog_name}.default.pharmgkb_relationships")

rel_bronze_count = df_rel.count()
print(f" relationships: {rel_bronze_count:,}")

df_rel.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Clean Relationships Data
print("\nCLEANING RELATIONSHIPS DATA")
print("="*80)

df_rel_clean = (
    df_rel
    .withColumn("entity1_id", trim(col("Entity1_id")))
    .withColumn("entity1_name", trim(col("Entity1_name")))
    .withColumn("entity1_type", trim(col("Entity1_type")))
    .withColumn("entity2_id", trim(col("Entity2_id")))
    .withColumn("entity2_name", trim(col("Entity2_name")))
    .withColumn("entity2_type", trim(col("Entity2_type")))
    .withColumn("evidence", trim(col("Evidence")))
    
    .filter(
        ((col("entity1_type") == "Chemical") & (col("entity2_type") == "Gene")) |
        ((col("entity1_type") == "Gene") & (col("entity2_type") == "Chemical"))
    )
    
    .select(
        "entity1_id",
        "entity1_name",
        "entity1_type",
        "entity2_id",
        "entity2_name",
        "entity2_type",
        "evidence"
    )
)

rel_clean_count = df_rel_clean.count()
print(f"Clean drug-gene relationships: {rel_clean_count:,}")

df_rel_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver Relationships
print("\nSAVING SILVER RELATIONSHIPS")
print("="*80)

df_rel_clean.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.pharmgkb_relationships")

print(f"Saved: {catalog_name}.silver.pharmgkb_relationships")

# COMMAND ----------

# DBTITLE 1,Load Variants
print("\nLOADING  VARIANTS")
print("="*80)

df_variants = spark.table(f"{catalog_name}.default.pharmgkb_variants")

variants_count = df_variants.count()
print(f" variants: {variants_count:,}")

df_variants.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Clean Variants Data
print("\nCLEANING VARIANTS DATA")
print("="*80)

df_variants_clean = (
    df_variants
    .withColumn("variant_id", trim(col("Variant ID")))
    .withColumn("variant_name", trim(col("Variant Name")))
    .withColumn("gene_symbols", upper(trim(col("Gene Symbols"))))
    .withColumn("location", trim(col("Location")))
    
    .filter(col("variant_name").isNotNull())
    .filter(col("variant_name") != "")
    
    .dropDuplicates(["variant_id"])
    
    .select(
        "variant_id",
        "variant_name",
        "gene_symbols",
        "location"
    )
)

variants_clean_count = df_variants_clean.count()
print(f"Clean variants: {variants_clean_count:,}")

df_variants_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Silver Variants
print("\nSAVING SILVER VARIANTS")
print("="*80)

df_variants_clean.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.pharmgkb_variants")

print(f"Saved: {catalog_name}.silver.pharmgkb_variants")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("PHARMGKB SILVER PROCESSING COMPLETE")
print("="*80)

print("\nTables created:")
print(f"  silver.pharmgkb_drugs: {spark.table(f'{catalog_name}.silver.pharmgkb_drugs').count():,}")
print(f"  silver.pharmgkb_genes: {spark.table(f'{catalog_name}.silver.pharmgkb_genes').count():,}")
print(f"  silver.pharmgkb_relationships: {spark.table(f'{catalog_name}.silver.pharmgkb_relationships').count():,}")
print(f"  silver.pharmgkb_variants: {spark.table(f'{catalog_name}.silver.pharmgkb_variants').count():,}")

print("\nSample drug-gene relationships:")
spark.table(f"{catalog_name}.silver.pharmgkb_relationships").show(3, truncate=60)
print("\nSample drug:")
spark.table(f"{catalog_name}.silver.pharmgkb_drugs").show(3, truncate=60)
print("\nSample gene:")
spark.table(f"{catalog_name}.silver.pharmgkb_genes").show(3, truncate=60)
print("\nSample variants:")
spark.table(f"{catalog_name}.silver.pharmgkb_variants").show(3, truncate=60)

print("\nProcessing complete")
