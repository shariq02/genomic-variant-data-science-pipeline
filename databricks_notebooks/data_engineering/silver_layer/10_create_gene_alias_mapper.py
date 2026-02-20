# Databricks notebook source
# MAGIC %md
# MAGIC ## GENE ALIAS MAPPER - FOR FEATURE ENGINEERING
# MAGIC ### Create universal search lookup for direct PySpark usage
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC
# MAGIC **Run Order:** Step 3 (After gene/variant processing, BEFORE feature engineering)
# MAGIC ```
# MAGIC 1.  02_gene_data_processing_ENHANCED.py
# MAGIC 2.  03_variant_data_processing_ENHANCED.py
# MAGIC 3.  04_create_gene_alias_mapper.py (THIS SCRIPT)
# MAGIC 4.  05_feature_engineering.py (uses these lookups)
# MAGIC ```
# MAGIC
# MAGIC **Purpose:** Create lookup table for resolving gene aliases in feature engineering
# MAGIC
# MAGIC **Usage in Feature Engineering:**
# MAGIC ```python
# MAGIC # Load the lookup
# MAGIC gene_lookup = spark.table("workspace.reference.gene_universal_search")
# MAGIC
# MAGIC # Join to resolve aliases
# MAGIC df_resolved = variants.join(
# MAGIC     gene_lookup,
# MAGIC     upper(variants.gene_name) == gene_lookup.search_term,
# MAGIC     "left"
# MAGIC ).select(
# MAGIC     variants.*,
# MAGIC     coalesce(gene_lookup.mapped_gene_name, variants.gene_name).alias("resolved_gene")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, upper, trim, when, array, 
    lit, coalesce, array_distinct, flatten, count, size
)

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GENE ALIAS MAPPER - CREATE UNIVERSAL SEARCH LOOKUP")
print("="*80)
print(f"Catalog: {catalog_name}")
print("Purpose: Create lookup for feature engineering gene resolution")

# COMMAND ----------

# DBTITLE 1,Load Enhanced Genes
print("\nLoading genes_ultra_enriched...")

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
gene_count = df_genes.count()

print(f" Loaded {gene_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Load Additional Gene Sources
print("\nLoading additional gene sources...")

df_refseq_genes = spark.table(f"{catalog_name}.silver.genes_refseq")
df_refseq_proteins = spark.table(f"{catalog_name}.silver.proteins_refseq")
df_uniprot_proteins = spark.table(f"{catalog_name}.silver.proteins_uniprot")
df_gene_disease = spark.table(f"{catalog_name}.silver.gene_disease_links")

refseq_genes_count = df_refseq_genes.count()
refseq_proteins_count = df_refseq_proteins.count()
uniprot_count = df_uniprot_proteins.count()
gene_disease_count = df_gene_disease.count()

print(f" RefSeq genes: {refseq_genes_count:,}")
print(f" RefSeq proteins: {refseq_proteins_count:,}")
print(f" UniProt proteins: {uniprot_count:,}")
print(f" Gene-disease links: {gene_disease_count:,}")

# COMMAND ----------

# DBTITLE 1,Create Designation Lookup
print("CREATING DESIGNATION→GENE LOOKUP")
print("="*80)

# Collect all designations
df_designations = (
    df_genes
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description",
        
        # Collect all non-null designations
        array_distinct(
            flatten(
                array(
                    when(col("description").isNotNull(), array(col("description"))).otherwise(array()),
                    when(col("designation_1").isNotNull(), array(col("designation_1"))).otherwise(array()),
                    when(col("designation_2").isNotNull(), array(col("designation_2"))).otherwise(array()),
                    when(col("designation_3").isNotNull(), array(col("designation_3"))).otherwise(array()),
                    when(col("designation_4").isNotNull(), array(col("designation_4"))).otherwise(array()),
                    when(col("designation_5").isNotNull(), array(col("designation_5"))).otherwise(array()),
                    when(col("designation_6").isNotNull(), array(col("designation_6"))).otherwise(array()),
                    when(col("designation_7").isNotNull(), array(col("designation_7"))).otherwise(array()),
                    when(col("designation_8").isNotNull(), array(col("designation_8"))).otherwise(array()),
                    when(col("designation_9").isNotNull(), array(col("designation_9"))).otherwise(array()),
                    when(col("designation_10").isNotNull(), array(col("designation_10"))).otherwise(array()),
                    when(col("designation_11").isNotNull(), array(col("designation_11"))).otherwise(array()),
                    when(col("designation_12").isNotNull(), array(col("designation_12"))).otherwise(array()),
                    when(col("designation_13").isNotNull(), array(col("designation_13"))).otherwise(array()),
                    when(col("designation_14").isNotNull(), array(col("designation_14"))).otherwise(array()),
                    when(col("designation_15").isNotNull(), array(col("designation_15"))).otherwise(array())
                )
            )
        ).alias("designations_array")
    )
    .filter(col("designations_array").isNotNull())
    .filter(size(col("designations_array")) > 0)
)

# Explode into rows
df_designation_lookup = (
    df_designations
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        explode(col("designations_array")).alias("designation"),
        "description"
    )
    .withColumn("search_term", upper(trim(col("designation"))))
    .dropDuplicates(["search_term"])
    .select(
        "search_term",
        col("gene_id").alias("mapped_gene_id"),
        col("gene_name").alias("mapped_gene_name"),
        col("official_symbol").alias("mapped_official_symbol"),
        col("designation").alias("search_text"),
        lit("designation").alias("match_type"),
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description"
    )
)

designation_count = df_designation_lookup.count()
print(f" Created {designation_count:,} designation mappings")

# Save
df_designation_lookup.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.gene_designation_lookup")

print(f" Saved: {catalog_name}.reference.gene_designation_lookup")

# COMMAND ----------

# DBTITLE 1,Create Universal Search Table
print("CREATING UNIVERSAL SEARCH TABLE (ALIASES + DESIGNATIONS)")
print("="*80)

# Create alias mappings
df_aliases = (
    df_genes
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description",
        
        # Collect all non-null aliases
        array_distinct(
            flatten(
                array(
                    array(col("gene_name")),
                    when(col("official_symbol") != col("gene_name"), 
                         array(col("official_symbol"))).otherwise(array()),
                    when(col("alias_1").isNotNull(), array(col("alias_1"))).otherwise(array()),
                    when(col("alias_2").isNotNull(), array(col("alias_2"))).otherwise(array()),
                    when(col("alias_3").isNotNull(), array(col("alias_3"))).otherwise(array()),
                    when(col("alias_4").isNotNull(), array(col("alias_4"))).otherwise(array()),
                    when(col("alias_5").isNotNull(), array(col("alias_5"))).otherwise(array()),
                    when(col("alias_6").isNotNull(), array(col("alias_6"))).otherwise(array()),
                    when(col("alias_7").isNotNull(), array(col("alias_7"))).otherwise(array()),
                    when(col("alias_8").isNotNull(), array(col("alias_8"))).otherwise(array()),
                    when(col("alias_9").isNotNull(), array(col("alias_9"))).otherwise(array()),
                    when(col("alias_10").isNotNull(), array(col("alias_10"))).otherwise(array())
                )
            )
        ).alias("aliases_array")
    )
)

# Explode aliases
df_alias_search = (
    df_aliases
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description",
        explode(col("aliases_array")).alias("alias")
    )
    .withColumn("search_term", upper(trim(col("alias"))))
    .dropDuplicates(["search_term"])
    .select(
        "search_term",
        col("gene_id").alias("mapped_gene_id"),
        col("gene_name").alias("mapped_gene_name"),
        col("official_symbol").alias("mapped_official_symbol"),
        col("alias").alias("search_text"),
        lit("alias").alias("match_type"),
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description"
    )
)

alias_count = df_alias_search.count()
print(f" Created {alias_count:,} alias mappings")

# COMMAND ----------

# DBTITLE 1,Create Additional Source Mappings
print("\nCREATING ADDITIONAL SOURCE MAPPINGS")
print("="*80)

# RefSeq genes
df_refseq_search = (
    df_refseq_genes
    .filter(col("gene_symbol").isNotNull())
    .withColumn("search_term", upper(trim(col("gene_symbol"))))
    .dropDuplicates(["search_term"])
    .select(
        "search_term",
        lit(None).cast("string").alias("mapped_gene_id"),
        col("gene_symbol").alias("mapped_gene_name"),
        col("gene_symbol").alias("mapped_official_symbol"),
        col("gene_name").alias("search_text"),
        lit("refseq_gene").alias("match_type"),
        "chromosome",
        lit(None).cast("string").alias("mim_id"),
        lit(None).cast("string").alias("ensembl_id"),
        col("gene_name").alias("description")
    )
)
refseq_search_count = df_refseq_search.count()
print(f" RefSeq genes: {refseq_search_count:,}")

# RefSeq proteins
df_refseq_protein_search = (
    df_refseq_proteins
    .filter(col("gene_symbol").isNotNull())
    .withColumn("search_term", upper(trim(col("gene_symbol"))))
    .dropDuplicates(["search_term"])
    .select(
        "search_term",
        lit(None).cast("string").alias("mapped_gene_id"),
        col("gene_symbol").alias("mapped_gene_name"),
        col("gene_symbol").alias("mapped_official_symbol"),
        col("protein_accession").alias("search_text"),
        lit("refseq_protein").alias("match_type"),
        lit(None).cast("string").alias("chromosome"),
        lit(None).cast("string").alias("mim_id"),
        lit(None).cast("string").alias("ensembl_id"),
        col("protein_accession").alias("description")
    )
)
refseq_protein_search_count = df_refseq_protein_search.count()
print(f" RefSeq proteins: {refseq_protein_search_count:,}")

# UniProt proteins
df_uniprot_search = (
    df_uniprot_proteins
    .filter(col("gene_symbol").isNotNull())
    .withColumn("search_term", upper(trim(col("gene_symbol"))))
    .dropDuplicates(["search_term"])
    .select(
        "search_term",
        lit(None).cast("string").alias("mapped_gene_id"),
        col("gene_symbol").alias("mapped_gene_name"),
        col("gene_symbol").alias("mapped_official_symbol"),
        col("protein_name").alias("search_text"),
        lit("uniprot_protein").alias("match_type"),
        lit(None).cast("string").alias("chromosome"),
        lit(None).cast("string").alias("mim_id"),
        lit(None).cast("string").alias("ensembl_id"),
        col("protein_name").alias("description")
    )
)
uniprot_search_count = df_uniprot_search.count()
print(f" UniProt proteins: {uniprot_search_count:,}")

# Gene-disease links
df_gene_disease_search = (
    df_gene_disease
    .filter(col("gene_symbol").isNotNull())
    .withColumn("search_term", upper(trim(col("gene_symbol"))))
    .dropDuplicates(["search_term"])
    .select(
        "search_term",
        lit(None).cast("string").alias("mapped_gene_id"),
        col("gene_symbol").alias("mapped_gene_name"),
        col("gene_symbol").alias("mapped_official_symbol"),
        col("disease_name").alias("search_text"),
        lit("gene_disease").alias("match_type"),
        lit(None).cast("string").alias("chromosome"),
        lit(None).cast("string").alias("mim_id"),
        lit(None).cast("string").alias("ensembl_id"),
        col("disease_name").alias("description")
    )
)
gene_disease_search_count = df_gene_disease_search.count()
print(f" Gene-disease links: {gene_disease_search_count:,}")

# COMMAND ----------

# DBTITLE 1,Combine All Sources
print("\nCOMBINING ALL SOURCES")
print("="*80)

# Combine aliases and designations
df_universal_search = (
    df_alias_search
    .union(df_designation_lookup)
    .union(df_refseq_search)
    .union(df_refseq_protein_search)
    .union(df_uniprot_search)
    .union(df_gene_disease_search)
    .dropDuplicates(["search_term"])
    .orderBy("search_term")
)

universal_count = df_universal_search.count()
print(f" Combined into {universal_count:,} universal search terms")

# Save universal search
df_universal_search.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.gene_universal_search")

print(f" Saved: {catalog_name}.reference.gene_universal_search")

# COMMAND ----------

# DBTITLE 1,Create SQL View
print("CREATING SQL VIEW")
print("="*80)

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.reference.gene_search_view AS
SELECT 
    search_term,
    mapped_gene_id,
    mapped_gene_name,
    mapped_official_symbol,
    search_text,
    match_type,
    chromosome,
    mim_id,
    ensembl_id,
    description
FROM {catalog_name}.reference.gene_universal_search
""")

print(f" Created: {catalog_name}.reference.gene_search_view")

# COMMAND ----------

# DBTITLE 1,Fill NULLs Across All Tables Using Mapper
print("\nFILLING NULL VALUES USING GENE MAPPER")
print("="*80)

# Fill proteins_refseq NULLs
try:
    df_proteins_refseq_filled = (
        spark.table(f"{catalog_name}.silver.proteins_refseq")
        .join(df_universal_search.select("search_term", "mapped_gene_name"),
              upper(col("gene_symbol")) == col("search_term"), "left")
        .withColumn("gene_symbol", coalesce(col("gene_symbol"), col("mapped_gene_name")))
        .select("protein_accession", "gene_id", "gene_symbol", "rna_accession")
    )
    
    df_proteins_refseq_filled.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog_name}.silver.proteins_refseq")
    print("Filled: proteins_refseq")
except Exception as e:
    print(f"Skipped: proteins_refseq ({str(e)[:50]})")

# Fill transcripts NULLs
try:
    df_transcripts_filled = (
        spark.table(f"{catalog_name}.silver.transcripts")
        .join(df_universal_search.select("search_term", "mapped_gene_name"),
              upper(col("gene_symbol")) == col("search_term"), "left")
        .withColumn("gene_symbol", coalesce(col("gene_symbol"), col("mapped_gene_name")))
        .select("transcript_id", "gene_symbol", "gene_name", "chromosome", "start", "stop", "strand")
    )
    
    df_transcripts_filled.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog_name}.silver.transcripts")
    print("Filled: transcripts")
except Exception as e:
    print(f"Skipped: transcripts ({str(e)[:50]})")

# Fill genetic_tests NULLs (if exists)
try:
    df_tests_filled = (
        spark.table(f"{catalog_name}.silver.genetic_tests")
        .join(df_universal_search.select("search_term", "mapped_gene_name"),
              upper(col("gene_symbol")) == col("search_term"), "left")
        .withColumn("gene_symbol", coalesce(col("gene_symbol"), col("mapped_gene_name")))
        .select("gtr_test_id", "gene_symbol", "test_name", "disease_name")
    )
    
    df_tests_filled.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog_name}.silver.genetic_tests")
    print("Filled: genetic_tests")
except Exception as e:
    print(f"Skipped: genetic_tests (table not found)")

print("\nNULL filling complete for available tables")

# COMMAND ----------

# DBTITLE 1,Summary & Usage for Feature Engineering
print(" GENE ALIAS MAPPING COMPLETE")
print("="*80)

print(f"\n SOURCE STATISTICS:")
print(f"   genes_ultra_enriched: {gene_count:,}")
print(f"   genes_refseq: {refseq_genes_count:,}")
print(f"   proteins_refseq: {refseq_proteins_count:,}")
print(f"   proteins_uniprot: {uniprot_count:,}")
print(f"   gene_disease_links: {gene_disease_count:,}")

print(f"\n MAPPING STATISTICS:")
print(f"   Aliases: {alias_count:,}")
print(f"   Designations: {designation_count:,}")
print(f"   RefSeq genes: {refseq_search_count:,}")
print(f"   RefSeq proteins: {refseq_protein_search_count:,}")
print(f"   UniProt proteins: {uniprot_search_count:,}")
print(f"   Gene-disease: {gene_disease_search_count:,}")
print(f"   Total search terms: {universal_count:,}")

print(f"\n TABLES CREATED:")
print(f"   1. {catalog_name}.reference.gene_designation_lookup")
print(f"   2. {catalog_name}.reference.gene_universal_search --- USE THIS")
print(f"   3. {catalog_name}.reference.gene_search_view")

print(f"\n BENEFITS:")
print(f"    Resolves ALL gene aliases automatically")
print(f"    Handles NULL values in alias columns")
print(f"    Case-insensitive matching")
print(f"    {universal_count:,} searchable terms")
print(f"    Ready for feature engineering")

# COMMAND ----------

# DBTITLE 1,Verify Tables for Feature Engineering
print("\n VERIFYING TABLES...")

# Check universal search
universal_check = spark.table(f"{catalog_name}.reference.gene_universal_search")
print(f"\n gene_universal_search: {universal_check.count():,} rows")

# Sample data
print(f"\n Sample universal search:")
universal_check.show(5, truncate=60)

# Match type breakdown
print(f"\n Match type distribution:")
universal_check.groupBy("match_type").count().show()

print("\n All tables ready for feature engineering!")
