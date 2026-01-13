# Databricks notebook source
# MAGIC %md
# MAGIC #### ENHANCED GENE DATA PROCESSING WITH TEXT PARSING
# MAGIC ##### Extract Keywords, Aliases, and Functions from Gene Metadata
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 13, 2026  
# MAGIC **Purpose:** Enhanced gene processing with text mining from description, aliases, designations
# MAGIC **Input:** workspace.default.gene_metadata_all (193K genes)  
# MAGIC **Output:** workspace.silver.genes_enriched (with parsed text data)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace, split, explode,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, 
    concat_ws, array_distinct, flatten, collect_set, size, array_contains,
    regexp_extract, initcap
)
from pyspark.sql.types import StringType, ArrayType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("="*70)
print("ENHANCED GENE DATA PROCESSING WITH TEXT MINING")
print("="*70)
print(f"Catalog: {catalog_name}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Raw Gene Data
print("\nReading gene metadata...")

df_genes_raw = spark.table(f"{catalog_name}.default.gene_metadata_all")

raw_count = df_genes_raw.count()
print(f"Loaded {raw_count:,} genes")
print(f"Columns: {len(df_genes_raw.columns)}")

# COMMAND ----------

# DBTITLE 1,Inspect Text Fields
print("\nSample text fields to parse:")
display(
    df_genes_raw.select(
        "gene_name", 
        "description", 
        "other_aliases", 
        "other_designations",
        "full_name"
    ).limit(5)
)

# COMMAND ----------

# DBTITLE 1,Parse Aliases into Array
print("\n" + "="*70)
print("STEP 1: PARSE ALIASES")
print("="*70)

df_parsed = (
    df_genes_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    .withColumn("official_symbol", 
                when(col("official_symbol").isNotNull(), 
                     upper(trim(col("official_symbol"))))
                .otherwise(col("gene_name")))
    
    # Parse other_aliases (pipe-separated: A1B|ABG|GAB)
    .withColumn("aliases_array",
                when(col("other_aliases").isNotNull() & (col("other_aliases") != "-"),
                     split(col("other_aliases"), "\\|"))
                .otherwise(array()))
    
    # Add official symbol to aliases
    .withColumn("all_aliases",
                array_distinct(
                    flatten(array(
                        col("aliases_array"),
                        array(col("gene_name")),
                        array(col("official_symbol"))
                    ))
                ))
    
    .withColumn("alias_count", size(col("all_aliases")))
)

print("Aliases parsed into arrays")

# COMMAND ----------

# DBTITLE 1,Sample Parsed Aliases
print("\nSample parsed aliases:")
display(
    df_parsed.select("gene_name", "other_aliases", "all_aliases", "alias_count")
              .filter(col("alias_count") > 2)
              .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Extract Keywords from Descriptions
print("\n" + "="*70)
print("STEP 2: EXTRACT KEYWORDS FROM DESCRIPTIONS")
print("="*70)

# Define keyword categories
df_keywords = (
    df_parsed
    
    # Parse other_designations (pipe-separated descriptions)
    .withColumn("designations_array",
                when(col("other_designations").isNotNull() & (col("other_designations") != "-"),
                     split(col("other_designations"), "\\|"))
                .otherwise(array()))
    
    # Combine description + full_name + designations
    .withColumn("full_text",
                concat_ws(" | ",
                          coalesce(col("description"), lit("")),
                          coalesce(col("full_name"), lit("")),
                          coalesce(col("other_designations"), lit(""))))
    
    # Extract function keywords
    .withColumn("is_kinase", 
                lower(col("full_text")).contains("kinase"))
    .withColumn("is_receptor",
                lower(col("full_text")).contains("receptor"))
    .withColumn("is_transcription_factor",
                lower(col("full_text")).contains("transcription factor"))
    .withColumn("is_enzyme",
                lower(col("full_text")).rlike("(?i)(enzyme|synthetase|dehydrogenase|transferase|ligase|hydrolase)"))
    .withColumn("is_transporter",
                lower(col("full_text")).contains("transport"))
    .withColumn("is_channel",
                lower(col("full_text")).rlike("(?i)(channel|pore)"))
    .withColumn("is_membrane_protein",
                lower(col("full_text")).rlike("(?i)(membrane|transmembrane)"))
    
    # Disease-related keywords
    .withColumn("cancer_related",
                lower(col("full_text")).rlike("(?i)(cancer|tumor|oncogene|carcinoma)"))
    .withColumn("immune_related",
                lower(col("full_text")).rlike("(?i)(immune|antibody|lymphocyte|interleukin)"))
    .withColumn("neurological_related",
                lower(col("full_text")).rlike("(?i)(brain|neural|neuron|synap)"))
    
    # Cellular location keywords
    .withColumn("nuclear",
                lower(col("full_text")).rlike("(?i)(nuclear|nucleus)"))
    .withColumn("mitochondrial",
                lower(col("full_text")).rlike("(?i)(mitochondri)"))
    .withColumn("cytoplasmic",
                lower(col("full_text")).rlike("(?i)(cytoplasm|cytosol)"))
)

print("Keywords extracted from descriptions")

# COMMAND ----------

# DBTITLE 1,Sample Keyword Extraction
print("\nSample keyword extraction:")
display(
    df_keywords.select(
        "gene_name",
        "description",
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "cancer_related"
    ).filter(col("is_kinase") | col("cancer_related"))
     .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Create Functional Categories
print("\n" + "="*70)
print("STEP 3: CREATE FUNCTIONAL CATEGORIES")
print("="*70)

df_categorized = (
    df_keywords
    
    # Primary function category
    .withColumn("primary_function",
                when(col("is_transcription_factor"), "Transcription Factor")
                .when(col("is_kinase"), "Kinase")
                .when(col("is_receptor"), "Receptor")
                .when(col("is_channel"), "Ion Channel")
                .when(col("is_transporter"), "Transporter")
                .when(col("is_enzyme"), "Enzyme")
                .otherwise("Other"))
    
    # Biological process
    .withColumn("biological_process",
                when(col("cancer_related"), "Cancer/Oncology")
                .when(col("immune_related"), "Immune Response")
                .when(col("neurological_related"), "Neurological")
                .otherwise("General"))
    
    # Cellular location
    .withColumn("cellular_location",
                when(col("is_membrane_protein"), "Membrane")
                .when(col("nuclear"), "Nuclear")
                .when(col("mitochondrial"), "Mitochondrial")
                .when(col("cytoplasmic"), "Cytoplasmic")
                .otherwise("Unknown"))
)

print("Functional categories assigned")

# COMMAND ----------

# DBTITLE 1,Category Distribution
print("\nFunctional category distribution:")

print("\n1. Primary Function:")
display(
    df_categorized.groupBy("primary_function")
                  .count()
                  .orderBy(col("count").desc())
)

print("\n2. Biological Process:")
display(
    df_categorized.groupBy("biological_process")
                  .count()
                  .orderBy(col("count").desc())
)

print("\n3. Cellular Location:")
display(
    df_categorized.groupBy("cellular_location")
                  .count()
                  .orderBy(col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Parse Database Cross-References
print("\n" + "="*70)
print("STEP 4: PARSE DATABASE XREFS")
print("="*70)

df_xrefs = (
    df_categorized
    
    # Parse db_xrefs (format: MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410)
    .withColumn("xrefs_array",
                when(col("db_xrefs").isNotNull(),
                     split(col("db_xrefs"), "\\|"))
                .otherwise(array()))
    
    # Extract specific database IDs
    .withColumn("has_mim", 
                array_contains(col("xrefs_array"), "MIM"))
    .withColumn("has_ensembl",
                size(col("xrefs_array")) > 0)
    .withColumn("xref_count", size(col("xrefs_array")))
)

print("Database cross-references parsed")

# COMMAND ----------

# DBTITLE 1,Clean and Standardize Core Fields
print("\n" + "="*70)
print("STEP 5: CLEAN CORE FIELDS")
print("="*70)

df_clean = (
    df_xrefs
    
    # Chromosome
    .withColumn("chromosome", 
                regexp_replace(upper(trim(col("chromosome"))), "^CHR", ""))
    .withColumn("chromosome",
                when(col("chromosome").isin(
                    '1','2','3','4','5','6','7','8','9','10',
                    '11','12','13','14','15','16','17','18','19','20',
                    '21','22','X','Y','MT'
                ), col("chromosome"))
                .otherwise(None))
    
    # Gene type
    .withColumn("gene_type_clean",
                when((col("gene_type") == "Unknown") | col("gene_type").isNull(),
                     when(lower(col("description")).contains("protein"), "protein-coding")
                     .when(lower(col("description")).contains("rna"), "RNA")
                     .when(lower(col("description")).contains("pseudogene"), "pseudogene")
                     .otherwise("unknown"))
                .otherwise(trim(col("gene_type"))))
    
    # Positions (convert to long)
    .withColumn("start_position",
                when((col("start_position").isNotNull()) & 
                     (col("start_position").cast("long") > 0), 
                     col("start_position").cast("long"))
                .otherwise(None))
    .withColumn("end_position",
                when((col("end_position").isNotNull()) & 
                     (col("end_position").cast("long") > 0), 
                     col("end_position").cast("long"))
                .otherwise(None))
    .withColumn("gene_length",
                when((col("start_position").isNotNull()) & 
                     (col("end_position").isNotNull()),
                     col("end_position") - col("start_position"))
                .otherwise(col("gene_length").cast("long")))
    
    # Enhanced description
    .withColumn("description_enhanced",
                when((col("description").isNull()) | 
                     (col("description") == "Unknown") |
                     (col("description") == ""),
                     coalesce(col("full_name"), 
                             concat_ws(" ", lit("Gene:"), col("gene_name"))))
                .otherwise(trim(col("description"))))
    
    # Filter valid records
    .filter(col("gene_name").isNotNull())
    .filter(col("gene_id").isNotNull())
    .filter(col("chromosome").isNotNull())
    .dropDuplicates(["gene_id"])
)

clean_count = df_clean.count()

print(f"\nCleaning complete!")
print(f"   Before: {raw_count:,} genes")
print(f"   After:  {clean_count:,} genes")
print(f"   Removed: {raw_count - clean_count:,} invalid rows")

# COMMAND ----------

# DBTITLE 1,Create Final Enriched Table
print("\n" + "="*70)
print("STEP 6: CREATE ENRICHED GENE TABLE")
print("="*70)

df_genes_enriched = df_clean.select(
    # Core identifiers
    "gene_id",
    "gene_name",
    "official_symbol",
    
    # Descriptions
    col("description_enhanced").alias("description"),
    "full_name",
    
    # Genomic location
    "chromosome",
    "map_location",
    "start_position",
    "end_position",
    "strand",
    "gene_length",
    
    # Gene classification
    col("gene_type_clean").alias("gene_type"),
    
    # NEW ENRICHED FIELDS
    "all_aliases",
    "alias_count",
    "primary_function",
    "biological_process",
    "cellular_location",
    
    # Keyword flags
    "is_kinase",
    "is_receptor",
    "is_transcription_factor",
    "is_enzyme",
    "cancer_related",
    "immune_related",
    "neurological_related",
    
    # Database xrefs
    "db_xrefs",
    "xref_count",
    
    # Metadata
    "nomenclature_status",
    "modification_date",
    "data_source"
)

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
print("\n" + "="*70)
print("SAVING ENRICHED GENES TO SILVER LAYER")
print("="*70)

df_genes_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_enriched")

saved_count = spark.table(f"{catalog_name}.silver.genes_enriched").count()
print(f"Saved to: {catalog_name}.silver.genes_enriched")
print(f"Verified: {saved_count:,} enriched genes")

# COMMAND ----------

# DBTITLE 1,Enrichment Summary Statistics
print("\n" + "="*70)
print("ENRICHMENT SUMMARY")
print("="*70)

enrichment_stats = {
    "total_genes": df_genes_enriched.count(),
    "genes_with_aliases": df_genes_enriched.filter(col("alias_count") > 1).count(),
    "kinases": df_genes_enriched.filter(col("is_kinase")).count(),
    "receptors": df_genes_enriched.filter(col("is_receptor")).count(),
    "transcription_factors": df_genes_enriched.filter(col("is_transcription_factor")).count(),
    "cancer_related": df_genes_enriched.filter(col("cancer_related")).count(),
    "immune_related": df_genes_enriched.filter(col("immune_related")).count(),
    "neurological": df_genes_enriched.filter(col("neurological_related")).count()
}

print("\nEnrichment Statistics:")
for key, value in enrichment_stats.items():
    print(f"  {key}: {value:,}")

# COMMAND ----------

# DBTITLE 1,Sample Enriched Data
print("\nSample enriched genes (cancer-related kinases):")
display(
    df_genes_enriched.filter(col("is_kinase") & col("cancer_related"))
                     .select("gene_name", "description", "primary_function", 
                            "biological_process", "all_aliases")
                     .limit(10)
)

print("\n" + "="*70)
print("GENE ENRICHMENT COMPLETE")
print("="*70)
print("Next: Use silver.genes_enriched instead of silver.genes_clean")
print("      in feature engineering for richer gene metadata")
print("="*70)
