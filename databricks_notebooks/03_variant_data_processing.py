# Databricks notebook source
# MAGIC %md
# MAGIC #### ENHANCED VARIANT DATA PROCESSING WITH TEXT PARSING
# MAGIC ##### Extract Disease Names, Phenotype IDs, Protein Changes, Multiple Accessions
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 13, 2026  
# MAGIC **Purpose:** Enhanced variant processing with text mining from multiple fields
# MAGIC **Input:** workspace.default.clinvar_all_variants (4M+ variants)  
# MAGIC **Output:** workspace.silver.variants_enriched (with parsed text data)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace, split, explode,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, 
    concat_ws, array_distinct, flatten, collect_set, size, array_contains,
    regexp_extract, array, first, row_number
)
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import Window

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("="*70)
print("ENHANCED VARIANT DATA PROCESSING WITH TEXT MINING")
print("="*70)
print(f"Catalog: {catalog_name}")
print(f"Input: default.clinvar_all_variants (4M+ variants)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Raw Variant Data
print("\nReading ALL variant data...")

df_variants_raw = spark.table(f"{catalog_name}.default.clinvar_all_variants")

raw_count = df_variants_raw.count()
print(f"Loaded {raw_count:,} raw variants")
print(f"Columns: {len(df_variants_raw.columns)}")

# COMMAND ----------

# DBTITLE 1,Save to Bronze Layer
print("\n" + "="*70)
print("SAVING TO BRONZE LAYER")
print("="*70)

df_variants_raw.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.variants_raw")

print(f"Saved to {catalog_name}.bronze.variants_raw")
print(f"Rows: {raw_count:,}")

# COMMAND ----------

# DBTITLE 1,Sample Text Fields to Parse
print("\nSample text fields:")
display(
    df_variants_raw.select(
        "gene_name",
        "disease",
        "phenotype_ids",
        "variant_name",
        "accession",
        "clinical_significance"
    ).limit(5)
)

# COMMAND ----------

# DBTITLE 1,Parse Disease Field (Pipe-Separated)
print("\n" + "="*70)
print("STEP 1: PARSE DISEASES")
print("="*70)

df_parsed = (
    df_variants_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    
    # Parse diseases (pipe-separated: Disease1|Disease2|Disease3)
    .withColumn("disease_array",
                when(col("disease").isNotNull() & (col("disease") != "not provided"),
                     split(col("disease"), "\\|"))
                .otherwise(array()))
    
    # Primary disease (first in list)
    .withColumn("primary_disease",
                when(size(col("disease_array")) > 0,
                     col("disease_array").getItem(0))
                .otherwise(None))
    
    # Count diseases
    .withColumn("disease_count", size(col("disease_array")))
    
    # Check for specific disease keywords
    .withColumn("has_cancer_disease",
                lower(col("disease")).rlike("(?i)(cancer|carcinoma|tumor|oncogene)"))
    .withColumn("has_syndrome",
                lower(col("disease")).rlike("(?i)(syndrome)"))
    .withColumn("has_hereditary",
                lower(col("disease")).rlike("(?i)(hereditary|familial)"))
)

print("Diseases parsed into arrays")

# COMMAND ----------

# DBTITLE 1,Sample Disease Parsing
print("\nSample disease parsing:")
display(
    df_parsed.select("gene_name", "disease", "disease_array", "primary_disease", "disease_count")
             .filter(col("disease_count") > 1)
             .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Parse Phenotype IDs
print("\n" + "="*70)
print("STEP 2: PARSE PHENOTYPE DATABASE IDS")
print("="*70)

df_phenotypes = (
    df_parsed
    
    # Parse phenotype IDs (format: MONDO:xxx,MedGen:yyy,OMIM:zzz|next_set)
    .withColumn("phenotype_array",
                when(col("phenotype_ids").isNotNull(),
                     split(col("phenotype_ids"), "\\|"))
                .otherwise(array()))
    
    # Check for specific databases
    .withColumn("has_omim",
                when(col("phenotype_ids").isNotNull(),
                     lower(col("phenotype_ids")).contains("omim"))
                .otherwise(False))
    .withColumn("has_orphanet",
                when(col("phenotype_ids").isNotNull(),
                     lower(col("phenotype_ids")).contains("orphanet"))
                .otherwise(False))
    .withColumn("has_mondo",
                when(col("phenotype_ids").isNotNull(),
                     lower(col("phenotype_ids")).contains("mondo"))
                .otherwise(False))
    
    .withColumn("phenotype_db_count",
                when(col("phenotype_ids").isNotNull(),
                     size(split(col("phenotype_ids"), ",")))
                .otherwise(0))
)

print("Phenotype IDs parsed")

# COMMAND ----------

# DBTITLE 1,Parse Accession IDs
print("\n" + "="*70)
print("STEP 3: PARSE ACCESSION IDS")
print("="*70)

df_accessions = (
    df_phenotypes
    
    # Parse accessions (pipe-separated RCV IDs: RCV000000012|RCV005255549)
    .withColumn("accession_array",
                when(col("accession").isNotNull() & (col("accession") != "Unknown"),
                     split(col("accession"), "\\|"))
                .otherwise(array()))
    
    # Primary accession (first RCV)
    .withColumn("primary_accession",
                when(size(col("accession_array")) > 0,
                     col("accession_array").getItem(0))
                .otherwise(col("accession")))
    
    # Count accessions (multiple submissions)
    .withColumn("accession_count", size(col("accession_array")))
)

print("Accession IDs parsed")

# COMMAND ----------

# DBTITLE 1,Parse Variant Name for Protein Change
print("\n" + "="*70)
print("STEP 4: EXTRACT PROTEIN CHANGE FROM VARIANT NAME")
print("="*70)

df_protein = (
    df_accessions
    
    # Extract protein change from variant_name
    # Format: NM_014855.3(AP5Z1):c.80_83del (p.Arg27_Ile28del)
    .withColumn("protein_change_extracted",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "\\(p\\.([^)]+)\\)", 1))
                .otherwise(None))
    
    # Extract cDNA change
    .withColumn("cdna_change",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), ":c\\.([^\\s]+)", 1))
                .otherwise(None))
    
    # Extract transcript ID
    .withColumn("transcript_id",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "(NM_\\d+\\.\\d+)", 1))
                .otherwise(None))
    
    # Flag frameshift variants
    .withColumn("is_frameshift",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).contains("fs"))
                .otherwise(False))
    
    # Flag nonsense variants (stop codon)
    .withColumn("is_nonsense",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("(?i)(ter|\\*|stop)"))
                .otherwise(False))
    
    # Flag splice variants
    .withColumn("is_splice",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("(?i)(splice|\\+|\\-)"))
                .otherwise(False))
)

print("Protein changes extracted from variant names")

# COMMAND ----------

# DBTITLE 1,Sample Protein Change Extraction
print("\nSample protein change extraction:")
display(
    df_protein.select(
        "gene_name",
        "variant_name",
        "protein_change_extracted",
        "cdna_change",
        "transcript_id",
        "is_frameshift",
        "is_nonsense"
    ).filter(col("protein_change_extracted").isNotNull())
     .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Parse Clinical Significance
print("\n" + "="*70)
print("STEP 5: NORMALIZE CLINICAL SIGNIFICANCE")
print("="*70)

df_clinical = (
    df_protein
    
    # Parse clinical_significance (can have semicolons: "Pathogenic; risk factor")
    .withColumn("clinical_sig_array",
                when(col("clinical_significance").isNotNull(),
                     split(col("clinical_significance"), ";"))
                .otherwise(array()))
    
    # Primary classification
    .withColumn("clinical_significance_normalized",
                when(col("clinical_significance").contains("Pathogenic") & 
                     ~col("clinical_significance").contains("Likely") &
                     ~col("clinical_significance").contains("Conflicting"), "Pathogenic")
                .when(col("clinical_significance").contains("Likely pathogenic"), "Likely Pathogenic")
                .when(col("clinical_significance").contains("Benign") & 
                      ~col("clinical_significance").contains("Likely") &
                      ~col("clinical_significance").contains("Conflicting"), "Benign")
                .when(col("clinical_significance").contains("Likely benign"), "Likely Benign")
                .when(col("clinical_significance").contains("Uncertain"), "Uncertain significance")
                .when(col("clinical_significance").contains("Conflicting"), "Conflicting interpretations")
                .otherwise(col("clinical_significance")))
    
    # Additional flags
    .withColumn("has_risk_factor",
                when(col("clinical_significance").isNotNull(),
                     lower(col("clinical_significance")).contains("risk factor"))
                .otherwise(False))
    
    .withColumn("has_drug_response",
                when(col("clinical_significance").isNotNull(),
                     lower(col("clinical_significance")).contains("drug response"))
                .otherwise(False))
)

print("Clinical significance normalized")

# COMMAND ----------

# DBTITLE 1,Parse Origin Field
print("\n" + "="*70)
print("STEP 6: PARSE ORIGIN")
print("="*70)

df_origin = (
    df_clinical
    
    # Parse origin (semicolon-separated: germline;maternal;paternal)
    .withColumn("origin_array",
                when(col("origin").isNotNull(),
                     split(col("origin"), ";"))
                .otherwise(array()))
    
    .withColumn("is_germline",
                when(col("origin").isNotNull(),
                     lower(col("origin")).contains("germline"))
                .otherwise(False))
    
    .withColumn("is_somatic",
                when(col("origin").isNotNull(),
                     lower(col("origin")).contains("somatic"))
                .otherwise(False))
    
    .withColumn("is_maternal",
                when(col("origin").isNotNull(),
                     lower(col("origin")).contains("maternal"))
                .otherwise(False))
    
    .withColumn("is_paternal",
                when(col("origin").isNotNull(),
                     lower(col("origin")).contains("paternal"))
                .otherwise(False))
)

print("Origin parsed")

# COMMAND ----------

# DBTITLE 1,Clean Core Fields
print("\n" + "="*70)
print("STEP 7: CLEAN CORE FIELDS")
print("="*70)

df_clean = (
    df_origin
    
    # Chromosome
    .withColumn("chromosome", 
                regexp_replace(upper(trim(col("chromosome"))), "^CHR", ""))
    .withColumn("chromosome",
                when(col("chromosome").isin(
                    '1','2','3','4','5','6','7','8','9','10',
                    '11','12','13','14','15','16','17','18','19','20',
                    '21','22','X','Y','MT'
                ), col("chromosome"))
                .otherwise(lit("1")))
    
    # Position (keep as string for now, will convert in processing)
    .withColumn("position_clean",
                when(col("position").isNotNull(),
                     col("position"))
                .otherwise(None))
    
    .withColumn("stop_position_clean",
                when(col("stop_position").isNotNull(),
                     col("stop_position"))
                .otherwise(col("position_clean")))
    
    # Variant type
    .withColumn("variant_type_clean",
                when((col("variant_type") != "Unknown") & col("variant_type").isNotNull(),
                     col("variant_type"))
                .otherwise("single nucleotide variant"))
    
    # Review status quality score
    .withColumn("review_quality_score",
                when(lower(col("review_status")).contains("expert"), 4)
                .when(lower(col("review_status")).contains("multiple submitters"), 3)
                .when(lower(col("review_status")).contains("single submitter"), 2)
                .when(lower(col("review_status")).contains("no assertion"), 1)
                .otherwise(0))
)

print("Core fields cleaned")

# COMMAND ----------

# DBTITLE 1,Calculate Enrichment Quality Score
print("\n" + "="*70)
print("STEP 8: CALCULATE DATA QUALITY SCORE")
print("="*70)

df_scored = (
    df_clean
    .withColumn("data_quality_score",
                (when(col("chromosome").isNotNull(), 1).otherwise(0)) +
                (when(col("position_clean").isNotNull(), 1).otherwise(0)) +
                (when(col("disease_count") > 0, 1).otherwise(0)) +
                (when(col("protein_change_extracted").isNotNull(), 1).otherwise(0)) +
                (when(col("phenotype_db_count") > 0, 1).otherwise(0)) +
                (when(col("review_quality_score") >= 2, 1).otherwise(0)))
    
    .withColumn("quality_tier",
                when(col("data_quality_score") >= 5, "High Quality")
                .when(col("data_quality_score") >= 3, "Medium Quality")
                .otherwise("Low Quality"))
)

print("Quality scoring complete")

# COMMAND ----------

# DBTITLE 1,Deduplication
print("\n" + "="*70)
print("STEP 9: DEDUPLICATION")
print("="*70)

df_dedup = (
    df_scored
    .dropDuplicates(["primary_accession"])
)

dedup_count = df_dedup.count()
print(f"After deduplication: {dedup_count:,} variants")
print(f"Removed: {raw_count - dedup_count:,} duplicates")

# COMMAND ----------

# DBTITLE 1,Create Final Enriched Table
print("\n" + "="*70)
print("STEP 10: CREATE ENRICHED VARIANT TABLE")
print("="*70)

df_variants_enriched = df_dedup.select(
    # Core identifiers
    col("variant_id"),
    col("primary_accession").alias("accession"),
    col("accession_count"),
    "gene_id",
    "gene_name",
    
    # Clinical classification
    col("clinical_significance_normalized").alias("clinical_significance"),
    col("clinical_significance").alias("clinical_significance_raw"),
    "has_risk_factor",
    "has_drug_response",
    "review_status",
    col("review_quality_score"),
    col("number_submitters"),
    
    # Disease information
    col("primary_disease").alias("disease"),
    col("disease_array"),
    col("disease_count"),
    "has_cancer_disease",
    "has_syndrome",
    "has_hereditary",
    
    # Phenotype databases
    "phenotype_ids",
    col("phenotype_db_count"),
    "has_omim",
    "has_orphanet",
    "has_mondo",
    
    # Genomic location
    "chromosome",
    col("position_clean").alias("position"),
    col("stop_position_clean").alias("stop_position"),
    "cytogenetic",
    col("variant_type_clean").alias("variant_type"),
    
    # Molecular details
    "variant_name",
    col("protein_change_extracted").alias("protein_change"),
    "cdna_change",
    "transcript_id",
    "is_frameshift",
    "is_nonsense",
    "is_splice",
    "reference_allele",
    "alternate_allele",
    
    # Origin
    col("origin_array"),
    "is_germline",
    "is_somatic",
    "is_maternal",
    "is_paternal",
    
    # Quality metrics
    "data_quality_score",
    "quality_tier",
    
    # Metadata
    "last_evaluated",
    "assembly"
)

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
print("\n" + "="*70)
print("SAVING ENRICHED VARIANTS TO SILVER LAYER")
print("="*70)

df_variants_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.variants_enriched")

saved_count = spark.table(f"{catalog_name}.silver.variants_enriched").count()
print(f"Saved to: {catalog_name}.silver.variants_enriched")
print(f"Verified: {saved_count:,} enriched variants")

# COMMAND ----------

# DBTITLE 1,Enrichment Summary Statistics
print("\n" + "="*70)
print("ENRICHMENT SUMMARY")
print("="*70)

enrichment_stats = {
    "total_variants": df_variants_enriched.count(),
    "with_protein_change": df_variants_enriched.filter(col("protein_change").isNotNull()).count(),
    "with_multiple_diseases": df_variants_enriched.filter(col("disease_count") > 1).count(),
    "with_omim": df_variants_enriched.filter(col("has_omim")).count(),
    "frameshift": df_variants_enriched.filter(col("is_frameshift")).count(),
    "nonsense": df_variants_enriched.filter(col("is_nonsense")).count(),
    "splice": df_variants_enriched.filter(col("is_splice")).count(),
    "germline": df_variants_enriched.filter(col("is_germline")).count(),
    "somatic": df_variants_enriched.filter(col("is_somatic")).count(),
    "high_quality": df_variants_enriched.filter(col("quality_tier") == "High Quality").count()
}

print("\nEnrichment Statistics:")
for key, value in enrichment_stats.items():
    print(f"  {key}: {value:,}")

# COMMAND ----------

# DBTITLE 1,Sample Enriched Data
print("\nSample enriched variants (high quality pathogenic):")
display(
    df_variants_enriched.filter(
        (col("clinical_significance") == "Pathogenic") &
        (col("quality_tier") == "High Quality")
    ).select(
        "gene_name",
        "disease",
        "protein_change",
        "variant_type",
        "disease_count",
        "review_quality_score"
    ).limit(10)
)

print("\n" + "="*70)
print("VARIANT ENRICHMENT COMPLETE")
print("="*70)
print("Next: Use silver.variants_enriched in feature engineering")
print("      for richer variant-disease associations")
print("="*70)
