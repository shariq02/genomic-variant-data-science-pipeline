# Databricks notebook source
# MAGIC %md
# MAGIC #### MAXIMUM DATA EXTRACTION - VARIANT DATA PROCESSING
# MAGIC ##### Replace "not specified/not provided" with actual disease names
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC **Purpose:** Extract ALL disease information and replace generic names
# MAGIC
# MAGIC **ENHANCEMENTS:**
# MAGIC 1. Parse `phenotype_ids` to extract ALL disease database IDs
# MAGIC 2. Create OMIM disease lookup table
# MAGIC 3. Replace "not specified/not provided" with actual disease names
# MAGIC 4. Split multiple `accession` (RCV) IDs into separate columns
# MAGIC 5. Extract variant components from `variant_name`
# MAGIC 6. Create comprehensive disease mapping

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace, split, explode,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, 
    concat_ws, array_distinct, flatten, size, array_contains,
    regexp_extract, array, concat, expr, element_at, first, collect_list,
    datediff, current_date, to_date, year, month, dayofyear, substring, instr
)
from pyspark.sql.types import StringType, ArrayType, IntegerType
from pyspark.sql import Window

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("MAXIMUM DATA EXTRACTION - VARIANT PROCESSING")
print("="*70)
print(f"Catalog: {catalog_name}")
print("Extracting disease information and creating mappings")

# COMMAND ----------

# DBTITLE 1,Read Raw Variant Data
print("\nReading variant data...")

df_variants_raw = spark.table(f"{catalog_name}.default.clinvar_all_variants")

raw_count = df_variants_raw.count()
print(f"Loaded {raw_count:,} raw variants")

# Show sample
print("\nSample data structure:")
df_variants_raw.select("gene_name", "disease", "phenotype_ids", "accession").show(3, truncate=80)

# COMMAND ----------

# DBTITLE 1,STEP 1: ULTRA-PARSE PHENOTYPE IDs
print("STEP 1: EXTRACT ALL DISEASE DATABASE IDs")
print("="*70)

# Example phenotype_ids:
# MONDO:MONDO:0013342,MedGen:C3150901,OMIM:613647,Orphanet:306511

df_phenotype_parsed = (
    df_variants_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    
    # Split phenotype_ids by multiple possible delimiters
    .withColumn("phenotype_ids_array",
                when(col("phenotype_ids").isNotNull(),
                     split(regexp_replace(col("phenotype_ids"), "[|;]", ","), ","))
                .otherwise(array()))
    
    # Extract ALL OMIM IDs (can be multiple)
    .withColumn("omim_id",
                when(col("phenotype_ids").isNotNull(),
                     trim(regexp_extract(col("phenotype_ids"), "OMIM:(\\d+)", 1)))
                .otherwise(None))
    .withColumn("omim_id",
                when((col("omim_id").isNull()) | (trim(col("omim_id")) == ""), None)
                .otherwise(col("omim_id")))
    
    # Extract additional OMIM IDs if present
    .withColumn("omim_id_secondary",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "OMIM:[^,]+,OMIM:(\\d+)", 1))
                .otherwise(None))
    
    # Extract Orphanet IDs
    .withColumn("orphanet_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "Orphanet:(\\d+)", 1))
                .otherwise(None))
    
    # Extract MONDO IDs
    .withColumn("mondo_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "MONDO:MONDO:(\\d+)", 1))
                .otherwise(None))
    
    # Extract MedGen IDs
    .withColumn("medgen_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "MedGen:(C\\d+)", 1))
                .otherwise(None))
    
    # Extract HPO (Human Phenotype Ontology) IDs
    .withColumn("hpo_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "HP:(\\d+)", 1))
                .otherwise(None))
    
    # Extract SNOMED IDs
    .withColumn("snomed_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "SNOMEDCT_US:(\\d+)", 1))
                .otherwise(None))
    
    # Count database coverage
    .withColumn("phenotype_db_count",
                (when(col("omim_id").isNotNull(), 1).otherwise(0)) +
                (when(col("orphanet_id").isNotNull(), 1).otherwise(0)) +
                (when(col("mondo_id").isNotNull(), 1).otherwise(0)) +
                (when(col("medgen_id").isNotNull(), 1).otherwise(0)) +
                (when(col("hpo_id").isNotNull(), 1).otherwise(0)) +
                (when(col("snomed_id").isNotNull(), 1).otherwise(0)))
    
    # Flag for well-annotated variants
    .withColumn("is_well_annotated", col("phenotype_db_count") >= 3)
)

print("Disease database IDs extracted: OMIM, Orphanet, MONDO, MedGen, HPO, SNOMED")

print("\nSample phenotype ID extraction:")
df_phenotype_parsed.select(
    "gene_name", "disease", "omim_id", "orphanet_id", "mondo_id", "phenotype_db_count"
).show(5, truncate=40)

# Show statistics
print("\nPhenotype DB Coverage:")
df_phenotype_parsed.groupBy("phenotype_db_count").count().orderBy("phenotype_db_count").show()

# COMMAND ----------

# DBTITLE 1,STEP 1.5: Join Allele ID Mapping
print("\nSTEP 1.5: INTEGRATE ALLELE ID MAPPING")
print("=" * 70)

# ------------------------------------------------------------------
# 1. Normalize ClinVar variant + allele IDs (CRITICAL FIX)
# ------------------------------------------------------------------
df_phenotype_parsed = (
    df_phenotype_parsed
    .withColumn("variant_id", trim(col("variant_id").cast("string")))
    .withColumn("allele_id", trim(col("allele_id").cast("string")))
)

# ------------------------------------------------------------------
# 2. Load allele mapping and normalize types
# ------------------------------------------------------------------
df_allele_mapping = (
    spark.table(f"{catalog_name}.default.allele_id_mapping")
    .withColumn("variation_id", trim(col("variation_id").cast("string")))
    .withColumn("allele_id", trim(col("allele_id").cast("string")))
)

allele_map_count = df_allele_mapping.count()
print(f"Loaded {allele_map_count:,} allele ID mappings")

# ------------------------------------------------------------------
# 3. Count BEFORE enrichment
# ------------------------------------------------------------------
total_variants = df_phenotype_parsed.count()

before_unknown = df_phenotype_parsed.filter(
    col("allele_id").isNull() | (col("allele_id") == "Unknown")
).count()

print(
    f"Before enrichment: {before_unknown:,} Unknown/Null allele_id "
    f"({before_unknown / total_variants * 100:.1f}%)"
)

# ------------------------------------------------------------------
# 4. Join to enrich allele_id
# ------------------------------------------------------------------
df_phenotype_parsed = (
    df_phenotype_parsed
    .join(
        df_allele_mapping.select(
            col("variation_id"),
            col("allele_id").alias("allele_id_new")
        ),
        df_phenotype_parsed.variant_id == df_allele_mapping.variation_id,
        "left"
    )
    .withColumn(
        "allele_id",
        coalesce(col("allele_id_new"), col("allele_id"), lit("Unknown"))
    )
    .drop("allele_id_new", "variation_id")
)

# ------------------------------------------------------------------
# 5. Count AFTER enrichment
# ------------------------------------------------------------------
after_unknown = df_phenotype_parsed.filter(
    col("allele_id").isNull() | (col("allele_id") == "Unknown")
).count()

print(
    f"After enrichment: {after_unknown:,} Unknown/Null allele_id "
    f"({after_unknown / total_variants * 100:.1f}%)"
)

print(f"Successfully enriched: {before_unknown - after_unknown:,} variants")


# COMMAND ----------

# DBTITLE 1,STEP 2: CREATE OMIM DISEASE LOOKUP TABLE
print("STEP 2: CREATE DISEASE NAME LOOKUP FROM OMIM")
print("="*70)

# Extract unique OMIM ID → Disease Name mappings
df_omim_lookup = (
    df_phenotype_parsed
    .filter(col("omim_id").isNotNull())
    .filter(col("disease").isNotNull())
    .filter(~lower(col("disease")).isin([
        "not provided", "not specified", "see cases", "", 
        "incidental discovery", "not applicable"
    ]))
    .select(
        col("omim_id"),
        col("disease").alias("disease_name")
    )
    .groupBy("omim_id")
    .agg(first("disease_name").alias("disease_name"))
    .orderBy("omim_id")
)

df_omim_lookup = (
    df_omim_lookup
    .withColumn("omim_id", trim(col("omim_id").cast("string")))
    .withColumn("disease_name", trim(col("disease_name")))
    .filter(trim(col("omim_id")) != "")
    .filter(trim(col("disease_name")) != "")
)

omim_count = df_omim_lookup.count()
print(f"Created OMIM lookup table with {omim_count:,} unique disease mappings")

print("\nSample OMIM lookup:")
df_omim_lookup.show(10, truncate=60)

# Save OMIM lookup table
df_omim_lookup.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.omim_disease_lookup")

print(f" Saved to: {catalog_name}.reference.omim_disease_lookup")

# COMMAND ----------

# DBTITLE 1,STEP 3: CREATE ORPHANET DISEASE LOOKUP TABLE
print("STEP 3: CREATE DISEASE NAME LOOKUP FROM ORPHANET")
print("="*70)

df_orphanet_lookup = (
    df_phenotype_parsed
    .filter(col("orphanet_id").isNotNull())
    .filter(col("disease").isNotNull())
    .filter(~lower(col("disease")).isin([
        "not provided", "not specified", "see cases", "",
        "incidental discovery", "not applicable"
    ]))
    .select(
        col("orphanet_id"),
        col("disease").alias("disease_name")
    )
    .groupBy("orphanet_id")
    .agg(first("disease_name").alias("disease_name"))
    .orderBy("orphanet_id")
)

orphanet_count = df_orphanet_lookup.count()
print(f"Created Orphanet lookup table with {orphanet_count:,} unique disease mappings")

print("\nSample Orphanet lookup:")
df_orphanet_lookup.show(10, truncate=60)

# Save Orphanet lookup table
df_orphanet_lookup.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.orphanet_disease_lookup")

print(f" Saved to: {catalog_name}.reference.orphanet_disease_lookup")

# COMMAND ----------

# DBTITLE 1,STEP 4: CREATE MONDO DISEASE LOOKUP TABLE
print("STEP 4: CREATE DISEASE NAME LOOKUP FROM MONDO")
print("="*70)

df_mondo_lookup = (
    df_phenotype_parsed
    .filter(col("mondo_id").isNotNull())
    .filter(col("disease").isNotNull())
    .filter(~lower(col("disease")).isin([
        "not provided", "not specified", "see cases", "",
        "incidental discovery", "not applicable"
    ]))
    .select(
        col("mondo_id"),
        col("disease").alias("disease_name")
    )
    .groupBy("mondo_id")
    .agg(first("disease_name").alias("disease_name"))
    .orderBy("mondo_id")
)

mondo_count = df_mondo_lookup.count()
print(f"Created MONDO lookup table with {mondo_count:,} unique disease mappings")

print("\nSample MONDO lookup:")
df_mondo_lookup.show(10, truncate=60)

# Save MONDO lookup table
df_mondo_lookup.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.mondo_disease_lookup")

print(f" Saved to: {catalog_name}.reference.mondo_disease_lookup")

# COMMAND ----------

# DBTITLE 1,STEP 5: REPLACE "NOT SPECIFIED" WITH ACTUAL DISEASE NAMES
print("STEP 5: ENRICH GENERIC DISEASE NAMES WITH REAL NAMES")
print("="*70)

# Load lookup tables
df_omim_lookup = spark.table(f"{catalog_name}.reference.omim_disease_lookup")
df_orphanet_lookup = spark.table(f"{catalog_name}.reference.orphanet_disease_lookup")
df_mondo_lookup = spark.table(f"{catalog_name}.reference.mondo_disease_lookup")

# Join to get disease names from OMIM
df_phenotype_for_omim = df_phenotype_parsed.withColumn(
    "omim_id_join", 
    when(col("omim_id").isNotNull(), trim(col("omim_id").cast("string"))).otherwise(None)
)

df_omim_for_join = df_omim_lookup.withColumn(
    "omim_id_join",
    trim(col("omim_id").cast("string"))
)

df_with_omim = (
    df_phenotype_for_omim
    .join(
        df_omim_for_join.select(
            col("omim_id_join"),
            col("disease_name").alias("omim_disease_name")
        ),
        "omim_id_join",
        "left"
    )
    .drop("omim_id_join")
)

join_count = df_with_omim.filter(col("omim_disease_name").isNotNull()).count()
print(f"OMIM join matches: {join_count:,}")


# Join to get disease names from Orphanet
df_with_orphanet = (
    df_with_omim
    .join(
        df_orphanet_lookup,
        df_with_omim.orphanet_id == df_orphanet_lookup.orphanet_id,
        "left"
    )
    .withColumnRenamed("disease_name", "orphanet_disease_name")
    .drop(df_orphanet_lookup.orphanet_id)
)

# Join to get disease names from MONDO
df_with_mondo = (
    df_with_orphanet
    .join(
        df_mondo_lookup,
        df_with_orphanet.mondo_id == df_mondo_lookup.mondo_id,
        "left"
    )
    .withColumnRenamed("disease_name", "mondo_disease_name")
    .drop(df_mondo_lookup.mondo_id)
)

# Create enriched disease column
df_disease_enriched = (
    df_with_mondo
    
    # Check if disease is generic/unspecified
    .withColumn("is_generic_disease",
                lower(col("disease")).isin([
                    "not provided", "not specified", "see cases", 
                    "incidental discovery", "", "not applicable"
                ]))
    
    # Replace generic names with actual disease names from databases
    .withColumn("disease_enriched",
                when(
                    col("is_generic_disease"),
                    # Try to get name from OMIM first, then Orphanet, then MONDO
                    coalesce(
                        col("omim_disease_name"),
                        col("orphanet_disease_name"),
                        col("mondo_disease_name"),
                        # If no name found, create ID reference
                        when(col("omim_id").isNotNull(), 
                             concat(lit("OMIM:"), col("omim_id"))),
                        when(col("orphanet_id").isNotNull(), 
                             concat(lit("Orphanet:"), col("orphanet_id"))),
                        when(col("mondo_id").isNotNull(), 
                             concat(lit("MONDO:"), col("mondo_id"))),
                        lit("Unknown disease")
                    )
                )
                .otherwise(col("disease")))
    
    # Flag for enriched diseases
    .withColumn("disease_was_enriched",
                when(
                    col("is_generic_disease") & 
                    (col("disease_enriched") != "Unknown disease"),
                    True
                ).otherwise(False))
    
    # Track enrichment source
    .withColumn("enrichment_source",
                when(col("disease_was_enriched"),
                     when(col("omim_disease_name").isNotNull(), "OMIM")
                     .when(col("orphanet_disease_name").isNotNull(), "Orphanet")
                     .when(col("mondo_disease_name").isNotNull(), "MONDO")
                     .otherwise("Database_ID"))
                .otherwise(None))
)

# Count enrichments
enriched_count = df_disease_enriched.filter(col("disease_was_enriched")).count()
generic_count = df_disease_enriched.filter(col("is_generic_disease")).count()
enrichment_rate = (enriched_count / generic_count * 100) if generic_count > 0 else 0

print(f"\n Disease Enrichment Statistics:")
print(f"   Generic diseases found: {generic_count:,}")
print(f"   Successfully enriched: {enriched_count:,}")
print(f"   Enrichment rate: {enrichment_rate:.1f}%")
print(f"   Remaining unknown: {generic_count - enriched_count:,}")

print("\n Sample disease enrichment:")
df_disease_enriched.filter(col("disease_was_enriched")).select(
    "gene_name", 
    col("disease").alias("original_disease"),
    "disease_enriched",
    "enrichment_source",
    "omim_id",
    "orphanet_id"
).show(10, truncate=50)

# Show enrichment source breakdown
print("\nEnrichment by source:")
df_disease_enriched.filter(col("disease_was_enriched")) \
    .groupBy("enrichment_source").count() \
    .orderBy(col("count").desc()).show()

# COMMAND ----------

# DBTITLE 1,STEP 6: PARSE ACCESSION (RCV) IDs
print("STEP 6: PARSE ACCESSION (RCV) IDs")
print("="*70)

# Example: RCV000000012|RCV005255549|RCV004998069

df_accession_parsed = (
    df_disease_enriched
    
    # Split accessions by pipe
    .withColumn("accession_array",
                when(col("accession").isNotNull(),
                     split(col("accession"), "\\|"))
                .otherwise(array()))
    
    # Extract individual RCV IDs (up to 10)
    .withColumn("rcv_id_1", when(size(col("accession_array")) >= 1, col("accession_array")[0]).otherwise(None))
    .withColumn("rcv_id_2", when(size(col("accession_array")) >= 2, col("accession_array")[1]).otherwise(None))
    .withColumn("rcv_id_3", when(size(col("accession_array")) >= 3, col("accession_array")[2]).otherwise(None))
    .withColumn("rcv_id_4", when(size(col("accession_array")) >= 4, col("accession_array")[3]).otherwise(None))
    .withColumn("rcv_id_5", when(size(col("accession_array")) >= 5, col("accession_array")[4]).otherwise(None))
    .withColumn("rcv_id_6", when(size(col("accession_array")) >= 6, col("accession_array")[5]).otherwise(None))
    .withColumn("rcv_id_7", when(size(col("accession_array")) >= 7, col("accession_array")[6]).otherwise(None))
    .withColumn("rcv_id_8", when(size(col("accession_array")) >= 8, col("accession_array")[7]).otherwise(None))
    .withColumn("rcv_id_9", when(size(col("accession_array")) >= 9, col("accession_array")[8]).otherwise(None))
    .withColumn("rcv_id_10", when(size(col("accession_array")) >= 10, col("accession_array")[9]).otherwise(None))
    
    .withColumn("total_rcv_ids", size(col("accession_array")))
    
    # Flag for variants with multiple submissions
    .withColumn("has_multiple_submissions", col("total_rcv_ids") > 1)
    
    # Extract primary RCV number
    .withColumn("rcv_extracted",
                when(col("rcv_id_1").isNotNull(),
                     regexp_extract(col("rcv_id_1"), "RCV(\\d+)", 1))
                .otherwise(lit("")))
    
    .withColumn("primary_rcv_number",
                when((col("rcv_extracted") != "") & (col("rcv_extracted").isNotNull()),
                     col("rcv_extracted").cast("long"))
                .otherwise(None))
    
    .drop("rcv_extracted")
)

print("RCV accession IDs parsed (up to 10 per variant)")

multi_count = df_accession_parsed.filter(col("has_multiple_submissions")).count()
print(f"\nVariants with multiple submissions: {multi_count:,}")

print("\nSample RCV parsing:")
df_accession_parsed.filter(col("has_multiple_submissions")).select(
    "gene_name", "rcv_id_1", "rcv_id_2", "rcv_id_3", "total_rcv_ids"
).show(5, truncate=40)

# COMMAND ----------

# DBTITLE 1,STEP 7: PARSE VARIANT NAME COMPONENTS
print("STEP 7: PARSE VARIANT NAME FOR COMPONENTS")
print("="*70)

# Example: NM_014855.3(AP5Z1):c.80_83delinsTGCTGTAAACTGTAACTGTAAA (p.Arg27_Ile28delinsLeuLeuTer)

df_variant_parsed = (
    df_accession_parsed
    
    # Extract transcript ID (NM_ RefSeq)
    .withColumn("transcript_id",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "(NM_\\d+\\.\\d+)", 1))
                .otherwise(None))
    
    # Extract transcript version
    .withColumn("transcript_version_str",
                when(col("transcript_id").isNotNull(),
                     regexp_extract(col("transcript_id"), "NM_\\d+\\.(\\d+)", 1))
                .otherwise(lit("")))
    
    .withColumn("transcript_version",
                when((col("transcript_version_str") != "") & (col("transcript_version_str").isNotNull()),
                     col("transcript_version_str").cast("int"))
                .otherwise(None))
    
    .drop("transcript_version_str")
    
    # Extract gene from variant name (as backup)
    .withColumn("variant_gene_symbol",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "\\(([A-Z0-9-]+)\\)", 1))
                .otherwise(None))
    
    # Extract cDNA change
    .withColumn("cdna_change",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), ":(c\\.[^\\s\\(]+)", 1))
                .otherwise(None))
    
    # Extract protein change
    .withColumn("protein_change",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "\\(p\\.([^)]+)\\)", 1))
                .otherwise(None))
    
    # Extract position from cDNA change
    .withColumn("cdna_position_str",
                when(col("cdna_change").isNotNull(),
                     regexp_extract(col("cdna_change"), "c\\.(\\d+)", 1))
                .otherwise(lit("")))
    
    .withColumn("cdna_position",
                when((col("cdna_position_str") != "") & (col("cdna_position_str").isNotNull()),
                     col("cdna_position_str").cast("int"))
                .otherwise(None))
    
    .drop("cdna_position_str")
    
    # Flag for frameshift variants
    .withColumn("is_frameshift_variant",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("frameshift|fs"))
                .otherwise(False))
    
    # Flag for nonsense variants (stop codon)
    .withColumn("is_nonsense_variant",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("ter|\\*|stop|x\\)"))
                .otherwise(False))
    
    # Flag for splice variants
    .withColumn("is_splice_variant",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("splice|\\+\\d+|\\-\\d+"))
                .otherwise(False))
    
    # Flag for missense variants
    .withColumn("is_missense_variant",
                when(col("variant_name").isNotNull() & col("protein_change").isNotNull(),
                     ~lower(col("variant_name")).rlike("ter|\\*|fs|frameshift|splice|del|ins|dup"))
                .otherwise(False))
)

print("Variant name components extracted")

print("\nSample variant parsing:")
df_variant_parsed.select(
    "gene_name", "transcript_id", "cdna_change", "protein_change", 
    "is_frameshift_variant", "is_nonsense_variant"
).show(5, truncate=40)

# Show mutation type distribution
print("\nMutation type distribution:")
df_variant_parsed.select(
    spark_sum(when(col("is_frameshift_variant"), 1).otherwise(0)).alias("Frameshift"),
    spark_sum(when(col("is_nonsense_variant"), 1).otherwise(0)).alias("Nonsense"),
    spark_sum(when(col("is_splice_variant"), 1).otherwise(0)).alias("Splice"),
    spark_sum(when(col("is_missense_variant"), 1).otherwise(0)).alias("Missense")
).show()

# COMMAND ----------

# DBTITLE 1,STEP 8: PARSE DISEASE INTO ARRAY
print("STEP 8: PARSE MULTIPLE DISEASES PER VARIANT")
print("="*70)

df_disease_array = (
    df_variant_parsed
    
    # Parse diseases (pipe-separated)
    .withColumn("disease_array",
                when(col("disease_enriched").isNotNull(),
                     split(col("disease_enriched"), "\\|"))
                .otherwise(array()))
    
    # Primary disease
    .withColumn("primary_disease",
                when(size(col("disease_array")) > 0,
                     col("disease_array").getItem(0))
                .otherwise(col("disease_enriched")))
    
    # Secondary disease (if exists)
    .withColumn("secondary_disease",
                when(size(col("disease_array")) > 1,
                     col("disease_array").getItem(1))
                .otherwise(None))
    
    # Count diseases per variant
    .withColumn("disease_count", size(col("disease_array")))
    
    # Flag for multi-disease variants
    .withColumn("has_multiple_diseases", col("disease_count") > 1)
)

print("Disease arrays parsed")

multi_disease_count = df_disease_array.filter(col("has_multiple_diseases")).count()
print(f"\nVariants associated with multiple diseases: {multi_disease_count:,}")

# COMMAND ----------

# DBTITLE 1,STEP 9: CLINICAL SIGNIFICANCE PARSING
print("STEP 9: PARSE CLINICAL SIGNIFICANCE")
print("="*70)

df_clinical = (
    df_disease_array
    
    # Standardize clinical significance
    .withColumn("clinical_significance_clean",
                when(col("clinical_significance").isNotNull(),
                     trim(col("clinical_significance")))
                .otherwise("Unknown"))
    
    # Simple classification
    .withColumn("clinical_significance_simple",
                when(lower(col("clinical_significance_clean")).contains("pathogenic") &
                     ~lower(col("clinical_significance_clean")).contains("likely") &
                     ~lower(col("clinical_significance_clean")).contains("conflict"),
                     "Pathogenic")
                .when(lower(col("clinical_significance_clean")).contains("likely pathogenic"),
                      "Likely Pathogenic")
                .when(lower(col("clinical_significance_clean")).contains("benign") &
                      ~lower(col("clinical_significance_clean")).contains("likely") &
                      ~lower(col("clinical_significance_clean")).contains("conflict"),
                      "Benign")
                .when(lower(col("clinical_significance_clean")).contains("likely benign"),
                      "Likely Benign")
                .when(lower(col("clinical_significance_clean")).contains("uncertain"),
                      "VUS")
                .when(lower(col("clinical_significance_clean")).contains("conflict"),
                      "Conflicting")
                .when(lower(col("clinical_significance_clean")).contains("risk"),
                      "Risk Factor")
                .when(lower(col("clinical_significance_clean")).contains("drug"),
                      "Drug Response")
                .otherwise("Other"))
    
    # Binary pathogenic flag
    .withColumn("is_pathogenic",
                col("clinical_significance_simple").isin(["Pathogenic", "Likely Pathogenic"]))
    
    .withColumn("is_benign",
                col("clinical_significance_simple").isin(["Benign", "Likely Benign"]))
    
    .withColumn("is_vus",
                col("clinical_significance_simple") == "VUS")
)

print("Clinical significance parsed")

print("\nClinical significance distribution:")
df_clinical.groupBy("clinical_significance_simple").count() \
    .orderBy(col("count").desc()).show()

# COMMAND ----------

# DBTITLE 1,STEP 10: ORIGIN AND INHERITANCE PARSING
print("STEP 10: PARSE VARIANT ORIGIN")
print("="*70)

df_origin = (
    df_clinical
    
    # Parse origin
    .withColumn("is_germline",
                lower(coalesce(col("origin_simple"), col("origin"), lit(""))).contains("germline"))
    
    .withColumn("is_somatic",
                lower(coalesce(col("origin_simple"), col("origin"), lit(""))).contains("somatic"))
    
    .withColumn("is_de_novo",
                lower(coalesce(col("origin_simple"), col("origin"), lit(""))).contains("de novo"))
    
    .withColumn("is_inherited",
                lower(coalesce(col("origin_simple"), col("origin"), lit(""))).rlike("inherited|maternal|paternal"))
    
    .withColumn("is_maternal",
                lower(coalesce(col("origin_simple"), col("origin"), lit(""))).contains("maternal"))
    
    .withColumn("is_paternal",
                lower(coalesce(col("origin_simple"), col("origin"), lit(""))).contains("paternal"))
)

print("Origin flags created")

print("\nOrigin distribution:")
df_origin.select(
    spark_sum(when(col("is_germline"), 1).otherwise(0)).alias("Germline"),
    spark_sum(when(col("is_somatic"), 1).otherwise(0)).alias("Somatic"),
    spark_sum(when(col("is_de_novo"), 1).otherwise(0)).alias("De Novo"),
    spark_sum(when(col("is_inherited"), 1).otherwise(0)).alias("Inherited")
).show()

# COMMAND ----------

# DBTITLE 1,STEP 11: VARIANT TYPE CLASSIFICATION
print("STEP 11: CLASSIFY VARIANT TYPES")
print("="*70)

df_variant_types = (
    df_origin
    
    # Classify by variant_type field
    .withColumn("is_snv",
                lower(coalesce(col("variant_type"), lit(""))).contains("single nucleotide"))
    
    .withColumn("is_deletion",
                lower(coalesce(col("variant_type"), lit(""))).contains("deletion"))
    
    .withColumn("is_insertion",
                lower(coalesce(col("variant_type"), lit(""))).contains("insertion"))
    
    .withColumn("is_duplication",
                lower(coalesce(col("variant_type"), lit(""))).contains("duplication"))
    
    .withColumn("is_indel",
                lower(coalesce(col("variant_type"), lit(""))).contains("indel"))
    
    .withColumn("is_cnv",
                lower(coalesce(col("variant_type"), lit(""))).rlike("copy number|cnv"))
    
    .withColumn("is_microsatellite",
                lower(coalesce(col("variant_type"), lit(""))).contains("microsatellite"))
)

print("Variant type flags created")

# COMMAND ----------

# DBTITLE 1,STEP 12: REVIEW STATUS SCORING
print("STEP 12: CALCULATE REVIEW QUALITY SCORE")
print("="*70)

df_quality = (
    df_variant_types
    
    # Review quality score (0-4)
    .withColumn("review_quality_score",
                when(lower(col("review_status")).contains("practice guideline"), 4)
                .when(lower(col("review_status")).contains("reviewed by expert panel"), 3)
                .when(lower(col("review_status")).contains("criteria provided, multiple submitters"), 2)
                .when(lower(col("review_status")).contains("criteria provided, single submitter"), 1)
                .otherwise(0))
    
    # Quality tier
    .withColumn("quality_tier",
                when(col("review_quality_score") >= 3, "High Quality")
                .when(col("review_quality_score") == 2, "Medium Quality")
                .when(col("review_quality_score") == 1, "Low Quality")
                .otherwise("No Criteria"))
    
    # Recent evaluation flag (within 2 years)
    .withColumn("last_evaluated_clean",
                when(
                    col("last_evaluated").isNotNull() & 
                    (trim(col("last_evaluated")) != "") &
                    (trim(col("last_evaluated")) != "-") &
                    (length(trim(col("last_evaluated"))) > 2),
                    trim(col("last_evaluated"))
                )
                .otherwise(None))
    
    .withColumn("last_evaluated_date",
                when(col("last_evaluated_clean").isNotNull(),
                     coalesce(
                         expr("try_cast(to_date(last_evaluated_clean, 'MMM dd, yyyy') as date)"),
                         expr("try_cast(to_date(last_evaluated_clean, 'yyyy-MM-dd') as date)"),
                         expr("try_cast(to_date(last_evaluated_clean, 'MM/dd/yyyy') as date)"),
                         expr("try_cast(to_date(last_evaluated_clean, 'dd-MMM-yy') as date)")
                     ))
                .otherwise(None))
    
    .withColumn("days_since_evaluation",
                when(col("last_evaluated_date").isNotNull(),
                     datediff(current_date(), col("last_evaluated_date")))
                .otherwise(None))
    
    .withColumn("is_recently_evaluated",
                when(col("days_since_evaluation").isNotNull() & (col("days_since_evaluation") <= 730), True)
                .otherwise(False))
    
    .drop("last_evaluated_clean")
)

print("Quality metrics calculated")

print("\nQuality tier distribution:")
df_quality.groupBy("quality_tier").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# DBTITLE 1,STEP 13: CREATE FINAL ULTRA-ENRICHED VARIANT TABLE
print("STEP 13: CREATE ULTRA-ENRICHED VARIANT TABLE")
print("="*70)

df_variants_ultra_enriched = df_quality.select(
    # Core identifiers
    "variant_id",
    "allele_id",
    "gene_id",
    "gene_name",
    
    # EXPANDED: RCV accession IDs (NEW!)
    "rcv_id_1", "rcv_id_2", "rcv_id_3", "rcv_id_4", "rcv_id_5",
    "rcv_id_6", "rcv_id_7", "rcv_id_8", "rcv_id_9", "rcv_id_10",
    "total_rcv_ids",
    "has_multiple_submissions",
    "primary_rcv_number",
    
    # EXPANDED: Disease information (ENRICHED!)
    col("disease").alias("disease_original"),
    "disease_enriched",
    "disease_was_enriched",
    "enrichment_source",
    "is_generic_disease",
    
    # EXPANDED: Disease arrays (NEW!)
    "disease_array",
    "primary_disease",
    "secondary_disease",
    "disease_count",
    "has_multiple_diseases",
    
    # EXPANDED: ALL disease database IDs (NEW!)
    "omim_id",
    "omim_id_secondary",
    "orphanet_id",
    "mondo_id",
    "medgen_id",
    "hpo_id",
    "snomed_id",
    "phenotype_db_count",
    "is_well_annotated",
    
    # EXPANDED: Variant name components (NEW!)
    "transcript_id",
    "transcript_version",
    "variant_gene_symbol",
    "cdna_change",
    "protein_change",
    "cdna_position",
    
    # EXPANDED: Mutation type flags (NEW!)
    "is_frameshift_variant",
    "is_nonsense_variant",
    "is_splice_variant",
    "is_missense_variant",
    
    # Clinical significance
    col("clinical_significance").alias("clinical_significance_original"),
    "clinical_significance_clean",
    "clinical_significance_simple",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    
    # Genomic location
    "chromosome",
    "position",
    "stop_position",
    "cytogenetic",
    
    # Variant details
    "variant_type",
    "variant_name",
    "reference_allele",
    "alternate_allele",
    
    # EXPANDED: Variant type flags (NEW!)
    "is_snv",
    "is_deletion",
    "is_insertion",
    "is_duplication",
    "is_indel",
    "is_cnv",
    "is_microsatellite",
    
    # Origin
    "origin",
    "origin_simple",
    "is_germline",
    "is_somatic",
    "is_de_novo",
    "is_inherited",
    "is_maternal",
    "is_paternal",
    
    # EXPANDED: Quality metrics (NEW!)
    "review_status",
    "review_quality_score",
    "quality_tier",
    "number_submitters",
    "last_evaluated",
    "last_evaluated_date",
    "days_since_evaluation",
    "is_recently_evaluated",
    
    # Assembly
    "assembly",
    
    # Original phenotype IDs for reference
    "phenotype_ids"
)

final_count = df_variants_ultra_enriched.count()
print(f" Ultra-enriched variant table created!")
print(f"   Total variants: {final_count:,}")
print(f"   Total columns: {len(df_variants_ultra_enriched.columns)}")

# COMMAND ----------

# DBTITLE 1,Save Ultra-Enriched Variants
print("SAVING ULTRA-ENRICHED VARIANTS TO SILVER LAYER")
print("="*70)

df_variants_ultra_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.variants_ultra_enriched")

saved_count = spark.table(f"{catalog_name}.silver.variants_ultra_enriched").count()
print(f" Saved to: {catalog_name}.silver.variants_ultra_enriched")
print(f" Verified: {saved_count:,} ultra-enriched variants")

# COMMAND ----------

# DBTITLE 1,Maximum Extraction Summary
print(" MAXIMUM DATA EXTRACTION COMPLETE - VARIANTS")
print("="*70)

print("\n NEW FEATURES ADDED:")
print("1.  Disease Database IDs: OMIM, Orphanet, MONDO, MedGen, HPO, SNOMED (12 columns)")
print("2.  Disease Name Enrichment: Replaced 'not specified' with real names (3 columns)")
print("3.  RCV Accession IDs: rcv_id_1 to rcv_id_10 (13 columns)")
print("4.  Variant Components: transcript_id, cdna_change, protein_change, etc. (6 columns)")
print("5.  Disease Arrays: Multiple diseases per variant (4 columns)")
print("6.  Mutation Type Flags: frameshift, nonsense, splice, missense (4 columns)")
print("7.  Variant Type Flags: SNV, deletion, insertion, CNV, etc. (7 columns)")
print("8.  Quality Metrics: review_quality_score, quality_tier, recency (6 columns)")
print("9.  Lookup Tables: OMIM, Orphanet, MONDO disease mappings (3 tables)")

# Statistics
enrichment_stats = {
    "total_variants": df_variants_ultra_enriched.count(),
    "generic_diseases_before": df_disease_enriched.filter(col("is_generic_disease")).count(),
    "successfully_enriched": df_disease_enriched.filter(col("disease_was_enriched")).count(),
    "enrichment_rate_pct": (enriched_count / generic_count * 100) if generic_count > 0 else 0,
    "with_omim_ids": df_variants_ultra_enriched.filter(col("omim_id").isNotNull()).count(),
    "with_orphanet_ids": df_variants_ultra_enriched.filter(col("orphanet_id").isNotNull()).count(),
    "with_mondo_ids": df_variants_ultra_enriched.filter(col("mondo_id").isNotNull()).count(),
    "multiple_submissions": df_variants_ultra_enriched.filter(col("has_multiple_submissions")).count(),
    "well_annotated": df_variants_ultra_enriched.filter(col("is_well_annotated")).count(),
    "high_quality": df_variants_ultra_enriched.filter(col("quality_tier") == "High Quality").count()
}

print("\n ENRICHMENT STATISTICS:")
for key, value in enrichment_stats.items():
    if "rate" in key or "pct" in key:
        print(f"  {key}: {value:.1f}%")
    else:
        pct = (value / enrichment_stats["total_variants"] * 100) if enrichment_stats["total_variants"] > 0 else 0
        print(f"  {key}: {value:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Sample Enriched Data
print(" SAMPLE ENRICHED VARIANTS")
print("="*70)

print("\n1. Disease Enrichment Examples (Before → After):")
display(
    df_variants_ultra_enriched
    .filter(col("disease_was_enriched"))
    .select(
        "gene_name",
        "disease_original",
        "disease_enriched",
        "enrichment_source",
        "omim_id",
        "orphanet_id"
    )
    .limit(20)
)

print("\n2. Multi-Disease Variants:")
display(
    df_variants_ultra_enriched
    .filter(col("has_multiple_diseases"))
    .select(
        "gene_name",
        "primary_disease",
        "secondary_disease",
        "disease_count"
    )
    .limit(10)
)

print("\n3. High-Quality Pathogenic Variants:")
display(
    df_variants_ultra_enriched
    .filter(col("is_pathogenic") & (col("quality_tier") == "High Quality"))
    .select(
        "gene_name",
        "disease_enriched",
        "clinical_significance_simple",
        "quality_tier",
        "review_quality_score",
        "transcript_id"
    )
    .limit(10)
)

print(" VARIANT PROCESSING COMPLETE!")
print("="*70)
print(f" Output Table: {catalog_name}.silver.variants_ultra_enriched")
print(f" Lookup Tables:")
print(f"   - {catalog_name}.reference.omim_disease_lookup")
print(f"   - {catalog_name}.reference.orphanet_disease_lookup")
print(f"   - {catalog_name}.reference.mondo_disease_lookup")
print("\nNext steps:")
print("1. Use disease_enriched in all downstream analysis")
print("2. Link genes to diseases via OMIM IDs")
print("3. Create comprehensive gene-disease association table")
print("4. Re-run statistical analysis to see improvements")
