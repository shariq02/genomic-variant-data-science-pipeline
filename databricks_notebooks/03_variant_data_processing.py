# Databricks notebook source
# MAGIC %md
# MAGIC #### ULTRA-ENRICHED VARIANT DATA PROCESSING
# MAGIC ##### Extract MAXIMUM data from all variant fields
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC **Purpose:** Extract every possible piece of information from variant data
# MAGIC **Input:** workspace.default.clinvar_all_variants (4M+ variants)  
# MAGIC **Output:** workspace.silver.variants_ultra_enriched

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace, split, explode,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, 
    concat_ws, array_distinct, flatten, size, array_contains,
    regexp_extract, array, datediff, current_date, to_date, year, month,
    dayofyear, concat, substring
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

print("="*70)
print("ULTRA-ENRICHED VARIANT DATA PROCESSING")
print("="*70)
print(f"Catalog: {catalog_name}")
print(f"Input: default.clinvar_all_variants (4M+ variants)")
print("Extracting MAXIMUM data from all fields")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Raw Variant Data
print("\nReading ALL variant data...")

df_variants_raw = spark.table(f"{catalog_name}.default.clinvar_all_variants")

raw_count = df_variants_raw.count()
print(f"Loaded {raw_count:,} raw variants")

# COMMAND ----------

# DBTITLE 1,Save to Bronze Layer
df_variants_raw.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.variants_raw")

print(f"Saved to {catalog_name}.bronze.variants_raw")

# COMMAND ----------

# DBTITLE 1,STEP 1: Ultra-Parse Disease with Phenotype ID Fallback
print("\n" + "="*70)
print("STEP 1: ULTRA-PARSE DISEASES WITH DATABASE IDS")
print("="*70)

df_parsed = (
    df_variants_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    
    # Parse diseases (pipe-separated)
    .withColumn("disease_array",
                when(col("disease").isNotNull() & (col("disease") != "not provided"),
                     split(col("disease"), "\\|"))
                .otherwise(array()))
    
    # Primary disease
    .withColumn("primary_disease",
                when(size(col("disease_array")) > 0,
                     col("disease_array").getItem(0))
                .otherwise(None))
    
    # Count diseases
    .withColumn("disease_count", size(col("disease_array")))
    
    # Extract ALL disease database IDs
    .withColumn("omim_disease_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "OMIM:(\\d+)", 1))
                .otherwise(None))
    
    .withColumn("orphanet_disease_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "Orphanet:(\\d+)", 1))
                .otherwise(None))
    
    .withColumn("mondo_disease_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "MONDO:MONDO:(\\d+)", 1))
                .otherwise(None))
    
    .withColumn("medgen_disease_id",
                when(col("phenotype_ids").isNotNull(),
                     regexp_extract(col("phenotype_ids"), "MedGen:(C\\d+)", 1))
                .otherwise(None))
    
    # Create enriched disease identifier (use DB IDs as fallback)
    .withColumn("disease_enriched",
                when(col("primary_disease").isNotNull() &
                     ~lower(col("primary_disease")).isin([
                         "not provided", "not specified", "see cases", 
                         "incidental discovery", ""
                     ]),
                     col("primary_disease"))
                .when(col("omim_disease_id").isNotNull(),
                     concat(lit("OMIM:"), col("omim_disease_id")))
                .when(col("orphanet_disease_id").isNotNull(),
                     concat(lit("Orphanet:"), col("orphanet_disease_id")))
                .when(col("mondo_disease_id").isNotNull(),
                     concat(lit("MONDO:"), col("mondo_disease_id")))
                .when(col("medgen_disease_id").isNotNull(),
                     concat(lit("MedGen:"), col("medgen_disease_id")))
                .otherwise(col("primary_disease")))
    
    # Disease keywords (expanded)
    .withColumn("has_cancer_disease",
                lower(col("disease")).rlike("(?i)(cancer|carcinoma|tumor|oncogene|sarcoma|lymphoma|leukemia|melanoma)"))
    .withColumn("has_syndrome",
                lower(col("disease")).rlike("(?i)(syndrome)"))
    .withColumn("has_hereditary",
                lower(col("disease")).rlike("(?i)(hereditary|familial|congenital)"))
    .withColumn("has_rare_disease",
                lower(col("disease")).rlike("(?i)(rare|orphan)"))
    
    # Disease severity keywords
    .withColumn("has_lethal_keyword",
                lower(col("disease")).rlike("(?i)(lethal|fatal|deadly)"))
    .withColumn("has_progressive_keyword",
                lower(col("disease")).rlike("(?i)(progressive|degenerative)"))
)

print("Diseases ultra-parsed with database IDs")

# COMMAND ----------

# DBTITLE 1,STEP 2: Ultra-Parse Phenotype Database IDs
print("\n" + "="*70)
print("STEP 2: COMPREHENSIVE PHENOTYPE DATABASE PARSING")
print("="*70)

df_phenotypes = (
    df_parsed
    
    # Parse phenotype IDs
    .withColumn("phenotype_array",
                when(col("phenotype_ids").isNotNull(),
                     split(col("phenotype_ids"), "\\|"))
                .otherwise(array()))
    
    # Check for ALL databases
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
    .withColumn("has_medgen",
                when(col("phenotype_ids").isNotNull(),
                     lower(col("phenotype_ids")).contains("medgen"))
                .otherwise(False))
    .withColumn("has_hpo",
                when(col("phenotype_ids").isNotNull(),
                     lower(col("phenotype_ids")).contains("human phenotype"))
                .otherwise(False))
    
    # Database coverage score
    .withColumn("phenotype_db_coverage",
                (when(col("has_omim"), 1).otherwise(0)) +
                (when(col("has_orphanet"), 1).otherwise(0)) +
                (when(col("has_mondo"), 1).otherwise(0)) +
                (when(col("has_medgen"), 1).otherwise(0)) +
                (when(col("has_hpo"), 1).otherwise(0)))
    
    .withColumn("phenotype_db_count",
                when(col("phenotype_ids").isNotNull(),
                     size(split(col("phenotype_ids"), ",")))
                .otherwise(0))
)

print("Phenotype databases comprehensively parsed")

# COMMAND ----------

# DBTITLE 1,STEP 3: Ultra-Parse Accession and Submission Details
print("\n" + "="*70)
print("STEP 3: COMPREHENSIVE ACCESSION AND SUBMISSION PARSING")
print("="*70)

df_accessions = (
    df_phenotypes
    
    # Parse accessions
    .withColumn("accession_array",
                when(col("accession").isNotNull() & (col("accession") != "Unknown"),
                     split(col("accession"), "\\|"))
                .otherwise(array()))
    
    # Primary accession
    .withColumn("primary_accession",
                when(size(col("accession_array")) > 0,
                     col("accession_array").getItem(0))
                .otherwise(col("accession")))
    
    # Submission metrics
    .withColumn("accession_count", size(col("accession_array")))
    .withColumn("is_multi_submission",
                when(col("accession_count") > 1, True).otherwise(False))
    
    # Extract submitter count from review_status
    .withColumn("has_multiple_submitters",
                when(col("review_status").isNotNull(),
                     lower(col("review_status")).contains("multiple"))
                .otherwise(False))
    
    .withColumn("has_expert_review",
                when(col("review_status").isNotNull(),
                     lower(col("review_status")).contains("expert"))
                .otherwise(False))
    
    .withColumn("has_conflicts",
                when(col("review_status").isNotNull(),
                     lower(col("review_status")).contains("conflict"))
                .otherwise(False))
    
    .withColumn("has_no_assertion",
                when(col("review_status").isNotNull(),
                     lower(col("review_status")).contains("no assertion"))
                .otherwise(False))
)

print("Accessions and submissions parsed")

# COMMAND ----------

# DBTITLE 1,STEP 4: ULTRA-Parse Variant Name for Maximum Detail
print("\n" + "="*70)
print("STEP 4: ULTRA-PARSE VARIANT NAME")
print("="*70)

df_protein = (
    df_accessions
    
    # Extract protein change
    .withColumn("protein_change_full",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "\\(p\\.([^)]+)\\)", 1))
                .otherwise(None))
    
    # Extract amino acid positions
    .withColumn("aa_start_position",
                when((col("protein_change_full").isNotNull()) &
                     (regexp_extract(col("protein_change_full"), "(\\d+)", 1) != ""),
                     regexp_extract(col("protein_change_full"), "(\\d+)", 1).cast(IntegerType()))
                .otherwise(None))
    
    # Extract reference amino acid
    .withColumn("ref_amino_acid",
                when(col("protein_change_full").isNotNull(),
                     regexp_extract(col("protein_change_full"), "^([A-Z][a-z]{2})", 1))
                .otherwise(None))
    
    # Extract alternate amino acid
    .withColumn("alt_amino_acid",
                when(col("protein_change_full").isNotNull(),
                     regexp_extract(col("protein_change_full"), "([A-Z][a-z]{2})$", 1))
                .otherwise(None))
    
    # Extract cDNA change details
    .withColumn("cdna_change_full",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), ":c\\.([^\\s\\(]+)", 1))
                .otherwise(None))
    
    # Extract nucleotide position
    .withColumn("cdna_position",
                when((col("cdna_change_full").isNotNull()) &
                     (regexp_extract(col("cdna_change_full"), "(\\d+)", 1) != ""),
                     regexp_extract(col("cdna_change_full"), "(\\d+)", 1).cast(IntegerType()))
                .otherwise(None))
    
    # Extract transcript ID and version
    .withColumn("transcript_id",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "(NM_\\d+)", 1))
                .otherwise(None))
    
    .withColumn("transcript_version",
                when(col("variant_name").isNotNull(),
                     regexp_extract(col("variant_name"), "NM_\\d+\\.(\\d+)", 1))
                .otherwise(None))
    
    # Detailed mutation type classification
    .withColumn("is_frameshift",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).contains("fs"))
                .otherwise(False))
    
    .withColumn("is_nonsense",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("(?i)(ter|\\*|stop)"))
                .otherwise(False))
    
    .withColumn("is_splice",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).rlike("(?i)(splice|\\+|\\-)"))
                .otherwise(False))
    
    .withColumn("is_missense",
                when((col("ref_amino_acid").isNotNull()) &
                     (col("alt_amino_acid").isNotNull()) &
                     (col("ref_amino_acid") != col("alt_amino_acid")) &
                     ~col("is_nonsense") &
                     ~col("is_frameshift"),
                     True)
                .otherwise(False))
    
    .withColumn("is_deletion",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).contains("del"))
                .otherwise(False))
    
    .withColumn("is_insertion",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).contains("ins"))
                .otherwise(False))
    
    .withColumn("is_duplication",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).contains("dup"))
                .otherwise(False))
    
    .withColumn("is_indel",
                when(col("variant_name").isNotNull(),
                     lower(col("variant_name")).contains("delins"))
                .otherwise(False))
    
    # Mutation severity score
    .withColumn("mutation_severity_score",
                when(col("is_nonsense"), 5)
                .when(col("is_frameshift"), 5)
                .when(col("is_splice"), 4)
                .when(col("is_deletion"), 3)
                .when(col("is_insertion"), 3)
                .when(col("is_missense"), 2)
                .otherwise(1))
)

print("Variant names ultra-parsed with mutation details")

# COMMAND ----------

# DBTITLE 1,STEP 5: Parse Cytogenetic Location Details
print("\n" + "="*70)
print("STEP 5: PARSE CYTOGENETIC LOCATION")
print("="*70)

df_cyto = (
    df_protein
    
    # Extract cytogenetic band details (e.g., 7p22.1)
    .withColumn("cyto_arm",
                when(col("cytogenetic").isNotNull(),
                     regexp_extract(col("cytogenetic"), "\\d+([pq])", 1))
                .otherwise(None))
    
    .withColumn("cyto_region",
                when(col("cytogenetic").isNotNull(),
                     regexp_extract(col("cytogenetic"), "[pq](\\d+)", 1))
                .otherwise(None))
    
    .withColumn("cyto_band",
                when(col("cytogenetic").isNotNull(),
                     regexp_extract(col("cytogenetic"), "[pq]\\d+\\.(\\d+)", 1))
                .otherwise(None))
    
    # Telomeric vs centromeric
    .withColumn("is_telomeric_variant",
                when((col("cyto_region").isNotNull()) &
                     (col("cyto_region") != "") &
                     (col("cyto_region").cast("int") >= 20), True)
                .otherwise(False))
    
    .withColumn("is_centromeric_variant",
                when((col("cyto_region").isNotNull()) &
                     (col("cyto_region") != "") &
                     (col("cyto_region").cast("int") <= 5), True)
                .otherwise(False))
)

print("Cytogenetic details extracted")

# COMMAND ----------

# DBTITLE 1,STEP 6: Ultra-Parse Clinical Significance
print("\n" + "="*70)
print("STEP 6: ULTRA-PARSE CLINICAL SIGNIFICANCE")
print("="*70)

df_clinical = (
    df_cyto
    
    # Parse clinical_significance
    .withColumn("clinical_sig_array",
                when(col("clinical_significance").isNotNull(),
                     split(col("clinical_significance"), ";"))
                .otherwise(array()))
    
    .withColumn("clinical_sig_count", size(col("clinical_sig_array")))
    
    # Primary classification (normalized)
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
    
    # Additional clinical flags
    .withColumn("has_risk_factor",
                when(col("clinical_significance").isNotNull(),
                     lower(col("clinical_significance")).contains("risk factor"))
                .otherwise(False))
    
    .withColumn("has_drug_response",
                when(col("clinical_significance").isNotNull(),
                     lower(col("clinical_significance")).contains("drug response"))
                .otherwise(False))
    
    .withColumn("has_protective_factor",
                when(col("clinical_significance").isNotNull(),
                     lower(col("clinical_significance")).contains("protective"))
                .otherwise(False))
    
    .withColumn("affects_drug_response",
                when(col("clinical_significance").isNotNull(),
                     lower(col("clinical_significance")).contains("affects"))
                .otherwise(False))
    
    # Clinical actionability score
    .withColumn("clinical_actionability_score",
                (when(col("clinical_significance_normalized").isin(["Pathogenic", "Likely Pathogenic"]), 2).otherwise(0)) +
                (when(col("has_drug_response"), 2).otherwise(0)) +
                (when(col("has_risk_factor"), 1).otherwise(0)))
)

print("Clinical significance ultra-parsed")

# COMMAND ----------

# DBTITLE 1,STEP 7: Ultra-Parse Origin and Inheritance
print("\n" + "="*70)
print("STEP 7: COMPREHENSIVE ORIGIN AND INHERITANCE PARSING")
print("="*70)

df_origin = (
    df_clinical
    
    # Parse origin
    .withColumn("origin_array",
                when(col("origin").isNotNull(),
                     split(col("origin"), ";"))
                .otherwise(array()))
    
    .withColumn("origin_count", size(col("origin_array")))
    
    # Origin flags
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
    
    .withColumn("is_biparental",
                when(col("origin").isNotNull(),
                     lower(col("origin")).contains("biparental"))
                .otherwise(False))
    
    .withColumn("is_de_novo",
                when(col("origin").isNotNull(),
                     lower(col("origin")).contains("de novo"))
                .otherwise(False))
    
    # Inheritance pattern inference
    .withColumn("inheritance_pattern",
                when(col("is_de_novo"), "De Novo")
                .when(col("is_biparental"), "Biparental")
                .when(col("is_maternal") & col("is_paternal"), "Biparental")
                .when(col("is_maternal"), "Maternal")
                .when(col("is_paternal"), "Paternal")
                .when(col("is_germline"), "Germline")
                .when(col("is_somatic"), "Somatic")
                .otherwise("Unknown"))
)

print("Origin and inheritance comprehensively parsed")

# COMMAND ----------

# DBTITLE 1,STEP 8: Parse Evaluation Date and Recency
print("\n" + "="*70)
print("STEP 8: EVALUATION RECENCY AND TEMPORAL ANALYSIS")
print("="*70)

df_temporal = (
    df_origin
    
    # Parse last_evaluated date
    .withColumn("last_evaluated_date",
                when(col("last_evaluated").isNotNull(),
                     to_date(col("last_evaluated"), "dd-MMM-yy"))
                .otherwise(None))
    
    # Calculate days since evaluation
    .withColumn("days_since_evaluation",
                when(col("last_evaluated_date").isNotNull(),
                     datediff(current_date(), col("last_evaluated_date")))
                .otherwise(None))
    
    # Recency categories
    .withColumn("evaluation_recency",
                when(col("days_since_evaluation").isNull(), "Unknown")
                .when(col("days_since_evaluation") < 365, "Recent")
                .when(col("days_since_evaluation") < 1825, "Moderate")
                .otherwise("Old"))
    
    .withColumn("is_recently_evaluated",
                when(col("days_since_evaluation").isNotNull() &
                     (col("days_since_evaluation") < 730),
                     True)
                .otherwise(False))
    
    # Extract evaluation year
    .withColumn("evaluation_year",
                when(col("last_evaluated_date").isNotNull(),
                     year(col("last_evaluated_date")))
                .otherwise(None))
)

print("Temporal analysis complete")

# COMMAND ----------

# DBTITLE 1,STEP 9: Calculate Comprehensive Quality Scores
print("\n" + "="*70)
print("STEP 9: COMPREHENSIVE QUALITY SCORING")
print("="*70)

df_quality = (
    df_temporal
    
    # Review status quality score
    .withColumn("review_quality_score",
                when(col("has_expert_review"), 5)
                .when(col("has_multiple_submitters") & ~col("has_conflicts"), 4)
                .when(col("has_multiple_submitters") & col("has_conflicts"), 3)
                .when(lower(col("review_status")).contains("single submitter"), 2)
                .when(col("has_no_assertion"), 1)
                .otherwise(0))
    
    # Data completeness score
    .withColumn("data_completeness_score",
                (when(col("disease_enriched").isNotNull(), 1).otherwise(0)) +
                (when(col("protein_change_full").isNotNull(), 1).otherwise(0)) +
                (when(col("phenotype_db_coverage") > 0, 1).otherwise(0)) +
                (when(col("is_recently_evaluated"), 1).otherwise(0)) +
                (when(col("review_quality_score") >= 3, 1).otherwise(0)) +
                (when(col("accession_count") > 1, 1).otherwise(0)))
    
    # Overall quality tier
    .withColumn("quality_tier",
                when((col("data_completeness_score") >= 5) & 
                     (col("review_quality_score") >= 3), "High Quality")
                .when(col("data_completeness_score") >= 3, "Medium Quality")
                .otherwise("Low Quality"))
    
    # Clinical utility score
    .withColumn("clinical_utility_score",
                (when(col("clinical_significance_normalized").isin(["Pathogenic", "Likely Pathogenic"]), 3).otherwise(0)) +
                (when(col("has_drug_response"), 2).otherwise(0)) +
                (when(col("phenotype_db_coverage") >= 2, 1).otherwise(0)) +
                (when(col("review_quality_score") >= 4, 1).otherwise(0)))
)

print("Comprehensive quality scoring complete")

# COMMAND ----------

# DBTITLE 1,STEP 10: Clean Core Fields
print("\n" + "="*70)
print("STEP 10: CLEAN CORE FIELDS")
print("="*70)

df_clean = (
    df_quality
    
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
    
    # Position
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
)

print("Core fields cleaned")

# COMMAND ----------

# DBTITLE 1,STEP 11: Deduplication
print("\n" + "="*70)
print("STEP 11: DEDUPLICATION")
print("="*70)

df_dedup = (
    df_clean
    .dropDuplicates(["primary_accession"])
)

dedup_count = df_dedup.count()
print(f"After deduplication: {dedup_count:,} variants")
print(f"Removed: {raw_count - dedup_count:,} duplicates")

# COMMAND ----------

# DBTITLE 1,STEP 12: Create Ultra-Enriched Final Table
print("\n" + "="*70)
print("STEP 12: CREATE ULTRA-ENRICHED VARIANT TABLE")
print("="*70)

df_variants_ultra_enriched = df_dedup.select(
    # Core identifiers
    col("variant_id"),
    col("primary_accession").alias("accession"),
    col("accession_count"),
    col("is_multi_submission"),
    "gene_id",
    "gene_name",
    
    # Clinical classification
    col("clinical_significance_normalized").alias("clinical_significance"),
    col("clinical_significance").alias("clinical_significance_raw"),
    col("clinical_sig_count"),
    "has_risk_factor",
    "has_drug_response",
    "has_protective_factor",
    "affects_drug_response",
    "clinical_actionability_score",
    
    # Review and submission details
    "review_status",
    col("review_quality_score"),
    col("number_submitters"),
    "has_multiple_submitters",
    "has_expert_review",
    "has_conflicts",
    "has_no_assertion",
    
    # Disease information (enriched with DB IDs)
    col("disease_enriched").alias("disease"),
    col("disease_array"),
    col("disease_count"),
    "omim_disease_id",
    "orphanet_disease_id",
    "mondo_disease_id",
    "medgen_disease_id",
    "has_cancer_disease",
    "has_syndrome",
    "has_hereditary",
    "has_rare_disease",
    "has_lethal_keyword",
    "has_progressive_keyword",
    
    # Phenotype databases
    "phenotype_ids",
    col("phenotype_db_count"),
    col("phenotype_db_coverage"),
    "has_omim",
    "has_orphanet",
    "has_mondo",
    "has_medgen",
    "has_hpo",
    
    # Genomic location
    "chromosome",
    col("position_clean").alias("position"),
    col("stop_position_clean").alias("stop_position"),
    "cytogenetic",
    col("variant_type_clean").alias("variant_type"),
    
    # Cytogenetic details
    "cyto_arm",
    "cyto_region",
    "cyto_band",
    "is_telomeric_variant",
    "is_centromeric_variant",
    
    # Molecular details (ultra-enriched)
    "variant_name",
    col("protein_change_full").alias("protein_change"),
    "ref_amino_acid",
    "alt_amino_acid",
    "aa_start_position",
    col("cdna_change_full").alias("cdna_change"),
    "cdna_position",
    "transcript_id",
    "transcript_version",
    
    # Mutation classification (detailed)
    "is_frameshift",
    "is_nonsense",
    "is_splice",
    "is_missense",
    "is_deletion",
    "is_insertion",
    "is_duplication",
    "is_indel",
    "mutation_severity_score",
    
    "reference_allele",
    "alternate_allele",
    
    # Origin and inheritance (detailed)
    col("origin_array"),
    col("origin_count"),
    "is_germline",
    "is_somatic",
    "is_maternal",
    "is_paternal",
    "is_biparental",
    "is_de_novo",
    "inheritance_pattern",
    
    # Temporal data
    "last_evaluated",
    "last_evaluated_date",
    "days_since_evaluation",
    "evaluation_recency",
    "is_recently_evaluated",
    "evaluation_year",
    
    # Quality metrics (comprehensive)
    "data_completeness_score",
    "quality_tier",
    "clinical_utility_score",
    
    # Metadata
    "assembly"
)

# COMMAND ----------

# DBTITLE 1,Save Ultra-Enriched Variants
print("\n" + "="*70)
print("SAVING ULTRA-ENRICHED VARIANTS TO SILVER LAYER")
print("="*70)

df_variants_ultra_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.variants_ultra_enriched")

saved_count = spark.table(f"{catalog_name}.silver.variants_ultra_enriched").count()
print(f"Saved to: {catalog_name}.silver.variants_ultra_enriched")
print(f"Verified: {saved_count:,} ultra-enriched variants")

# COMMAND ----------

# DBTITLE 1,Ultra-Enrichment Summary
print("\n" + "="*70)
print("ULTRA-ENRICHMENT SUMMARY")
print("="*70)

enrichment_stats = {
    "total_variants": df_variants_ultra_enriched.count(),
    "with_disease_db_id": df_variants_ultra_enriched.filter(col("omim_disease_id").isNotNull()).count(),
    "with_protein_change": df_variants_ultra_enriched.filter(col("protein_change").isNotNull()).count(),
    "with_amino_acid_details": df_variants_ultra_enriched.filter(col("ref_amino_acid").isNotNull()).count(),
    "missense": df_variants_ultra_enriched.filter(col("is_missense")).count(),
    "frameshift": df_variants_ultra_enriched.filter(col("is_frameshift")).count(),
    "nonsense": df_variants_ultra_enriched.filter(col("is_nonsense")).count(),
    "splice": df_variants_ultra_enriched.filter(col("is_splice")).count(),
    "germline": df_variants_ultra_enriched.filter(col("is_germline")).count(),
    "somatic": df_variants_ultra_enriched.filter(col("is_somatic")).count(),
    "de_novo": df_variants_ultra_enriched.filter(col("is_de_novo")).count(),
    "high_quality": df_variants_ultra_enriched.filter(col("quality_tier") == "High Quality").count(),
    "clinically_actionable": df_variants_ultra_enriched.filter(col("clinical_actionability_score") >= 3).count(),
    "recently_evaluated": df_variants_ultra_enriched.filter(col("is_recently_evaluated")).count(),
    "with_drug_response": df_variants_ultra_enriched.filter(col("has_drug_response")).count()
}

print("\nUltra-Enrichment Statistics:")
for key, value in enrichment_stats.items():
    print(f"  {key}: {value:,}")

print(f"\nTotal columns: {len(df_variants_ultra_enriched.columns)}")

# COMMAND ----------

# DBTITLE 1,Sample Ultra-Enriched Data
print("\nSample ultra-enriched variants (high clinical utility):")
display(
    df_variants_ultra_enriched.filter(col("clinical_utility_score") >= 5)
                              .select("gene_name", "disease", "protein_change",
                                     "mutation_severity_score", "clinical_utility_score",
                                     "quality_tier", "inheritance_pattern")
                              .limit(10)
)

print("\n" + "="*70)
print("ULTRA-ENRICHMENT COMPLETE")
print("="*70)
print(f"Total fields extracted: {len(df_variants_ultra_enriched.columns)}")
print("Next: Use silver.variants_ultra_enriched in feature engineering")
print("="*70)
