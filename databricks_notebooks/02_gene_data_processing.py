# Databricks notebook source
# MAGIC %md
# MAGIC #### ULTRA-ENRICHED GENE DATA PROCESSING
# MAGIC ##### Extract MAXIMUM data from all text fields
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC **Purpose:** Extract every possible piece of information from gene metadata
# MAGIC **Input:** workspace.default.gene_metadata_all (193K genes)  
# MAGIC **Output:** workspace.silver.genes_ultra_enriched

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace, split, explode,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, 
    concat_ws, array_distinct, flatten, collect_set, size, array_contains,
    regexp_extract, array, initcap, substring, instr, datediff, current_date,
    to_date, year, month
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
print("ULTRA-ENRICHED GENE DATA PROCESSING")
print("="*70)
print(f"Catalog: {catalog_name}")
print("Extracting MAXIMUM data from all fields")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Raw Gene Data
print("\nReading gene metadata...")

df_genes_raw = spark.table(f"{catalog_name}.default.gene_metadata_all")

raw_count = df_genes_raw.count()
print(f"Loaded {raw_count:,} genes")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse ALL Database Cross-References
print("\n" + "="*70)
print("STEP 1: ULTRA-PARSE DATABASE XREFS")
print("="*70)

df_xrefs = (
    df_genes_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    .withColumn("official_symbol", 
                when(col("official_symbol").isNotNull(), 
                     upper(trim(col("official_symbol"))))
                .otherwise(col("gene_name")))
    
    # Parse db_xrefs (format: MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410)
    .withColumn("xrefs_array",
                when(col("db_xrefs").isNotNull(),
                     split(col("db_xrefs"), "\\|"))
                .otherwise(array()))
    
    # Extract specific database IDs
    .withColumn("mim_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "MIM:(\\d+)", 1))
                .otherwise(None))
    
    .withColumn("hgnc_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "HGNC:HGNC:(\\d+)", 1))
                .otherwise(None))
    
    .withColumn("ensembl_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "Ensembl:(ENSG\\d+)", 1))
                .otherwise(None))
    
    .withColumn("alliance_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "AllianceGenome:HGNC:(\\d+)", 1))
                .otherwise(None))
    
    .withColumn("xref_count", size(col("xrefs_array")))
    
    # Database coverage score
    .withColumn("database_coverage_score",
                (when(col("mim_id").isNotNull(), 1).otherwise(0)) +
                (when(col("hgnc_id").isNotNull(), 1).otherwise(0)) +
                (when(col("ensembl_id").isNotNull(), 1).otherwise(0)) +
                (when(col("alliance_id").isNotNull(), 1).otherwise(0)))
)

print("Database cross-references extracted")

# COMMAND ----------

# DBTITLE 1,STEP 2: Parse Cytogenetic Location in Detail
print("\n" + "="*70)
print("STEP 2: PARSE CYTOGENETIC LOCATION")
print("="*70)

df_cyto = (
    df_xrefs
    
    # Extract cytogenetic band details from map_location (e.g., 19q13.43)
    .withColumn("cyto_arm",
                when(col("map_location").isNotNull(),
                     regexp_extract(col("map_location"), "\\d+([pq])", 1))
                .otherwise(None))
    
    .withColumn("cyto_region",
                when(col("map_location").isNotNull(),
                     regexp_extract(col("map_location"), "[pq](\\d+)", 1))
                .otherwise(None))
    
    .withColumn("cyto_band",
                when(col("map_location").isNotNull(),
                     regexp_extract(col("map_location"), "[pq]\\d+\\.(\\d+)", 1))
                .otherwise(None))
    
    # Create integer version for comparison (handle empty strings)
    .withColumn("cyto_region_int",
                when((col("cyto_region").isNotNull()) & (col("cyto_region") != ""),
                     col("cyto_region").cast("int"))
                .otherwise(None))
    
    # Telomeric vs centromeric location
    .withColumn("is_telomeric",
                when(col("cyto_region_int") >= 20, True)
                .otherwise(False))
    
    .withColumn("is_centromeric",
                when(col("cyto_region_int") <= 5, True)
                .otherwise(False))
)

print("Cytogenetic details extracted")

# COMMAND ----------

# DBTITLE 1,STEP 3: Deep Parse Other Designations
print("\n" + "="*70)
print("STEP 3: ULTRA-PARSE OTHER DESIGNATIONS")
print("="*70)

df_designations = (
    df_cyto
    
    # Parse other_designations (pipe-separated protein names and descriptions)
    .withColumn("designations_array",
                when(col("other_designations").isNotNull() & (col("other_designations") != "-"),
                     split(col("other_designations"), "\\|"))
                .otherwise(array()))
    
    .withColumn("designation_count", size(col("designations_array")))
    
    # Extract protein-related keywords
    .withColumn("has_protein_keyword",
                when(col("other_designations").isNotNull(),
                     lower(col("other_designations")).contains("protein"))
                .otherwise(False))
    
    # Extract domain information
    .withColumn("has_domain_info",
                when(col("other_designations").isNotNull(),
                     lower(col("other_designations")).rlike("(?i)(domain|repeat|motif)"))
                .otherwise(False))
    
    # Extract binding partner info
    .withColumn("has_binding_info",
                when(col("other_designations").isNotNull(),
                     lower(col("other_designations")).rlike("(?i)(binding|interact|partner)"))
                .otherwise(False))
    
    # Extract tissue specificity
    .withColumn("tissue_specific",
                when(col("other_designations").isNotNull(),
                     lower(col("other_designations")).rlike("(?i)(liver|kidney|brain|heart|lung|muscle|blood|bone)"))
                .otherwise(False))
    
    # Extract secretory/membrane info
    .withColumn("is_secretory",
                when(col("other_designations").isNotNull(),
                     lower(col("other_designations")).rlike("(?i)(secret|excret)"))
                .otherwise(False))
)

print("Designation details extracted")

# COMMAND ----------

# DBTITLE 1,STEP 4: Parse Aliases into Structured Array
print("\n" + "="*70)
print("STEP 4: COMPREHENSIVE ALIAS PARSING")
print("="*70)

df_aliases = (
    df_designations
    
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
    
    # Check for numeric aliases (often indicates multiple isoforms)
    .withColumn("has_numeric_aliases",
                when(col("other_aliases").isNotNull(),
                     lower(col("other_aliases")).rlike("\\d+"))
                .otherwise(False))
)

print("Aliases comprehensively parsed")

# COMMAND ----------

# DBTITLE 1,STEP 5: Advanced Functional Classification
print("\n" + "="*70)
print("STEP 5: ADVANCED FUNCTIONAL CLASSIFICATION")
print("="*70)

df_functions = (
    df_aliases
    
    # Combine all text fields for comprehensive analysis
    .withColumn("full_text",
                concat_ws(" | ",
                          coalesce(col("description"), lit("")),
                          coalesce(col("full_name"), lit("")),
                          coalesce(col("other_designations"), lit(""))))
    
    # Primary function categories
    .withColumn("is_kinase", 
                lower(col("full_text")).contains("kinase"))
    .withColumn("is_phosphatase",
                lower(col("full_text")).contains("phosphatase"))
    .withColumn("is_receptor",
                lower(col("full_text")).contains("receptor"))
    .withColumn("is_transcription_factor",
                lower(col("full_text")).contains("transcription factor"))
    .withColumn("is_enzyme",
                lower(col("full_text")).rlike("(?i)(enzyme|synthetase|dehydrogenase|transferase|ligase|hydrolase|reductase|oxidase)"))
    .withColumn("is_transporter",
                lower(col("full_text")).rlike("(?i)(transport|carrier|exchanger)"))
    .withColumn("is_channel",
                lower(col("full_text")).rlike("(?i)(channel|pore)"))
    .withColumn("is_membrane_protein",
                lower(col("full_text")).rlike("(?i)(membrane|transmembrane)"))
    
    # Signaling-related
    .withColumn("is_gpcr",
                lower(col("full_text")).rlike("(?i)(g protein|gpcr)"))
    .withColumn("is_growth_factor",
                lower(col("full_text")).rlike("(?i)(growth factor)"))
    
    # Structural proteins
    .withColumn("is_structural",
                lower(col("full_text")).rlike("(?i)(collagen|actin|tubulin|keratin|elastin)"))
    
    # Regulatory proteins
    .withColumn("is_regulatory",
                lower(col("full_text")).rlike("(?i)(regulat|control|modulator)"))
    
    # Metabolic enzymes
    .withColumn("is_metabolic",
                lower(col("full_text")).rlike("(?i)(metabol|glycolysis|citric|tca)"))
    
    # DNA/RNA related
    .withColumn("is_dna_binding",
                lower(col("full_text")).rlike("(?i)(dna binding|dna repair)"))
    .withColumn("is_rna_binding",
                lower(col("full_text")).rlike("(?i)(rna binding|splicing)"))
    
    # Protein modification
    .withColumn("is_ubiquitin_related",
                lower(col("full_text")).rlike("(?i)(ubiquit|sumo)"))
    .withColumn("is_protease",
                lower(col("full_text")).rlike("(?i)(protease|peptidase)"))
)

print("Advanced functional classification complete")

# COMMAND ----------

# DBTITLE 1,STEP 6: Disease and Pathway Association
print("\n" + "="*70)
print("STEP 6: DISEASE AND PATHWAY CLASSIFICATION")
print("="*70)

df_disease = (
    df_functions
    
    # Disease-related keywords (expanded)
    .withColumn("cancer_related",
                lower(col("full_text")).rlike("(?i)(cancer|tumor|oncogene|carcinoma|sarcoma|lymphoma|leukemia)"))
    .withColumn("immune_related",
                lower(col("full_text")).rlike("(?i)(immune|antibody|lymphocyte|interleukin|interferon|cytokine)"))
    .withColumn("neurological_related",
                lower(col("full_text")).rlike("(?i)(brain|neural|neuron|synap|alzheimer|parkinson)"))
    .withColumn("cardiovascular_related",
                lower(col("full_text")).rlike("(?i)(heart|cardiac|vascular|blood pressure|artery)"))
    .withColumn("metabolic_related",
                lower(col("full_text")).rlike("(?i)(diabetes|insulin|glucose|lipid|cholesterol)"))
    .withColumn("developmental_related",
                lower(col("full_text")).rlike("(?i)(development|embryo|morphogenesis)"))
    
    # Specific disease mentions
    .withColumn("alzheimer_related",
                lower(col("full_text")).contains("alzheimer"))
    .withColumn("diabetes_related",
                lower(col("full_text")).contains("diabetes"))
    .withColumn("breast_cancer_related",
                lower(col("full_text")).rlike("(?i)(breast cancer|brca)"))
)

print("Disease associations extracted")

# COMMAND ----------

# DBTITLE 1,STEP 7: Cellular Location Details
print("\n" + "="*70)
print("STEP 7: DETAILED CELLULAR LOCALIZATION")
print("="*70)

df_location = (
    df_disease
    
    # Cellular compartments
    .withColumn("nuclear",
                lower(col("full_text")).rlike("(?i)(nuclear|nucleus)"))
    .withColumn("mitochondrial",
                lower(col("full_text")).rlike("(?i)(mitochondri)"))
    .withColumn("cytoplasmic",
                lower(col("full_text")).rlike("(?i)(cytoplasm|cytosol)"))
    .withColumn("membrane",
                lower(col("full_text")).rlike("(?i)(membrane|transmembrane)"))
    .withColumn("extracellular",
                lower(col("full_text")).rlike("(?i)(extracellular|secreted)"))
    .withColumn("endoplasmic_reticulum",
                lower(col("full_text")).rlike("(?i)(endoplasmic reticulum|er)"))
    .withColumn("golgi",
                lower(col("full_text")).contains("golgi"))
    .withColumn("lysosomal",
                lower(col("full_text")).rlike("(?i)(lysosom)"))
    .withColumn("peroxisomal",
                lower(col("full_text")).rlike("(?i)(peroxisom)"))
    
    # Primary location (hierarchy)
    .withColumn("primary_location",
                when(col("mitochondrial"), "Mitochondrial")
                .when(col("nuclear"), "Nuclear")
                .when(col("membrane"), "Membrane")
                .when(col("extracellular"), "Extracellular")
                .when(col("cytoplasmic"), "Cytoplasmic")
                .when(col("endoplasmic_reticulum"), "ER")
                .otherwise("Unknown"))
)

print("Cellular location details extracted")

# COMMAND ----------

# DBTITLE 1,STEP 8: Create Comprehensive Function Category
print("\n" + "="*70)
print("STEP 8: COMPREHENSIVE FUNCTIONAL CATEGORIZATION")
print("="*70)

df_categorized = (
    df_location
    
    # Primary function (detailed hierarchy)
    .withColumn("primary_function",
                when(col("is_transcription_factor"), "Transcription Factor")
                .when(col("is_kinase"), "Kinase")
                .when(col("is_phosphatase"), "Phosphatase")
                .when(col("is_receptor"), "Receptor")
                .when(col("is_gpcr"), "GPCR")
                .when(col("is_channel"), "Ion Channel")
                .when(col("is_transporter"), "Transporter")
                .when(col("is_protease"), "Protease")
                .when(col("is_enzyme"), "Enzyme")
                .when(col("is_structural"), "Structural Protein")
                .when(col("is_growth_factor"), "Growth Factor")
                .when(col("is_regulatory"), "Regulatory Protein")
                .otherwise("Other"))
    
    # Biological process (primary)
    .withColumn("biological_process",
                when(col("cancer_related"), "Cancer/Oncology")
                .when(col("immune_related"), "Immune Response")
                .when(col("neurological_related"), "Neurological")
                .when(col("cardiovascular_related"), "Cardiovascular")
                .when(col("metabolic_related"), "Metabolism")
                .when(col("developmental_related"), "Development")
                .otherwise("General"))
    
    # Cellular location (simplified)
    .withColumn("cellular_location", col("primary_location"))
    
    # Druggability score (0-5 based on function)
    .withColumn("druggability_score",
                (when(col("is_kinase"), 1).otherwise(0)) +
                (when(col("is_receptor"), 1).otherwise(0)) +
                (when(col("is_gpcr"), 1).otherwise(0)) +
                (when(col("is_channel"), 1).otherwise(0)) +
                (when(col("is_enzyme"), 1).otherwise(0)))
)

print("Comprehensive categorization complete")

# COMMAND ----------

# DBTITLE 1,STEP 9: Extract Metadata Quality Metrics
print("\n" + "="*70)
print("STEP 9: METADATA QUALITY SCORING")
print("="*70)

df_quality = (
    df_categorized
    
    # Calculate data completeness score
    .withColumn("metadata_completeness",
                (when(col("description").isNotNull() & (col("description") != "Unknown"), 1).otherwise(0)) +
                (when(col("full_name").isNotNull(), 1).otherwise(0)) +
                (when(col("other_aliases").isNotNull() & (col("other_aliases") != "-"), 1).otherwise(0)) +
                (when(col("other_designations").isNotNull() & (col("other_designations") != "-"), 1).otherwise(0)) +
                (when(col("map_location").isNotNull(), 1).otherwise(0)) +
                (when(col("gene_type").isNotNull() & (col("gene_type") != "Unknown"), 1).otherwise(0)) +
                (when(col("db_xrefs").isNotNull(), 1).otherwise(0)))
    
    # Information richness (text length score)
    .withColumn("description_length",
                when(col("description").isNotNull(),
                     length(col("description")))
                .otherwise(0))
    
    .withColumn("is_well_characterized",
                when((col("metadata_completeness") >= 5) &
                     (col("description_length") > 50) &
                     (col("database_coverage_score") >= 2),
                     True)
                .otherwise(False))
    
    # Modification recency (if available)
    .withColumn("days_since_modification",
                when(col("modification_date").isNotNull(),
                     datediff(current_date(), to_date(col("modification_date"), "yyyyMMdd")))
                .otherwise(None))
    
    .withColumn("is_recently_updated",
                when(col("days_since_modification").isNotNull() &
                     (col("days_since_modification") < 365),
                     True)
                .otherwise(False))
)

print("Quality metrics calculated")

# COMMAND ----------

# DBTITLE 1,STEP 10: Clean and Standardize Core Fields
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

# COMMAND ----------

# DBTITLE 1,STEP 11: Create Ultra-Enriched Final Table
print("\n" + "="*70)
print("STEP 11: CREATE ULTRA-ENRICHED GENE TABLE")
print("="*70)

df_genes_ultra_enriched = df_clean.select(
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
    
    # Cytogenetic details (NEW) - keep as strings
    "cyto_arm",
    "cyto_region",
    "cyto_band",
    "is_telomeric",
    "is_centromeric",
    
    # Gene classification
    col("gene_type_clean").alias("gene_type"),
    
    # Aliases (NEW - expanded)
    "all_aliases",
    "alias_count",
    "has_numeric_aliases",
    
    # Database IDs (NEW - extracted) - keep as strings
    "mim_id",
    "hgnc_id",
    "ensembl_id",
    "alliance_id",
    "database_coverage_score",
    
    # Designation details (NEW)
    "designation_count",
    "has_protein_keyword",
    "has_domain_info",
    "has_binding_info",
    "tissue_specific",
    "is_secretory",
    
    # Primary function categories
    "primary_function",
    "biological_process",
    "cellular_location",
    "primary_location",
    "druggability_score",
    
    # Functional flags (expanded)
    "is_kinase",
    "is_phosphatase",
    "is_receptor",
    "is_gpcr",
    "is_transcription_factor",
    "is_enzyme",
    "is_transporter",
    "is_channel",
    "is_membrane_protein",
    "is_growth_factor",
    "is_structural",
    "is_regulatory",
    "is_metabolic",
    "is_dna_binding",
    "is_rna_binding",
    "is_ubiquitin_related",
    "is_protease",
    
    # Disease associations (expanded)
    "cancer_related",
    "immune_related",
    "neurological_related",
    "cardiovascular_related",
    "metabolic_related",
    "developmental_related",
    "alzheimer_related",
    "diabetes_related",
    "breast_cancer_related",
    
    # Cellular localization (detailed)
    "nuclear",
    "mitochondrial",
    "cytoplasmic",
    "membrane",
    "extracellular",
    "endoplasmic_reticulum",
    "golgi",
    "lysosomal",
    "peroxisomal",
    
    # Quality metrics (NEW)
    "metadata_completeness",
    "description_length",
    "is_well_characterized",
    "days_since_modification",
    "is_recently_updated",
    
    # Metadata
    "db_xrefs",
    "xref_count",
    "nomenclature_status",
    "modification_date",
    "data_source"
)

# COMMAND ----------

# DBTITLE 1,Save Ultra-Enriched Genes
print("\n" + "="*70)
print("SAVING ULTRA-ENRICHED GENES TO SILVER LAYER")
print("="*70)

df_genes_ultra_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")

saved_count = spark.table(f"{catalog_name}.silver.genes_ultra_enriched").count()
print(f"Saved to: {catalog_name}.silver.genes_ultra_enriched")
print(f"Verified: {saved_count:,} ultra-enriched genes")

# COMMAND ----------

# DBTITLE 1,Ultra-Enrichment Summary
print("\n" + "="*70)
print("ULTRA-ENRICHMENT SUMMARY")
print("="*70)

enrichment_stats = {
    "total_genes": df_genes_ultra_enriched.count(),
    "with_mim_id": df_genes_ultra_enriched.filter(col("mim_id").isNotNull()).count(),
    "with_ensembl_id": df_genes_ultra_enriched.filter(col("ensembl_id").isNotNull()).count(),
    "well_characterized": df_genes_ultra_enriched.filter(col("is_well_characterized")).count(),
    "recently_updated": df_genes_ultra_enriched.filter(col("is_recently_updated")).count(),
    "kinases": df_genes_ultra_enriched.filter(col("is_kinase")).count(),
    "receptors": df_genes_ultra_enriched.filter(col("is_receptor")).count(),
    "gpcrs": df_genes_ultra_enriched.filter(col("is_gpcr")).count(),
    "transcription_factors": df_genes_ultra_enriched.filter(col("is_transcription_factor")).count(),
    "cancer_related": df_genes_ultra_enriched.filter(col("cancer_related")).count(),
    "druggable": df_genes_ultra_enriched.filter(col("druggability_score") >= 2).count(),
    "tissue_specific": df_genes_ultra_enriched.filter(col("tissue_specific")).count()
}

print("\nUltra-Enrichment Statistics:")
for key, value in enrichment_stats.items():
    print(f"  {key}: {value:,}")

print(f"\nTotal columns: {len(df_genes_ultra_enriched.columns)}")

# COMMAND ----------

# DBTITLE 1,Sample Ultra-Enriched Data
print("\nSample ultra-enriched genes (druggable kinases):")
display(
    df_genes_ultra_enriched.filter(col("is_kinase") & (col("druggability_score") >= 3))
                           .select("gene_name", "description", "primary_function", 
                                  "druggability_score", "mim_id", "ensembl_id")
                           .limit(10)
)

print("\n" + "="*70)
print("ULTRA-ENRICHMENT COMPLETE")
print("="*70)
print(f"Total fields extracted: {len(df_genes_ultra_enriched.columns)}")
print("Next: Use silver.genes_ultra_enriched in feature engineering")
print("="*70)
