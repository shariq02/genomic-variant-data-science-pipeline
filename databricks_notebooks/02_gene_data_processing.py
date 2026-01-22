# Databricks notebook source
# MAGIC %md
# MAGIC #### ENHANCED GENE DATA PROCESSING - 17 PROTEIN TYPES + 9 CELLULAR LOCATIONS
# MAGIC ##### Maximum Feature Extraction with 102 Total Columns
# MAGIC
# MAGIC **DNA Gene Mapping Project**   
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 17, 2026 (ENHANCED)
# MAGIC **Purpose:** Extract ALL features including 17 protein types and cellular location flags
# MAGIC
# MAGIC **ENHANCEMENTS OVER COMPLETE VERSION:**
# MAGIC 1. **17 Protein Types** (was 5): Added GPCR, transcription factor, channel, membrane protein, growth factor, structural, regulatory, metabolic, DNA binding, RNA binding, ubiquitin, protease
# MAGIC 2. **9 Cellular Location Booleans** (was 1 text): Added nuclear, mitochondrial, cytoplasmic, membrane, extracellular, ER, golgi, lysosomal, peroxisomal flags
# MAGIC 3. **Druggability 0-4 Scale** (was 0.3-0.85): Better distribution with 10x range
# MAGIC 4. **Output:** 102 columns in silver.genes_ultra_enriched (was 81)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace, split, explode,
    length, countDistinct, count, avg, sum as spark_sum, lit, coalesce, 
    concat_ws, array_distinct, flatten, collect_set, size, array_contains,
    regexp_extract, array, initcap, substring, instr, datediff, current_date,
    to_date, year, month, expr, regexp_extract_all, element_at, concat
)
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")


print("MAXIMUM DATA EXTRACTION - GENE PROCESSING")
print("="*70)
print(f"Catalog: {catalog_name}")
print("Extracting ALL hidden data from text fields")

# COMMAND ----------

# DBTITLE 1,Read Raw Gene Data
print("\nReading gene metadata...")

df_genes_raw = spark.table(f"{catalog_name}.default.gene_metadata_all")

raw_count = df_genes_raw.count()
print(f"Loaded {raw_count:,} genes")

# Show sample to understand data structure
print("\nSample raw data:")
df_genes_raw.select("gene_name", "other_aliases", "other_designations", "db_xrefs").show(3, truncate=50)

# COMMAND ----------

# DBTITLE 1,STEP 1: MAXIMUM ALIAS EXTRACTION
print("STEP 1: EXTRACT ALL ALIASES INTO SEPARATE COLUMNS")
print("="*70)

# Example: A1B|ABG|GAB|HYST2477 → alias_1, alias_2, alias_3, alias_4

df_aliases_expanded = (
    df_genes_raw
    .withColumn("gene_name", upper(trim(col("gene_name"))))
    .withColumn("official_symbol", 
                when(col("official_symbol").isNotNull(), 
                     upper(trim(col("official_symbol"))))
                .otherwise(col("gene_name")))
    
    # Split aliases by pipe
    .withColumn("aliases_array",
                when(col("other_aliases").isNotNull() & (col("other_aliases") != "-"),
                     split(col("other_aliases"), "\\|"))
                .otherwise(array()))
    
    # Extract individual aliases (up to 10 aliases per gene)
    .withColumn("alias_1", when(size(col("aliases_array")) >= 1, col("aliases_array")[0]).otherwise(None))
    .withColumn("alias_2", when(size(col("aliases_array")) >= 2, col("aliases_array")[1]).otherwise(None))
    .withColumn("alias_3", when(size(col("aliases_array")) >= 3, col("aliases_array")[2]).otherwise(None))
    .withColumn("alias_4", when(size(col("aliases_array")) >= 4, col("aliases_array")[3]).otherwise(None))
    .withColumn("alias_5", when(size(col("aliases_array")) >= 5, col("aliases_array")[4]).otherwise(None))
    .withColumn("alias_6", when(size(col("aliases_array")) >= 6, col("aliases_array")[5]).otherwise(None))
    .withColumn("alias_7", when(size(col("aliases_array")) >= 7, col("aliases_array")[6]).otherwise(None))
    .withColumn("alias_8", when(size(col("aliases_array")) >= 8, col("aliases_array")[7]).otherwise(None))
    .withColumn("alias_9", when(size(col("aliases_array")) >= 9, col("aliases_array")[8]).otherwise(None))
    .withColumn("alias_10", when(size(col("aliases_array")) >= 10, col("aliases_array")[9]).otherwise(None))
    
    .withColumn("total_aliases", size(col("aliases_array")))
)

print("Aliases extracted into separate columns (alias_1 to alias_10)")

# Show sample
print("\nSample alias extraction:")
df_aliases_expanded.select(
    "gene_name", "alias_1", "alias_2", "alias_3", "alias_4", "total_aliases"
).show(5, truncate=30)

# COMMAND ----------

# DBTITLE 1,STEP 2: MAXIMUM DESIGNATION EXTRACTION
print("STEP 2: EXTRACT ALL DESIGNATIONS INTO SEPARATE COLUMNS")
print("="*70)

# Example: alpha-1B-glycoprotein|HEL-S-163pA|epididymis secretory... 
# → designation_1, designation_2, designation_3...

df_designations_expanded = (
    df_aliases_expanded
    
    # Split designations by pipe
    .withColumn("designations_array",
                when(col("other_designations").isNotNull() & (col("other_designations") != "-"),
                     split(col("other_designations"), "\\|"))
                .otherwise(array()))
    
    # Extract individual designations (up to 15 per gene)
    .withColumn("designation_1", when(size(col("designations_array")) >= 1, col("designations_array")[0]).otherwise(None))
    .withColumn("designation_2", when(size(col("designations_array")) >= 2, col("designations_array")[1]).otherwise(None))
    .withColumn("designation_3", when(size(col("designations_array")) >= 3, col("designations_array")[2]).otherwise(None))
    .withColumn("designation_4", when(size(col("designations_array")) >= 4, col("designations_array")[3]).otherwise(None))
    .withColumn("designation_5", when(size(col("designations_array")) >= 5, col("designations_array")[4]).otherwise(None))
    .withColumn("designation_6", when(size(col("designations_array")) >= 6, col("designations_array")[5]).otherwise(None))
    .withColumn("designation_7", when(size(col("designations_array")) >= 7, col("designations_array")[6]).otherwise(None))
    .withColumn("designation_8", when(size(col("designations_array")) >= 8, col("designations_array")[7]).otherwise(None))
    .withColumn("designation_9", when(size(col("designations_array")) >= 9, col("designations_array")[8]).otherwise(None))
    .withColumn("designation_10", when(size(col("designations_array")) >= 10, col("designations_array")[9]).otherwise(None))
    .withColumn("designation_11", when(size(col("designations_array")) >= 11, col("designations_array")[10]).otherwise(None))
    .withColumn("designation_12", when(size(col("designations_array")) >= 12, col("designations_array")[11]).otherwise(None))
    .withColumn("designation_13", when(size(col("designations_array")) >= 13, col("designations_array")[12]).otherwise(None))
    .withColumn("designation_14", when(size(col("designations_array")) >= 14, col("designations_array")[13]).otherwise(None))
    .withColumn("designation_15", when(size(col("designations_array")) >= 15, col("designations_array")[14]).otherwise(None))
    
    .withColumn("total_designations", size(col("designations_array")))
    
    # Extract protein type keywords from designations
    .withColumn("has_glycoprotein", 
                lower(coalesce(col("other_designations"), lit(""))).contains("glycoprotein"))
    .withColumn("has_receptor_keyword",
                lower(coalesce(col("other_designations"), lit(""))).contains("receptor"))
    .withColumn("has_enzyme_keyword",
                lower(coalesce(col("other_designations"), lit(""))).contains("enzyme"))
    .withColumn("has_kinase_keyword",
                lower(coalesce(col("other_designations"), lit(""))).contains("kinase"))
    .withColumn("has_binding_keyword",
                lower(coalesce(col("other_designations"), lit(""))).contains("binding"))
)

print("Designations extracted into separate columns (designation_1 to designation_15)")

print("\nSample designation extraction:")
df_designations_expanded.select(
    "gene_name", "designation_1", "designation_2", "total_designations"
).show(5, truncate=40)

# COMMAND ----------

# DBTITLE 1,STEP 3: ULTRA DATABASE XREFS EXTRACTION
print("STEP 3: EXTRACT ALL DATABASE IDs INTO SEPARATE COLUMNS")
print("="*70)

# Example: MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410|AllianceGenome:HGNC:5

df_xrefs_expanded = (
    df_designations_expanded
    
    # Extract ALL database IDs
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
    
    # Additional databases that might be present
    .withColumn("uniprot_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "UniProtKB:([A-Z0-9]+)", 1))
                .otherwise(None))
    
    .withColumn("refseq_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "RefSeq:(NM_\\d+)", 1))
                .otherwise(None))
    
    .withColumn("entrez_id",
                when(col("db_xrefs").isNotNull(),
                     regexp_extract(col("db_xrefs"), "GeneID:(\\d+)", 1))
                .otherwise(None))
    
    # Count how many databases this gene is in
    .withColumn("database_count",
                (when(col("mim_id").isNotNull(), 1).otherwise(0)) +
                (when(col("hgnc_id").isNotNull(), 1).otherwise(0)) +
                (when(col("ensembl_id").isNotNull(), 1).otherwise(0)) +
                (when(col("alliance_id").isNotNull(), 1).otherwise(0)) +
                (when(col("uniprot_id").isNotNull(), 1).otherwise(0)) +
                (when(col("refseq_id").isNotNull(), 1).otherwise(0)) +
                (when(col("entrez_id").isNotNull(), 1).otherwise(0)))
    
    # Flag for well-annotated genes
    .withColumn("is_well_annotated", col("database_count") >= 4)
)

print("Database IDs extracted: MIM, HGNC, Ensembl, Alliance, UniProt, RefSeq, Entrez")

print("\nSample database extraction:")
df_xrefs_expanded.select(
    "gene_name", "mim_id", "hgnc_id", "ensembl_id", "database_count"
).show(5)

# COMMAND ----------

# DBTITLE 1,STEP 4: ENHANCED DESCRIPTION PARSING
print("STEP 4: PARSE DESCRIPTION FIELD FOR HIDDEN DATA")
print("="*70)

df_description_parsed = (
    df_xrefs_expanded
    
    # Extract protein family from description
    .withColumn("protein_family",
                when(col("description").isNotNull(),
                     regexp_extract(lower(col("description")), 
                                  "(\\w+) family", 1))
                .otherwise(None))
    
    # Extract domain information
    .withColumn("has_domain_in_description",
                lower(coalesce(col("description"), lit(""))).rlike("domain|repeat|motif"))
    
    # Extract subunit information
    .withColumn("has_subunit_info",
                lower(coalesce(col("description"), lit(""))).contains("subunit"))
    
    # Extract chain information (alpha, beta, etc.)
    .withColumn("chain_type",
                when(col("description").isNotNull(),
                     regexp_extract(lower(col("description")), 
                                  "(alpha|beta|gamma|delta|epsilon|zeta)", 1))
                .otherwise(None))
    
    # Extract numbered variants (e.g., "member 1", "type 2")
    .withColumn("member_number",
                when(col("description").isNotNull(),
                     regexp_extract(col("description"), "member (\\d+)", 1))
                .otherwise(None))
    
    .withColumn("type_number",
                when(col("description").isNotNull(),
                     regexp_extract(col("description"), "type (\\d+)", 1))
                .otherwise(None))
    
    # Description complexity score
    .withColumn("description_word_count",
                when(col("description").isNotNull(),
                     size(split(col("description"), "\\s+")))
                .otherwise(0))
)

print("Description parsed for: protein family, domains, chains, members")

print("\nSample description parsing:")
df_description_parsed.select(
    "gene_name", "description", "protein_family", "chain_type", "member_number"
).show(5, truncate=50)

# COMMAND ----------

# DBTITLE 1,STEP 5: CYTOGENETIC LOCATION ULTRA-PARSING
print("STEP 5: ULTRA-PARSE CYTOGENETIC LOCATION")
print("="*70)

df_cyto_parsed = (
    df_description_parsed
    
    # Parse map_location (e.g., 19q13.43)
    .withColumn("cyto_chromosome",
                when(col("map_location").isNotNull(),
                     regexp_extract(col("map_location"), "^(\\d+|X|Y|MT)", 1))
                .otherwise(None))
    
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
    
    .withColumn("cyto_sub_band",
                when(col("map_location").isNotNull(),
                     regexp_extract(col("map_location"), "[pq]\\d+\\.\\d+(\\d)", 1))
                .otherwise(None))
    
    # Telomeric/centromeric classification
    .withColumn("cyto_region_int",
                when((col("cyto_region").isNotNull()) & (col("cyto_region") != ""),
                     col("cyto_region").cast("int"))
                .otherwise(None))
    
    .withColumn("is_telomeric",
                when(col("cyto_region_int") >= 20, True).otherwise(False))
    
    .withColumn("is_centromeric",
                when(col("cyto_region_int") <= 5, True).otherwise(False))
    
    .withColumn("is_pericentromeric",
                when((col("cyto_region_int") >= 6) & (col("cyto_region_int") <= 12), True)
                .otherwise(False))
)

print("Cytogenetic location ultra-parsed: chromosome, arm, region, band, sub-band")

print("\nSample cytogenetic parsing:")
df_cyto_parsed.select(
    "gene_name", "map_location", "cyto_arm", "cyto_region", "cyto_band", 
    "is_telomeric", "is_centromeric"
).show(5)

# COMMAND ----------

# DBTITLE 1,STEP 6: GENOMIC POSITION HANDLING
print("STEP 6: HANDLE GENOMIC POSITIONS")
print("="*70)

df_positions = (
    df_cyto_parsed
    
    # Clean and cast positions
    .withColumn("start_position_clean",
                when((col("start_position").isNotNull()) & 
                     (trim(col("start_position")) != "") &
                     (col("start_position") != "Unknown"),
                     col("start_position").cast("long"))
                .otherwise(None))
    
    .withColumn("end_position_clean",
                when((col("end_position").isNotNull()) & 
                     (trim(col("end_position")) != "") &
                     (col("end_position") != "Unknown"),
                     col("end_position").cast("long"))
                .otherwise(None))
    
    # Calculate gene length
    .withColumn("gene_length_calculated",
                when((col("start_position_clean").isNotNull()) & 
                     (col("end_position_clean").isNotNull()),
                     col("end_position_clean") - col("start_position_clean"))
                .otherwise(None))
    
    # Clean strand
    .withColumn("strand_clean",
                when(col("strand").isin("+", "-"), col("strand"))
                .otherwise(None))
    
    # Flag for missing positions (these need Ensembl lookup)
    .withColumn("needs_position_lookup",
                when((col("start_position_clean").isNull()) & 
                     (col("ensembl_id").isNotNull()),
                     True)
                .otherwise(False))
)

# Count genes with missing positions
missing_positions = df_positions.filter(col("needs_position_lookup")).count()
print(f"Genes needing position lookup: {missing_positions:,}")

print("\nSample position data:")
df_positions.select(
    "gene_name", "ensembl_id", "start_position_clean", "end_position_clean",
    "gene_length_calculated", "needs_position_lookup"
).show(5)

# COMMAND ----------

# DBTITLE 1,STEP 7: MERGE WITH EXISTING FUNCTIONAL CLASSIFICATION
print("STEP 7: ADD FUNCTIONAL CLASSIFICATION")
print("="*70)

# Basic functional classification (expand with your full logic)
df_with_functions = (
    df_positions
    # Core protein types (5)
    .withColumn("is_kinase", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("kinase"))
    .withColumn("is_receptor", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("receptor"))
    .withColumn("is_enzyme", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(enzyme|ase\\b)"))
    .withColumn("is_phosphatase", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("phosphatase"))
    .withColumn("is_transporter", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("transport"))
    
    # Additional protein types (12 - ENHANCED)
    .withColumn("is_gpcr", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(g protein.coupled receptor|gpcr)"))
    .withColumn("is_transcription_factor", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(transcription factor|dna.binding protein)"))
    .withColumn("is_channel", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(channel|pore)"))
    .withColumn("is_membrane_protein", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(membrane|transmembrane)"))
    .withColumn("is_growth_factor", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("growth factor"))
    .withColumn("is_structural", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(structural|cytoskeleton|collagen|tubulin|actin)"))
    .withColumn("is_regulatory", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(regulat|modulat)"))
    .withColumn("is_metabolic", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(metabol)"))
    .withColumn("is_dna_binding", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("dna binding"))
    .withColumn("is_rna_binding", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("rna binding"))
    .withColumn("is_ubiquitin_related", lower(coalesce(col("description"), col("full_name"), lit(""))).contains("ubiquitin"))
    .withColumn("is_protease", lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("(?i)(protease|peptidase)"))
    
    # Derive primary function from ALL protein flags (ENHANCED - uses all 17 types)
    .withColumn("primary_function",
        when(col("is_kinase"), "Kinase")
        .when(col("is_phosphatase"), "Phosphatase")
        .when(col("is_gpcr"), "GPCR")  # NEW
        .when(col("is_transcription_factor"), "Transcription Factor")  # NEW
        .when(col("is_receptor"), "Receptor")
        .when(col("is_channel"), "Channel")  # NEW
        .when(col("is_protease"), "Protease")  # NEW
        .when(col("is_growth_factor"), "Growth Factor")  # NEW
        .when(col("is_enzyme") & ~col("is_kinase") & ~col("is_phosphatase") & ~col("is_protease"), "Enzyme")
        .when(col("is_transporter"), "Transporter")
        .when(col("is_membrane_protein") & ~col("is_receptor") & ~col("is_channel"), "Membrane Protein")  # NEW
        .when(col("is_structural"), "Structural")  # NEW
        .when(col("is_regulatory"), "Regulatory")  # NEW
        .when(col("is_metabolic"), "Metabolic")  # NEW
        .when(col("is_dna_binding"), "DNA Binding")  # NEW
        .when(col("is_rna_binding"), "RNA Binding")  # NEW
        .when(col("is_ubiquitin_related"), "Ubiquitin Related")  # NEW
        .when(col("has_receptor_keyword"), "Receptor-related")
        .when(col("has_enzyme_keyword"), "Enzyme-related")
        .otherwise("Other"))
    
    # Extract biological process from description
    .withColumn("biological_process",
        when(lower(col("description")).rlike("signal transduction|signaling"), "Cell Signaling")
        .when(lower(col("description")).rlike("metabol"), "Metabolism")
        .when(lower(col("description")).rlike("transport|carrier"), "Transport")
        .when(lower(col("description")).rlike("transcription factor|dna binding"), "Transcription Regulation")
        .when(lower(col("description")).rlike("repair|replication"), "DNA Repair/Replication")
        .when(lower(col("description")).rlike("immun|defense"), "Immune Response")
        .when(lower(col("description")).rlike("apoptosis|cell death"), "Apoptosis")
        .when(lower(col("description")).rlike("cell cycle|mitosis"), "Cell Cycle")
        .when(lower(col("description")).rlike("growth|proliferation"), "Cell Growth")
        .otherwise("Other"))
    
    # Extract cellular location from MULTIPLE fields (IMPROVED)
    .withColumn("cellular_location",
        # Check description and full_name together
        when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("membrane|transmembrane") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("membrane|transmembrane"),
            "Membrane"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("nuclear|nucleus") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("nuclear|nucleus"),
            "Nuclear"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("mitochondri") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("mitochondri"),
            "Mitochondrial"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("cytoplasm|cytosol|cytoplasmic") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("cytoplasm|cytosol|cytoplasmic"),
            "Cytoplasmic"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("extracellular|secreted|plasma") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("extracellular|secreted"),
            "Extracellular"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("endoplasmic|reticulum|\\ber\\b") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("endoplasmic|reticulum") |
            (lower(col("description")).contains("glycoprotein") & ~lower(col("description")).rlike("membrane|nuclear|mitochondrial")),  # Glycoproteins often in ER
            "Endoplasmic Reticulum"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("golgi") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("golgi"),
            "Golgi Apparatus"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("lysosom") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("lysosom"),
            "Lysosomal"
        )
        .when(
            lower(coalesce(col("description"), col("full_name"), lit(""))).rlike("peroxisom") |
            lower(coalesce(col("designation_1"), col("designation_2"), lit(""))).rlike("peroxisom"),
            "Peroxisomal"
        )
        .otherwise("Unknown"))
    
    # Add cellular location boolean flags (9 - ENHANCED)
    .withColumn("nuclear", col("cellular_location") == "Nuclear")
    .withColumn("mitochondrial", col("cellular_location") == "Mitochondrial")
    .withColumn("cytoplasmic", col("cellular_location") == "Cytoplasmic")
    .withColumn("membrane", col("cellular_location") == "Membrane")
    .withColumn("extracellular", col("cellular_location") == "Extracellular")
    .withColumn("endoplasmic_reticulum", col("cellular_location") == "Endoplasmic Reticulum")
    .withColumn("golgi", col("cellular_location") == "Golgi Apparatus")
    .withColumn("lysosomal", col("cellular_location") == "Lysosomal")
    .withColumn("peroxisomal", col("cellular_location") == "Peroxisomal")
    
    # Calculate druggability score (0-4 scale - ENHANCED)
    .withColumn("druggability_score",
        when(col("is_kinase"), 4.0)  # Kinases highly druggable
        .when(col("is_gpcr"), 4.0)  # GPCRs highly druggable
        .when(col("is_receptor") & lower(col("description")).contains("growth factor"), 3.5)
        .when(col("is_receptor"), 3.0)
        .when(col("is_enzyme") & lower(col("description")).contains("protease"), 3.5)
        .when(col("is_enzyme"), 3.0)
        .when(col("is_phosphatase"), 2.5)
        .when(col("is_transporter"), 2.5)
        .when(col("is_channel"), 3.0)  # Ion channels druggable
        .when(lower(col("description")).rlike("ion channel"), 3.0)
        .when(col("is_protease"), 3.5)
        .when(col("is_growth_factor"), 3.0)
        .when(col("cellular_location") == "Membrane", 2.0)
        .when(col("cellular_location") == "Extracellular", 2.5)
        .when(col("cellular_location") == "Nuclear", 1.0)
        .when(col("cellular_location") == "Cytoplasmic", 1.5)
        .otherwise(0.5))  # Default for unknown
)

print("\nFunctional classification applied - ENHANCED")
print("  Core protein types: 5")
print("  Additional protein types: 12")
print("  Total protein types: 17")
print("  Cellular location categories: 9")
print("  Druggability scale: 0.5-4.0")

# COMMAND ----------

# DBTITLE 1,STEP 8: CREATE FINAL ULTRA-ENRICHED TABLE
print("STEP 8: CREATE ULTRA-ENRICHED GENE TABLE")
print("="*70)

df_genes_ultra_enriched = df_with_functions.select(
    # Core identifiers
    "gene_id",
    "gene_name",
    "official_symbol",
    
    # EXPANDED: Individual aliases (NEW!)
    "alias_1", "alias_2", "alias_3", "alias_4", "alias_5",
    "alias_6", "alias_7", "alias_8", "alias_9", "alias_10",
    "total_aliases",
    
    # EXPANDED: Individual designations (NEW!)
    "designation_1", "designation_2", "designation_3", "designation_4", "designation_5",
    "designation_6", "designation_7", "designation_8", "designation_9", "designation_10",
    "designation_11", "designation_12", "designation_13", "designation_14", "designation_15",
    "total_designations",
    
    # EXPANDED: All database IDs (NEW!)
    "mim_id",
    "hgnc_id",
    "ensembl_id",
    "alliance_id",
    "uniprot_id",
    "refseq_id",
    "entrez_id",
    "database_count",
    "is_well_annotated",
    
    # EXPANDED: Description parsing (NEW!)
    "description",
    "protein_family",
    "chain_type",
    "member_number",
    "type_number",
    "description_word_count",
    "has_domain_in_description",
    "has_subunit_info",
    
    # EXPANDED: Cytogenetic details (NEW!)
    "chromosome",
    "map_location",
    "cyto_chromosome",
    "cyto_arm",
    "cyto_region",
    "cyto_band",
    "cyto_sub_band",
    "is_telomeric",
    "is_centromeric",
    "is_pericentromeric",
    
    # EXPANDED: Genomic positions (NEW!)
    col("start_position_clean").alias("start_position"),
    col("end_position_clean").alias("end_position"),
    col("strand_clean").alias("strand"),
    col("gene_length_calculated").alias("gene_length"),
    "needs_position_lookup",
    
    # Core protein types (5)
    "is_kinase",
    "is_receptor",
    "is_enzyme",
    "is_phosphatase",
    "is_transporter",
    
    # Additional protein types (12 - ENHANCED)
    "is_gpcr",
    "is_transcription_factor",
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
    
    # Designation keywords
    "has_glycoprotein",
    "has_receptor_keyword",
    "has_enzyme_keyword",
    "has_kinase_keyword",
    "has_binding_keyword",
    
    # Derived functional classifications
    "primary_function",
    "biological_process",
    "cellular_location",
    "druggability_score",
    
    # Cellular location boolean flags (9 - ENHANCED)
    "nuclear",
    "mitochondrial",
    "cytoplasmic",
    "membrane",
    "extracellular",
    "endoplasmic_reticulum",
    "golgi",
    "lysosomal",
    "peroxisomal",
    
    
    # Metadata
    "gene_type",
    "full_name",
    "nomenclature_status",
    "modification_date",
    "data_source"
)

print(f"Ultra-enriched table created with {len(df_genes_ultra_enriched.columns)} columns!")

# COMMAND ----------

# DBTITLE 1,Save Ultra-Enriched Genes
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
print("MAXIMUM DATA EXTRACTION SUMMARY")
print("="*70)

print("\n NEW COLUMNS ADDED:")
print("1. Aliases: alias_1 to alias_10 (10 columns)")
print("2. Designations: designation_1 to designation_15 (15 columns)")
print("3. Database IDs: uniprot_id, refseq_id, entrez_id (3 new)")
print("4. Description parsing: protein_family, chain_type, member_number, type_number (4 new)")
print("5. Cytogenetic: cyto_sub_band, is_pericentromeric (2 new)")
print("6. Position handling: needs_position_lookup flag (1 new)")
print("7. Keywords: has_glycoprotein, has_*_keyword (5 new)")

print(f"\n Column Statistics:")
print(f"   Total columns: {len(df_genes_ultra_enriched.columns)}")
print(f"   Original columns: ~70")
print(f"   NEW columns added: ~40")
print(f"   Total enriched: ~110 columns")

# Statistics
enrichment_stats = {
    "total_genes": df_genes_ultra_enriched.count(),
    "with_aliases": df_genes_ultra_enriched.filter(col("total_aliases") > 0).count(),
    "with_designations": df_genes_ultra_enriched.filter(col("total_designations") > 0).count(),
    "well_annotated": df_genes_ultra_enriched.filter(col("is_well_annotated")).count(),
    "needs_position_lookup": df_genes_ultra_enriched.filter(col("needs_position_lookup")).count(),
    "with_protein_family": df_genes_ultra_enriched.filter(col("protein_family").isNotNull()).count(),
    "with_chain_type": df_genes_ultra_enriched.filter(col("chain_type").isNotNull()).count()
}

print("\n Extraction Statistics:")
for key, value in enrichment_stats.items():
    pct = (value / enrichment_stats["total_genes"] * 100) if enrichment_stats["total_genes"] > 0 else 0
    print(f"  {key}: {value:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Sample Ultra-Enriched Data
print("\nSample ultra-enriched genes with multiple aliases:")
display(
    df_genes_ultra_enriched
    .filter(col("total_aliases") >= 3)
    .select(
        "gene_name",
        "alias_1", "alias_2", "alias_3",
        "designation_1", "designation_2",
        "mim_id", "ensembl_id",
        "protein_family",
        "database_count"
    )
    .limit(10)
)

print("\nGenes needing position lookup:")
display(
    df_genes_ultra_enriched
    .filter(col("needs_position_lookup"))
    .select(
        "gene_name", "ensembl_id", "chromosome", "map_location"
    )
    .limit(10)
)

print(" MAXIMUM DATA EXTRACTION COMPLETE")
print("="*70)
print("Next steps:")
print("1. Use these enriched fields in variant processing")
print("2. Create OMIM disease mapping from mim_id")
print("3. Fetch missing positions using Ensembl API")
