# Databricks notebook source
# MAGIC %md
# MAGIC ## ENHANCED FEATURE ENGINEERING - 95 GOLD COLUMNS
# MAGIC ### 17 Protein Types + 9 Disease Categories + 9 Cellular Locations
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 17, 2026 (ENHANCED)
# MAGIC
# MAGIC **Purpose:** Create comprehensive analytical features with maximum enrichment
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    when, round as spark_round, countDistinct, explode, size,
    lit, concat, expr, coalesce, upper, lower, trim, split, array
)
from pyspark.sql.functions import max as spark_max, min as spark_min

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
print("SparkSession initialized")
print("Spark version: {}".format(spark.version))

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql("USE CATALOG {}".format(catalog_name))

print("COMPREHENSIVE FEATURE ENGINEERING")
print("="*80)
print("Catalog: {}".format(catalog_name))
print("Integrating ALL silver tables for maximum enrichment")

# COMMAND ----------

# DBTITLE 1,Load All Silver Tables
print("\nLOADING ALL SILVER TABLES")
print("="*80)

# Core tables
df_genes = spark.table("{}.silver.genes_ultra_enriched".format(catalog_name))
df_variants = spark.table("{}.silver.variants_ultra_enriched".format(catalog_name))
df_gene_search = spark.table("{}.reference.gene_universal_search".format(catalog_name))

print("Core tables:")
print("  genes_ultra_enriched: {:,}".format(df_genes.count()))
print("  variants_ultra_enriched: {:,}".format(df_variants.count()))
print("  gene_universal_search: {:,}".format(df_gene_search.count()))

# NEW: Disease tables
df_diseases = spark.table("{}.silver.diseases".format(catalog_name))
df_disease_hierarchy = spark.table("{}.silver.disease_hierarchy".format(catalog_name))

print("\nDisease tables:")
print("  diseases: {:,}".format(df_diseases.count()))
print("  disease_hierarchy: {:,}".format(df_disease_hierarchy.count()))

# NEW: Gene-disease links
df_gene_disease_links = spark.table("{}.silver.gene_disease_links".format(catalog_name))
df_omim_catalog = spark.table("{}.silver.omim_catalog".format(catalog_name))

print("\nGene-disease tables:")
print("  gene_disease_links: {:,}".format(df_gene_disease_links.count()))
print("  omim_catalog: {:,}".format(df_omim_catalog.count()))

# NEW: RefSeq annotations
df_genes_refseq = spark.table("{}.silver.genes_refseq".format(catalog_name))
df_transcripts = spark.table("{}.silver.transcripts".format(catalog_name))

print("\nRefSeq tables:")
print("  genes_refseq: {:,}".format(df_genes_refseq.count()))
print("  transcripts: {:,}".format(df_transcripts.count()))

# NEW: Protein data
df_proteins_refseq = spark.table("{}.silver.proteins_refseq".format(catalog_name))
df_proteins_uniprot = spark.table("{}.silver.proteins_uniprot".format(catalog_name))

print("\nProtein tables:")
print("  proteins_refseq: {:,}".format(df_proteins_refseq.count()))
print("  proteins_uniprot: {:,}".format(df_proteins_uniprot.count()))

# NEW: Genetic tests
df_genetic_tests = spark.table("{}.silver.genetic_tests".format(catalog_name))

print("\nGenetic tests:")
print("  genetic_tests: {:,}".format(df_genetic_tests.count()))

# NEW: Structural variants
df_structural_variants = spark.table("{}.silver.structural_variants".format(catalog_name))

print("\nStructural variants:")
print("  structural_variants: {:,}".format(df_structural_variants.count()))

# COMMAND ----------

# DBTITLE 1,Resolve Gene Name Aliases
print("\nRESOLVING GENE NAME ALIASES")
print("="*80)

# Preprocess gene names
df_variants = (
    df_variants
    .withColumn("gene_name_clean",
        when(col("gene_name").contains(";"), 
             split(col("gene_name"), ";")[0])
        .otherwise(col("gene_name")))
    .withColumn("gene_name", col("gene_name_clean"))
    .drop("gene_name_clean")
)

# Join with gene lookup
df_variants_resolved = (
    df_variants
    .join(
        df_gene_search.select(
            col("search_term").alias("variant_gene_key"),
            col("mapped_gene_name").alias("resolved_gene_name"),
            col("mapped_gene_id").alias("resolved_gene_id")
        ),
        upper(trim(col("gene_name"))) == col("variant_gene_key"),
        "left"
    )
    .withColumn("gene_name_original", col("gene_name"))
    .withColumn("gene_name", coalesce(col("resolved_gene_name"), col("gene_name")))
    .withColumn("gene_id", coalesce(col("resolved_gene_id"), col("gene_id")))
    .drop("variant_gene_key", "resolved_gene_name", "resolved_gene_id")
)

resolved_count = df_variants_resolved.filter(col("gene_name") != col("gene_name_original")).count()
print("Resolved {:,} gene aliases".format(resolved_count))

df_variants = df_variants_resolved

# COMMAND ----------

# DBTITLE 1,Create Disease Category Flags
print("\nCREATING DISEASE CATEGORY FLAGS")
print("="*80)

df_variants = df_variants \
    .withColumn("has_cancer_disease",
                lower(coalesce(col("disease_enriched"), lit(""))).rlike("cancer|carcinoma|tumor|tumour|neoplasm|melanoma|leukemia|lymphoma|sarcoma")) \
    .withColumn("has_syndrome",
                lower(coalesce(col("disease_enriched"), lit(""))).contains("syndrome")) \
    .withColumn("has_hereditary",
                lower(coalesce(col("disease_enriched"), lit(""))).rlike("hereditary|familial|inherited")) \
    .withColumn("has_rare_disease",
                lower(coalesce(col("disease_enriched"), lit(""))).rlike("rare|orphan")) \
    .withColumn("has_drug_response",
                lower(coalesce(col("clinical_significance_simple"), lit(""))).contains("Drug Response")) \
    .withColumn("has_risk_factor",
                lower(coalesce(col("clinical_significance_simple"), lit(""))).contains("Risk Factor"))

print("Disease category flags added")

# COMMAND ----------

# DBTITLE 1,Aggregate Protein Annotations
print("\nAGGREGATING PROTEIN ANNOTATIONS")
print("="*80)

# RefSeq proteins per gene
df_refseq_protein_stats = (
    df_proteins_refseq
    .groupBy("gene_symbol")
    .agg(
        countDistinct("protein_accession").alias("refseq_protein_count"),
        countDistinct("rna_accession").alias("refseq_transcript_count")
    )
)

print("RefSeq protein stats: {:,} genes".format(df_refseq_protein_stats.count()))

# UniProt proteins per gene
df_uniprot_stats = (
    df_proteins_uniprot
    .groupBy("gene_symbol")
    .agg(
        countDistinct("uniprot_accession").alias("uniprot_protein_count")
    )
)

print("UniProt protein stats: {:,} genes".format(df_uniprot_stats.count()))

# COMMAND ----------

# DBTITLE 1,Aggregate Transcript Annotations
print("\nAGGREGATING TRANSCRIPT ANNOTATIONS")
print("="*80)

df_transcript_stats = (
    df_transcripts
    .groupBy("gene_symbol")
    .agg(
        countDistinct("transcript_id").alias("transcript_count"),
        avg(col("stop") - col("start")).alias("avg_transcript_length"),
        spark_min(col("start")).alias("min_transcript_start"),
        spark_max(col("stop")).alias("max_transcript_stop")
    )
)

print("Transcript stats: {:,} genes".format(df_transcript_stats.count()))

# COMMAND ----------

# DBTITLE 1,Aggregate Genetic Test Coverage
print("\nAGGREGATING GENETIC TEST COVERAGE")
print("="*80)

df_test_stats = (
    df_genetic_tests
    .groupBy("gene_symbol")
    .agg(
        countDistinct("gtr_test_id").alias("genetic_test_count"),
        countDistinct("disease_name").alias("tested_disease_count")
    )
)

print("Genetic test stats: {:,} genes".format(df_test_stats.count()))

# COMMAND ----------

# DBTITLE 1,Aggregate Structural Variant Overlaps
print("\nAGGREGATING STRUCTURAL VARIANT OVERLAPS")
print("="*80)

df_sv_stats = (
    df_structural_variants
    .groupBy("chromosome")
    .agg(
        count("*").alias("sv_count"),
        countDistinct("variant_type").alias("sv_type_count")
    )
)

print("Structural variant stats by chromosome: {:,}".format(df_sv_stats.count()))

# COMMAND ----------

# DBTITLE 1,Aggregate Disease Annotations
print("\nAGGREGATING DISEASE ANNOTATIONS")
print("="*80)

df_gene_disease_stats = (
    df_gene_disease_links
    .groupBy("gene_symbol")
    .agg(
        countDistinct("medgen_id").alias("linked_disease_count"),
        countDistinct("omim_id").alias("linked_omim_count")
    )
)

print("Gene-disease stats: {:,} genes".format(df_gene_disease_stats.count()))

# COMMAND ----------

# DBTITLE 1,Create Enhanced Gene Features
print("\nCREATING ENHANCED GENE FEATURES")
print("="*80)

# Start with base gene aggregations from variants
df_gene_base = (
    df_variants
    .groupBy("gene_name")
    .agg(
        count("*").alias("mutation_count"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance_simple") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance_simple") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        spark_sum(when(col("clinical_significance_simple") == "Uncertain Significance", 1).otherwise(0)).alias("vus_count"),
        
        # Mutation types
        spark_sum(when(col("is_frameshift_variant"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense_variant"), 1).otherwise(0)).alias("nonsense_count"),
        spark_sum(when(col("is_splice_variant"), 1).otherwise(0)).alias("splice_count"),
        spark_sum(when(col("is_missense_variant"), 1).otherwise(0)).alias("missense_count"),
        spark_sum(when(col("is_deletion"), 1).otherwise(0)).alias("deletion_count"),
        spark_sum(when(col("is_insertion"), 1).otherwise(0)).alias("insertion_count"),
        spark_sum(when(col("is_duplication"), 1).otherwise(0)).alias("duplication_count"),
        spark_sum(when(col("is_indel"), 1).otherwise(0)).alias("indel_count"),
        
        # Origin patterns
        spark_sum(when(col("is_germline"), 1).otherwise(0)).alias("germline_count"),
        spark_sum(when(col("is_somatic"), 1).otherwise(0)).alias("somatic_count"),
        spark_sum(when(col("is_de_novo"), 1).otherwise(0)).alias("de_novo_count"),
        
        # Clinical utility
        spark_sum(when(col("has_drug_response"), 1).otherwise(0)).alias("drug_response_count"),
        spark_sum(when(col("has_risk_factor"), 1).otherwise(0)).alias("risk_factor_count"),
        avg(col("review_quality_score")).alias("avg_review_quality"),
        
        # Disease categories
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0)).alias("cancer_variant_count"),
        spark_sum(when(col("has_syndrome"), 1).otherwise(0)).alias("syndrome_variant_count"),
        spark_sum(when(col("has_hereditary"), 1).otherwise(0)).alias("hereditary_variant_count"),
        spark_sum(when(col("has_rare_disease"), 1).otherwise(0)).alias("rare_disease_variant_count"),
        
        # Distinct diseases
        countDistinct("disease_enriched").alias("disease_count")
    )
)

print("Base gene features: {:,} genes".format(df_gene_base.count()))

# Join with genes_ultra_enriched
df_gene_features_full = (
    df_genes
    .join(df_gene_base, df_genes.gene_name == df_gene_base.gene_name, "left")
    .drop(df_gene_base.gene_name)
)

# COMMAND ----------

# DBTITLE 1,Enrich with Protein Data
print("\nENRICHING WITH PROTEIN DATA")
print("="*80)

df_gene_features_full = (
    df_gene_features_full
    .join(df_refseq_protein_stats, 
          df_gene_features_full.official_symbol == df_refseq_protein_stats.gene_symbol, 
          "left")
    .drop(df_refseq_protein_stats.gene_symbol)
    .join(df_uniprot_stats,
          df_gene_features_full.official_symbol == df_uniprot_stats.gene_symbol,
          "left")
    .drop(df_uniprot_stats.gene_symbol)
)

print("Protein data enriched")

# COMMAND ----------

# DBTITLE 1,Enrich with Transcript Data
print("\nENRICHING WITH TRANSCRIPT DATA")
print("="*80)

df_gene_features_full = (
    df_gene_features_full
    .join(df_transcript_stats,
          df_gene_features_full.official_symbol == df_transcript_stats.gene_symbol,
          "left")
    .drop(df_transcript_stats.gene_symbol)
)

print("Transcript data enriched")

# COMMAND ----------

# DBTITLE 1,Enrich with Genetic Test Coverage
print("\nENRICHING WITH GENETIC TEST COVERAGE")
print("="*80)

df_gene_features_full = (
    df_gene_features_full
    .join(df_test_stats,
          df_gene_features_full.official_symbol == df_test_stats.gene_symbol,
          "left")
    .drop(df_test_stats.gene_symbol)
)

print("Genetic test data enriched")

# COMMAND ----------

# DBTITLE 1,Enrich with Disease Links
print("\nENRICHING WITH DISEASE LINKS")
print("="*80)

df_gene_features_full = (
    df_gene_features_full
    .join(df_gene_disease_stats,
          df_gene_features_full.official_symbol == df_gene_disease_stats.gene_symbol,
          "left")
    .drop(df_gene_disease_stats.gene_symbol)
)

print("Disease link data enriched")

# COMMAND ----------

# DBTITLE 1,Add Calculated Features
print("\nADDING CALCULATED FEATURES")
print("="*80)

df_gene_features_full = (
    df_gene_features_full
    .fillna(0, subset=[
        "mutation_count", "pathogenic_count", "likely_pathogenic_count",
        "refseq_protein_count", "uniprot_protein_count", "transcript_count",
        "genetic_test_count", "linked_disease_count"
    ])
    .withColumn("total_pathogenic", col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign", col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_pathogenic") / col("mutation_count"))
                    .otherwise(0), 4))
    .withColumn("germline_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("germline_count") / col("mutation_count"))
                    .otherwise(0), 4))
    .withColumn("severe_mutation_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         (col("frameshift_count") + col("nonsense_count")) / col("mutation_count"))
                    .otherwise(0), 4))
    # NEW: Protein annotation completeness score
    .withColumn("protein_annotation_score",
                when(col("refseq_protein_count") > 0, 1).otherwise(0) +
                when(col("uniprot_protein_count") > 0, 1).otherwise(0) +
                when(col("transcript_count") > 0, 1).otherwise(0))
    # NEW: Clinical test availability flag
    .withColumn("has_clinical_test",
                when(col("genetic_test_count") > 0, True).otherwise(False))
    # NEW: Disease database coverage score
    .withColumn("disease_db_coverage",
                when(col("linked_disease_count") > 0, 1).otherwise(0) +
                when(col("linked_omim_count") > 0, 1).otherwise(0))
)

gene_features_count = df_gene_features_full.count()
print("Enhanced gene features: {:,} genes".format(gene_features_count))
print("Total columns: {}".format(len(df_gene_features_full.columns)))

# COMMAND ----------

# DBTITLE 1,Create Chromosome Features with SV Data
print("\nCREATING CHROMOSOME FEATURES WITH SV DATA")
print("="*80)

df_chromosome_features = (
    df_gene_features_full
    .groupBy("chromosome")
    .agg(
        count("*").alias("gene_count"),
        spark_sum("mutation_count").alias("total_mutations"),
        spark_sum("pathogenic_count").alias("total_pathogenic"),
        avg("pathogenic_ratio").alias("avg_pathogenic_ratio"),
        spark_sum("refseq_protein_count").alias("total_proteins"),
        spark_sum("transcript_count").alias("total_transcripts"),
        spark_sum("genetic_test_count").alias("total_genetic_tests"),
        spark_sum("linked_disease_count").alias("total_linked_diseases"),
        spark_sum(when(col("has_clinical_test"), 1).otherwise(0)).alias("genes_with_tests")
    )
    .join(df_sv_stats, "chromosome", "left")
    .fillna(0, subset=["sv_count", "sv_type_count"])
    .orderBy("chromosome")
)

chrom_count = df_chromosome_features.count()
print("Chromosome features: {} chromosomes".format(chrom_count))

# COMMAND ----------

# DBTITLE 1,Create Gene-Disease Association Table
print("\nCREATING GENE-DISEASE ASSOCIATION TABLE")
print("="*80)

df_gene_disease = (
    df_variants
    .groupBy("gene_name", "disease_enriched", "omim_id")
    .agg(
        count("*").alias("mutation_count"),
        spark_sum(when(col("is_pathogenic"), 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance_simple") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("is_benign"), 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance_simple") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        avg(col("review_quality_score")).alias("avg_quality")
    )
    .withColumnRenamed("disease_enriched", "disease")
    .withColumn("total_pathogenic", col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("pathogenic_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_pathogenic") / col("mutation_count"))
                    .otherwise(0), 4))
    .filter(col("mutation_count") >= 1)
    .orderBy(col("pathogenic_ratio").desc())
)

assoc_count = df_gene_disease.count()
print("Gene-disease associations: {:,}".format(assoc_count))

# COMMAND ----------

# DBTITLE 1,Create ML Features
print("\nCREATING ML FEATURES")
print("="*80)

df_ml_features = (
    df_gene_features_full
    .select(
        "gene_name",
        "gene_id",
        "chromosome",
        "official_symbol",
        
        # Mutation metrics
        "mutation_count",
        "pathogenic_count",
        "total_pathogenic",
        "pathogenic_ratio",
        "severe_mutation_ratio",
        "germline_ratio",
        
        # Mutation types
        "frameshift_count",
        "nonsense_count",
        "splice_count",
        "missense_count",
        
        # Disease categories
        "disease_count",
        "cancer_variant_count",
        "syndrome_variant_count",
        "hereditary_variant_count",
        
        # NEW: Protein annotations
        "refseq_protein_count",
        "uniprot_protein_count",
        "transcript_count",
        "protein_annotation_score",
        
        # NEW: Clinical utility
        "genetic_test_count",
        "has_clinical_test",
        "tested_disease_count",
        
        # NEW: Disease links
        "linked_disease_count",
        "linked_omim_count",
        "disease_db_coverage",
        
        # Protein type flags (from genes_ultra_enriched)
        "is_kinase",
        "is_receptor",
        "is_enzyme",
        "is_transcription_factor",
        "is_gpcr",
        
        # Cellular location flags
        "nuclear",
        "membrane",
        "cytoplasmic",
        "mitochondrial",
        "extracellular",
        
        # Quality metrics
        "avg_review_quality",
        
        # Functional scores
        "druggability_score",
        "primary_function",
        "biological_process",
        
        # Database IDs
        "mim_id",
        "hgnc_id",
        "ensembl_id"
    )
)

ml_count = df_ml_features.count()
ml_columns = len(df_ml_features.columns)
print("ML features: {:,} genes, {} columns".format(ml_count, ml_columns))

# COMMAND ----------

# DBTITLE 1,Save to Gold Layer
print("\nSAVING TO GOLD LAYER")
print("="*80)

df_gene_features_full.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.gold.gene_features".format(catalog_name))
print("Saved: gold.gene_features")

df_chromosome_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.gold.chromosome_features".format(catalog_name))
print("Saved: gold.chromosome_features")

df_gene_disease.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.gold.gene_disease_association".format(catalog_name))
print("Saved: gold.gene_disease_association")

df_ml_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.gold.ml_features".format(catalog_name))
print("Saved: gold.ml_features")

# COMMAND ----------

# DBTITLE 1,Create Disease Network Gold Table
print("\nCREATING GOLD.DISEASE_NETWORK")
print("="*80)

df_disease_network = (
    df_disease_hierarchy
    .join(df_diseases.select(col("medgen_id").alias("source_id"), 
                             col("disease_name").alias("source_disease")),
          col("source_medgen_id") == col("source_id"), "left")
    .join(df_diseases.select(col("medgen_id").alias("target_id"),
                             col("disease_name").alias("target_disease")),
          col("target_medgen_id") == col("target_id"), "left")
    .select("source_medgen_id", "source_disease", "target_medgen_id", 
            "target_disease", "relationship", "relationship_detail")
)

df_disease_network.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.disease_network")
print("Saved: gold.disease_network")

# COMMAND ----------

# DBTITLE 1,Create Clinical Tests Detailed Gold Table

print("\nCREATING GOLD.CLINICAL_TESTS_DETAILED")
print("="*80)

df_clinical_tests = (
    df_genetic_tests
    .join(df_gene_features_full.select("gene_name", "official_symbol", "chromosome"),
          df_genetic_tests.gene_symbol == df_gene_features_full.official_symbol, "left")
    .select(
        "gene_symbol",
        "gene_name",
        "chromosome",
        "gtr_test_id",
        "test_name",
        "disease_name",
        lit("GTR").alias("source")
    )
)

df_clinical_tests.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.clinical_tests_detailed")
print("Saved: gold.clinical_tests_detailed")

# COMMAND ----------

# DBTITLE 1,Create Transcript Variant Map Gold Table
print("\nCREATING GOLD.TRANSCRIPT_VARIANT_MAP")
print("="*80)

df_transcript_variant = (
    df_variants
    .join(df_transcripts,
          (df_variants.gene_name == df_transcripts.gene_symbol) &
          (df_variants.chromosome == df_transcripts.chromosome), "inner")
    .filter((col("position") >= col("start")) & (col("position") <= col("stop")))
    .select(
        "accession",
        "variant_id", 
        "gene_name",
        "chromosome",
        "position",
        "transcript_id",
        "start",
        "stop",
        "clinical_significance_simple"
    )
)

df_transcript_variant.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.transcript_variant_map")
print("Saved: gold.transcript_variant_map")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("SUCCESS: COMPREHENSIVE FEATURE ENGINEERING COMPLETE")
print("="*80)

print("\nGOLD LAYER TABLES:")
print("  1. gene_features: {:,} genes, {} columns".format(gene_features_count, len(df_gene_features_full.columns)))
print("  2. chromosome_features: {} chromosomes".format(chrom_count))
print("  3. gene_disease_association: {:,} associations".format(assoc_count))
print("  4. ml_features: {:,} genes, {} columns".format(ml_count, ml_columns))

print("\nNEW ENRICHMENTS ADDED:")
print("  - RefSeq protein counts: {:,} genes".format(df_refseq_protein_stats.count()))
print("  - UniProt protein counts: {:,} genes".format(df_uniprot_stats.count()))
print("  - Transcript annotations: {:,} genes".format(df_transcript_stats.count()))
print("  - Genetic test coverage: {:,} genes".format(df_test_stats.count()))
print("  - Disease links: {:,} genes".format(df_gene_disease_stats.count()))
print("  - Structural variants: by chromosome")

print("\nNEW FEATURES:")
print("  - protein_annotation_score (0-3)")
print("  - has_clinical_test (boolean)")
print("  - disease_db_coverage (0-2)")
print("  - refseq_protein_count, uniprot_protein_count")
print("  - transcript_count, genetic_test_count")
print("  - linked_disease_count, linked_omim_count")
print("  - sv_count (structural variants by chromosome)")

print("\n" + "="*80)
print("READY FOR ANALYSIS AND ML MODELING")
