# Databricks notebook source
# MAGIC %md
# MAGIC #### ULTRA-ENRICHED FEATURE ENGINEERING
# MAGIC ##### Using Ultra-Enriched Genes and Variants
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC
# MAGIC **Purpose:** Create comprehensive analytical features using ultra-enriched data
# MAGIC
# MAGIC **Input Tables (Silver Layer - ULTRA ENRICHED):**  
# MAGIC - workspace.silver.genes_ultra_enriched (193K genes with 70+ columns)  
# MAGIC - workspace.silver.variants_ultra_enriched (4M+ variants with 90+ columns)
# MAGIC
# MAGIC **Output Tables (Gold Layer - ULTRA ENRICHED):**  
# MAGIC - workspace.gold.gene_features (with 60+ enriched features)
# MAGIC - workspace.gold.chromosome_features
# MAGIC - workspace.gold.gene_disease_association (with disease DB IDs)
# MAGIC - workspace.gold.ml_features (with 50+ ML-ready features)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    when, round as spark_round, countDistinct, explode, size,
    lit, concat, expr
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

print("SparkSession initialized")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("\n" + "="*70)
print("ULTRA-ENRICHED FEATURE ENGINEERING")
print("="*70)
print(f"Catalog: {catalog_name}")
print("Using: silver.genes_ultra_enriched + silver.variants_ultra_enriched")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Ultra-Enriched Silver Layer Tables
print("\nReading ULTRA-ENRICHED data...")

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched")

gene_count = df_genes.count()
variant_count = df_variants.count()

print(f"Loaded {gene_count:,} ultra-enriched genes")
print(f"Loaded {variant_count:,} ultra-enriched variants")

# COMMAND ----------

# DBTITLE 1,Inspect Ultra-Enriched Data
print("\nUltra-Enriched Data Preview:")

print("\n1. Gene Primary Functions:")
display(
    df_genes.groupBy("primary_function")
            .count()
            .orderBy(col("count").desc())
)

print("\n2. Gene Druggability Scores:")
display(
    df_genes.groupBy("druggability_score")
            .count()
            .orderBy("druggability_score")
)

print("\n3. Variant Mutation Types:")
display(
    df_variants.groupBy("is_missense", "is_nonsense", "is_frameshift")
               .count()
               .orderBy(col("count").desc())
               .limit(10)
)

print("\n4. Variant Quality Tiers:")
display(
    df_variants.groupBy("quality_tier")
               .count()
               .orderBy(col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Create Ultra-Enriched Gene-Level Aggregations
print("\n" + "="*70)
print("CREATING ULTRA-ENRICHED GENE-LEVEL FEATURES")
print("="*70)

df_gene_features = (
    df_variants
    .groupBy("gene_name")
    .agg(
        count("*").alias("mutation_count"),
        
        # Clinical significance counts
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        spark_sum(when(col("clinical_significance") == "Uncertain significance", 1).otherwise(0)).alias("vus_count"),
        spark_sum(when(col("clinical_significance") == "Conflicting interpretations", 1).otherwise(0)).alias("conflicting_count"),
        
        # Mutation type counts (ultra-enriched)
        spark_sum(when(col("is_frameshift"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense"), 1).otherwise(0)).alias("nonsense_count"),
        spark_sum(when(col("is_splice"), 1).otherwise(0)).alias("splice_count"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_count"),
        spark_sum(when(col("is_deletion"), 1).otherwise(0)).alias("deletion_count"),
        spark_sum(when(col("is_insertion"), 1).otherwise(0)).alias("insertion_count"),
        spark_sum(when(col("is_duplication"), 1).otherwise(0)).alias("duplication_count"),
        spark_sum(when(col("is_indel"), 1).otherwise(0)).alias("indel_count"),
        
        # Origin counts (ultra-enriched)
        spark_sum(when(col("is_germline"), 1).otherwise(0)).alias("germline_count"),
        spark_sum(when(col("is_somatic"), 1).otherwise(0)).alias("somatic_count"),
        spark_sum(when(col("is_de_novo"), 1).otherwise(0)).alias("de_novo_count"),
        spark_sum(when(col("is_maternal"), 1).otherwise(0)).alias("maternal_count"),
        spark_sum(when(col("is_paternal"), 1).otherwise(0)).alias("paternal_count"),
        
        # Disease flags (ultra-enriched)
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0)).alias("cancer_variant_count"),
        spark_sum(when(col("has_syndrome"), 1).otherwise(0)).alias("syndrome_variant_count"),
        spark_sum(when(col("has_hereditary"), 1).otherwise(0)).alias("hereditary_variant_count"),
        spark_sum(when(col("has_rare_disease"), 1).otherwise(0)).alias("rare_disease_count"),
        
        # Clinical utility (ultra-enriched)
        spark_sum(when(col("has_drug_response"), 1).otherwise(0)).alias("drug_response_count"),
        spark_sum(when(col("has_risk_factor"), 1).otherwise(0)).alias("risk_factor_count"),
        avg(col("clinical_actionability_score")).alias("avg_clinical_actionability"),
        avg(col("clinical_utility_score")).alias("avg_clinical_utility"),
        avg(col("mutation_severity_score")).alias("avg_mutation_severity"),
        
        # Quality metrics (ultra-enriched)
        avg(col("review_quality_score")).alias("avg_review_quality"),
        spark_sum(when(col("quality_tier") == "High Quality", 1).otherwise(0)).alias("high_quality_count"),
        spark_sum(when(col("is_recently_evaluated"), 1).otherwise(0)).alias("recently_evaluated_count"),
        
        # Disease and phenotype counts
        countDistinct("disease").alias("disease_count"),
        spark_sum(when(col("has_omim"), 1).otherwise(0)).alias("omim_linked_count"),
        spark_sum(when(col("has_orphanet"), 1).otherwise(0)).alias("orphanet_linked_count"),
        avg(col("phenotype_db_coverage")).alias("avg_phenotype_db_coverage"),
        
        # Positional data
        countDistinct("variant_type").alias("variant_type_count"),
        avg(when(col("position").isNotNull(), col("position"))).alias("avg_position"),
        avg(when(col("aa_start_position").isNotNull(), col("aa_start_position"))).alias("avg_aa_position")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_pathogenic") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("benign_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_benign") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("vus_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("vus_count") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("severe_mutation_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         (col("frameshift_count") + col("nonsense_count")) / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("germline_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("germline_count") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    # Enhanced risk level with more granular thresholds
    .withColumn("risk_level",
                when(col("pathogenic_ratio") >= 0.5, "High")
                .when(col("pathogenic_ratio") >= 0.2, "Medium")
                .when(col("pathogenic_ratio") >= 0.05, "Low")
                .otherwise("Very Low"))
    # Comprehensive risk score
    .withColumn("risk_score",
                spark_round(
                    (col("pathogenic_ratio") * 50) +
                    (col("mutation_count") / 100) +
                    (col("disease_count") * 2) +
                    (col("severe_mutation_ratio") * 20) +
                    (col("avg_clinical_actionability") * 5) +
                    (col("de_novo_count") * 0.5),
                    2
                ))
)

feature_count = df_gene_features.count()
print(f"Created features for {feature_count:,} genes")

# COMMAND ----------

# DBTITLE 1,Join Ultra-Enriched Gene Metadata
df_gene_features = df_gene_features.join(
    df_genes.select(
        "gene_name", 
        "gene_id", 
        "chromosome", 
        "gene_type", 
        "gene_length",
        "map_location",
        
        # Cytogenetic details
        "cyto_arm",
        "cyto_region",
        "cyto_band",
        "is_telomeric",
        "is_centromeric",
        
        # Database IDs
        "mim_id",
        "hgnc_id",
        "ensembl_id",
        "database_coverage_score",
        
        # Functional categories
        "primary_function",
        "biological_process",
        "cellular_location",
        "druggability_score",
        
        # Functional flags (17 functions)
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
        
        # Disease associations (9 categories)
        "cancer_related",
        "immune_related",
        "neurological_related",
        "cardiovascular_related",
        "metabolic_related",
        "developmental_related",
        "alzheimer_related",
        "diabetes_related",
        "breast_cancer_related",
        
        # Cellular locations (9 compartments)
        "nuclear",
        "mitochondrial",
        "cytoplasmic",
        "membrane",
        "extracellular",
        "endoplasmic_reticulum",
        "golgi",
        "lysosomal",
        "peroxisomal",
        
        # Quality metrics
        "metadata_completeness",
        "is_well_characterized"
    ),
    on="gene_name",
    how="left"
)

print("Joined with ultra-enriched gene metadata")

# COMMAND ----------

# DBTITLE 1,Mutation Density Calculation
df_gene_features = df_gene_features.withColumn("mutation_density",
    when(col("gene_length").isNotNull() & (col("gene_length") > 0),
         spark_round(col("mutation_count") / col("gene_length") * 1000000, 6))
    .otherwise(None)
)

print("Mutation density calculated")

# Check coverage
genes_with_density = df_gene_features.filter(col("mutation_density").isNotNull()).count()
print(f"Genes with mutation density: {genes_with_density:,}")

# COMMAND ----------

# DBTITLE 1,Sample Ultra-Enriched Gene Features
print("\nSample Ultra-Enriched Gene Features (Top 10 druggable kinases with high risk):")
display(
    df_gene_features.filter(col("is_kinase") & (col("druggability_score") >= 2))
                    .orderBy(col("risk_score").desc())
                    .select(
                        "gene_name",
                        "primary_function",
                        "mutation_count",
                        "pathogenic_ratio",
                        "severe_mutation_ratio",
                        "druggability_score",
                        "risk_level",
                        "avg_clinical_actionability"
                    )
                    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Chromosome-Level Features
print("\n" + "="*70)
print("CREATING CHROMOSOME-LEVEL FEATURES")
print("="*70)

df_chromosome_features = (
    df_variants
    .filter(col("chromosome").isNotNull())
    .groupBy("chromosome")
    .agg(
        countDistinct("gene_name").alias("gene_count"),
        count("*").alias("variant_count"),
        
        # Clinical significance
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        spark_sum(when(col("clinical_significance") == "Uncertain significance", 1).otherwise(0)).alias("vus_count"),
        
        # Mutation types (ultra-enriched)
        spark_sum(when(col("is_frameshift"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense"), 1).otherwise(0)).alias("nonsense_count"),
        spark_sum(when(col("is_splice"), 1).otherwise(0)).alias("splice_count"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_count"),
        
        # Origin
        spark_sum(when(col("is_germline"), 1).otherwise(0)).alias("germline_count"),
        spark_sum(when(col("is_somatic"), 1).otherwise(0)).alias("somatic_count"),
        
        # Clinical utility
        avg(col("clinical_actionability_score")).alias("avg_actionability"),
        avg(col("mutation_severity_score")).alias("avg_severity")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_percentage",
                spark_round(
                    when(col("variant_count") > 0,
                         (col("total_pathogenic") / col("variant_count")) * 100)
                    .otherwise(0),
                    2
                ))
    .withColumn("avg_mutations_per_gene",
                spark_round(
                    when(col("gene_count") > 0,
                         col("variant_count") / col("gene_count"))
                    .otherwise(0),
                    2
                ))
    .orderBy("chromosome")
)

chrom_count = df_chromosome_features.count()
print(f"Created chromosome features for {chrom_count} chromosomes")

# COMMAND ----------

# DBTITLE 1,Ultra-Enriched Gene-Disease Associations
print("\n" + "="*70)
print("CREATING ULTRA-ENRICHED GENE-DISEASE ASSOCIATIONS")
print("="*70)

# Explode disease arrays with enriched disease identifiers
df_variants_exploded = (
    df_variants
    .filter(col("disease_array").isNotNull())
    .filter(size(col("disease_array")) > 0)
    .select(
        "gene_name",
        explode(col("disease_array")).alias("disease"),
        "clinical_significance",
        
        # Disease database IDs (ultra-enriched)
        "omim_disease_id",
        "orphanet_disease_id",
        "mondo_disease_id",
        "medgen_disease_id",
        
        # Disease flags
        "has_cancer_disease",
        "has_syndrome",
        "has_hereditary",
        "has_rare_disease",
        
        # Mutation details
        "is_frameshift",
        "is_nonsense",
        "is_missense",
        "mutation_severity_score",
        
        # Quality and utility
        "quality_tier",
        "review_quality_score",
        "clinical_actionability_score",
        "clinical_utility_score"
    )
    .filter(col("disease").isNotNull())
    .filter(col("disease") != "")
)

df_gene_disease = (
    df_variants_exploded
    .groupBy("gene_name", "disease")
    .agg(
        count("*").alias("mutation_count"),
        
        # Clinical significance
        spark_sum(when(col("clinical_significance") == "Pathogenic", 1).otherwise(0)).alias("pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Likely Pathogenic", 1).otherwise(0)).alias("likely_pathogenic_count"),
        spark_sum(when(col("clinical_significance") == "Benign", 1).otherwise(0)).alias("benign_count"),
        spark_sum(when(col("clinical_significance") == "Likely Benign", 1).otherwise(0)).alias("likely_benign_count"),
        
        # Disease database IDs (take first non-null)
        expr("first(omim_disease_id, true)").alias("omim_disease_id"),
        expr("first(orphanet_disease_id, true)").alias("orphanet_disease_id"),
        expr("first(mondo_disease_id, true)").alias("mondo_disease_id"),
        
        # Disease characteristics
        spark_sum(when(col("has_cancer_disease"), 1).otherwise(0)).alias("cancer_disease_flag"),
        spark_sum(when(col("has_syndrome"), 1).otherwise(0)).alias("syndrome_flag"),
        spark_sum(when(col("has_hereditary"), 1).otherwise(0)).alias("hereditary_flag"),
        spark_sum(when(col("has_rare_disease"), 1).otherwise(0)).alias("rare_disease_flag"),
        
        # Mutation types
        spark_sum(when(col("is_frameshift"), 1).otherwise(0)).alias("frameshift_count"),
        spark_sum(when(col("is_nonsense"), 1).otherwise(0)).alias("nonsense_count"),
        spark_sum(when(col("is_missense"), 1).otherwise(0)).alias("missense_count"),
        
        # Quality and utility scores
        avg(col("review_quality_score")).alias("avg_quality"),
        avg(col("clinical_actionability_score")).alias("avg_actionability"),
        avg(col("clinical_utility_score")).alias("avg_clinical_utility"),
        avg(col("mutation_severity_score")).alias("avg_severity")
    )
    .withColumn("total_pathogenic",
                col("pathogenic_count") + col("likely_pathogenic_count"))
    .withColumn("total_benign",
                col("benign_count") + col("likely_benign_count"))
    .withColumn("pathogenic_ratio",
                spark_round(
                    when(col("mutation_count") > 0,
                         col("total_pathogenic") / col("mutation_count"))
                    .otherwise(0),
                    4
                ))
    .withColumn("association_strength",
                when(col("pathogenic_ratio") >= 0.8, "Strong")
                .when(col("pathogenic_ratio") >= 0.5, "Moderate")
                .otherwise("Weak"))
    .filter(col("mutation_count") >= 1)
    .orderBy(col("pathogenic_ratio").desc())
)

assoc_count = df_gene_disease.count()
print(f"Created {assoc_count:,} ultra-enriched gene-disease associations")

# COMMAND ----------

# DBTITLE 1,Sample Gene-Disease Associations
print("\nTop Gene-Disease Associations (high clinical utility):")
display(
    df_gene_disease.filter(col("avg_clinical_utility") >= 3)
                   .orderBy(col("pathogenic_ratio").desc())
                   .select(
                       "gene_name",
                       "disease",
                       "omim_disease_id",
                       "mutation_count",
                       "pathogenic_ratio",
                       "avg_clinical_utility",
                       "avg_actionability",
                       "association_strength"
                   )
                   .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Save Ultra-Enriched Gene Features
print("\n" + "="*70)
print("SAVING TO GOLD LAYER")
print("="*70)

df_gene_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_features")

print(f"Saved: {catalog_name}.gold.gene_features")

# COMMAND ----------

# DBTITLE 1,Save Chromosome Features
df_chromosome_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.chromosome_features")

print(f"Saved: {catalog_name}.gold.chromosome_features")

# COMMAND ----------

# DBTITLE 1,Save Gene-Disease Associations
df_gene_disease.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gene_disease_association")

print(f"Saved: {catalog_name}.gold.gene_disease_association")

# COMMAND ----------

# DBTITLE 1,Prepare Ultra-Enriched ML Features
print("\n" + "="*70)
print("CREATING ULTRA-ENRICHED ML FEATURES")
print("="*70)

df_ml_features = df_gene_features.select(
    # Core identifiers
    "gene_name", 
    "gene_id",
    "chromosome",
    "map_location",
    
    # Genomic characteristics
    "gene_length",
    "cyto_arm",
    "cyto_region",
    "is_telomeric",
    "is_centromeric",
    
    # Mutation counts
    "mutation_count", 
    "pathogenic_count",
    "likely_pathogenic_count",
    "total_pathogenic",
    "benign_count",
    "likely_benign_count",
    "total_benign",
    "vus_count",
    
    # Mutation type counts (ultra-enriched)
    "frameshift_count",
    "nonsense_count",
    "splice_count",
    "missense_count",
    "deletion_count",
    "insertion_count",
    "duplication_count",
    "indel_count",
    
    # Origin counts
    "germline_count",
    "somatic_count",
    "de_novo_count",
    "maternal_count",
    "paternal_count",
    
    # Disease counts
    "cancer_variant_count",
    "syndrome_variant_count",
    "hereditary_variant_count",
    "rare_disease_count",
    
    # Clinical utility
    "drug_response_count",
    "risk_factor_count",
    "avg_clinical_actionability",
    "avg_clinical_utility",
    "avg_mutation_severity",
    
    # Ratios
    "pathogenic_ratio",
    "benign_ratio",
    "vus_ratio",
    "severe_mutation_ratio",
    "germline_ratio",
    
    # Quality metrics
    "avg_review_quality",
    "high_quality_count",
    "recently_evaluated_count",
    "omim_linked_count",
    "orphanet_linked_count",
    "avg_phenotype_db_coverage",
    
    # Metrics
    "disease_count",
    "variant_type_count", 
    "mutation_density", 
    "risk_level",
    "risk_score",
    
    # Database IDs
    "mim_id",
    "hgnc_id",
    "ensembl_id",
    "database_coverage_score",
    
    # Functional categories
    "primary_function",
    "biological_process",
    "cellular_location",
    "druggability_score",
    
    # Functional flags (17 functions)
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
    
    # Disease associations (9 categories)
    "cancer_related",
    "immune_related",
    "neurological_related",
    "cardiovascular_related",
    "metabolic_related",
    "developmental_related",
    "alzheimer_related",
    "diabetes_related",
    "breast_cancer_related",
    
    # Cellular locations (9 compartments)
    "nuclear",
    "mitochondrial",
    "cytoplasmic",
    "membrane",
    "extracellular",
    "endoplasmic_reticulum",
    "golgi",
    "lysosomal",
    "peroxisomal",
    
    # Quality
    "metadata_completeness",
    "is_well_characterized"
)

ml_count = df_ml_features.count()
print(f"Created ML features for {ml_count:,} genes")
print(f"Total ML feature columns: {len(df_ml_features.columns)}")

# COMMAND ----------

# DBTITLE 1,Save ML Features
df_ml_features.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.ml_features")

print(f"Saved: {catalog_name}.gold.ml_features")

# COMMAND ----------

# DBTITLE 1,Verification & Summary
print("\n" + "="*70)
print("ULTRA-ENRICHED FEATURE ENGINEERING COMPLETE!")
print("="*70)
print("\nGold Layer Tables Created:")
print(f"  gene_features: {feature_count:,} rows")
print(f"  chromosome_features: {chrom_count:,} rows")
print(f"  gene_disease_association: {assoc_count:,} rows")
print(f"  ml_features: {ml_count:,} rows")

print("\n" + "="*70)
print("ULTRA-ENRICHMENT IMPROVEMENTS")
print("="*70)
print("Gene features now include:")
print("  - 17 functional protein types (kinase, GPCR, protease, etc.)")
print("  - 8 mutation types (missense, frameshift, indel, etc.)")
print("  - 5 inheritance patterns (germline, de novo, maternal, etc.)")
print("  - 9 disease categories (cancer, immune, metabolic, etc.)")
print("  - 9 cellular locations (nuclear, mitochondrial, ER, etc.)")
print("  - Clinical utility scores (actionability, severity, utility)")
print("  - Disease database IDs (OMIM, Orphanet, MONDO, MedGen)")
print("  - Quality metrics (review scores, evaluation recency)")
print(f"\nTotal ML features: {len(df_ml_features.columns)} columns")
print("\n" + "="*70)
print("READY FOR EXPORT AND ADVANCED ML MODEL TRAINING")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Ultra-Enriched Statistics
print("\nUltra-Enriched Statistics:")

print("\n1. Genes by Function and Risk:")
display(
    df_gene_features.groupBy("primary_function", "risk_level")
                    .count()
                    .orderBy("primary_function", col("count").desc())
)

print("\n2. Top 10 Druggable Genes (High Clinical Utility):")
display(
    df_gene_features.filter(col("druggability_score") >= 3)
                    .orderBy(col("avg_clinical_utility").desc())
                    .select(
                        "gene_name",
                        "primary_function",
                        "druggability_score",
                        "mutation_count",
                        "pathogenic_ratio",
                        "avg_clinical_utility",
                        "avg_clinical_actionability",
                        "risk_level"
                    )
                    .limit(10)
)

print("\n3. Top 10 Cancer Kinases:")
display(
    df_gene_features.filter(col("is_kinase") & col("cancer_related"))
                    .orderBy(col("cancer_variant_count").desc())
                    .select(
                        "gene_name",
                        "mutation_count",
                        "cancer_variant_count",
                        "pathogenic_ratio",
                        "druggability_score",
                        "avg_clinical_utility",
                        "risk_level"
                    )
                    .limit(10)
)

print("\n4. Distribution by Cellular Location:")
display(
    df_gene_features.groupBy("cellular_location")
                    .agg(
                        count("*").alias("gene_count"),
                        avg("mutation_count").alias("avg_mutations"),
                        avg("druggability_score").alias("avg_druggability")
                    )
                    .orderBy(col("gene_count").desc())
)
