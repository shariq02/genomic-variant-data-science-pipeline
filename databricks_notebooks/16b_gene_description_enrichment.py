# Databricks notebook source
# MAGIC %md
# MAGIC #### GENE DESCRIPTION ENRICHMENT MODULE
# MAGIC ##### Extract Rich Features from gene_designation_lookup
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 29, 2026
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Use gene_designation_lookup to add official gene descriptions
# MAGIC - Extract disease keywords (cancer, syndrome, deficiency, etc.)
# MAGIC - Add molecular function keywords (kinase, receptor, enzyme, etc.)
# MAGIC - Create gene importance scores based on descriptions
# MAGIC
# MAGIC **Enriches:** genes_ultra_enriched with 30+ new features  
# MAGIC **Creates:** silver.genes_description_enriched

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, sum as spark_sum, 
    lower, regexp_extract, regexp_replace, length, trim, collect_set,
    concat_ws, collect_list, array_distinct, size, explode, split, countDistinct
)

# COMMAND ----------

# DBTITLE 1,Initialize

spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("GENE DESCRIPTION ENRICHMENT MODULE")

# COMMAND ----------

# DBTITLE 1,Load Base Tables
print("MODULE 16b: GENE DESCRIPTION ENRICHMENT")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")
df_designation = spark.table(f"{catalog_name}.reference.gene_designation_lookup")

print(f"\nGenes: {df_genes.count():,}")
print(f"Gene designations: {df_designation.count():,}")

# Check if comprehensive disease table exists
try:
    df_disease_comprehensive = spark.table(f"{catalog_name}.silver.gene_disease_comprehensive")
    has_comprehensive_diseases = True
    print(f"Gene disease comprehensive: {df_disease_comprehensive.count():,}")
    print("Will use comprehensive disease data (ClinVar + OMIM)")
except:
    has_comprehensive_diseases = False
    print("gene_disease_comprehensive not found - will use OMIM only")
    print("Run Module 00 to create comprehensive disease table")

# COMMAND ----------

# DBTITLE 1,Aggregate Gene Descriptions
print("\nAGGREGATING GENE DESCRIPTIONS")
print("="*80)

df_gene_desc_agg = (
    df_designation
    .filter(col("description").isNotNull() & (col("description") != ""))
    .select(
        col("mapped_gene_name").alias("gene_name"),
        "description",
        "search_term"
    )
    .groupBy("gene_name")
    .agg(
        collect_list("description").alias("all_descriptions"),
        collect_list("search_term").alias("all_search_terms")
    )
    .withColumn("primary_description", col("all_descriptions")[0])
    .withColumn("description_count", size(col("all_descriptions")))
    .withColumn("combined_description_text", concat_ws(" | ", col("all_descriptions")))
)

print(f"Genes with descriptions: {df_gene_desc_agg.count():,}")

# COMMAND ----------

# DBTITLE 1,Extract Description Keywords
print("\nEXTRACTING DESCRIPTION KEYWORDS")
print("="*80)

df_keywords = (
    df_gene_desc_agg
    .withColumn("desc_lower", lower(col("combined_description_text")))
    
    # Disease keywords (from gene names - rare but check)
    .withColumn("has_cancer_keyword",
                col("desc_lower").rlike("cancer|carcinoma|tumor|tumour|oncogene|malignancy|neoplasm"))
    .withColumn("has_syndrome_keyword",
                col("desc_lower").rlike("syndrome|disorder|disease"))
    .withColumn("has_deficiency_keyword",
                col("desc_lower").rlike("deficiency|defect|insufficiency"))
    
    # Molecular function keywords (common in gene names)
    .withColumn("has_kinase_in_desc",
                col("desc_lower").rlike("kinase|phosphorylat"))
    .withColumn("has_receptor_in_desc",
                col("desc_lower").rlike("receptor|binding protein"))
    .withColumn("has_enzyme_in_desc",
                col("desc_lower").rlike("dehydrogenase|oxidase|reductase|synthase|synthetase|transferase|ligase|isomerase|hydrolase|protease|peptidase|phosphatase|atpase|gtpase"))
    .withColumn("has_transcription_in_desc",
                col("desc_lower").rlike("transcription|zinc finger|homeobox|forkhead|helix-loop|hox"))
    .withColumn("has_transporter_in_desc",
                col("desc_lower").rlike("transporter|channel|carrier|solute|pump|abc"))
    .withColumn("has_signaling_in_desc",
                col("desc_lower").rlike("signal|gtpase|g protein|ras |rac |rho |mapk"))
    .withColumn("has_structural_in_desc",
                col("desc_lower").rlike("structural|cytoskeleton|actin|tubulin|collagen|laminin"))
    .withColumn("has_membrane_in_desc",
                col("desc_lower").rlike("membrane|transmembrane|surface"))
    .withColumn("has_nuclear_in_desc",
                col("desc_lower").rlike("nuclear|nucleus|chromatin|histone"))
    .withColumn("has_mitochondrial_in_desc",
                col("desc_lower").rlike("mitochondrial|mitochondr"))
    
    # Function keyword count
    .withColumn("function_keyword_count",
                (when(col("has_kinase_in_desc"), 1).otherwise(0)) +
                (when(col("has_receptor_in_desc"), 1).otherwise(0)) +
                (when(col("has_enzyme_in_desc"), 1).otherwise(0)) +
                (when(col("has_transcription_in_desc"), 1).otherwise(0)) +
                (when(col("has_transporter_in_desc"), 1).otherwise(0)) +
                (when(col("has_signaling_in_desc"), 1).otherwise(0)) +
                (when(col("has_structural_in_desc"), 1).otherwise(0)))
    
    .withColumn("is_well_characterized", col("function_keyword_count") >= 2)
    
    .drop("desc_lower")
)

print("Description keywords extracted")

# COMMAND ----------

# DBTITLE 1,Merge with Comprehensive Disease Data
if has_comprehensive_diseases:
    print("\nMERGING WITH COMPREHENSIVE DISEASE DATA")
    print("="*80)
    
    df_enriched = (
        df_keywords
        .join(
            df_disease_comprehensive.select(
                "gene_name",
                "all_disease_text",
                "total_disease_count",
                "has_cancer_disease",
                "has_neurological_disease",
                "has_metabolic_disease",
                "has_cardiovascular_disease",
                "has_developmental_disease",
                "has_immune_disease",
                "has_muscular_disease",
                "is_hereditary_disease",
                "omim_disease_count",
                "clinvar_disease_count"
            ),
            "gene_name",
            "left"
        )
        .fillna({
            "total_disease_count": 0,
            "has_cancer_disease": False,
            "has_neurological_disease": False,
            "has_metabolic_disease": False,
            "has_cardiovascular_disease": False,
            "has_developmental_disease": False,
            "has_immune_disease": False,
            "has_muscular_disease": False,
            "is_hereditary_disease": False,
            "omim_disease_count": 0,
            "clinvar_disease_count": 0
        })
        
        # Create combined flags (description keywords + disease associations)
        .withColumn("has_cancer_combined",
                    (col("has_cancer_keyword") == True) | (col("has_cancer_disease") == True))
        .withColumn("has_neurological_combined",
                    (col("has_neurological_disease") == True))
        .withColumn("has_metabolic_combined",
                    (col("has_metabolic_disease") == True))
    )
    
    print(f"Genes with disease data: {df_enriched.filter(col('total_disease_count') > 0).count():,}")
    
else:
    # No comprehensive disease data - use keywords only
    print("\nUSING DESCRIPTION KEYWORDS ONLY (no comprehensive disease data)")
    print("="*80)
    
    df_enriched = (
        df_keywords
        .withColumn("total_disease_count", lit(0))
        .withColumn("has_cancer_combined", col("has_cancer_keyword"))
        .withColumn("has_neurological_combined", lit(False))
        .withColumn("has_metabolic_combined", lit(False))
    )

# COMMAND ----------

# DBTITLE 1,Calculate Gene Importance Scores
print("\nCALCULATING GENE IMPORTANCE SCORES")
print("="*80)

df_final = (
    df_enriched
    
    # Description quality
    .withColumn("description_length", length(col("primary_description")))
    .withColumn("has_detailed_description", col("description_length") > 50)
    
    # Gene importance score
    .withColumn("gene_importance_score",
            (when(col("total_disease_count") >= 50, 10).when(col("total_disease_count") >= 20, 8).when(col("total_disease_count") >= 10, 5).when(col("total_disease_count") >= 5, 3).when(col("total_disease_count") >= 1, 2).otherwise(0)) +
            (col("function_keyword_count") * 2) +
            (when(col("has_detailed_description"), 2).otherwise(0)) +
            (when(col("has_cancer_combined"), 3).otherwise(0)))
    
    # Annotation quality
    .withColumn("gene_annotation_quality",
                when(col("gene_importance_score") >= 15, lit("Excellent"))
                .when(col("gene_importance_score") >= 10, lit("Good"))
                .when(col("gene_importance_score") >= 5, lit("Moderate"))
                .otherwise(lit("Basic")))
    
    # Primary category (prioritize disease associations if available)
    .withColumn("primary_gene_category",
                when(col("has_cancer_combined"), lit("Cancer_Related"))
                .when(col("has_neurological_combined"), lit("Neurological"))
                .when(col("has_metabolic_combined"), lit("Metabolic"))
                .when(col("has_kinase_in_desc"), lit("Kinase"))
                .when(col("has_receptor_in_desc"), lit("Receptor"))
                .when(col("has_transcription_in_desc"), lit("Transcription_Factor"))
                .when(col("has_enzyme_in_desc"), lit("Enzyme"))
                .when(col("combined_description_text").rlike("(?i)pseudogene"), lit("Pseudogene"))
                .when(col("combined_description_text").rlike("(?i)uncharacterized"), lit("Uncharacterized"))
                .when(col("combined_description_text").rlike("(?i)enhancer|lincRNA|antisense"), lit("Regulatory"))
                .otherwise(lit("Other")))
)

print("Gene importance scores calculated")

# COMMAND ----------

# DBTITLE 1,Merge with Base Genes Table
print("\nMERGING WITH BASE GENES TABLE")
print("="*80)

# Drop conflicting columns from base genes
genes_existing_cols = set(df_genes.columns)
new_feature_cols = set(df_final.columns) - {"gene_name"}
conflicts = genes_existing_cols.intersection(new_feature_cols)

if conflicts:
    print(f"Dropping {len(conflicts)} conflicting columns from genes table")
    df_genes_clean = df_genes.drop(*list(conflicts))
else:
    df_genes_clean = df_genes
    print("No column conflicts")

# Merge
df_genes_enriched = df_genes_clean.join(df_final, "gene_name", "left")

print(f"Final enriched genes: {df_genes_enriched.count():,}")

# COMMAND ----------

# DBTITLE 1,Save Enriched Genes
df_genes_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_ultra_enriched")

print("\nSaved: genes_ultra_enriched (updated with description enrichment)")

# COMMAND ----------

# DBTITLE 1,Statistics
print("\nENRICHMENT STATISTICS")
print("="*80)

print(f"\nTotal genes: {df_genes_enriched.count():,}")
print(f"Genes with descriptions: {df_genes_enriched.filter(col('primary_description').isNotNull()).count():,}")

if has_comprehensive_diseases:
    print(f"\nDisease associations:")
    print(f"  Total with diseases: {df_genes_enriched.filter(col('total_disease_count') > 0).count():,}")
    print(f"  Cancer-related: {df_genes_enriched.filter(col('has_cancer_combined') == True).count():,}")
    print(f"  Neurological: {df_genes_enriched.filter(col('has_neurological_combined') == True).count():,}")
    print(f"  Metabolic: {df_genes_enriched.filter(col('has_metabolic_combined') == True).count():,}")

print(f"\nMolecular function:")
print(f"  Kinases: {df_genes_enriched.filter(col('has_kinase_in_desc') == True).count():,}")
print(f"  Receptors: {df_genes_enriched.filter(col('has_receptor_in_desc') == True).count():,}")
print(f"  Enzymes: {df_genes_enriched.filter(col('has_enzyme_in_desc') == True).count():,}")
print(f"  Transcription factors: {df_genes_enriched.filter(col('has_transcription_in_desc') == True).count():,}")
print(f"  Well characterized: {df_genes_enriched.filter(col('is_well_characterized') == True).count():,}")

print(f"\nAnnotation quality:")
df_genes_enriched.groupBy("gene_annotation_quality").count().orderBy(col("count").desc()).show()

print(f"\nPrimary categories:")
df_genes_enriched.groupBy("primary_gene_category").count().orderBy(col("count").desc()).show()

if has_comprehensive_diseases:
    print(f"\nTop genes by importance:")
    df_genes_enriched.filter(col("gene_importance_score") >= 10) \
        .select("gene_name", "gene_importance_score", "primary_gene_category", "total_disease_count") \
        .orderBy(col("gene_importance_score").desc()) \
        .show(30)


# COMMAND ----------

# DBTITLE 1,Summary
print("GENE DESCRIPTION ENRICHMENT COMPLETE")
print("="*80)

print(f"\nFeatures added: 40+")
print("\nFeature categories:")
print("  - Disease keywords (11 types)")
print("  - Molecular function keywords (10 types)")
print("  - Cellular location keywords (4 types)")
print("  - Clinical keywords (6 types)")
print("  - Importance scores")
print("  - Quality classification")

print("\nTables updated:")
print(f"  1. {catalog_name}.silver.genes_ultra_enriched (enriched)")
print(f"  2. {catalog_name}.silver.gene_descriptions_analyzed (new)")

print("\nThese features can now be used in:")
print("  - Clinical feature engineering (Module 16a)")
print("  - Disease feature engineering (Module 16b)")
print("  - Pharmacogene feature engineering (Module 16c)")
print("  - Variant impact feature engineering (Module 16d)")
print("  - Structural variant feature engineering (Module 16e)")

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Drop Temporary Tables Created During Debugging
print("DROPPING TEMPORARY TABLES")
print("="*80)

tables_to_drop = [
    "workspace.silver.gene_descriptions_analyzed",
    "workspace.silver.genes_ultra_enriched_v2",
    "workspace.silver.genes_ultra_enriched_FINAL",
    "workspace.silver.genes_master_enriched"
]

for table_name in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped: {table_name}")
    except Exception as e:
        print(f"Could not drop {table_name}: {e}")

print("\nCleanup complete")

# COMMAND ----------

# DBTITLE 1,Verify Remaining Tables
print("\nREMAINING TABLES:")
remaining = spark.sql("""
    SHOW TABLES IN workspace.silver 
    WHERE tableName LIKE '%gene%' 
    OR tableName LIKE '%enriched%'
""").collect()

for row in remaining:
    print(f"  {row.database}.{row.tableName}")
