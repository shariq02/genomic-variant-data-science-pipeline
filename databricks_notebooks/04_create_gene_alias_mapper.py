# Databricks notebook source
# MAGIC %md
# MAGIC ## GENE ALIAS & DESIGNATION MAPPING DICTIONARY
# MAGIC ### Resolve NULL values and enable comprehensive gene search
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** January 14, 2026  
# MAGIC
# MAGIC **Run Order:** After gene processing, BEFORE feature engineering
# MAGIC ```
# MAGIC 1. ✅ 02_gene_data_processing_ENHANCED.py
# MAGIC 2. ✅ 03_variant_data_processing_ENHANCED.py
# MAGIC 3. ▶️ 06_create_gene_alias_mapper.py (THIS SCRIPT)
# MAGIC 4. ⏭️ 04_feature_engineering.py
# MAGIC ```
# MAGIC
# MAGIC **Purpose:** Create lookup tables to map ANY alias/designation to its primary gene
# MAGIC
# MAGIC **Problem Solved:**
# MAGIC ```
# MAGIC BEFORE (with NULLs):
# MAGIC gene_name: "A1BG"
# MAGIC alias_1: "A1B"
# MAGIC alias_2: "ABG"
# MAGIC alias_3: "GAB"
# MAGIC alias_4: null  ← Can't search!
# MAGIC alias_5: null  ← Can't search!
# MAGIC
# MAGIC AFTER (with mapping):
# MAGIC Search: "A1B"  → "A1BG" ✅
# MAGIC Search: "ABG"  → "A1BG" ✅
# MAGIC Search: "GAB"  → "A1BG" ✅
# MAGIC Search: "A1BG" → "A1BG" ✅
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, upper, trim, when, array, concat_ws,
    collect_list, struct, lit, coalesce, array_distinct, flatten,
    lower, count, countDistinct
)
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("="*80)
print("GENE ALIAS & DESIGNATION MAPPING DICTIONARY")
print("="*80)
print(f"SparkSession initialized")
print(f"Spark version: {spark.version}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print(f"\n📁 Catalog: {catalog_name}")
print(f"📥 Input:  {catalog_name}.silver.genes_ultra_enriched")
print(f"📤 Output: {catalog_name}.reference.gene_*_lookup tables")
print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Read Ultra-Enriched Genes
print("STEP 1: READING ULTRA-ENRICHED GENES")
print("="*80)

df_genes = spark.table(f"{catalog_name}.silver.genes_ultra_enriched")

gene_count = df_genes.count()
print(f"✅ Loaded {gene_count:,} ultra-enriched genes")

# Show sample data structure
print("\n📋 Sample gene data:")
df_genes.select(
    "gene_name", 
    "official_symbol",
    "alias_1", "alias_2", "alias_3",
    "designation_1", "designation_2"
).show(5, truncate=40)

# COMMAND ----------

# DBTITLE 1,METHOD 2: Create Designation→Gene Lookup Table
print("\n" + "="*80)
print("METHOD 2: CREATE DESIGNATION→GENE LOOKUP TABLE")
print("="*80)
print("Maps protein designations to genes (e.g., 'alpha-1B-glycoprotein' → 'A1BG')")
print("="*80)

# Collect ALL designations into a single array (filtering out NULLs)
df_all_designations = (
    df_genes
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "description",
        "chromosome",
        "mim_id",
        "ensembl_id",
        
        # Create comprehensive designation array (only non-null values)
        array_distinct(
            flatten(
                array(
                    # Include description as a searchable term
                    when(col("description").isNotNull(), array(col("description"))).otherwise(array()),
                    
                    # Include all designation columns
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
        ).alias("all_designations_array")
    )
    # Only keep genes that have at least one designation
    .filter(col("all_designations_array").isNotNull())
    .filter(col("all_designations_array") != array())
)

print(f"✅ Collected designations from {df_all_designations.count():,} genes")

# Explode designations into separate rows (one row per designation)
df_designation_mapping = (
    df_all_designations
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        explode(col("all_designations_array")).alias("designation"),
        "description"
    )
    # Create uppercase search term for case-insensitive matching
    .withColumn("search_term", upper(trim(col("designation"))))
    
    # Remove duplicates (same designation might appear for multiple genes)
    # Keep first occurrence
    .dropDuplicates(["search_term"])
    
    .select(
        col("search_term").alias("search_term"),
        col("gene_id").alias("mapped_gene_id"),
        col("gene_name").alias("mapped_gene_name"),
        col("official_symbol").alias("mapped_official_symbol"),
        col("designation").alias("original_designation"),
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description"
    )
    .orderBy("search_term")
)

designation_count = df_designation_mapping.count()
print(f"✅ Created designation mapping table with {designation_count:,} searchable designations")

# Show sample mappings
print("\n📋 Sample designation mappings:")
df_designation_mapping.show(10, truncate=60)

# Show statistics
print("\n📊 Designation Statistics:")
print(f"   Total unique designations: {designation_count:,}")
print(f"   Genes with designations: {df_all_designations.count():,}")
print(f"   Avg designations per gene: {designation_count / df_all_designations.count():.2f}")

# COMMAND ----------

# DBTITLE 1,Save Designation Lookup Table
print("\n" + "="*80)
print("SAVING DESIGNATION LOOKUP TABLE")
print("="*80)

df_designation_mapping.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.gene_designation_lookup")

saved_count = spark.table(f"{catalog_name}.reference.gene_designation_lookup").count()
print(f"✅ Saved to: {catalog_name}.reference.gene_designation_lookup")
print(f"✅ Verified: {saved_count:,} designation mappings")

# COMMAND ----------

# DBTITLE 1,METHOD 3: Create Universal Search Table (Aliases + Designations)
print("\n" + "="*80)
print("METHOD 3: CREATE UNIVERSAL GENE SEARCH TABLE")
print("="*80)
print("Combines BOTH aliases and designations for comprehensive search")
print("="*80)

# First, create alias mappings
print("\n📦 Creating alias mappings...")

df_all_aliases = (
    df_genes
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description",
        
        # Create comprehensive alias array (non-null values only)
        array_distinct(
            flatten(
                array(
                    # Always include gene_name and official_symbol
                    array(col("gene_name")),
                    when(col("official_symbol") != col("gene_name"), 
                         array(col("official_symbol"))).otherwise(array()),
                    
                    # Include all alias columns (only non-null)
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
        ).alias("all_aliases_array")
    )
)

# Explode aliases into separate rows
df_alias_search = (
    df_all_aliases
    .select(
        "gene_id",
        "gene_name",
        "official_symbol",
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description",
        explode(col("all_aliases_array")).alias("alias")
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
print(f"✅ Created {alias_count:,} alias mappings")

# Create designation search records
print("\n📦 Creating designation search records...")

df_designation_search = (
    df_designation_mapping
    .select(
        "search_term",
        col("mapped_gene_id").alias("mapped_gene_id"),
        col("mapped_gene_name").alias("mapped_gene_name"),
        col("mapped_official_symbol").alias("mapped_official_symbol"),
        col("original_designation").alias("search_text"),
        lit("designation").alias("match_type"),
        "chromosome",
        "mim_id",
        "ensembl_id",
        "description"
    )
)

print(f"✅ Using {designation_count:,} designation mappings")

# Union both tables to create universal search
print("\n🔗 Combining aliases and designations...")

df_universal_search = (
    df_alias_search
    .union(df_designation_search)
    .dropDuplicates(["search_term"])
    .orderBy("search_term")
)

universal_count = df_universal_search.count()
print(f"✅ Created universal search table with {universal_count:,} total searchable terms")

# Show sample from both match types
print("\n📋 Sample universal search entries:")
print("\n▶️ Alias matches:")
df_universal_search.filter(col("match_type") == "alias").show(5, truncate=60)

print("\n▶️ Designation matches:")
df_universal_search.filter(col("match_type") == "designation").show(5, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Universal Search Table
print("\n" + "="*80)
print("SAVING UNIVERSAL GENE SEARCH TABLE")
print("="*80)

df_universal_search.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.reference.gene_universal_search")

saved_universal = spark.table(f"{catalog_name}.reference.gene_universal_search").count()
print(f"✅ Saved to: {catalog_name}.reference.gene_universal_search")
print(f"✅ Verified: {saved_universal:,} searchable terms")

# COMMAND ----------

# DBTITLE 1,Create Statistics Summary
print("\n" + "="*80)
print("📊 SEARCH TABLE STATISTICS")
print("="*80)

# Calculate comprehensive statistics
stats = {
    "total_genes": gene_count,
    "alias_mappings": alias_count,
    "designation_mappings": designation_count,
    "universal_search_terms": universal_count,
    "avg_aliases_per_gene": alias_count / gene_count,
    "avg_designations_per_gene": designation_count / gene_count,
    "avg_search_terms_per_gene": universal_count / gene_count
}

print("\n📈 Coverage Statistics:")
for key, value in stats.items():
    if "avg" in key:
        print(f"   {key:.<40} {value:.2f}")
    else:
        print(f"   {key:.<40} {value:,}")

# Match type distribution
print("\n📋 Universal Search - Match Type Distribution:")
df_universal_search.groupBy("match_type").count().orderBy(col("count").desc()).show()

# Top genes by number of search terms
print("\n🏆 Top 10 Genes by Number of Search Terms:")
df_universal_search.groupBy("mapped_gene_name") \
    .agg(count("*").alias("search_term_count")) \
    .orderBy(col("search_term_count").desc()) \
    .show(10)

# COMMAND ----------

# DBTITLE 1,Create SQL View for Easy Querying
print("\n" + "="*80)
print("CREATING SQL VIEWS")
print("="*80)

# Create SQL view for easy querying
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

print(f"✅ Created SQL view: {catalog_name}.reference.gene_search_view")

# Test the view
print("\n🧪 Testing SQL view:")
spark.sql(f"""
    SELECT match_type, COUNT(*) as count 
    FROM {catalog_name}.reference.gene_search_view 
    GROUP BY match_type
""").show()

# COMMAND ----------

# DBTITLE 1,USAGE EXAMPLES - SQL Queries
print("\n" + "="*80)
print("💡 USAGE EXAMPLES - SQL QUERIES")
print("="*80)

# Load the view for examples
df_search = spark.table(f"{catalog_name}.reference.gene_search_view")

print("\n▶️ Example 1: Search for gene by exact alias 'BRCA1'")
print("-" * 80)
result1 = spark.sql(f"""
    SELECT * 
    FROM {catalog_name}.reference.gene_search_view 
    WHERE search_term = 'BRCA1'
""")
result1.show(truncate=False)

print("\n▶️ Example 2: Get ALL aliases for a specific gene 'BRCA1'")
print("-" * 80)
result2 = spark.sql(f"""
    SELECT search_text, match_type 
    FROM {catalog_name}.reference.gene_search_view 
    WHERE mapped_gene_name = 'BRCA1'
    ORDER BY match_type, search_text
""")
result2.show(20, truncate=False)

print("\n▶️ Example 3: Search for genes containing 'KINASE' (case-insensitive)")
print("-" * 80)
result3 = spark.sql(f"""
    SELECT mapped_gene_name, search_text, match_type 
    FROM {catalog_name}.reference.gene_search_view 
    WHERE search_term LIKE '%KINASE%'
    LIMIT 10
""")
result3.show(truncate=60)

print("\n▶️ Example 4: Find genes by MIM ID")
print("-" * 80)
result4 = spark.sql(f"""
    SELECT DISTINCT mapped_gene_name, mim_id, chromosome 
    FROM {catalog_name}.reference.gene_search_view 
    WHERE mim_id = '113705'
""")
result4.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,USAGE EXAMPLES - PySpark Queries
print("\n" + "="*80)
print("💡 USAGE EXAMPLES - PYSPARK QUERIES")
print("="*80)

print("\n▶️ Example 5: Search using PySpark DataFrame API")
print("-" * 80)
result5 = df_search.filter(col("search_term") == "TP53")
result5.show(truncate=False)

print("\n▶️ Example 6: Find all aliases for multiple genes")
print("-" * 80)
genes_of_interest = ["BRCA1", "BRCA2", "TP53"]
result6 = df_search.filter(col("mapped_gene_name").isin(genes_of_interest)) \
                   .select("mapped_gene_name", "search_text", "match_type") \
                   .orderBy("mapped_gene_name", "match_type")
result6.show(30, truncate=60)

print("\n▶️ Example 7: Fuzzy search - genes with 'receptor' in designation")
print("-" * 80)
result7 = df_search.filter(
    (lower(col("search_term")).contains("receptor")) & 
    (col("match_type") == "designation")
).select("mapped_gene_name", "search_text") \
 .orderBy("mapped_gene_name") \
 .limit(10)
result7.show(truncate=60)

# COMMAND ----------

# DBTITLE 1,USAGE EXAMPLES - Join with Variant Data
print("\n" + "="*80)
print("💡 USAGE EXAMPLES - JOIN WITH VARIANT DATA")
print("="*80)

print("\n▶️ Example 8: Resolve variant gene names using alias lookup")
print("-" * 80)

# Load variants (sample)
df_variants = spark.table(f"{catalog_name}.silver.variants_ultra_enriched").limit(1000)

# Join with gene search to resolve any aliases
df_variants_resolved = (
    df_variants
    .join(
        df_search.select(
            col("search_term").alias("gene_lookup_key"),
            col("mapped_gene_name").alias("resolved_gene_name"),
            col("mapped_gene_id").alias("resolved_gene_id"),
            col("chromosome").alias("resolved_chromosome")
        ),
        upper(df_variants.gene_name) == col("gene_lookup_key"),
        "left"
    )
    .withColumn(
        "final_gene_name",
        coalesce(col("resolved_gene_name"), col("gene_name"))
    )
    .withColumn(
        "gene_was_resolved",
        when(col("resolved_gene_name").isNotNull() & 
             (col("resolved_gene_name") != col("gene_name")), True)
        .otherwise(False)
    )
)

# Show resolved genes
print("Sample resolved genes:")
df_variants_resolved.filter(col("gene_was_resolved")) \
    .select(
        col("gene_name").alias("original_gene"),
        "final_gene_name",
        "resolved_chromosome"
    ).show(10, truncate=False)

resolved_count = df_variants_resolved.filter(col("gene_was_resolved")).count()
print(f"\n✅ Resolved {resolved_count} gene names using alias lookup (in sample of 1000)")

# COMMAND ----------

# DBTITLE 1,Create Helper Functions
print("\n" + "="*80)
print("CREATING HELPER FUNCTIONS")
print("="*80)

# Function 1: Lookup gene by alias
def lookup_gene_by_alias(alias_term, catalog="workspace"):
    """
    Look up a gene by any alias, designation, or gene name.
    
    Args:
        alias_term (str): The term to search for (case-insensitive)
        catalog (str): Catalog name (default: workspace)
    
    Returns:
        DataFrame: Matched gene information
    
    Example:
        >>> lookup_gene_by_alias("BRCA1")
        >>> lookup_gene_by_alias("breast cancer type 1")
    """
    df_search = spark.table(f"{catalog}.reference.gene_universal_search")
    
    result = df_search.filter(
        upper(col("search_term")) == upper(alias_term.strip())
    )
    
    return result

# Function 2: Get all aliases for a gene
def get_all_aliases(gene_name, catalog="workspace"):
    """
    Get all aliases and designations for a specific gene.
    
    Args:
        gene_name (str): Gene name to look up
        catalog (str): Catalog name (default: workspace)
    
    Returns:
        DataFrame: All aliases and designations for the gene
    
    Example:
        >>> get_all_aliases("BRCA1")
    """
    df_search = spark.table(f"{catalog}.reference.gene_universal_search")
    
    result = df_search.filter(
        upper(col("mapped_gene_name")) == upper(gene_name.strip())
    ).select("search_text", "match_type").orderBy("match_type", "search_text")
    
    return result

# Function 3: Batch lookup multiple genes
def batch_lookup_genes(gene_list, catalog="workspace"):
    """
    Look up multiple genes at once.
    
    Args:
        gene_list (list): List of gene names/aliases to look up
        catalog (str): Catalog name (default: workspace)
    
    Returns:
        DataFrame: All matched genes
    
    Example:
        >>> batch_lookup_genes(["BRCA1", "BRCA2", "TP53"])
    """
    df_search = spark.table(f"{catalog}.reference.gene_universal_search")
    
    # Convert list to uppercase for matching
    gene_list_upper = [g.upper().strip() for g in gene_list]
    
    result = df_search.filter(
        col("search_term").isin(gene_list_upper)
    ).select(
        "search_term",
        "mapped_gene_name",
        "mapped_gene_id",
        "match_type",
        "chromosome"
    ).orderBy("mapped_gene_name")
    
    return result

print("✅ Helper functions created:")
print("   1. lookup_gene_by_alias(alias_term)")
print("   2. get_all_aliases(gene_name)")
print("   3. batch_lookup_genes(gene_list)")

# Test the functions
print("\n🧪 Testing helper functions:")

print("\n▶️ Test 1: lookup_gene_by_alias('BRCA1')")
test1 = lookup_gene_by_alias("BRCA1")
test1.show(truncate=False)

print("\n▶️ Test 2: get_all_aliases('BRCA1')")
test2 = get_all_aliases("BRCA1")
test2.show(10, truncate=False)

print("\n▶️ Test 3: batch_lookup_genes(['BRCA1', 'BRCA2', 'TP53'])")
test3 = batch_lookup_genes(["BRCA1", "BRCA2", "TP53"])
test3.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Final Summary & Documentation
print("\n" + "="*80)
print("🎉 GENE ALIAS MAPPING COMPLETE!")
print("="*80)

print("\n📁 CREATED TABLES:")
print("-" * 80)
print(f"1. {catalog_name}.reference.gene_designation_lookup")
print(f"   ├─ {designation_count:,} designation mappings")
print(f"   ├─ Maps protein designations → genes")
print(f"   └─ Example: 'alpha-1B-glycoprotein' → 'A1BG'")
print()
print(f"2. {catalog_name}.reference.gene_universal_search")
print(f"   ├─ {universal_count:,} total searchable terms")
print(f"   ├─ Combines aliases + designations")
print(f"   ├─ Alias matches: {alias_count:,}")
print(f"   ├─ Designation matches: {designation_count:,}")
print(f"   └─ Universal search for ANY gene identifier")
print()
print(f"3. {catalog_name}.reference.gene_search_view")
print(f"   └─ SQL view for easy querying")

print("\n💡 HOW TO USE:")
print("-" * 80)
print("\n1. 🐍 Python/PySpark:")
print("   result = lookup_gene_by_alias('BRCA1')")
print("   aliases = get_all_aliases('BRCA1')")
print("   batch = batch_lookup_genes(['BRCA1', 'BRCA2', 'TP53'])")
print()
print("2. 📊 SQL:")
print("   SELECT * FROM workspace.reference.gene_search_view")
print("   WHERE search_term = 'BRCA1'")
print()
print("3. 🔗 Join with your data:")
print("   variants_df.join(")
print("       gene_search_df,")
print("       variants_df.gene_name == gene_search_df.search_term,")
print("       'left'")
print("   )")

print("\n✨ BENEFITS:")
print("-" * 80)
print("✅ NO MORE NULL VALUES - All aliases are searchable")
print("✅ COMPREHENSIVE SEARCH - {0:,} searchable terms for {1:,} genes".format(universal_count, gene_count))
print("✅ FAST LOOKUPS - Indexed by search_term")
print("✅ FLEXIBLE - Works with ANY gene identifier (alias or designation)")
print("✅ SQL FRIENDLY - Easy to query and join")
print("✅ CASE INSENSITIVE - All searches are uppercase")

print("\n📈 SEARCH COVERAGE:")
print("-" * 80)
print(f"   Genes: {gene_count:,}")
print(f"   Total search terms: {universal_count:,}")
print(f"   Avg terms per gene: {universal_count / gene_count:.2f}")
print(f"   Coverage increase: {(universal_count / gene_count):.1f}x")

print("\n⏭️ NEXT STEPS:")
print("-" * 80)
print("1. ✅ Gene alias mapping complete")
print("2. ▶️ Run 04_feature_engineering.py")
print("3. ▶️ Use these lookup tables in your analysis")
print("4. ▶️ Re-run statistical analysis to see improvements")

print("\n" + "="*80)
print("✅ SCRIPT COMPLETE - READY FOR FEATURE ENGINEERING")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Export Sample Data for Review
print("\n📤 EXPORTING SAMPLE DATA FOR REVIEW")
print("="*80)

# Create sample CSV for review
sample_search = df_universal_search.limit(1000).toPandas()

print(f"✅ Exported {len(sample_search):,} sample search terms to Pandas DataFrame")
print("\nSample data preview:")
print(sample_search.head(10))

# Show how many terms per gene (distribution)
print("\n📊 Search Terms per Gene Distribution:")
terms_per_gene = df_universal_search.groupBy("mapped_gene_name") \
    .agg(count("*").alias("term_count")) \
    .groupBy("term_count") \
    .agg(count("*").alias("gene_count")) \
    .orderBy("term_count")

terms_per_gene.show(20)

print("\n" + "="*80)
print("🎉 ALL DONE!")
print("="*80)
