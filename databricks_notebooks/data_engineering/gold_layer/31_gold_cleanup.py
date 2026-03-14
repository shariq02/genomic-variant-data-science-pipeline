# Databricks notebook source
# MAGIC %md
# MAGIC #### GOLD LAYER CLEANUP - METADATA AND LEAKAGE REMOVAL
# MAGIC ##### Post-Processing: Remove Metadata and AUC Leakage Columns
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** March 14, 2026
# MAGIC
# MAGIC **Purpose:**
# MAGIC Runs AFTER all gold notebooks (17a-17e, 23-29) complete.
# MAGIC Removes metadata/identifier columns and AUC >= 0.99 leakage from all 18 gold tables.
# MAGIC
# MAGIC **DRY RUN MODE:**
# MAGIC Set DRY_RUN = True to preview exclusions without modifying tables.
# MAGIC Set DRY_RUN = False to actually clean and overwrite tables.
# MAGIC
# MAGIC **Creates:**
# MAGIC - gold.cleanup_audit_log (tracks all exclusions)
# MAGIC - Overwrites 18 gold tables with cleaned versions

# COMMAND ----------

# DBTITLE 1,Configuration
DRY_RUN = True  # SET TO FALSE TO ACTUALLY CLEAN TABLES

catalog_name = "workspace"
print("="*80)
print("GOLD LAYER CLEANUP - METADATA AND LEAKAGE REMOVAL")
print("="*80)
print(f"Catalog: {catalog_name}")
print(f"Mode: {'DRY RUN (preview only)' if DRY_RUN else 'LIVE (will modify tables)'}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder.getOrCreate()
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Exclusion Lists - All 18 Tables
print("\nDEFINING EXCLUSION LISTS")
print("="*80)

# Exclusion lists per table (metadata + AUC >= 0.99 columns)
EXCLUSION_LISTS = {
    "clinical_ml_features": [
        # Metadata (4 columns)
        "is_cancer_relevant",
        "protein_impact_category", 
        "x_linked_risk_modifier",
        "inheritance_pathogenicity_modifier"
    ],
    
    "disease_ml_features": [
        # Metadata (10 columns)
        "disease_count_category",
        "disease_association_strength",
        "variant_disease_link_quality",
        "disease_complexity",
        "polygenic_risk_contribution",
        "gene_priority_tier",
        "disease_is_well_annotated",
        "disease_name_is_generic",
        "is_polygenic_disease",
        "is_clinically_actionable",
        # AUC leakage (2 columns)
        "disease_pathogenic_ratio",
        "disease_has_high_pathogenic_burden"
    ],
    
    "pharmacogene_ml_features": [
        # Metadata (7 columns)
        "variant_impact_burden",
        "expression_breadth",
        "drug_metabolism_tissue_expression",
        "cancer_mutation_burden",
        "primary_indication_category",
        "pharmacogene_priority",
        "pharmacogene_category_enhanced"
    ],
    
    "variant_impact_ml_features": [
        # Metadata (12 columns)
        "review_status",
        "variant_impact_tier",
        "lof_category",
        "splice_impact_category",
        "domain_impact_category",
        "conservation_tier",
        "is_loss_of_function",
        "is_splice_affecting",
        "is_domain_affecting",
        "is_very_high_impact",
        "is_conservation_constrained",
        "is_highly_conserved_region",
        # AUC leakage (1 column)
        "pathogenicity_score"
    ],
    
    "structural_variant_ml_features": [
        # Metadata (3 columns)
        "sv_impact_category",
        "gene_disruption_severity",
        "clinical_actionability",
        # AUC leakage (6 columns)
        "disrupted_gene_count",
        "critical_gene_count",
        "disease_gene_count",
        "pharmacogene_count",
        "cancer_gene_count",
        "clinical_priority_score"
    ],
    
    "variant_drug_response_ml_features": [
        # Metadata (9 columns)
        "drug_response_priority",
        "clinical_actionability",
        "pharmacogene_evidence_level",
        "variant_functional_class",
        "metabolizer_phenotype",
        "dosing_impact",
        "adverse_reaction_risk",
        "is_actionable_pharmacogene_variant",
        "is_high_priority_drug_variant",
        # AUC leakage (1 column)
        "drug_response_priority_score"
    ],
    
    "variant_cancer_ml_features": [
        # Metadata (7 columns)
        "mutation_frequency_category",
        "somatic_vs_germline_classification",
        "expression_change_relevance",
        "is_recurrent_mutation",
        "is_hotspot_mutation",
        "is_high_impact_cancer_variant",
        "hereditary_cancer_syndrome",
        # AUC leakage (2 columns)
        "driver_likelihood_score",
        "therapeutic_target_score"
    ],
    
    "variant_population_ml_features": [
        # Metadata (6 columns)
        "frequency_category",
        "carrier_screening_tier",
        "population_risk_category",
        "is_rare",
        "is_ultra_rare",
        "is_common",
        # AUC_HIGH (4 columns - 0.85-0.98)
        "allele_frequency_global",
        "founder_population_enrichment",
        "population_specificity_score",
        "carrier_screening_priority_score"
    ],
    
    "population_frequency_ml_features": [
        # Metadata (5 columns)
        "frequency_tier",
        "actionability_tier",
        "population_specificity",
        "is_ultra_rare",
        "is_rare_actionable",
        # AUC leakage (1 column)
        "clinical_actionability_score"
    ],
    
    "gene_pharmacogene_ml_features": [
        # Metadata (10 columns)
        "has_pharmgkb_annotation",
        "variant_impact_burden",
        "expression_breadth",
        "drug_metabolism_tissue_expression",
        "cancer_mutation_burden",
        "primary_indication_category",
        "pharmacogene_priority",
        "pharmacogene_category_enhanced",
        "clinical_actionability_tier",
        "is_high_priority_pharmacogene",
        # AUC leakage (1 column)
        "clinical_utility_score"
    ],
    
    "gene_expression_ml_features": [
        # Metadata (5 columns)
        "tissue_specificity_category",
        "expression_pattern",
        "primary_tissue",
        "clinical_relevance_tier",
        "is_clinically_relevant_expression",
        # AUC leakage (1 column)
        "clinical_relevance_score"
    ],
    
    "gene_protein_family_ml_features": [
        # Metadata (4 columns)
        "protein_family_tier",
        "druggability_tier",
        "functional_domain_category",
        "is_high_value_protein_family",
        # AUC leakage (1 column)
        "protein_family_priority_score"
    ],
    
    "gene_test_availability_ml_features": [
        # Metadata (6 columns)
        "test_availability_tier",
        "clinical_utility_tier",
        "disease_test_coverage",
        "is_high_priority_test_gene",
        "has_clinical_test_available",
        "has_multiple_test_types",
        # AUC leakage (7 columns)
        "total_test_count",
        "unique_test_count",
        "test_type_diversity",
        "disease_coverage_score",
        "test_availability_score",
        "clinical_utility_score",
        "test_priority_score"
    ],
    
    "transcript_expression_ml_features": [
        # Metadata (1 column)
        "is_clinically_relevant_expression",
        # AUC leakage (1 column)
        "clinical_relevance_score"
    ],
    
    "cancer_variant_ml_features": [
        # Metadata (1 column)
        "gene_cancer_role"
        # NOTE: gene_cancer_role is old multiclass target, keep for reference until cleanup
        # New target is_driver_gene replaces it
    ]
}

# Count totals
total_exclusions = sum(len(cols) for cols in EXCLUSION_LISTS.values())
print(f"Total exclusions defined: {total_exclusions} columns across {len(EXCLUSION_LISTS)} tables")
print()

for table, cols in EXCLUSION_LISTS.items():
    print(f"  {table}: {len(cols)} exclusions")

# COMMAND ----------

# DBTITLE 1,Create Audit Log Table Schema
print("\nCREATING AUDIT LOG SCHEMA")
print("="*80)

audit_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("exclusion_reason", StringType(), False),
    StructField("rows_before", IntegerType(), False),
    StructField("rows_after", IntegerType(), False),
    StructField("columns_before", IntegerType(), False),
    StructField("columns_after", IntegerType(), False),
    StructField("cleanup_timestamp", TimestampType(), False)
])

print("Audit log schema ready")

# COMMAND ----------

# DBTITLE 1,Cleanup Function
def clean_table(table_name, exclusion_list, dry_run=True):
    """
    Clean a single gold table by removing excluded columns.
    
    Args:
        table_name: Name of table in gold schema
        exclusion_list: List of column names to remove
        dry_run: If True, only preview changes without modifying table
    
    Returns:
        List of audit records
    """
    full_table_name = f"{catalog_name}.gold.{table_name}"
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Processing: {table_name}")
    print("-" * 80)
    
    # Read table
    try:
        df = spark.table(full_table_name)
    except Exception as e:
        print(f"  ERROR: Cannot read table {full_table_name}: {e}")
        return []
    
    rows_before = df.count()
    cols_before = len(df.columns)
    
    print(f"  Before: {rows_before:,} rows, {cols_before} columns")
    
    # Find which exclusions actually exist in table
    existing_exclusions = [col for col in exclusion_list if col in df.columns]
    missing_exclusions = [col for col in exclusion_list if col not in df.columns]
    
    if missing_exclusions:
        print(f"  WARNING: {len(missing_exclusions)} exclusions not found in table:")
        for col in missing_exclusions:
            print(f"    - {col}")
    
    if not existing_exclusions:
        print(f"  SKIP: No exclusions to apply")
        return []
    
    print(f"  Excluding {len(existing_exclusions)} columns:")
    for col in existing_exclusions:
        print(f"    - {col}")
    
    # Select all columns except excluded ones
    keep_columns = [col for col in df.columns if col not in existing_exclusions]
    df_clean = df.select(*keep_columns)
    
    rows_after = df_clean.count()
    cols_after = len(df_clean.columns)
    
    print(f"  After: {rows_after:,} rows, {cols_after} columns")
    print(f"  Removed: {cols_before - cols_after} columns")
    
    # Create audit records
    audit_records = []
    for col_name in existing_exclusions:
        # Determine exclusion reason
        if any(keyword in col_name.lower() for keyword in ['tier', 'category', 'priority', 'severity']):
            reason = "METADATA"
        elif 'score' in col_name.lower():
            reason = "AUC_LEAKAGE"
        elif col_name.startswith('is_') and col_name != 'is_driver_gene':
            reason = "METADATA"
        else:
            reason = "METADATA"
        
        audit_records.append({
            "table_name": table_name,
            "column_name": col_name,
            "exclusion_reason": reason,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "columns_before": cols_before,
            "columns_after": cols_after,
            "cleanup_timestamp": None  # Will be set by current_timestamp()
        })
    
    # Write cleaned table (only if not dry run)
    if not dry_run:
        print(f"  Writing cleaned table to {full_table_name}...")
        df_clean.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)
        print(f"  SUCCESS: Table overwritten")
    else:
        print(f"  DRY RUN: Table NOT modified")
    
    return audit_records

# COMMAND ----------

# DBTITLE 1,Process All Tables
print("\n" + "="*80)
print("PROCESSING ALL TABLES")
print("="*80)

all_audit_records = []

for table_name, exclusion_list in EXCLUSION_LISTS.items():
    audit_records = clean_table(table_name, exclusion_list, dry_run=DRY_RUN)
    all_audit_records.extend(audit_records)

print("\n" + "="*80)
print("PROCESSING COMPLETE")
print("="*80)
print(f"Total audit records: {len(all_audit_records)}")

# COMMAND ----------

# DBTITLE 1,Save Audit Log
print("\nSAVING AUDIT LOG")
print("="*80)

if all_audit_records:
    # Create DataFrame from audit records
    df_audit = spark.createDataFrame(all_audit_records, schema=audit_schema)
    df_audit = df_audit.withColumn("cleanup_timestamp", current_timestamp())
    
    if not DRY_RUN:
        # Write to gold.cleanup_audit_log
        df_audit.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{catalog_name}.gold.cleanup_audit_log")
        
        print(f"Audit log saved to {catalog_name}.gold.cleanup_audit_log")
        print(f"Total exclusions logged: {len(all_audit_records)}")
    else:
        print("DRY RUN: Audit log preview:")
        df_audit.show(20, truncate=False)
        print(f"\nTotal exclusions that would be logged: {len(all_audit_records)}")
else:
    print("No exclusions applied - no audit log created")

# COMMAND ----------

# DBTITLE 1,Summary Report
print("\n" + "="*80)
print("CLEANUP SUMMARY")
print("="*80)

if all_audit_records:
    df_audit = spark.createDataFrame(all_audit_records, schema=audit_schema)
    
    print("\nExclusions by table:")
    df_audit.groupBy("table_name").count() \
        .orderBy("count", ascending=False) \
        .show(20, truncate=False)
    
    print("\nExclusions by reason:")
    df_audit.groupBy("exclusion_reason").count() \
        .orderBy("count", ascending=False) \
        .show()
    
    print("\nSample excluded columns:")
    df_audit.select("table_name", "column_name", "exclusion_reason") \
        .show(30, truncate=False)

print("\n" + "="*80)
if DRY_RUN:
    print("DRY RUN COMPLETE - NO TABLES MODIFIED")
    print("To actually clean tables, set DRY_RUN = False and re-run")
else:
    print("CLEANUP COMPLETE - ALL TABLES UPDATED")
    print("Audit log: gold.cleanup_audit_log")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VALIDATION QUERIES
# MAGIC Run these after cleanup to verify tables are clean

# COMMAND ----------

# DBTITLE 1,Verify Column Counts
# MAGIC %sql
# MAGIC -- Check column counts for all 18 tables
# MAGIC SELECT 
# MAGIC   'clinical_ml_features' as table_name,
# MAGIC   COUNT(*) as column_count
# MAGIC FROM (DESCRIBE TABLE workspace.gold.clinical_ml_features)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC   'disease_ml_features' as table_name,
# MAGIC   COUNT(*) as column_count
# MAGIC FROM (DESCRIBE TABLE workspace.gold.disease_ml_features)
# MAGIC 
# MAGIC -- Add remaining 16 tables...
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,Check Audit Log
# MAGIC %sql
# MAGIC -- View audit log summary
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   COUNT(*) as exclusions,
# MAGIC   MAX(columns_before) as cols_before,
# MAGIC   MAX(columns_after) as cols_after,
# MAGIC   MAX(columns_before) - MAX(columns_after) as cols_removed
# MAGIC FROM workspace.gold.cleanup_audit_log
# MAGIC GROUP BY table_name
# MAGIC ORDER BY exclusions DESC;

# COMMAND ----------

print("\nGOLD CLEANUP NOTEBOOK COMPLETE")
print("="*80)
print("Next steps:")
print("  1. Review DRY RUN output above")
print("  2. If satisfied, set DRY_RUN = False")
print("  3. Re-run notebook to actually clean tables")
print("  4. Verify with check_gold_correlations.py")
print("  5. Proceed to Postgres reload")
print("="*80)
