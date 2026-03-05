# Databricks notebook source
# MAGIC %md
# MAGIC #### LEAKAGE DIAGNOSTIC AND FIX
# MAGIC ##### Notebook 29b: Pre-Split Gold Table Leakage Guard
# MAGIC
# MAGIC **DNA Gene Mapping Project**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** February 2026
# MAGIC
# MAGIC **Position in pipeline:** Runs AFTER all gold notebooks (17a-29), BEFORE split notebooks (30-40)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC
# MAGIC Mode 1 - Tables WITH targets (8 tables):
# MAGIC   - Pearson correlation scan of every numeric/boolean feature against each target
# MAGIC   - Threshold: abs(r) >= 0.98
# MAGIC   - Findings printed for review. Nothing written until Write cell is run.
# MAGIC
# MAGIC Mode 2 - Tables WITHOUT targets (5 tables):
# MAGIC   - No scan. Correlation leakage is not applicable without a target column.
# MAGIC   - No changes made to these tables.
# MAGIC
# MAGIC **TWO-STEP EXECUTION:**  
# MAGIC   Step 1 - Run the Scan cell. Review all findings printed to output.  
# MAGIC   Step 2 - Only run the Write cell after confirming findings are correct.
# MAGIC
# MAGIC **VACUUM note:** Not supported on Databricks Serverless.
# MAGIC Old Parquet files expire naturally after 30 days per Delta default retention.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, corr
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, DoubleType, FloatType, ShortType
)
import datetime

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("LEAKAGE DIAGNOSTIC AND FIX - NOTEBOOK 29b")
print("="*80)
print(f"Catalog:               {catalog_name}")
print(f"Correlation threshold: 0.98")
print(f"Run timestamp:         {datetime.datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONFIGURATION

# COMMAND ----------

# DBTITLE 1,Table Registry
CORRELATION_THRESHOLD = 0.98

# MODE 1: Tables with ML targets - correlation scan
MODE_1_REGISTRY = [
    {
        "table":       "clinical_ml_features",
        "primary_key": "variant_id",
        "targets":     ["target_is_pathogenic", "target_is_benign"]
    },
    {
        "table":       "gene_pharmacogene_ml_features",
        "primary_key": "gene_symbol",
        "targets":     ["is_high_priority_pharmacogene"]
    },
    {
        "table":       "variant_drug_response_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_actionable_pharmacogene_variant"]
    },
    {
        "table":       "gene_expression_ml_features",
        "primary_key": "gene_symbol",
        "targets":     ["is_clinically_relevant_expression"]
    },
    {
        "table":       "transcript_expression_ml_features",
        "primary_key": "gene_symbol",
        "targets":     ["is_clinically_relevant_expression"]
    },
    {
        "table":       "gene_protein_family_ml_features",
        "primary_key": "gene_symbol",
        "targets":     ["is_high_value_protein_family"]
    },
    {
        "table":       "gene_test_availability_ml_features",
        "primary_key": "gene_symbol",
        "targets":     ["is_high_priority_test_gene"]
    },
    {
        "table":       "variant_cancer_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_driver_candidate"]
    },
]

# MODE 2: Tables without ML targets - no scan needed
MODE_2_REGISTRY = [
    {"table": "disease_ml_features",            "primary_key": "variant_id"},
    {"table": "pharmacogene_ml_features",       "primary_key": "variant_id"},
    {"table": "variant_impact_ml_features",     "primary_key": "variant_id"},
    {"table": "structural_variant_ml_features", "primary_key": "sv_id"},
    {"table": "variant_population_ml_features", "primary_key": "variant_id"},
]

print(f"Mode 1 tables (correlation scan): {len(MODE_1_REGISTRY)}")
print(f"Mode 2 tables (no scan):          {len(MODE_2_REGISTRY)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### HELPER FUNCTIONS

# COMMAND ----------

# DBTITLE 1,Helper: Get Numeric Feature Columns
def get_numeric_feature_cols(df, exclude_cols):
    """
    Returns numeric and boolean column names excluding primary keys and targets.
    BooleanType is included because it is cast to 0/1 for correlation.
    """
    numeric_types = (
        BooleanType, IntegerType, LongType,
        DoubleType, FloatType, ShortType
    )
    return [
        f.name for f in df.schema.fields
        if f.name not in exclude_cols
        and isinstance(f.dataType, numeric_types)
    ]

# COMMAND ----------

# DBTITLE 1,Helper: Cast Booleans to Int for Correlation
def cast_booleans_to_int(df):
    """
    Casts all BooleanType columns to IntegerType.
    PySpark corr() does not accept boolean columns directly.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, BooleanType):
            df = df.withColumn(field.name, col(field.name).cast(IntegerType()))
    return df

# COMMAND ----------

# DBTITLE 1,Helper: Correlation Scan
def correlation_scan(df, feature_cols, target_col, threshold):
    """
    Computes Pearson correlation of each feature column against target_col.
    Returns list of (column_name, r_value) where abs(r) >= threshold.
    Skips columns that cannot be correlated (all nulls, constant, etc.).
    """
    flagged = []
    for feat in feature_cols:
        try:
            r_val = df.select(corr(col(feat), col(target_col))).collect()[0][0]
            if r_val is not None and abs(r_val) >= threshold:
                flagged.append((feat, round(float(r_val), 6)))
        except Exception:
            pass
    return flagged

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - SCAN
# MAGIC Run this cell and review ALL output before running the Write cell.
# MAGIC Nothing is written to any table in this cell.

# COMMAND ----------

# DBTITLE 1,Scan: Correlation Check All Mode 1 Tables
CORRELATION_THRESHOLD = 0.98

print("SCAN RESULTS - CORRELATION CHECK")
print("="*80)
print("NOTE: No tables are modified in this cell.")
print("Review all findings below before running the Write cell.")
print()

# scan_findings holds everything the Write cell needs.
# Structure: { table_name: { col_name: { drop_reason, correlation_value, target_column } } }
scan_findings = {}

for entry in MODE_1_REGISTRY:
    table_name  = entry["table"]
    primary_key = entry["primary_key"]
    targets     = entry["targets"]
    full_table  = f"{catalog_name}.gold.{table_name}"

    print(f"Table: {table_name}")
    print("-"*60)

    try:
        df = spark.table(full_table)
    except Exception as e:
        print(f"  SKIP: Could not load table. Error: {str(e)}")
        print()
        continue

    df_numeric   = cast_booleans_to_int(df)
    exclude_cols = set([primary_key] + targets)
    feature_cols = get_numeric_feature_cols(df_numeric, exclude_cols)

    print(f"  Rows:                     {df.count():,}")
    print(f"  Total columns:            {len(df.columns)}")
    print(f"  Numeric features to scan: {len(feature_cols)}")

    cols_to_drop = {}

    for target_col in targets:
        if target_col not in df_numeric.columns:
            print(f"  WARN: Target '{target_col}' not found in table. Skipping.")
            continue

        print(f"  Scanning against target: {target_col}")
        flagged = correlation_scan(df_numeric, feature_cols, target_col, CORRELATION_THRESHOLD)

        for (feat, r_val) in flagged:
            if feat not in cols_to_drop:
                cols_to_drop[feat] = {
                    "drop_reason":       f"correlation >= {CORRELATION_THRESHOLD}",
                    "correlation_value": r_val,
                    "target_column":     target_col
                }
                print(f"    FLAGGED: {feat} | r={r_val} vs {target_col}")

    if cols_to_drop:
        scan_findings[table_name] = cols_to_drop
        print(f"  Total flagged: {len(cols_to_drop)} columns")
    else:
        print(f"  OK: No leakage found.")

    print()

# Print summary
total_flagged = sum(len(v) for v in scan_findings.values())

print("="*80)
print("SCAN SUMMARY")
print(f"  Tables scanned:        {len(MODE_1_REGISTRY)}")
print(f"  Tables with findings:  {len(scan_findings)}")
print(f"  Total columns flagged: {total_flagged}")
print()

if scan_findings:
    print("FLAGGED COLUMNS BY TABLE:")
    for tname, cols in scan_findings.items():
        print(f"  {tname}:")
        for cname, info in cols.items():
            print(f"    - {cname} | r={info['correlation_value']} | target={info['target_column']}")
    print()
    print("ACTION REQUIRED: Review the above. If correct, run the Write cell.")
else:
    print("Nothing flagged. Do not run the Write cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - WRITE
# MAGIC Only run this cell after reviewing the Scan output above and confirming
# MAGIC every flagged column is a genuine leakage column, not a legitimate feature.

# COMMAND ----------

# DBTITLE 1,Write: Drop Flagged Columns and Rewrite Tables
CORRELATION_THRESHOLD = 0.98

if "scan_findings" not in dir() or not scan_findings:
    raise RuntimeError(
        "scan_findings is empty or undefined. "
        "Run the Scan cell first and review the output before running this cell."
    )

print("WRITING - DROPPING FLAGGED COLUMNS")
print("="*80)
print(f"Tables to modify: {list(scan_findings.keys())}")
print()

run_timestamp  = datetime.datetime.now()
all_audit_rows = []

for table_name, cols_to_drop in scan_findings.items():
    full_table = f"{catalog_name}.gold.{table_name}"

    print(f"Table: {table_name}")
    print("-"*60)

    df                 = spark.table(full_table)
    original_col_count = len(df.columns)
    drop_list          = list(cols_to_drop.keys())

    df_clean = df.drop(*drop_list)

    df_clean.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table)

    print(f"  Dropped {len(drop_list)} columns: {drop_list}")
    print(f"  Columns before: {original_col_count} | after: {len(df_clean.columns)}")
    print(f"  Rewritten: {full_table}")
    print()

    for col_name, info in cols_to_drop.items():
        all_audit_rows.append((
            run_timestamp,
            table_name,
            col_name,
            info["drop_reason"],
            info["correlation_value"],
            info["target_column"],
            1
        ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUDIT LOG

# COMMAND ----------

# DBTITLE 1,Write Audit Log
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType as DT, IntegerType as IT
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.leakage_audit_log (
        run_timestamp     TIMESTAMP,
        gold_table        STRING,
        column_name       STRING,
        drop_reason       STRING,
        correlation_value DOUBLE,
        target_column     STRING,
        mode              INT
    )
    USING DELTA
""")

print("WRITING AUDIT LOG")
print("="*80)

if "all_audit_rows" in dir() and all_audit_rows:
    audit_schema = StructType([
        StructField("run_timestamp",     TimestampType(), True),
        StructField("gold_table",        StringType(),    True),
        StructField("column_name",       StringType(),    True),
        StructField("drop_reason",       StringType(),    True),
        StructField("correlation_value", DT(),            True),
        StructField("target_column",     StringType(),    True),
        StructField("mode",              IT(),            True),
    ])

    df_audit = spark.createDataFrame(all_audit_rows, schema=audit_schema)

    df_audit.write \
        .mode("append") \
        .saveAsTable(f"{catalog_name}.gold.leakage_audit_log")

    print(f"Audit rows written: {len(all_audit_rows)}")
    df_audit.show(truncate=False)
else:
    print("No audit rows to write. Write cell was not run or nothing was dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL SUMMARY

# COMMAND ----------

# DBTITLE 1,Final Summary
print("FINAL SUMMARY")
print("="*80)
print(f"Mode 1 tables scanned:   {len(MODE_1_REGISTRY)}")
print(f"Mode 2 tables (no scan): {len(MODE_2_REGISTRY)}")

if "all_audit_rows" in dir() and all_audit_rows:
    print(f"Total columns dropped:   {len(all_audit_rows)}")
    print()
    print("Dropped column breakdown:")
    spark.table(f"{catalog_name}.gold.leakage_audit_log") \
        .filter(col("run_timestamp") == lit(run_timestamp).cast("timestamp")) \
        .select("gold_table", "column_name", "correlation_value", "target_column") \
        .orderBy("gold_table") \
        .show(truncate=False)
else:
    print("Total columns dropped:   0")

print()
print("Mode 1 gold tables ready for split notebooks (30-40).")
print("Mode 2 gold tables unchanged - no targets, no scan needed.")
print("Audit history: gold.leakage_audit_log")
print("Processing complete")
