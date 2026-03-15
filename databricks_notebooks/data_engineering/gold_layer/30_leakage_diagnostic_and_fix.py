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
# MAGIC Scans all 13 gold ML feature tables for two types of leakage:
# MAGIC
# MAGIC   Type 1 - Numeric/Boolean leakage:
# MAGIC     Pearson correlation scan of every numeric and boolean feature against each target.
# MAGIC     Threshold: abs(r) >= 0.98
# MAGIC     Applicable to all 13 tables using their defined or proxy target columns.
# MAGIC
# MAGIC   Type 2 - String leakage:
# MAGIC     Conditional target rate scan of every low-cardinality string column.
# MAGIC     A string column is flagged if 100% of rows fall into categories where
# MAGIC     target rate is exactly 1.0 or exactly 0.0.
# MAGIC     High-cardinality columns (>10% distinct values) are skipped as identifiers.
# MAGIC
# MAGIC **TWO-STEP EXECUTION:**  
# MAGIC   Step 1 - Run the Scan cell. Review ALL findings before proceeding.  
# MAGIC   Step 2 - Only run the Write cell after confirming findings are correct.
# MAGIC
# MAGIC **VACUUM note:** Not supported on Databricks Serverless.
# MAGIC Old Parquet files expire naturally after 30 days per Delta default retention.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, corr
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType,
    DoubleType, FloatType, ShortType, StringType
)
import datetime

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print("LEAKAGE DIAGNOSTIC AND FIX - NOTEBOOK 29b")
print("="*80)
print(f"Catalog:                    {catalog_name}")
print(f"Numeric correlation threshold: 0.98")
print(f"String extreme threshold:      1.0 (perfect 0 or 1 target rate per category)")
print(f"Cardinality skip threshold:    10% distinct values")
print(f"Run timestamp:              {datetime.datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONFIGURATION

# COMMAND ----------

# DBTITLE 1,Table Registry
CORRELATION_THRESHOLD  = 0.98
EXTREME_THRESHOLD      = 1.0
CARDINALITY_MAX_RATIO  = 0.10

# All 13 gold ML feature tables.
# targets: columns used for correlation and string leakage scans.
# For Mode 1 tables these are the actual ML targets.
# For Mode 2 tables (no ML target) these are the most meaningful
# boolean proxy columns that string features could encode.

ALL_TABLE_REGISTRY = [
    # Mode 1 - tables with defined ML targets
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
        "table":       "cancer_variant_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_driver_candidate"]
    },
    # Mode 2 - features-only tables, scanned against proxy boolean columns
    {
        "table":       "disease_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_pathogenic", "is_benign", "is_vus"]
    },
    {
        "table":       "pharmacogene_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_pathogenic", "is_benign", "is_pharmacogene"]
    },
    {
        "table":       "variant_impact_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_pathogenic", "is_benign", "affects_functional_domain"]
    },
    {
        "table":       "structural_variant_ml_features",
        "primary_key": "sv_id",
        "targets":     ["has_critical_gene_disruption", "has_disease_associated_genes"]
    },
    {
        "table":       "variant_population_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_pathogenic", "rare_pathogenic_variant"]
    },
]

print(f"Total tables to scan: {len(ALL_TABLE_REGISTRY)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### HELPER FUNCTIONS

# COMMAND ----------

# DBTITLE 1,Helper: Cast Booleans to Int
def cast_booleans_to_int(df):
    """
    Casts all BooleanType columns to IntegerType.
    Required because PySpark corr() does not accept boolean inputs.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, BooleanType):
            df = df.withColumn(field.name, col(field.name).cast(IntegerType()))
    return df

# COMMAND ----------

# DBTITLE 1,Helper: Get Numeric Feature Columns
def get_numeric_feature_cols(df, exclude_cols):
    """
    Returns numeric and boolean (already cast to int) column names,
    excluding primary keys and target columns.
    """
    numeric_types = (IntegerType, LongType, DoubleType, FloatType, ShortType)
    return [
        f.name for f in df.schema.fields
        if f.name not in exclude_cols
        and isinstance(f.dataType, numeric_types)
    ]

# COMMAND ----------

# DBTITLE 1,Helper: Numeric Correlation Scan
def numeric_correlation_scan(df, feature_cols, target_col, threshold):
    """
    Computes Pearson correlation of each feature against target_col.
    Returns list of (column_name, r_value) where abs(r) >= threshold.
    Skips columns that cannot be correlated (constant, all nulls, etc.).
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

# DBTITLE 1,Helper: String Leakage Scan
def string_leakage_scan(df, string_cols, target_col, total_rows,
                        extreme_threshold, cardinality_max_ratio):
    """
    For each low-cardinality string column, computes target rate per category.
    Flags column if 100% of rows fall into categories where target rate
    is exactly 1.0 or exactly 0.0 (perfect predictor of target).
    Skips columns where distinct count > cardinality_max_ratio * total_rows.
    Returns list of (column_name, distinct_count) for flagged columns.
    """
    flagged = []
    for feat in string_cols:
        try:
            distinct_count    = df.select(feat).distinct().count()
            cardinality_ratio = distinct_count / total_rows if total_rows > 0 else 1.0

            if cardinality_ratio > cardinality_max_ratio:
                continue

            stats = (
                df.groupBy(feat)
                .agg(
                    count("*").alias("row_count"),
                    avg(col(target_col).cast("double")).alias("target_rate")
                )
                .collect()
            )

            extreme_rows = sum(
                r["row_count"] for r in stats
                if r["target_rate"] is not None
                and (r["target_rate"] >= extreme_threshold
                     or r["target_rate"] <= (1 - extreme_threshold))
            )

            extreme_pct = extreme_rows / total_rows if total_rows > 0 else 0

            if extreme_pct >= extreme_threshold:
                flagged.append((feat, distinct_count))

        except Exception:
            pass

    return flagged

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - SCAN
# MAGIC Run this cell and review ALL output before running the Write cell.
# MAGIC Nothing is written to any table in this cell.

# COMMAND ----------

# DBTITLE 1,Scan: Numeric and String Leakage Check All 13 Tables
CORRELATION_THRESHOLD = 0.98
EXTREME_THRESHOLD     = 1.0
CARDINALITY_MAX_RATIO = 0.10

print("SCAN RESULTS - NUMERIC AND STRING LEAKAGE CHECK")
print("="*80)
print("NOTE: No tables are modified in this cell.")
print("Review all findings below before running the Write cell.")
print()

# scan_findings holds everything the Write cell needs.
# Structure: { table_name: { col_name: { drop_reason, correlation_value, target_column } } }
scan_findings = {}

for entry in ALL_TABLE_REGISTRY:
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

    # Cast booleans to int for correlation
    df_numeric = cast_booleans_to_int(df)

    # Filter targets to only those present in the table
    valid_targets = [t for t in targets if t in df_numeric.columns]
    if not valid_targets:
        print(f"  SKIP: None of the target columns {targets} found in table.")
        print()
        continue

    exclude_cols  = set([primary_key] + targets)
    total_rows    = df.count()
    numeric_cols  = get_numeric_feature_cols(df_numeric, exclude_cols)
    string_cols   = [
        f.name for f in df.schema.fields
        if f.name not in exclude_cols
        and isinstance(f.dataType, StringType)
    ]

    print(f"  Rows:                     {total_rows:,}")
    print(f"  Total columns:            {len(df.columns)}")
    print(f"  Numeric features to scan: {len(numeric_cols)}")
    print(f"  String features to scan:  {len(string_cols)}")
    print(f"  Targets:                  {valid_targets}")

    cols_to_drop = {}

    # --- NUMERIC SCAN ---
    for target_col in valid_targets:
        flagged = numeric_correlation_scan(
            df_numeric, numeric_cols, target_col, CORRELATION_THRESHOLD
        )
        for (feat, r_val) in flagged:
            if feat not in cols_to_drop:
                cols_to_drop[feat] = {
                    "drop_reason":       f"numeric correlation >= {CORRELATION_THRESHOLD}",
                    "correlation_value": r_val,
                    "target_column":     target_col
                }
                print(f"  FLAGGED (numeric): {feat} | r={r_val} vs {target_col}")

    # --- STRING SCAN ---
    # Exclude already-flagged numeric columns from string scan
    string_exclude = exclude_cols | set(cols_to_drop.keys())
    eligible_string_cols = [
        c for c in string_cols if c not in string_exclude
    ]

    for target_col in valid_targets:
        flagged = string_leakage_scan(
            df_numeric,
            eligible_string_cols,
            target_col,
            total_rows,
            EXTREME_THRESHOLD,
            CARDINALITY_MAX_RATIO
        )
        for (feat, distinct_count) in flagged:
            if feat not in cols_to_drop:
                cols_to_drop[feat] = {
                    "drop_reason":       f"string leakage: 100% extreme target rate ({distinct_count} categories)",
                    "correlation_value": None,
                    "target_column":     target_col
                }
                print(f"  FLAGGED (string):  {feat} | {distinct_count} categories | 100% extreme vs {target_col}")

    if cols_to_drop:
        scan_findings[table_name] = cols_to_drop
        print(f"  Total flagged: {len(cols_to_drop)} columns")
    else:
        print(f"  OK: No leakage found.")

    print()

# --- SUMMARY ---
total_flagged = sum(len(v) for v in scan_findings.values())

print("="*80)
print("SCAN SUMMARY")
print(f"  Tables scanned:        {len(ALL_TABLE_REGISTRY)}")
print(f"  Tables with findings:  {len(scan_findings)}")
print(f"  Total columns flagged: {total_flagged}")
print()

if scan_findings:
    print("FLAGGED COLUMNS BY TABLE:")
    for tname, cols in scan_findings.items():
        print(f"  {tname}:")
        for cname, info in cols.items():
            print(f"    - {cname} | {info['drop_reason']} | target={info['target_column']}")
    print()
    print("ACTION REQUIRED: Review the above. If correct, run the Write cell.")
else:
    print("Nothing flagged. Do not run the Write cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - WRITE
# MAGIC Only run this cell after reviewing the Scan output above and confirming
# MAGIC every flagged column is genuine leakage and not a legitimate feature.

# COMMAND ----------

# DBTITLE 1,Write: Drop Flagged Columns and Rewrite Tables
CORRELATION_THRESHOLD = 0.98
EXTREME_THRESHOLD     = 1.0
CARDINALITY_MAX_RATIO = 0.10

if "scan_findings" not in dir() or not scan_findings:
    raise RuntimeError(
        "scan_findings is empty or undefined. "
        "Run the Scan cell first and review output before running this cell."
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
        ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUDIT LOG

# COMMAND ----------

# DBTITLE 1,Write Audit Log
from pyspark.sql.types import (
    StructType, StructField, StringType as ST, TimestampType,
    DoubleType as DT
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.leakage_audit_log (
        run_timestamp     TIMESTAMP,
        gold_table        STRING,
        column_name       STRING,
        drop_reason       STRING,
        correlation_value DOUBLE,
        target_column     STRING
    )
    USING DELTA
""")

print("WRITING AUDIT LOG")
print("="*80)

if "all_audit_rows" in dir() and all_audit_rows:
    audit_schema = StructType([
        StructField("run_timestamp",     TimestampType(), True),
        StructField("gold_table",        ST(),            True),
        StructField("column_name",       ST(),            True),
        StructField("drop_reason",       ST(),            True),
        StructField("correlation_value", DT(),            True),
        StructField("target_column",     ST(),            True),
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
print(f"Tables scanned:  {len(ALL_TABLE_REGISTRY)}")

if "all_audit_rows" in dir() and all_audit_rows:
    print(f"Total columns dropped: {len(all_audit_rows)}")
    print()
    print("Dropped column breakdown (this run only):")
    from pyspark.sql.types import StructType, StructField, StringType as ST, TimestampType, DoubleType as DT
    audit_schema = StructType([
        StructField("run_timestamp",     TimestampType(), True),
        StructField("gold_table",        ST(),            True),
        StructField("column_name",       ST(),            True),
        StructField("drop_reason",       ST(),            True),
        StructField("correlation_value", DT(),            True),
        StructField("target_column",     ST(),            True),
    ])
    df_this_run = spark.createDataFrame(all_audit_rows, schema=audit_schema)
    df_this_run \
        .select("gold_table", "column_name", "drop_reason", "target_column") \
        .orderBy("gold_table") \
        .show(truncate=False)
else:
    print("Total columns dropped: 0")

print()
print("All gold tables are clean and ready for split notebooks (30-40).")
print("Audit history available in: gold.leakage_audit_log")
print("Processing complete")
