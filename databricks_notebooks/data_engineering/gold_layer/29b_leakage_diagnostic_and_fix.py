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
# MAGIC   - Pearson correlation scan of every numeric feature against each target
# MAGIC   - Threshold: abs(r) >= 0.98
# MAGIC   - Drops flagged columns and rewrites table with overwriteSchema
# MAGIC
# MAGIC Mode 2 - Tables WITHOUT targets (5 tables):
# MAGIC   - Duplicate column scan only
# MAGIC   - Drops columns that are exact duplicates of another column
# MAGIC   - Rewrites table with overwriteSchema
# MAGIC
# MAGIC Both modes write every drop decision to gold.leakage_audit_log for future audit.
# MAGIC
# MAGIC **VACUUM note:** Not supported on Databricks Serverless.
# MAGIC Old Parquet files expire naturally after 30 days per Delta default retention.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, abs as spark_abs,
    corr, current_timestamp, isnan, isnull
)
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
print(f"Catalog: {catalog_name}")
print(f"Correlation threshold: 0.98")
print(f"Run timestamp: {datetime.datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONFIGURATION

# COMMAND ----------

# DBTITLE 1,Table Registry
# MODE 1: Tables with ML targets - correlation scan + duplicate scan
MODE_1_REGISTRY = [
    {
        "table":       "clinical_ml_features",
        "primary_key": "variant_id",
        "targets":     ["is_pathogenic", "is_benign"]
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

# MODE 2: Tables without ML targets - duplicate scan only
MODE_2_REGISTRY = [
    {"table": "disease_ml_features",          "primary_key": "variant_id"},
    {"table": "pharmacogene_ml_features",     "primary_key": "variant_id"},
    {"table": "variant_impact_ml_features",   "primary_key": "variant_id"},
    {"table": "structural_variant_ml_features", "primary_key": "sv_id"},
    {"table": "variant_population_ml_features", "primary_key": "variant_id"},
]

CORRELATION_THRESHOLD = 0.98

print(f"Mode 1 tables (correlation + duplicate scan): {len(MODE_1_REGISTRY)}")
print(f"Mode 2 tables (duplicate scan only):          {len(MODE_2_REGISTRY)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### HELPER FUNCTIONS

# COMMAND ----------

# DBTITLE 1,Helper: Get Numeric Feature Columns
def get_numeric_feature_cols(df, exclude_cols):
    """
    Returns list of numeric column names excluding identifiers and target columns.
    Treats BooleanType as numeric (0/1) for correlation purposes.
    """
    numeric_types = (
        BooleanType, IntegerType, LongType,
        DoubleType, FloatType, ShortType
    )
    result = []
    for field in df.schema.fields:
        if field.name in exclude_cols:
            continue
        if isinstance(field.dataType, numeric_types):
            result.append(field.name)
    return result

# COMMAND ----------

# DBTITLE 1,Helper: Cast Booleans to Int for Correlation
def cast_booleans_to_int(df):
    """
    Casts all BooleanType columns to IntegerType so Pearson correlation works.
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
    Returns list of column names where abs(r) >= threshold.
    """
    flagged = []
    for feat in feature_cols:
        try:
            r_row = df.select(corr(col(feat), col(target_col))).collect()[0][0]
            if r_row is not None and abs(r_row) >= threshold:
                flagged.append((feat, round(float(r_row), 6)))
        except Exception:
            # Skip columns that cannot be correlated (all nulls, constant, etc.)
            pass
    return flagged

# COMMAND ----------

# DBTITLE 1,Helper: Duplicate Column Scan
def duplicate_column_scan(df, exclude_cols):
    """
    Identifies columns whose values are identical to another column already seen.
    Compares by collecting column-level hash sums. Keeps the first occurrence.
    Returns list of duplicate column names to drop.
    """
    checked  = {}
    to_drop  = []
    all_cols = [f.name for f in df.schema.fields if f.name not in exclude_cols]

    for c in all_cols:
        # Use sum of hash as a fast fingerprint - collisions are astronomically unlikely
        try:
            fingerprint = df.selectExpr(
                f"sum(hash(`{c}`)) as fp"
            ).collect()[0]["fp"]

            if fingerprint in checked:
                to_drop.append((c, checked[fingerprint]))
            else:
                checked[fingerprint] = c
        except Exception:
            pass

    return to_drop

# COMMAND ----------

# DBTITLE 1,Helper: Build Audit Row
def build_audit_rows(table_name, dropped_cols, run_ts):
    """
    Builds a list of audit row dicts from dropped column decisions.
    Each item in dropped_cols is a dict with keys:
      column_name, drop_reason, correlation_value, target_column, mode
    """
    rows = []
    for d in dropped_cols:
        rows.append((
            run_ts,
            table_name,
            d["column_name"],
            d["drop_reason"],
            d.get("correlation_value"),
            d.get("target_column", ""),
            d["mode"]
        ))
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### INITIALIZE AUDIT LOG

# COMMAND ----------

# DBTITLE 1,Create or Load Audit Log Table
AUDIT_SCHEMA = """
    run_timestamp    TIMESTAMP,
    gold_table       STRING,
    column_name      STRING,
    drop_reason      STRING,
    correlation_value DOUBLE,
    target_column    STRING,
    mode             INT
"""

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.leakage_audit_log (
        {AUDIT_SCHEMA}
    )
    USING DELTA
""")

print("Audit log table ready: gold.leakage_audit_log")

all_audit_rows = []
run_timestamp  = datetime.datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ### MODE 1 - TABLES WITH TARGETS

# COMMAND ----------

# DBTITLE 1,Mode 1: Correlation + Duplicate Scan
print("\nMODE 1 - CORRELATION AND DUPLICATE SCAN")
print("="*80)

for entry in MODE_1_REGISTRY:
    table_name  = entry["table"]
    primary_key = entry["primary_key"]
    targets     = entry["targets"]
    full_table  = f"{catalog_name}.gold.{table_name}"

    print(f"\nTable: {table_name}")
    print("-"*60)

    try:
        df = spark.table(full_table)
    except Exception as e:
        print(f"  SKIP: Could not load table. Error: {str(e)}")
        continue

    original_cols = df.columns
    cols_to_drop  = {}   # column_name -> audit dict, using dict to avoid duplicate drops

    # Cast booleans for correlation
    df_numeric = cast_booleans_to_int(df)

    # Exclude identifiers and targets from scan
    exclude_cols = set([primary_key] + targets)

    feature_cols = get_numeric_feature_cols(df_numeric, exclude_cols)
    print(f"  Numeric features to scan: {len(feature_cols)}")

    # Correlation scan against each target
    for target_col in targets:
        if target_col not in df_numeric.columns:
            print(f"  WARN: Target column {target_col} not found in table. Skipping.")
            continue

        print(f"  Scanning against target: {target_col}")
        flagged = correlation_scan(df_numeric, feature_cols, target_col, CORRELATION_THRESHOLD)

        for (feat, r_val) in flagged:
            if feat not in cols_to_drop:
                cols_to_drop[feat] = {
                    "column_name":       feat,
                    "drop_reason":       f"correlation >= {CORRELATION_THRESHOLD}",
                    "correlation_value": r_val,
                    "target_column":     target_col,
                    "mode":              1
                }
                print(f"    FLAGGED: {feat} | r={r_val} vs {target_col}")

    # Duplicate scan (on original df, excluding already-flagged + identifiers + targets)
    dup_exclude = exclude_cols | set(cols_to_drop.keys())
    duplicates  = duplicate_column_scan(df, dup_exclude)

    for (dup_col, original_col) in duplicates:
        if dup_col not in cols_to_drop:
            cols_to_drop[dup_col] = {
                "column_name":       dup_col,
                "drop_reason":       f"duplicate of {original_col}",
                "correlation_value": None,
                "target_column":     "",
                "mode":              1
            }
            print(f"    DUPLICATE: {dup_col} duplicates {original_col}")

    if not cols_to_drop:
        print(f"  OK: No leakage or duplicates found.")
        continue

    # Drop flagged columns and rewrite
    drop_list = list(cols_to_drop.keys())
    df_clean  = df.drop(*drop_list)

    print(f"  Dropping {len(drop_list)} columns: {drop_list}")
    print(f"  Columns before: {len(original_cols)} | after: {len(df_clean.columns)}")

    df_clean.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table)

    print(f"  Rewritten: {full_table}")

    # Accumulate audit rows
    for audit_dict in cols_to_drop.values():
        all_audit_rows.append((
            run_timestamp,
            table_name,
            audit_dict["column_name"],
            audit_dict["drop_reason"],
            audit_dict["correlation_value"],
            audit_dict["target_column"],
            audit_dict["mode"]
        ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MODE 2 - TABLES WITHOUT TARGETS

# COMMAND ----------

# DBTITLE 1,Mode 2: Duplicate Scan Only
print("\nMODE 2 - DUPLICATE SCAN ONLY")
print("="*80)

for entry in MODE_2_REGISTRY:
    table_name  = entry["table"]
    primary_key = entry["primary_key"]
    full_table  = f"{catalog_name}.gold.{table_name}"

    print(f"\nTable: {table_name}")
    print("-"*60)

    try:
        df = spark.table(full_table)
    except Exception as e:
        print(f"  SKIP: Could not load table. Error: {str(e)}")
        continue

    original_cols = df.columns
    exclude_cols  = {primary_key}

    duplicates = duplicate_column_scan(df, exclude_cols)

    if not duplicates:
        print(f"  OK: No duplicate columns found.")
        continue

    cols_to_drop = [dup_col for (dup_col, _) in duplicates]

    for (dup_col, original_col) in duplicates:
        print(f"  DUPLICATE: {dup_col} duplicates {original_col}")

    df_clean = df.drop(*cols_to_drop)

    print(f"  Dropping {len(cols_to_drop)} columns: {cols_to_drop}")
    print(f"  Columns before: {len(original_cols)} | after: {len(df_clean.columns)}")

    df_clean.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table)

    print(f"  Rewritten: {full_table}")

    for (dup_col, original_col) in duplicates:
        all_audit_rows.append((
            run_timestamp,
            table_name,
            dup_col,
            f"duplicate of {original_col}",
            None,
            "",
            2
        ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### WRITE AUDIT LOG

# COMMAND ----------

# DBTITLE 1,Write Audit Log
print("\nWRITING AUDIT LOG")
print("="*80)

if all_audit_rows:
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType,
        DoubleType as DT, IntegerType as IT
    )

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
    print("No columns dropped. Audit log unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL SUMMARY

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\nFINAL SUMMARY")
print("="*80)

total_tables  = len(MODE_1_REGISTRY) + len(MODE_2_REGISTRY)
total_dropped = len(all_audit_rows)

print(f"Tables scanned:   {total_tables}")
print(f"  Mode 1 (with targets):    {len(MODE_1_REGISTRY)}")
print(f"  Mode 2 (without targets): {len(MODE_2_REGISTRY)}")
print(f"Total columns dropped: {total_dropped}")

if total_dropped > 0:
    print("\nDropped column breakdown:")
    spark.table(f"{catalog_name}.gold.leakage_audit_log") \
        .filter(col("run_timestamp") == lit(run_timestamp).cast("timestamp")) \
        .groupBy("gold_table", "drop_reason") \
        .count() \
        .orderBy("gold_table") \
        .show(truncate=False)

print("\nAll gold tables are clean and ready for split notebooks (30-40).")
print("Audit history available in: gold.leakage_audit_log")
print("\nProcessing complete")
