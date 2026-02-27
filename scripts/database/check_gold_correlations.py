"""
check_gold_correlations.py
Post-write correlation checker for gold tables.
Run after any gold table is rebuilt in Postgres to catch leakage before
feature selection begins.

Usage:
    python check_gold_correlations.py --table clinical_ml_features
    python check_gold_correlations.py --table all
    python check_gold_correlations.py --table variant_drug_response_ml_features --threshold 0.80

Output:
    Console summary
    data/quality/correlation_check_{table}_{date}.csv
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text


# ============================================================
# TABLE REGISTRY
# Maps each gold table to its target column and primary key
# ============================================================

TABLE_REGISTRY = {
    "clinical_ml_features": {
        "target": "target_is_pathogenic",
        "primary_key": "variant_id",
        "sample_pct": 10,
    },
    "disease_ml_features": {
        "target": "is_pathogenic",
        "primary_key": "variant_id",
        "sample_pct": 10,
    },
    "pharmacogene_ml_features": {
        "target": "is_pathogenic",
        "primary_key": "variant_id",
        "sample_pct": 10,
    },
    "variant_impact_ml_features": {
        "target": "is_high_impact",
        "primary_key": "variant_id",
        "sample_pct": 10,
    },
    "structural_variant_ml_features": {
        "target": "sv_classification",
        "primary_key": "sv_id",
        "sample_pct": 100,
    },
    "variant_drug_response_ml_features": {
        "target": "is_actionable_pharmacogene_variant",
        "primary_key": "variant_id",
        "sample_pct": 10,
    },
    "variant_cancer_ml_features": {
        "target": "is_driver_candidate",
        "primary_key": "variant_id",
        "sample_pct": 10,
    },
    "variant_population_ml_features": {
        "target": "is_carrier_screening_candidate",
        "primary_key": "variant_id",
        "sample_pct": 100,
    },
    "population_frequency_ml_features": {
        "target": "is_clinically_actionable_rare_variant",
        "primary_key": "variant_id",
        "sample_pct": 100,
    },
    "gene_pharmacogene_ml_features": {
        "target": "is_high_priority_pharmacogene",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "gene_expression_ml_features": {
        "target": "is_clinically_relevant_expression",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "gene_protein_family_ml_features": {
        "target": "is_high_value_protein_family",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "gene_test_availability_ml_features": {
        "target": "is_high_priority_test_gene",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "transcript_expression_ml_features": {
        "target": "is_clinically_relevant_expression",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "cancer_variant_ml_features": {
        "target": "gene_cancer_role",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "drug_response_ml_features": {
        "target": "drug_response_priority",
        "primary_key": "variant_id",
        "sample_pct": 100,
    },
    "protein_family_ml_features": {
        "target": "protein_family_priority",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
    "genetic_test_ml_features": {
        "target": "test_priority",
        "primary_key": "gene_symbol",
        "sample_pct": 100,
    },
}

# ============================================================
# THRESHOLDS
# ============================================================

THRESHOLD_WARN = 0.85
THRESHOLD_CRITICAL = 0.98

# ============================================================
# KNOWN LEAKAGE COLUMNS (confirmed from SQL investigation)
# If any of these appear in a table they must be flagged
# regardless of their correlation value
# ============================================================

KNOWN_LEAKAGE_COLUMNS = {
    "clinical_ml_features": [
        "is_cancer_relevant",
        "clinical_significance_simple",
        "clinvar_pathogenicity_class",
        "protein_impact_category",
        "x_linked_risk_modifier",
        "inheritance_pathogenicity_modifier",
    ],
    "disease_ml_features": [
        "clinical_significance_simple",
        "disease_enriched",
        "primary_disease",
        "disease_name_enriched",
        "variant_disease_link_quality",
        "disease_complexity",
        "gene_priority_tier",
        "annotation_priority_level",
    ],
    "pharmacogene_ml_features": [
        "clinical_significance_simple",
        "variant_type",
        "pharmacogene_evidence_level",
        "drug_target_category",
        "drug_response_impact",
        "gene_pharmacogene_priority",
        "gene_pharmacogene_burden",
        "drug_response_frequency_context",
    ],
    "variant_impact_ml_features": [
        "clinical_significance_simple",
        "clinvar_pathogenicity_class",
        "review_status",
        "domain_impact_severity",
        "variant_impact_tier",
        "conservation_impact_class",
        "splice_impact_severity",
        "lof_category",
        "clinical_impact_priority",
        "expression_impact_context",
        "cancer_variant_priority",
        "disease_impact_category",
        "disease_specific_priority",
        "gene_impact_burden",
        "gene_lof_tolerance",
        "gene_variant_impact_priority",
    ],
    "structural_variant_ml_features": [
        "sv_pathogenicity_risk",
        "gene_list",
        "disease_sv_priority",
        "sv_clinical_priority",
        "sv_impact_tier",
    ],
    "variant_drug_response_ml_features": [
        "has_pharmgkb_annotation",
        "pharmacogene_annotation_score",
        "affects_drug_efficacy",
        "clinical_significance_simple",
        "drug_response_priority",
        "drug_response_category",
        "clinical_actionability",
        "drug_response_frequency_context",
        "primary_indication_category",
    ],
    "variant_cancer_ml_features": [
        "is_high_impact_cancer_variant",
        "gene_cancer_role",
        "mutation_frequency_category",
        "clinvar_is_pathogenic",
        "clinvar_pathogenicity",
        "somatic_vs_germline_classification",
        "expression_change_relevance",
    ],
    "variant_population_ml_features": [
        "is_rare_variant",
        "population_priority",
        "screening_recommendation",
        "frequency_category",
        "frequency_tier",
        "clinical_significance",
        "disease_allele_frequency",
        "carrier_frequency_by_disease",
        "expression_frequency_correlation",
        "gene_mutation_tolerance",
    ],
    "population_frequency_ml_features": [
        "frequency_category",
        "frequency_tier",
        "clinical_significance",
        "population_priority",
        "screening_recommendation",
    ],
    "gene_pharmacogene_ml_features": [
        "pharmacogene_priority",
        "pharmacogene_category",
        "pharmacogene_category_enhanced",
        "drug_metabolism_role",
        "clinical_actionability_tier",
        "variant_impact_burden",
        "drug_metabolism_tissue_expression",
        "cancer_mutation_burden",
        "primary_indication_category",
        "expression_breadth",
    ],
    "gene_expression_ml_features": [
        "expression_priority",
        "disease_specific_expression_pattern",
        "expression_function_correlation",
        "cancer_expression_relevance",
        "domain_expression_correlation",
    ],
    "gene_protein_family_ml_features": [
        "protein_family_priority",
        "variant_disease_domain_correlation",
        "cancer_protein_classification",
        "oncogenic_domain_alterations",
        "disease_specific_domains",
    ],
    "gene_test_availability_ml_features": [
        "test_priority",
        "test_recommendation_tier",
        "disease_test_correlation",
        "variant_test_coverage_level",
        "population_test_priority",
        "primary_test_type",
    ],
    "transcript_expression_ml_features": [
        "expression_priority",
    ],
    "cancer_variant_ml_features": [
        "mutation_frequency_category",
        "clinvar_is_pathogenic",
        "clinvar_pathogenicity",
        "somatic_vs_germline_classification",
        "expression_change_relevance",
    ],
}

# ============================================================
# ID COLUMNS TO EXCLUDE FROM CORRELATION CHECK
# ============================================================

ID_COLUMNS = {
    "variant_id", "gene_symbol", "gene_name", "chromosome", "position",
    "ref_allele", "alt_allele", "sv_id", "transcript_id", "protein_id",
    "gene_id", "ensembl_gene_id", "refseq_gene_id", "hgnc_id",
}


# ============================================================
# CORE FUNCTIONS
# ============================================================

def get_engine():
    from dotenv import load_dotenv
    load_dotenv()
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db   = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    pw   = os.getenv("POSTGRES_PASSWORD")
    url  = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(url)


def load_table_sample(engine, table_name, target_col, sample_pct):
    if sample_pct >= 100:
        query = f"SELECT * FROM gold.{table_name}"
    else:
        query = f"SELECT * FROM gold.{table_name} TABLESAMPLE SYSTEM ({sample_pct})"
    print(f"  Loading {table_name} ({sample_pct}% sample)...")
    df = pd.read_sql(query, engine)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


def check_target_distribution(df, target_col, table_name):
    print(f"\n  Target: {target_col}")

    if target_col not in df.columns:
        print(f"  ERROR: target column {target_col} not found in table")
        return {"status": "ERROR", "reason": "target column missing"}

    target = df[target_col]

    # Handle multiclass targets
    if target.dtype == object or target.nunique() > 2:
        counts = target.value_counts()
        total = len(target)
        print(f"  Multiclass target. Class distribution:")
        for cls, cnt in counts.items():
            print(f"    {cls}: {cnt:,} ({cnt/total*100:.2f}%)")
        dominant_pct = counts.iloc[0] / total * 100
        status = "WARN" if dominant_pct > 90 else "OK"
        if dominant_pct > 95:
            status = "CRITICAL"
            print(f"  CRITICAL: Dominant class is {dominant_pct:.1f}% of data")
        return {"status": status, "dominant_pct": dominant_pct}

    # Binary target
    # Convert to numeric safely
    if target.dtype == object:
        target_num = target.map({"true": 1, "false": 0, "True": 1, "False": 0,
                                  "t": 1, "f": 0, "1": 1, "0": 0})
    else:
        target_num = pd.to_numeric(target, errors="coerce")

    total = len(target_num.dropna())
    positive = int(target_num.sum())
    positive_pct = positive / total * 100 if total > 0 else 0
    imbalance = (total - positive) / positive if positive > 0 else 999

    print(f"  Rows: {total:,}")
    print(f"  Positive: {positive:,} ({positive_pct:.2f}%)")
    print(f"  Imbalance ratio: {imbalance:.1f}:1")

    status = "OK"
    if positive == 0:
        status = "CRITICAL"
        print(f"  CRITICAL: Zero positive examples. Target is all-False.")
    elif positive_pct < 1.0:
        status = "CRITICAL"
        print(f"  CRITICAL: Positive rate {positive_pct:.2f}% is below 1%. Near-unusable.")
    elif positive_pct > 90:
        status = "CRITICAL"
        print(f"  CRITICAL: Positive rate {positive_pct:.2f}% is above 90%. Target may be inverted.")
    elif positive_pct < 3.0:
        status = "WARN"
        print(f"  WARN: Positive rate {positive_pct:.2f}% is very low. Severe imbalance risk.")

    return {
        "status": status,
        "positive": positive,
        "total": total,
        "positive_pct": round(positive_pct, 3),
        "imbalance_ratio": round(imbalance, 2),
    }


def compute_correlations(df, target_col, table_name):
    results = []

    if target_col not in df.columns:
        return results

    target = df[target_col]

    # Convert target to numeric
    if target.dtype == object:
        target_num = target.map({"true": 1, "false": 0, "True": 1, "False": 0,
                                  "t": 1, "f": 0, "1": 1, "0": 0})
    else:
        target_num = pd.to_numeric(target, errors="coerce")

    # For multiclass skip Pearson correlation
    if target_num.nunique() > 2:
        print(f"  Multiclass target: skipping Pearson correlation check")
        return results

    known_leakage = KNOWN_LEAKAGE_COLUMNS.get(table_name, [])

    for col in df.columns:
        if col == target_col:
            continue
        if col in ID_COLUMNS:
            continue

        # Flag known leakage columns regardless of correlation value
        if col in known_leakage:
            results.append({
                "column": col,
                "correlation": None,
                "abs_correlation": None,
                "status": "KNOWN_LEAKAGE",
                "flag": "Column in confirmed leakage list. Must be absent after gold fix.",
            })
            continue

        # Convert feature to numeric
        col_data = df[col]
        if col_data.dtype == object:
            col_num = col_data.map({"true": 1, "false": 0, "True": 1, "False": 0,
                                     "t": 1, "f": 0, "1": 1, "0": 0})
            col_num = pd.to_numeric(col_num, errors="coerce")
        else:
            col_num = pd.to_numeric(col_data, errors="coerce")

        # Skip columns that are all null or all same value after conversion
        if col_num.isna().all() or col_num.nunique() <= 1:
            continue

        # Compute correlation on rows where both are not null
        valid = target_num.notna() & col_num.notna()
        if valid.sum() < 100:
            continue

        try:
            r = col_num[valid].corr(target_num[valid])
        except Exception:
            continue

        if pd.isna(r):
            continue

        abs_r = abs(r)
        status = "OK"
        flag = ""

        if abs_r >= THRESHOLD_CRITICAL:
            status = "CRITICAL"
            flag = (
                f"Correlation {r:.4f} is at or above {THRESHOLD_CRITICAL}. "
                f"Likely leakage. Run SQL cross-tab: SELECT {col}, {target_col}, "
                f"COUNT(*) FROM gold.{table_name} GROUP BY 1, 2 ORDER BY 1, 2"
            )
        elif abs_r >= THRESHOLD_WARN:
            status = "WARN"
            flag = (
                f"Correlation {r:.4f} is at or above {THRESHOLD_WARN}. "
                f"Review before feature selection. "
                f"Run SQL cross-tab to verify independence."
            )

        results.append({
            "column": col,
            "correlation": round(r, 6),
            "abs_correlation": round(abs_r, 6),
            "status": status,
            "flag": flag,
        })

    return results


def check_known_leakage_presence(df, table_name):
    known = KNOWN_LEAKAGE_COLUMNS.get(table_name, [])
    present = [col for col in known if col in df.columns]
    absent  = [col for col in known if col not in df.columns]
    return present, absent


def run_check(table_name, engine, threshold_warn, output_dir):
    print(f"\n{'='*70}")
    print(f"CHECKING: gold.{table_name}")
    print(f"{'='*70}")

    if table_name not in TABLE_REGISTRY:
        print(f"  ERROR: {table_name} not in TABLE_REGISTRY. Add it to check.")
        return None

    config = TABLE_REGISTRY[table_name]
    target_col  = config["target"]
    sample_pct  = config["sample_pct"]

    # Load data
    try:
        df = load_table_sample(engine, table_name, target_col, sample_pct)
    except Exception as e:
        print(f"  ERROR loading table: {e}")
        return None

    # Check 1: Target distribution
    print(f"\n  CHECK 1: Target distribution")
    target_result = check_target_distribution(df, target_col, table_name)

    # Check 2: Known leakage columns present
    print(f"\n  CHECK 2: Known leakage columns")
    present, absent = check_known_leakage_presence(df, table_name)
    if present:
        print(f"  CRITICAL: {len(present)} known leakage column(s) still present:")
        for col in present:
            print(f"    - {col}")
    else:
        print(f"  OK: No known leakage columns present in table.")
    if absent:
        print(f"  Confirmed absent ({len(absent)} columns): {', '.join(absent[:5])}"
              + (f" ... and {len(absent)-5} more" if len(absent) > 5 else ""))

    # Check 3: Correlation with target
    print(f"\n  CHECK 3: Feature-target correlation scan")
    corr_results = compute_correlations(df, target_col, table_name)

    critical = [r for r in corr_results if r["status"] == "CRITICAL"]
    warn     = [r for r in corr_results if r["status"] == "WARN"]
    known_lk = [r for r in corr_results if r["status"] == "KNOWN_LEAKAGE"]
    ok       = [r for r in corr_results if r["status"] == "OK"]

    print(f"  Features checked: {len(corr_results)}")
    print(f"  CRITICAL (abs_r >= {THRESHOLD_CRITICAL}): {len(critical)}")
    print(f"  WARN     (abs_r >= {threshold_warn}):     {len(warn)}")
    print(f"  KNOWN_LEAKAGE still present:              {len(known_lk)}")
    print(f"  OK:                                       {len(ok)}")

    if critical:
        print(f"\n  CRITICAL FEATURES:")
        for r in sorted(critical, key=lambda x: x["abs_correlation"] or 0, reverse=True):
            print(f"    {r['column']:<50} r={r['correlation']}")
            print(f"    -> {r['flag']}")

    if warn:
        print(f"\n  WARN FEATURES (review before feature selection):")
        for r in sorted(warn, key=lambda x: x["abs_correlation"] or 0, reverse=True):
            print(f"    {r['column']:<50} r={r['correlation']}")

    # Overall table status
    has_critical = len(critical) > 0 or len(present) > 0 or target_result.get("status") == "CRITICAL"
    has_warn     = len(warn) > 0 or target_result.get("status") == "WARN"
    table_status = "CRITICAL" if has_critical else ("WARN" if has_warn else "OK")

    print(f"\n  OVERALL STATUS: {table_status}")
    if table_status == "CRITICAL":
        print(f"  ACTION REQUIRED: Do not proceed to feature selection or training.")
        print(f"  Fix all CRITICAL issues and re-run this check.")
    elif table_status == "WARN":
        print(f"  ACTION RECOMMENDED: Review WARN items before feature selection.")
    else:
        print(f"  Table passes all leakage checks. Safe to proceed.")

    # Save results to CSV
    os.makedirs(output_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(output_dir, f"correlation_check_{table_name}_{date_str}.csv")

    rows = []
    for r in corr_results:
        rows.append({
            "table": table_name,
            "target": target_col,
            "column": r["column"],
            "correlation": r["correlation"],
            "abs_correlation": r["abs_correlation"],
            "status": r["status"],
            "flag": r["flag"],
        })

    if rows:
        pd.DataFrame(rows).sort_values("abs_correlation", ascending=False,
                                        na_position="first").to_csv(out_path, index=False)
        print(f"\n  Results saved to: {out_path}")

    return {
        "table": table_name,
        "target": target_col,
        "table_status": table_status,
        "target_status": target_result.get("status"),
        "positive_pct": target_result.get("positive_pct"),
        "critical_features": len(critical),
        "warn_features": len(warn),
        "known_leakage_present": len(present),
        "output_file": out_path if rows else None,
    }


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Check gold table for leakage and target integrity after rebuild."
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Table name to check, or 'all' to check all registered tables.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=THRESHOLD_WARN,
        help=f"Warn threshold for abs correlation (default: {THRESHOLD_WARN})",
    )
    parser.add_argument(
        "--output_dir",
        default="data/quality",
        help="Directory to save CSV reports (default: data/quality)",
    )
    args = parser.parse_args()

    engine = get_engine()

    if args.table == "all":
        tables = list(TABLE_REGISTRY.keys())
        print(f"Running check on all {len(tables)} registered tables...")
    else:
        tables = [args.table]

    summary_rows = []
    for table in tables:
        result = run_check(table, engine, args.threshold, args.output_dir)
        if result:
            summary_rows.append(result)

    # Print summary if multiple tables
    if len(summary_rows) > 1:
        print(f"\n\n{'='*70}")
        print("SUMMARY ACROSS ALL TABLES")
        print(f"{'='*70}")
        summary_df = pd.DataFrame(summary_rows)
        print(summary_df[["table", "table_status", "positive_pct",
                           "critical_features", "warn_features",
                           "known_leakage_present"]].to_string(index=False))

        critical_tables = [r["table"] for r in summary_rows if r["table_status"] == "CRITICAL"]
        warn_tables     = [r["table"] for r in summary_rows if r["table_status"] == "WARN"]
        ok_tables       = [r["table"] for r in summary_rows if r["table_status"] == "OK"]

        print(f"\nCRITICAL: {len(critical_tables)} tables")
        for t in critical_tables:
            print(f"  - {t}")
        print(f"WARN:     {len(warn_tables)} tables")
        print(f"OK:       {len(ok_tables)} tables")

        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_path = os.path.join(args.output_dir, f"correlation_check_summary_{date_str}.csv")
        os.makedirs(args.output_dir, exist_ok=True)
        summary_df.to_csv(summary_path, index=False)
        print(f"\nSummary saved to: {summary_path}")


if __name__ == "__main__":
    main()
