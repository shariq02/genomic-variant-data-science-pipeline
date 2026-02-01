# ====================================================================
# Load Gold Layer to PostgreSQL
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 2026
# ====================================================================
# Loads all 5 Gold layer ML feature tables from exported CSVs
# into PostgreSQL with correct boolean type handling.
#
# Tables:
#   clinical_ml_features            (69 cols, 35 booleans)
#   disease_ml_features             (83 cols, 37 booleans)
#   pharmacogene_ml_features        (64 cols, 29 booleans)
#   structural_variant_ml_features  (46 cols,  6 booleans)
#   variant_impact_ml_features      (75 cols, 35 booleans)
# ====================================================================

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent.parent
EXPORTS_DIR = PROJECT_ROOT / "data" / "processed"

print(f"Project Root: {PROJECT_ROOT}")
print(f"Looking for CSVs in: {EXPORTS_DIR}")

DB_CONFIG = {
    'host':     os.getenv('POSTGRES_HOST', 'localhost'),
    'port':     int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'genome_db'),
    'user':     os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD')
}


def get_engine():
    """Create database engine."""
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


# ====================================================================
# BOOLEAN COLUMNS — derived directly from gold_table_schema.csv
# Every BOOLEAN column in every table, grouped by table for clarity.
# The loader applies all of them in one pass; pandas ignores names
# that are not present in a given CSV, so the single list is safe.
# ====================================================================
BOOLEAN_COLUMNS = [
    # --- clinical_ml_features (35) ---
    'gene_is_validated',
    'gene_has_omim',
    'gene_has_ensembl',
    'gene_is_well_characterized',
    'target_is_pathogenic',
    'target_is_benign',
    'target_is_vus',
    'clinical_sig_is_uncertain',
    'has_strong_evidence',
    'is_coding_variant',
    'is_regulatory_variant',
    'is_missense_variant',
    'is_frameshift_variant',
    'is_nonsense_variant',
    'is_splice_variant',
    'is_highly_conserved',
    'is_constrained',
    'is_likely_deleterious',
    'is_high_impact',
    'is_very_high_impact',
    'is_domain_affecting',
    'is_loss_of_function',
    'is_deleterious_by_cadd',
    'has_functional_domain',
    'has_conservation_data',
    'has_complete_annotation',
    'is_mitochondrial_variant',
    'is_y_linked_variant',
    'is_x_linked_variant',
    'is_autosomal_variant',
    'gene_is_pathogenic_enriched',
    'gene_is_benign_enriched',
    'gene_is_vus_enriched',
    'gene_has_high_lof_burden',
    'gene_has_quality_annotations',

    # --- disease_ml_features (37) ---
    'is_pathogenic',
    'is_benign',
    'is_vus',
    'has_omim_disease',
    'has_mondo_disease',
    'has_orphanet_disease',
    'disease_is_well_annotated',
    'disease_name_is_generic',
    'is_disease_associated',
    'is_multi_disease_gene',
    'is_omim_gene',
    'is_polygenic_disease',
    'disease_has_high_pathogenic_burden',
    'is_clinically_actionable',
    'is_research_candidate',
    'is_pharmacogene',
    'is_pharmacogene_priority',
    'has_excellent_annotation',
    'is_cancer_gene_variant',
    'is_neurological_gene_variant',
    'is_cardiovascular_gene_variant',
    'is_metabolic_gene_variant',
    'is_rare_disease_gene_variant',
    'has_drug_development_potential',
    'is_highly_actionable',
    'has_cancer_keyword',
    'has_syndrome_keyword',
    'has_neurological_keyword',
    'has_cardiovascular_keyword',
    'has_metabolic_keyword',
    'has_immune_keyword',
    'has_rare_disease_keyword',
    'is_disease_related_gene',
    'has_drug_target_in_desc',
    'has_biomarker_in_desc',
    'has_essential_in_desc',
    'is_well_characterized',

    # --- pharmacogene_ml_features (29) ---
    'gene_description_mentions_drug',
    'has_complete_pharmacogene_annotation',
    'is_drug_target',
    'is_metabolizing_enzyme',
    'is_cyp_gene',
    'is_phase2_metabolism',
    'is_drug_transporter',
    'is_hla_gene',
    'is_hla_variant',
    'is_kinase',
    'is_phosphatase',
    'is_receptor',
    'is_enzyme',
    'is_gpcr',
    'is_transporter',
    'has_drug_interaction_potential',
    'is_metabolizer_variant',
    'is_transporter_variant',
    'is_kinase_inhibitor_target',
    'has_pharmgkb_annotation',
    'gene_has_multiple_drug_variants',
    'is_clinical_pharmacogene',
    # shared with clinical / variant_impact (already listed above, kept for clarity):
    # gene_is_validated, is_pathogenic, is_benign, is_vus,
    # is_missense_variant, is_loss_of_function

    # --- structural_variant_ml_features (6) ---
    'has_gene_overlap',
    'is_multi_gene_sv',
    'affects_pharmacogenes',
    'affects_omim_genes',
    'is_high_risk_sv',
    'is_autosomal',

    # --- variant_impact_ml_features (35) ---
    'is_snv',
    'alters_protein_length',
    'has_multiple_domain_types',
    'has_zinc_finger',
    'has_kinase_domain',
    'has_receptor_domain',
    'has_sh2_domain',
    'has_sh3_domain',
    'has_ph_domain',
    'affects_functional_domain',
    'is_missense_in_conserved_domain',
    'is_conservation_constrained',
    'has_protein_annotation',
    'has_conservation_scores',
    'is_critical_splice_variant',
    'splice_site_is_well_defined',
    'gene_has_high_impact_burden',
    'gene_is_well_annotated',
    # shared with clinical (already listed above):
    # gene_is_validated, is_pathogenic, is_benign, is_vus,
    # is_missense_variant, is_frameshift_variant, is_nonsense_variant,
    # is_splice_variant, has_functional_domain, is_highly_conserved,
    # is_constrained, is_likely_deleterious, is_high_impact,
    # is_very_high_impact, is_domain_affecting, is_loss_of_function,
    # is_deleterious_by_cadd
]


def convert_booleans(df):
    """Convert all known boolean columns present in the dataframe."""
    bool_map = {
        'true': True,  'True': True,  'TRUE': True,  True: True,
        'false': False, 'False': False, 'FALSE': False, False: False,
        1: True, 0: False, '1': True, '0': False
    }
    converted = 0
    for col in BOOLEAN_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(bool_map)
            converted += 1
    return df, converted


def load_gold_tables():
    """Load all Gold tables to PostgreSQL."""

    print("\n" + "="*70)
    print("LOADING GOLD LAYER TO POSTGRESQL")
    print("="*70)
    print("\nGold layer tables (5):")
    print("  clinical_ml_features            — 69 cols, 35 booleans")
    print("  disease_ml_features             — 83 cols, 37 booleans")
    print("  pharmacogene_ml_features        — 64 cols, 29 booleans")
    print("  structural_variant_ml_features  — 46 cols,  6 booleans")
    print("  variant_impact_ml_features      — 75 cols, 35 booleans")
    print("="*70)

    if not EXPORTS_DIR.exists():
        logger.error(f"Directory does not exist: {EXPORTS_DIR}")
        return

    files_in_dir = list(EXPORTS_DIR.glob("*.csv"))
    if files_in_dir:
        print(f"\nFound {len(files_in_dir)} CSV files in {EXPORTS_DIR}:")
        for f in sorted(files_in_dir):
            print(f"  {f.name} ({f.stat().st_size / (1024*1024):.2f} MB)")
    else:
        print(f"\nNo CSV files found in {EXPORTS_DIR}")
        return

    engine = get_engine()

    # (csv_name, pg_schema, pg_table)
    tables = [
        ("clinical_ml_features",            "gold", "clinical_ml_features"),
        ("disease_ml_features",             "gold", "disease_ml_features"),
        ("pharmacogene_ml_features",        "gold", "pharmacogene_ml_features"),
        ("structural_variant_ml_features",  "gold", "structural_variant_ml_features"),
        ("variant_impact_ml_features",      "gold", "variant_impact_ml_features"),
    ]

    for csv_name, schema, table_name in tables:
        csv_path = EXPORTS_DIR / f"{csv_name}.csv"

        if not csv_path.exists():
            logger.warning(f"File not found, skipping: {csv_path}")
            continue

        logger.info(f"Loading {csv_name} -> {schema}.{table_name}")

        try:
            df = pd.read_csv(csv_path)
            logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")

            # Convert booleans
            df, n_converted = convert_booleans(df)
            if n_converted:
                logger.info(f"  Converted {n_converted} boolean columns")

            # Drop existing table
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE"))
                conn.commit()

            # Load in chunks for large tables
            chunk_size = 10000
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk.to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists='replace' if i == 0 else 'append',
                    index=False,
                    method='multi'
                )
                if (i + chunk_size) % 50000 == 0:
                    logger.info(f"    Loaded {i + chunk_size:,} rows...")

            logger.info(f"  SUCCESS: {len(df):,} rows -> {schema}.{table_name}")

        except Exception as e:
            logger.error(f"  ERROR loading {csv_name}: {e}")

    # ================================================================
    # SUMMARY
    # ================================================================
    print("\n" + "="*70)
    print("LOADING COMPLETE")
    print("="*70)
    print("\nVerify in PostgreSQL:")
    print("  SELECT COUNT(*) FROM gold.clinical_ml_features;")
    print("  SELECT COUNT(*) FROM gold.disease_ml_features;")
    print("  SELECT COUNT(*) FROM gold.pharmacogene_ml_features;")
    print("  SELECT COUNT(*) FROM gold.structural_variant_ml_features;")
    print("  SELECT COUNT(*) FROM gold.variant_impact_ml_features;")
    print("\nCheck column counts:")
    print("  SELECT table_name, COUNT(*) as col_count")
    print("  FROM information_schema.columns")
    print("  WHERE table_schema = 'gold'")
    print("  GROUP BY table_name ORDER BY table_name;")
    print("="*70)


if __name__ == "__main__":
    load_gold_tables()
