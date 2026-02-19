# ====================================================================
# GENE ↔ DISEASE EXTRACTION (NCBI mim2gene_medgen)
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# ====================================================================

import csv
import logging
import urllib.request
from pathlib import Path
from datetime import datetime

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

GENE_DIR = PROJECT_ROOT / "data" / "raw" / "genes"
MEDGEN_DIR = PROJECT_ROOT / "data" / "raw" / "medgen"

GENE_INFO_FILE = GENE_DIR / "gene_metadata_all.csv"
MEDGEN_CONCEPTS_FILE = MEDGEN_DIR / "medgen_concepts_raw.csv"

MIM2GENE_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen"
MIM2GENE_FILE = GENE_DIR / "mim2gene_medgen"

OUTPUT_CSV = GENE_DIR / "gene_disease_ncbi.csv"

DOWNLOAD_DATE = datetime.now().strftime("%Y-%m-%d")

# --------------------------------------------------------------------
# Download if missing
# --------------------------------------------------------------------
def download_if_missing(url: str, dest: Path):
    if dest.exists():
        logger.info(f"{dest.name} already exists, skipping download")
        return
    logger.info(f"Downloading {dest.name}")
    urllib.request.urlretrieve(url, dest)
    logger.info(f"Downloaded {dest.name}")

# --------------------------------------------------------------------
# Load gene metadata (schema-safe)
# --------------------------------------------------------------------
def load_gene_metadata():
    logger.info("Loading gene metadata")

    gene_map = {}

    with open(GENE_INFO_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Detect symbol column safely
        fieldnames = reader.fieldnames
        symbol_col = None
        for candidate in ["symbol", "gene_symbol", "gene_name", "Symbol"]:
            if candidate in fieldnames:
                symbol_col = candidate
                break

        if symbol_col is None:
            logger.warning("Gene symbol column not found; symbols will be NULL")

        for row in reader:
            gene_id = row.get("gene_id")
            if not gene_id:
                continue

            gene_map[gene_id] = row.get(symbol_col) if symbol_col else None

    logger.info(f"Loaded {len(gene_map):,} genes")
    return gene_map

# --------------------------------------------------------------------
# Load MedGen preferred disease names
# --------------------------------------------------------------------
def load_medgen_names():
    logger.info("Loading MedGen disease names")

    medgen_map = {}

    with open(MEDGEN_CONCEPTS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["is_preferred"] == "Y":
                medgen_map[row["medgen_id"]] = row["disease_name"]

    logger.info(f"Loaded {len(medgen_map):,} MedGen disease names")
    return medgen_map

# --------------------------------------------------------------------
# Extract gene–disease associations
# --------------------------------------------------------------------
def extract_gene_disease(gene_map, medgen_map):
    logger.info("Extracting gene–disease associations")

    written = 0
    total = 0

    with open(MIM2GENE_FILE, encoding="utf-8") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:

        writer = csv.writer(out)
        writer.writerow([
            "gene_id",
            "gene_symbol",
            "medgen_id",
            "omim_id",
            "disease_name",
            "association_type",
            "source",
            "data_source",
            "download_date"
        ])

        for line in infile:
            if line.startswith("#"):
                continue

            total += 1
            fields = line.rstrip().split("\t")

            omim_id = fields[0]
            gene_id = fields[1]
            assoc_type = fields[2]
            source = fields[3]
            medgen_id = fields[4]

            # Skip invalid rows
            if gene_id == "-" or medgen_id == "-":
                continue

            writer.writerow([
                gene_id,
                gene_map.get(gene_id),
                medgen_id,
                omim_id,
                medgen_map.get(medgen_id),
                assoc_type,
                source,
                "NCBI mim2gene_medgen",
                DOWNLOAD_DATE
            ])

            written += 1

            if written % 50_000 == 0:
                logger.info(f"Written {written:,} gene–disease records")

    logger.info("Extraction complete")
    logger.info(f"Lines processed: {total:,}")
    logger.info(f"Records written: {written:,}")
    logger.info(f"Output: {OUTPUT_CSV}")

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    print("\n" + "=" * 80)
    print("GENE ↔ DISEASE EXTRACTION (NCBI mim2gene_medgen)")
    print("=" * 80)

    if not GENE_INFO_FILE.exists():
        raise FileNotFoundError("Missing gene_metadata_all.csv")

    if not MEDGEN_CONCEPTS_FILE.exists():
        raise FileNotFoundError("Missing medgen_concepts_raw.csv")

    download_if_missing(MIM2GENE_URL, MIM2GENE_FILE)

    gene_map = load_gene_metadata()
    medgen_map = load_medgen_names()

    extract_gene_disease(gene_map, medgen_map)

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)

if __name__ == "__main__":
    main()
