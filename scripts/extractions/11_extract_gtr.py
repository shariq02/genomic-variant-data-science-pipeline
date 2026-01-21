# ====================================================================
# Genetic Testing Registry (GTR) Extraction
# Source: test_condition_gene.txt (NCBI official)
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
# Paths & config
# --------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

GTR_DIR = PROJECT_ROOT / "data" / "raw" / "gtr"
GTR_DIR.mkdir(parents=True, exist_ok=True)

GTR_URL = "https://ftp.ncbi.nlm.nih.gov/pub/GTR/data/test_condition_gene.txt"
GTR_TXT = GTR_DIR / "test_condition_gene.txt"
OUTPUT_CSV = GTR_DIR / "gtr_gene_disease_tests.csv"

DOWNLOAD_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_EVERY = 500_000

# --------------------------------------------------------------------
# Download helper (robust)
# --------------------------------------------------------------------
def download_if_missing(url: str, dest: Path):
    """
    Downloads a file from a given URL if it doesn't exist locally.

    Args:
        url (str): The URL of the file to download.
        dest (Path): The local file path to download the file to.

    Returns:
        None
    """
    if dest.exists():
        logger.info(f"{dest.name} already exists, skipping download")
        return

    logger.info(f"Downloading {dest.name}")
    urllib.request.urlretrieve(url, dest)
    logger.info(f"Downloaded {dest.name}")

# --------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------
def extract_gtr():
    """
    Extracts gene–disease–test relationships from the Genetic Testing Registry (GTR)
    dataset provided by NCBI.

    The GTR dataset is a tab-delimited file containing information about
    genetic tests, genes, diseases, and the relationships between them.

    The function downloads the file if it doesn't exist, then extracts the
    gene–disease–test relationships and writes them to a CSV file.

    Args:
        None

    Returns:
        None

    Notes:
        The GTR dataset is updated weekly, but the download date is stored
        in the CSV output file for tracking purposes.

    The function logs its progress every LOG_EVERY lines, and outputs
    the total number of rows processed, the number of rows written, and
    the output file path upon completion.
    """
    logger.info("Extracting GTR gene–disease–test relationships")

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    total = 0
    written = 0

    with open(GTR_TXT, encoding="utf-8") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:

        reader = csv.DictReader(infile, delimiter="\t")
        writer = csv.writer(out)

        writer.writerow([
            "gtr_test_id",
            "test_name",
            "gene_symbol",
            "gene_id",
            "disease_name",
            "disease_id",
            "data_source",
            "download_date"
        ])

        for row in reader:
            total += 1

            writer.writerow([
                row.get("test_id"),
                row.get("test_name"),
                row.get("gene_symbol"),
                row.get("gene_id"),
                row.get("condition_name"),
                row.get("condition_id"),
                "NCBI GTR",
                DOWNLOAD_DATE
            ])
            written += 1

            if total % LOG_EVERY == 0:
                logger.info(
                    f"Processed {total:,} rows | "
                    f"Written {written:,}"
                )

    logger.info("GTR extraction complete")
    logger.info(f"Rows processed: {total:,}")
    logger.info(f"Rows written: {written:,}")
    logger.info(f"Output: {OUTPUT_CSV}")

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    """
    Main entry point for GTR extraction script
    Prints headers and executes download_if_missing and extract_gtr functions
    """
    print("\n" + "=" * 80)
    print("GENETIC TESTING REGISTRY (GTR) EXTRACTION")
    print("Source: test_condition_gene.txt")
    print("=" * 80)

    download_if_missing(GTR_URL, GTR_TXT)
    extract_gtr()

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)

if __name__ == "__main__":
    main()
