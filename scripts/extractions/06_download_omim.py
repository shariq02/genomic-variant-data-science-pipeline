# ====================================================================
# OMIM RAW EXTRACTION (NCBI SAFE)
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 18 January 2026
# ====================================================================
# FILE: scripts/extractions/06_download_omim_mim2gene_medgen.py
# Purpose:
#   Download and parse OMIM → Gene → MedGen bridge file
#   (mim2gene_medgen) from NCBI
# ====================================================================

import urllib.request
import pandas as pd
from pathlib import Path
import logging
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
# Configuration
# --------------------------------------------------------------------
OMIM_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen"
CHUNK_SIZE = 100_000

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "omim"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RAW_FILE = OUTPUT_DIR / "mim2gene_medgen"
OUTPUT_FILE = OUTPUT_DIR / "omim_mim2gene_medgen_raw.csv"

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")

# --------------------------------------------------------------------
# Download
# --------------------------------------------------------------------
def download_mim2gene():
    """
    Downloads the mim2gene_medgen file from NCBI FTP if it doesn't exist.

    Returns:
        Path: The path to the downloaded file.
    """
    if RAW_FILE.exists():
        logger.info("OMIM file already exists")
        return RAW_FILE

    logger.info("Downloading mim2gene_medgen from NCBI...")
    urllib.request.urlretrieve(OMIM_URL, RAW_FILE)
    logger.info("Download complete")
    return RAW_FILE

# --------------------------------------------------------------------
# Parse
# --------------------------------------------------------------------
def parse_mim2gene(file_path: Path):
    """
    Parse the mim2gene_medgen file from NCBI FTP and return the total number
    of extracted relationships.

    Parameters:
        file_path (Path): The path to the mim2gene_medgen file

    Returns:
        int: The total number of extracted relationships
    """
    logger.info("Parsing mim2gene_medgen (RAW, lossless)")

    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()

    columns = [
        "mim_number",
        "gene_id",
        "entry_type",
        "source",
        "medgen_cui",
        "comment"
    ]

    total = 0
    chunk_num = 0

    for chunk in pd.read_csv(
        file_path,
        sep="\t",
        comment="#",
        names=columns,
        dtype=str,
        chunksize=CHUNK_SIZE
    ):
        chunk_num += 1

        df = chunk.copy()

        # Normalize missing values
        df["gene_id"] = df["gene_id"].replace("-", None)
        df["medgen_cui"] = df["medgen_cui"].replace("-", None)

        # Metadata
        df["data_source"] = "NCBI OMIM (mim2gene_medgen)"
        df["download_date"] = datetime.now().strftime("%Y-%m-%d")

        df.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=(chunk_num == 1),
            index=False
        )

        total += len(df)
        logger.info(f"  Chunk {chunk_num}: {len(df):,} rows")

    logger.info(f"OMIM extraction complete: {total:,} rows")
    return total

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    """
    Main entry point for OMIM raw extraction.

    This function downloads the NCBI OMIM file (mim2gene_medgen) and
    parses it into a standard CSV format.

    Parameters:
    None

    Returns:
    int - The number of rows extracted from the OMIM file
    """
    print("\n" + "=" * 80)
    print("OMIM RAW EXTRACTION (NCBI SAFE & FINAL)")
    print("=" * 80)

    raw_file = download_mim2gene()
    row_count = parse_mim2gene(raw_file)

    print("\n" + "=" * 80)
    print("SUCCESS - OMIM RAW LAYER READY")
    print("=" * 80)
    print(f"Rows extracted: {row_count:,}")
    print(f"Output file: {OUTPUT_FILE}")
    print("\nREADY FOR SILVER LAYER INTEGRATION")
    print("=" * 80)

if __name__ == "__main__":
    main()
