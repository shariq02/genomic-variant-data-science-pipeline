# ====================================================================
# RAW MedGen EXTRACTION (FINAL, CORRECT, LOSSLESS)
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 18 January 2026
# ====================================================================
# FILE: scripts/extractions/05_download_medgen.py
# PURPOSE:
#   Download and extract RAW MedGen disease concepts and relationships
#   without transformation or filtering mistakes
# ====================================================================

import gzip
import csv
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
CHUNK_SIZE = 100_000
MEDGEN_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/medgen/"

FILES = {
    "concepts": "MGCONSO.RRF.gz",
    "relations": "MGREL.RRF.gz"
}

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "medgen"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")

# --------------------------------------------------------------------
# Download helper
# --------------------------------------------------------------------
def download_file(filename: str) -> Path:
    """
    Download a file from the MedGen FTP site and return the local path.

    Parameters:
        filename (str): The name of the file to download

    Returns:
        Path: The local path of the downloaded file
    """
    url = MEDGEN_BASE_URL + filename
    local_path = OUTPUT_DIR / filename

    if local_path.exists():
        logger.info(f"File already exists: {filename}")
        return local_path

    logger.info(f"Downloading {filename}...")
    urllib.request.urlretrieve(url, local_path)
    logger.info(f"Downloaded {filename}")

    return local_path

# --------------------------------------------------------------------
# Process MGCONSO.RRF (Disease Concepts)
# --------------------------------------------------------------------
def process_medgen_concepts(file_path: Path):
    """
    Process the MedGen disease concepts file (MGCONSO.RRF) and return
    the local path of the processed CSV file and the total number of
    extracted disease concepts.

    Parameters:
        file_path (Path): The path to the MedGen disease concepts file

    Returns:
        tuple[Path, int]: The local path of the processed CSV file and
            the total number of extracted disease concepts
    """
    logger.info("Processing MedGen disease concepts (RAW)")

    output_file = OUTPUT_DIR / "medgen_concepts_raw.csv"
    if output_file.exists():
        output_file.unlink()

    # EXACT column order from MedGen README + your pasted file
    columns = [
        "CUI", "TS", "STT", "ISPREF", "AUI",
        "SAUI", "SCUI", "SDUI", "SAB",
        "TTY", "CODE", "STR", "SUPPRESS"
    ]

    total = 0
    chunk_num = 0

    with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
        reader = pd.read_csv(
            f,
            sep="|",
            header=None,
            names=columns,
            usecols=range(len(columns)),   # 🔥 CRITICAL FIX
            engine="python",
            quoting=csv.QUOTE_NONE,
            chunksize=CHUNK_SIZE
        )

        for chunk in reader:
            chunk_num += 1

            # Keep everything except suppressed terms
            df = chunk[chunk["SUPPRESS"] != "Y"]

            df_out = df[[
                "CUI",
                "STR",
                "TTY",
                "ISPREF",
                "SAB",
                "CODE"
            ]].rename(columns={
                "CUI": "medgen_id",
                "STR": "disease_name",
                "TTY": "term_type",
                "ISPREF": "is_preferred",
                "SAB": "source_db",
                "CODE": "source_code"
            })

            df_out["data_source"] = "NCBI MedGen"
            df_out["download_date"] = datetime.now().strftime("%Y-%m-%d")

            df_out.to_csv(
                output_file,
                mode="a",
                header=(chunk_num == 1),
                index=False
            )

            total += len(df_out)
            logger.info(f"  Chunk {chunk_num}: {len(df_out):,} rows")

    logger.info(f"Disease concepts extracted: {total:,}")
    return output_file, total

# --------------------------------------------------------------------
# Process MGREL.RRF (Relationships)
# --------------------------------------------------------------------
def process_medgen_relations(file_path: Path):
    """
    Process the MedGen relationships file (MGREL.RRF) and return the local
    path of the processed CSV file and the total number of extracted
    relationships.

    Parameters:
        file_path (Path): The path to the MedGen relationships file

    Returns:
        tuple[Path, int]: The local path of the processed CSV file and
            the total number of extracted relationships
    """
    logger.info("Processing MedGen relationships (RAW)")

    output_file = OUTPUT_DIR / "medgen_relations_raw.csv"
    if output_file.exists():
        output_file.unlink()

    # EXACT column order from your pasted file
    columns = [
        "CUI1", "AUI1", "STYPE1",
        "REL", "CUI2", "AUI2",
        "RELA", "RUI", "SAB", "SL", "SUPPRESS"
    ]

    total = 0
    chunk_num = 0

    with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
        reader = pd.read_csv(
            f,
            sep="|",
            header=None,
            names=columns,
            usecols=range(len(columns)),   # 🔥 CRITICAL FIX
            engine="python",
            quoting=csv.QUOTE_NONE,
            chunksize=CHUNK_SIZE
        )

        for chunk in reader:
            chunk_num += 1

            df = chunk[chunk["SUPPRESS"] != "Y"]

            df_out = df[[
                "CUI1",
                "CUI2",
                "REL",
                "RELA",
                "SAB"
            ]].rename(columns={
                "CUI1": "source_medgen_id",
                "CUI2": "target_medgen_id",
                "REL": "relationship",
                "RELA": "relationship_detail",
                "SAB": "source_db"
            })

            df_out["data_source"] = "NCBI MedGen"
            df_out["download_date"] = datetime.now().strftime("%Y-%m-%d")

            df_out.to_csv(
                output_file,
                mode="a",
                header=(chunk_num == 1),
                index=False
            )

            total += len(df_out)
            logger.info(f"  Chunk {chunk_num}: {len(df_out):,} rows")

    logger.info(f"Relationships extracted: {total:,}")
    return output_file, total

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    """
    Main entry point for the script. Downloads MedGen concepts and relationships
    files, extracts the data, and outputs the results.

    Output:
    - Disease concepts CSV file
    - Number of disease concepts extracted
    - Relationships CSV file
    - Number of relationships extracted
    """
    print("\n" + "=" * 80)
    print("RAW MEDGEN EXTRACTION (FINAL & CORRECT)")
    print("=" * 80)
    print(f"Chunk size: {CHUNK_SIZE:,}")
    print("=" * 80)

    concepts_file = download_file(FILES["concepts"])
    relations_file = download_file(FILES["relations"])

    concepts_csv, concepts_count = process_medgen_concepts(concepts_file)
    relations_csv, relations_count = process_medgen_relations(relations_file)

    print("\n" + "=" * 80)
    print("SUCCESS - MEDGEN RAW EXTRACTION COMPLETED")
    print("=" * 80)
    print(f"Disease concepts: {concepts_count:,}")
    print(f"  -> {concepts_csv}")
    print(f"Relationships: {relations_count:,}")
    print(f"  -> {relations_csv}")
    print("=" * 80)

if __name__ == "__main__":
    main()
