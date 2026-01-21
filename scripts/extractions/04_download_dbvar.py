# ====================================================================
# OPTIMIZED dbVar STRUCTURAL VARIANT EXTRACTION - FULL METADATA
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 17 January 2026
# ====================================================================

"""
dbVar Structural Variant Extraction (GRCh38)
Data Source: https://ftp.ncbi.nlm.nih.gov/pub/dbVar/data/Homo_sapiens/by_study/vcf/

Extracted Columns:
- variant_id, variant_name, variant_type, variant_sub_type
- assembly, clinical_significance
- chromosome, start_position, end_position
- method, platform, study_id
- data_source, download_date
"""

import gzip
import urllib.request
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import re

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
DBVAR_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/dbVar/data/Homo_sapiens/by_study/vcf/"

study_files = [
    "nstd102.GRCh38.variant_region.vcf.gz",
    "nstd102.GRCh38.variant_call.vcf.gz",
    "estd214.GRCh38.variant_region.vcf.gz"
]

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "dbvar"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")

# --------------------------------------------------------------------
# Download helper
# --------------------------------------------------------------------
def download_file(filename: str) -> Path:
    """
    Download a file from the dbVar FTP site and return the local path

    Parameters:
        filename (str): The name of the file to download

    Returns:
        Path: The local path of the downloaded file
    """
    url = DBVAR_BASE_URL + filename
    local_path = OUTPUT_DIR / filename

    if local_path.exists():
        logger.info(f"File already exists: {filename}")
        return local_path

    logger.info(f"Downloading {filename} from {url}...")
    urllib.request.urlretrieve(url, local_path)

    size_mb = local_path.stat().st_size / (1024 * 1024)
    logger.info(f"Downloaded {filename} ({size_mb:.2f} MB)")
    return local_path

# --------------------------------------------------------------------
# Parse VCF INFO field for key=value pairs
# --------------------------------------------------------------------
def parse_info_field(info_str):
    """
    Parse the INFO field from a VCF file into a dictionary of key-value pairs.

    Parameters:
        info_str (str): The INFO field string to parse

    Returns:
        dict: A dictionary of key-value pairs parsed from the INFO field
    """
    info_dict = {}
    for item in info_str.split(";"):
        if "=" in item:
            key, val = item.split("=", 1)
            info_dict[key] = val
        else:
            info_dict[item] = True
    return info_dict

# --------------------------------------------------------------------
# Parse VCF file and extract all relevant columns
# --------------------------------------------------------------------
def parse_vcf_file(file_path: Path):
    """
    Extract:
    - variant_id, variant_name, type, sub_type, assembly, clinical_significance
    - chromosome, start, end
    - method, platform, study_id
    """
    logger.info(f"Parsing {file_path.name}")
    output_file = OUTPUT_DIR / f"{file_path.stem}_parsed_full.csv"
    if output_file.exists():
        output_file.unlink()

    total_rows = 0
    chunk_num = 0

    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for chunk in pd.read_csv(
            f,
            sep="\t",
            comment="#",
            names=["chromosome","start","end","id","ref","alt","qual","filter","info"],
            chunksize=CHUNK_SIZE
        ):
            chunk_num += 1
            df_list = []

            for _, row in chunk.iterrows():
                info = parse_info_field(str(row["info"]))
                df_list.append({
                    "variant_id": row["id"] if row["id"] != "." else info.get("variant_id","Unknown"),
                    "variant_name": info.get("variant_name","Unknown"),
                    "variant_type": info.get("type","Unknown"),
                    "variant_sub_type": info.get("sub_type","Unknown"),
                    "assembly": info.get("reference_assembly","GRCh38"),
                    "clinical_significance": info.get("clinical_significance","Unknown"),
                    "chromosome": row["chromosome"],
                    "start_position": row["start"],
                    "end_position": row["end"],
                    "method": info.get("method","Unknown"),
                    "platform": info.get("platform","Unknown"),
                    "study_id": info.get("study_id","Unknown"),
                    "data_source": "NCBI dbVar",
                    "download_date": datetime.now().strftime("%Y-%m-%d")
                })

            df_chunk = pd.DataFrame(df_list)
            df_chunk.to_csv(
                output_file,
                mode="a",
                header=(chunk_num == 1),
                index=False
            )

            total_rows += len(df_chunk)
            logger.info(f"  Chunk {chunk_num}: {len(df_chunk):,} variants processed")

    logger.info(f"Completed parsing {file_path.name}: {total_rows:,} variants")
    return output_file, total_rows

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    """
    Main entry point for optimized dbVar structural variant extraction.

    Prints banner with metadata and chunk size.
    Downloads each study file and parses the VCF file using `parse_vcf_file`.
    Prints summary of the number of variants saved and the output file path.
    Prints success banner upon completion.
    """
    print("\n" + "=" * 80)
    print("OPTIMIZED dbVar STRUCTURAL VARIANT EXTRACTION - FULL METADATA")
    print("=" * 80)
    print(f"Chunk size: {CHUNK_SIZE:,}")
    print("Assembly: GRCh38 (Human)")
    print("=" * 80)

    for file_name in study_files:
        local_file = download_file(file_name)
        parsed_file, total = parse_vcf_file(local_file)
        print(f"Saved {total:,} variants -> {parsed_file}")

    print("\n" + "=" * 80)
    print("SUCCESS - dbVar EXTRACTION COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()
