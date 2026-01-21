# ====================================================================
# NCBI PROTEIN CLUSTERS (PCLA) - RAW EXTRACTION
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 19 January 2026
# ====================================================================
# Purpose:
#   Download and process NCBI Protein Clusters (PCLA)
#   using chunked parsing (memory safe)
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
BASE_URL = "https://ftp.ncbi.nih.gov/genomes/CLUSTERS/"

FILES = {
    "clusters": "PCLA_clusters.txt",
    "proteins": "PCLA_proteins.txt"
}

CHUNK_SIZE = 200_000

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "protein_clusters"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------
# Download helper
# --------------------------------------------------------------------
def download_file(filename: str) -> Path:
    """
    Download a file from the NCBI Protein Clusters FTP site and return the local path.

    Parameters:
        filename (str): The name of the file to download

    Returns:
        Path: The local path of the downloaded file
    """
    url = BASE_URL + filename
    local_path = OUTPUT_DIR / filename

    if local_path.exists():
        logger.info(f"File already exists: {filename}")
        return local_path

    logger.info(f"Downloading {filename}...")
    urllib.request.urlretrieve(url, local_path)
    logger.info(f"Downloaded {filename}")
    return local_path

# --------------------------------------------------------------------
# Process PCLA_clusters.txt (CORRECTED)
# --------------------------------------------------------------------
def process_clusters(file_path: Path):
    """
    Process the PCLA_clusters.txt file and save the contents to a CSV file.

    This function reads the PCLA_clusters.txt file and saves the contents to a CSV file.
    The following columns are added to the output file:

    - data_source: The name of the data source (NCBI Protein Clusters (PCLA))
    - download_date: The date the file was downloaded (YYYY-MM-DD)

    Parameters:
        file_path (Path): The path to the PCLA_clusters.txt file

    Returns:
        Path: The path to the output CSV file
        int: The number of clusters extracted
    """
    logger.info("Processing PCLA cluster metadata (correct schema)")

    output_file = OUTPUT_DIR / "protein_clusters_raw.csv"
    if output_file.exists():
        output_file.unlink()

    columns = [
        "internal_id",
        "cluster_id",
        "definition",
        "protein_count",
        "organism_count",
        "conserved_in_organism",
        "conserved_in_taxid",
        "gene_symbol",
        "hmm_models"
    ]

    df = pd.read_csv(
        file_path,
        sep="\t",
        comment="#",
        names=columns,
        low_memory=False
    )

    df["data_source"] = "NCBI Protein Clusters (PCLA)"
    df["download_date"] = datetime.now().strftime("%Y-%m-%d")

    df.to_csv(output_file, index=False)

    logger.info(f"Clusters extracted: {len(df):,}")
    return output_file, len(df)

# --------------------------------------------------------------------
# Process PCLA_proteins.txt (CORRECTED, CHUNKED)
# --------------------------------------------------------------------
def process_proteins(file_path: Path):
    """
    Process the PCLA_proteins.txt file (correct schema) and store the results in a CSV file.

    Parameters
    ----------
    file_path : Path
        The path to the PCLA_proteins.txt file.

    Returns
    -------
    Path
        The output CSV file path.
    int
        The total number of protein memberships extracted.
    """
    
    logger.info("Processing PCLA protein memberships (correct schema)")

    output_file = OUTPUT_DIR / "protein_cluster_members_raw.csv"
    if output_file.exists():
        output_file.unlink()

    columns = [
        "cluster_id",
        "protein_accession",
        "protein_definition",
        "organism_name",
        "taxid",
        "protein_length"
    ]

    total = 0
    chunk_num = 0

    for chunk in pd.read_csv(
        file_path,
        sep="\t",
        comment="#",
        names=columns,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunk_num += 1

        chunk["data_source"] = "NCBI Protein Clusters (PCLA)"
        chunk["download_date"] = datetime.now().strftime("%Y-%m-%d")

        chunk.to_csv(
            output_file,
            mode="a",
            header=(chunk_num == 1),
            index=False
        )

        total += len(chunk)
        logger.info(f"  Chunk {chunk_num}: {len(chunk):,} proteins")

    logger.info(f"Protein memberships extracted: {total:,}")
    return output_file, total

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    """
    Main entry point for the script.

    Downloads the NCBI Protein Clusters (PCLA) and Protein datasets,
    processes them into a raw extraction format, and prints the
    success message along with the number of clusters and protein
    memberships extracted.

    Returns:
        None
    """
    print("\n" + "=" * 80)
    print("NCBI PROTEIN CLUSTERS (PCLA) - RAW EXTRACTION (FIXED)")
    print("=" * 80)

    clusters_file = download_file(FILES["clusters"])
    proteins_file = download_file(FILES["proteins"])

    c_file, c_count = process_clusters(clusters_file)
    p_file, p_count = process_proteins(proteins_file)

    print("\n" + "=" * 80)
    print("SUCCESS - PROTEIN CLUSTERS RAW LAYER FIXED")
    print("=" * 80)
    print(f"Clusters: {c_count:,}")
    print(f"Protein memberships: {p_count:,}")

if __name__ == "__main__":
    main()
