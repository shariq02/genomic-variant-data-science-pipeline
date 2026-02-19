# ====================================================================
# NCBI PROTEIN DATABASE – GENE2REFSEQ (HUMAN-ONLY, STREAMING)
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# ====================================================================

import gzip
import urllib.request
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import os
import time

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
GENE2REFSEQ_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz"
CHUNK_SIZE = 500_000            # Safe for low-memory systems
HUMAN_TAX_ID = 9606

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "protein"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "protein_gene_mapping_human.csv"
TEMP_GZ = OUTPUT_DIR / "gene2refseq.gz"

# --------------------------------------------------------------------
# Column schema (from NCBI README)
# --------------------------------------------------------------------
COLUMNS = [
    "tax_id",
    "gene_id",
    "status",
    "rna_accession",
    "rna_gi",
    "protein_accession",
    "protein_gi",
    "genomic_accession",
    "genomic_gi",
    "start_position",
    "end_position",
    "orientation",
    "assembly",
    "mature_peptide_accession",
    "mature_peptide_gi",
    "symbol"
]

# --------------------------------------------------------------------
# Main processing logic
# --------------------------------------------------------------------
def main():
    """
    Main entry point for NCBI protein database extraction

    Downloads gene2refseq.gz, filters human-only records with valid protein accessions,
    and writes the output to a CSV file.

    :return: None
    :rtype: NoneType
    """
    print("\n" + "=" * 80)
    print("NCBI PROTEIN DATABASE – HUMAN-ONLY STREAMING EXTRACTION")
    print("=" * 80)

    # --------------------------------------------------------------
    # Download
    # --------------------------------------------------------------
    if not TEMP_GZ.exists():
        logger.info("Downloading gene2refseq.gz (this may take time)...")
        urllib.request.urlretrieve(GENE2REFSEQ_URL, TEMP_GZ)
        logger.info("Download completed")
    else:
        logger.info("gene2refseq.gz already exists")

    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()

    start_time = time.time()
    total_human_rows = 0
    chunk_num = 0

    logger.info("Streaming + filtering human protein mappings...")

    # --------------------------------------------------------------
    # Stream parse
    # --------------------------------------------------------------
    with gzip.open(TEMP_GZ, "rt", encoding="utf-8", errors="ignore") as f:
        for chunk in pd.read_csv(
            f,
            sep="\t",
            comment="#",
            names=COLUMNS,
            chunksize=CHUNK_SIZE,
            low_memory=False
        ):
            chunk_num += 1

            # Convert tax_id safely
            chunk["tax_id"] = pd.to_numeric(chunk["tax_id"], errors="coerce")

            # Filter human + valid protein accessions
            human = chunk[
                (chunk["tax_id"] == HUMAN_TAX_ID) &
                (chunk["protein_accession"] != "-") &
                (chunk["protein_accession"].notna())
            ]

            if not human.empty:
                human_out = human[[
                    "gene_id",
                    "symbol",
                    "protein_accession",
                    "rna_accession",
                    "genomic_accession",
                    "start_position",
                    "end_position",
                    "orientation"
                ]].copy()

                human_out["data_source"] = "NCBI Gene2RefSeq"
                human_out["download_date"] = datetime.now().strftime("%Y-%m-%d")

                human_out.to_csv(
                    OUTPUT_FILE,
                    mode="a",
                    header=(total_human_rows == 0),
                    index=False
                )

                total_human_rows += len(human_out)

            elapsed = time.time() - start_time
            logger.info(
                f"Chunk {chunk_num}: "
                f"+{len(human):,} human rows | "
                f"Total: {total_human_rows:,} | "
                f"Elapsed: {elapsed/60:.1f} min"
            )

    # --------------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------------
    logger.info("Cleaning up large temporary file...")
    os.remove(TEMP_GZ)

    print("\n" + "=" * 80)
    print("SUCCESS – HUMAN PROTEIN DATABASE READY")
    print("=" * 80)
    print(f"Rows extracted: {total_human_rows:,}")
    print(f"Output file: {OUTPUT_FILE}")
    print("No temporary files retained")
    print("=" * 80)


if __name__ == "__main__":
    main()
