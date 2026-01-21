# ====================================================================
# UNIPROT SWISS-PROT HUMAN → CSV
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# ====================================================================

import gzip
import csv
import urllib.request
from pathlib import Path
import logging
import time
import sys
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

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "uniprot"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------
# UniProt configuration
# --------------------------------------------------------------------
UNIPROT_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot/"
    "current_release/knowledgebase/complete/"
    "uniprot_sprot.dat.gz"
)

GZ_PATH = OUTPUT_DIR / "uniprot_sprot.dat.gz"
CSV_PATH = OUTPUT_DIR / "uniprot_swissprot_human.csv"

HUMAN_TAXID = "9606"

# --------------------------------------------------------------------
# Download with progress
# --------------------------------------------------------------------
def download_with_progress(url: str, dest: Path):
    if dest.exists():
        logger.info(f"{dest.name} already exists, skipping download")
        return

    logger.info(f"Downloading {dest.name}")

    with urllib.request.urlopen(url) as response:
        total_size = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        start = time.time()

        with open(dest, "wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break

                out.write(chunk)
                downloaded += len(chunk)

                percent = downloaded / total_size * 100
                elapsed = time.time() - start
                speed = downloaded / max(elapsed, 1)
                eta = (total_size - downloaded) / max(speed, 1)

                sys.stdout.write(
                    f"\rDownloaded {downloaded/1e6:.1f}/{total_size/1e6:.1f} MB "
                    f"({percent:.2f}%) | "
                    f"{speed/1e6:.2f} MB/s | ETA {eta/60:.1f} min"
                )
                sys.stdout.flush()

    sys.stdout.write("\n")
    logger.info("Download complete")

# --------------------------------------------------------------------
# Parse UniProt DAT (streaming, human only)
# --------------------------------------------------------------------
def extract_human_uniprot():
    logger.info("Extracting Homo sapiens (Swiss-Prot)")

    if CSV_PATH.exists():
        CSV_PATH.unlink()

    total_records = 0
    human_records = 0
    start_time = time.time()

    with gzip.open(GZ_PATH, "rt", encoding="utf-8", errors="replace") as f, \
         open(CSV_PATH, "w", newline="", encoding="utf-8") as out:

        writer = csv.writer(out)
        writer.writerow([
            "uniprot_accession",
            "gene_symbol",
            "protein_name",
            "taxid",
            "data_source",
            "download_date"
        ])

        record = {}
        is_human = False

        for line in f:
            if line.startswith("ID"):
                record = {}
                is_human = False

            elif line.startswith("AC"):
                record["accession"] = line.split()[1].rstrip(";")

            elif line.startswith("DE   Rec"):
                record["protein_name"] = line.replace("DE   RecName: Full=", "").strip(" ;\n")

            elif line.startswith("GN   Name="):
                record["gene"] = line.split("Name=")[1].split(";")[0]

            elif line.startswith("OX   NCBI_TaxID="):
                taxid = line.split("=")[1].strip().rstrip(";")
                record["taxid"] = taxid
                if taxid == HUMAN_TAXID:
                    is_human = True

            elif line.startswith("//"):
                total_records += 1

                if is_human and "accession" in record:
                    writer.writerow([
                        record.get("accession"),
                        record.get("gene"),
                        record.get("protein_name"),
                        record.get("taxid"),
                        "UniProt Swiss-Prot",
                        datetime.now().strftime("%Y-%m-%d")
                    ])
                    human_records += 1

                if total_records % 50_000 == 0:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Records processed: {total_records:,} | "
                        f"Human: {human_records:,} | "
                        f"Elapsed: {elapsed/60:.1f} min"
                    )

    logger.info(f"Total UniProt records: {total_records:,}")
    logger.info(f"Human Swiss-Prot records: {human_records:,}")
    logger.info(f"Output written to {CSV_PATH}")

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    print("\n" + "=" * 80)
    print("UNIPROT SWISS-PROT HUMAN → CSV")
    print("=" * 80)

    download_with_progress(UNIPROT_URL, GZ_PATH)
    extract_human_uniprot()

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)

if __name__ == "__main__":
    main()
