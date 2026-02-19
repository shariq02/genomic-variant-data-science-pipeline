# ====================================================================
# RefSeq GTF → CSV Extraction (Transcript-derived Gene Model)
# UCSC RefSeq, Human GRCh38
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# ====================================================================

import gzip
import csv
import logging
import urllib.request
from pathlib import Path

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

REFERENCE_DIR = PROJECT_ROOT / "data" / "raw" / "references"
REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

GTF_URL = (
    "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/genes/"
    "hg38.ncbiRefSeq.gtf.gz"
)

GTF_PATH = REFERENCE_DIR / "hg38.ncbiRefSeq.gtf.gz"

GENE_CSV = REFERENCE_DIR / "refseq_genes_grch38.csv"
TRANSCRIPT_CSV = REFERENCE_DIR / "refseq_transcripts_grch38.csv"

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
# Helpers
# --------------------------------------------------------------------
def parse_attributes(attr_str: str) -> dict:
    attrs = {}
    for item in attr_str.strip().split(";"):
        item = item.strip()
        if not item:
            continue
        key, value = item.split(" ", 1)
        attrs[key] = value.replace('"', "")
    return attrs

# --------------------------------------------------------------------
# Main extraction
# --------------------------------------------------------------------
def extract_gtf():
    logger.info("Starting RefSeq GTF extraction")
    logger.info(f"Input file: {GTF_PATH.name}")

    line_count = 0
    transcript_count = 0

    # gene_id -> gene record
    genes = {}

    with gzip.open(GTF_PATH, "rt", encoding="utf-8") as gtf, \
         open(TRANSCRIPT_CSV, "w", newline="", encoding="utf-8") as tx_out:

        tx_writer = csv.writer(tx_out)
        tx_writer.writerow([
            "transcript_id",
            "gene_id",
            "gene_name",
            "chromosome",
            "start",
            "end",
            "strand",
            "source"
        ])

        for line in gtf:
            if line.startswith("#"):
                continue

            line_count += 1
            chrom, source, feature, start, end, _, strand, _, attrs = line.rstrip().split("\t")
            attributes = parse_attributes(attrs)

            if feature == "transcript":
                transcript_id = attributes.get("transcript_id")
                gene_id = attributes.get("gene_id")
                gene_name = attributes.get("gene_name")

                tx_writer.writerow([
                    transcript_id,
                    gene_id,
                    gene_name,
                    chrom,
                    start,
                    end,
                    strand,
                    source
                ])
                transcript_count += 1

                # derive gene from transcript
                if gene_id and gene_id not in genes:
                    genes[gene_id] = [
                        gene_id,
                        gene_name,
                        chrom,
                        strand,
                        source
                    ]

            if line_count % 1_000_000 == 0:
                logger.info(
                    f"Lines processed: {line_count:,} | "
                    f"Transcripts: {transcript_count:,} | "
                    f"Genes (derived): {len(genes):,}"
                )

    # Write genes
    with open(GENE_CSV, "w", newline="", encoding="utf-8") as gene_out:
        gene_writer = csv.writer(gene_out)
        gene_writer.writerow([
            "gene_id",
            "gene_name",
            "chromosome",
            "strand",
            "source"
        ])
        for row in genes.values():
            gene_writer.writerow(row)

    logger.info("Extraction complete")
    logger.info(f"Genes written: {len(genes):,}")
    logger.info(f"Transcripts written: {transcript_count:,}")
    logger.info(f"Output files:")
    logger.info(f"  - {GENE_CSV}")
    logger.info(f"  - {TRANSCRIPT_CSV}")

# --------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------
def main():
    print("\n" + "=" * 80)
    print("RefSeq GTF → CSV EXTRACTION (UCSC RefSeq, transcript-derived genes)")
    print("=" * 80)

    download_if_missing(GTF_URL, GTF_PATH)
    extract_gtf()

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)

if __name__ == "__main__":
    main()
