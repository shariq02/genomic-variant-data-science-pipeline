"""
GENCODE Gene Coordinates Extraction
Converts GTF/GTF.gz to CSV with gene boundaries
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import gzip
import csv
import re
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
# Paths & Config
# --------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

GENCODE_DIR = PROJECT_ROOT / "data" / "raw" / "references"
GENCODE_DIR.mkdir(parents=True, exist_ok=True)

# GENCODE v44 (hg38/GRCh38) - Basic gene annotation
GENCODE_URL = "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_44/gencode.v44.basic.annotation.gtf.gz"
GENCODE_GZ = GENCODE_DIR / "gencode.v44.basic.annotation.gtf.gz"
GENCODE_GTF = GENCODE_DIR / "gencode.v44.basic.annotation.gtf"
OUTPUT_CSV = GENCODE_DIR / "gencode_gene_coordinates.csv"

EXTRACT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_EVERY = 100_000

# --------------------------------------------------------------------
# Download Helper
# --------------------------------------------------------------------
def download_if_missing(url: str, dest: Path):
    """
    Downloads a file from URL if it doesn't exist locally.
    
    Args:
        url: File URL
        dest: Local destination path
    """
    if dest.exists():
        logger.info(f"{dest.name} already exists, skipping download")
        return
    
    logger.info(f"Downloading {dest.name} from GENCODE...")
    logger.info(f"URL: {url}")
    logger.info("This may take several minutes (~50MB file)...")
    
    try:
        urllib.request.urlretrieve(url, dest)
        logger.info(f" Downloaded {dest.name}")
        logger.info(f"  Size: {dest.stat().st_size / 1024 / 1024:.1f} MB")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        logger.error("Please download manually from:")
        logger.error(f"  {url}")
        logger.error(f"  Save to: {dest}")
        raise

# --------------------------------------------------------------------
# Extract from Gzip if needed
# --------------------------------------------------------------------
def extract_gz_if_needed(gz_path: Path, gtf_path: Path):
    """
    Extracts .gz file if .gtf doesn't exist.
    
    Args:
        gz_path: Path to .gtf.gz file
        gtf_path: Path to output .gtf file
    """
    if gtf_path.exists():
        logger.info(f"{gtf_path.name} already extracted, skipping")
        return
    
    if not gz_path.exists():
        logger.error(f"Source file not found: {gz_path}")
        return
    
    logger.info(f"Extracting {gz_path.name}...")
    
    with gzip.open(gz_path, 'rb') as f_in:
        with open(gtf_path, 'wb') as f_out:
            f_out.write(f_in.read())
    
    logger.info(f"Extracted to {gtf_path.name}")
    logger.info(f"  Size: {gtf_path.stat().st_size / 1024 / 1024:.1f} MB")

# --------------------------------------------------------------------
# GTF to CSV Conversion
# --------------------------------------------------------------------
def parse_gtf_to_csv():
    """
    Parses GENCODE GTF file and extracts gene coordinates to CSV.
    
    GTF Format (tab-separated):
        chr1  HAVANA  gene  11869  14409  .  +  .  gene_id "ENSG00000..."; gene_name "DDX11L1"; ...
    
    Output CSV:
        gene_name, chromosome, start_position, end_position, gene_length, source
    """
    logger.info("Parsing GTF file to extract gene coordinates")
    
    # Determine input file (.gtf or .gtf.gz)
    if GENCODE_GTF.exists():
        input_file = GENCODE_GTF
        use_gzip = False
    elif GENCODE_GZ.exists():
        input_file = GENCODE_GZ
        use_gzip = True
    else:
        logger.error("No GTF file found!")
        logger.error(f"Expected: {GENCODE_GTF} or {GENCODE_GZ}")
        return 0
    
    logger.info(f"Reading: {input_file.name}")
    
    genes = []
    genes_seen = set()
    lines_processed = 0
    
    # Open file (gzipped or plain)
    if use_gzip:
        file_handle = gzip.open(input_file, 'rt', encoding='utf-8')
    else:
        file_handle = open(input_file, 'r', encoding='utf-8')
    
    with file_handle as f:
        for line in f:
            lines_processed += 1
            
            # Skip comments
            if line.startswith('#'):
                continue
            
            # Split by tab
            parts = line.strip().split('\t')
            
            if len(parts) < 9:
                continue
            
            # Only process gene entries (skip transcript, exon, CDS, etc.)
            feature_type = parts[2]
            if feature_type != 'gene':
                continue
            
            chromosome = parts[0]
            start_pos = parts[3]
            end_pos = parts[4]
            attributes = parts[8]
            
            # Extract gene_name from attributes
            # Format: gene_name "SYMBOL";
            gene_name_match = re.search(r'gene_name "([^"]+)"', attributes)
            
            if not gene_name_match:
                continue
            
            gene_name = gene_name_match.group(1)
            
            # Clean chromosome (remove 'chr' prefix if present)
            chromosome_clean = chromosome.replace('chr', '')
            
            # Keep only standard chromosomes
            valid_chroms = [str(i) for i in range(1, 23)] + ['X', 'Y', 'MT']
            if chromosome_clean not in valid_chroms:
                continue
            
            # Avoid duplicates (same gene on same chromosome)
            gene_key = f"{gene_name}_{chromosome_clean}"
            
            if gene_key in genes_seen:
                continue
            
            genes_seen.add(gene_key)
            
            genes.append({
                'gene_name': gene_name,
                'chromosome': chromosome_clean,
                'start_position': int(start_pos),
                'end_position': int(end_pos),
                'gene_length': int(end_pos) - int(start_pos),
                'source': 'gencode_v44'
            })
            
            if lines_processed % LOG_EVERY == 0:
                logger.info(f"  Processed {lines_processed:,} lines | Found {len(genes):,} genes")
    
    logger.info(f"GTF parsing complete")
    logger.info(f"  Total lines processed: {lines_processed:,}")
    logger.info(f"  Unique genes extracted: {len(genes):,}")
    
    # Write to CSV
    logger.info(f"Writing to CSV: {OUTPUT_CSV.name}")
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['gene_name', 'chromosome', 'start_position', 'end_position', 'gene_length', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(genes)
    
    logger.info(f"CSV created successfully")
    logger.info(f"  Output: {OUTPUT_CSV}")
    logger.info(f"  Rows: {len(genes):,}")
    
    # Show sample
    logger.info("\nSample genes (first 10):")
    for i, gene in enumerate(genes[:10], 1):
        logger.info(f"  {i:2}. {gene['gene_name']:15} chr{gene['chromosome']:3} "
                   f"{gene['start_position']:12,} - {gene['end_position']:12,} "
                   f"({gene['gene_length']:9,} bp)")
    
    # Show statistics
    logger.info("\nChromosome distribution:")
    chr_counts = {}
    for gene in genes:
        chr_counts[gene['chromosome']] = chr_counts.get(gene['chromosome'], 0) + 1
    
    for chrom in ['1', '2', '3', 'X', 'Y', 'MT']:
        if chrom in chr_counts:
            logger.info(f"  chr{chrom:3}: {chr_counts[chrom]:5,} genes")
    
    return len(genes)

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    """
    Main entry point for GENCODE gene coordinate extraction
    """
    print("\n" + "=" * 80)
    print("GENCODE GENE COORDINATES EXTRACTION")
    print("Source: gencode.v44.basic.annotation.gtf.gz")
    print("=" * 80)
    print()
    
    # Step 1: Download if needed
    download_if_missing(GENCODE_URL, GENCODE_GZ)
    
    # Step 2: Extract .gz if needed
    extract_gz_if_needed(GENCODE_GZ, GENCODE_GTF)
    
    # Step 3: Parse GTF and create CSV
    gene_count = parse_gtf_to_csv()
    
    if gene_count > 0:
        print("\n" + "=" * 80)
        print("SUCCESS")
        print("=" * 80)
        print(f"\nExtracted {gene_count:,} genes from GENCODE v44")
        print(f"\nOutput file: {OUTPUT_CSV}")
        print(f"File size: {OUTPUT_CSV.stat().st_size / 1024:.1f} KB")
        print("\nNext steps:")
        print("  1. Upload CSV to Databricks Unity Catalog")
        print("  2. Run gene enrichment script (17e3_load_gene_coordinates_v2.py)")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("FAILED - No genes extracted")
        print("=" * 80)

if __name__ == "__main__":
    main()
