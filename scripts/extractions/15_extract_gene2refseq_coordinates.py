"""
NCBI gene2refseq Coordinate Extraction
Maps gene_id to genomic coordinates
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import gzip
import csv
import urllib.request
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "references"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GENE2REFSEQ_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz"
GENE2REFSEQ_GZ = OUTPUT_DIR / "gene2refseq.gz"
OUTPUT_CSV = OUTPUT_DIR / "ncbi_gene2refseq_coordinates.csv"

def download_if_missing(url, dest):
    if dest.exists():
        logger.info(f"{dest.name} already exists, skipping download")
        return
    
    logger.info(f"Downloading {dest.name}...")
    logger.info(f"URL: {url}")
    logger.info("This may take 5-10 minutes...")
    
    try:
        urllib.request.urlretrieve(url, dest)
        logger.info(f"Downloaded successfully: {dest.name}")
        logger.info(f"  Size: {dest.stat().st_size / 1024 / 1024:.1f} MB")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

def parse_gene2refseq():
    logger.info("Parsing gene2refseq.gz...")
    
    genes = {}
    lines_processed = 0
    human_lines = 0
    skipped_bad_coords = 0
    
    with gzip.open(GENE2REFSEQ_GZ, 'rt', encoding='utf-8') as f:
        for line in f:
            if line.startswith('#tax_id'):
                continue
            
            lines_processed += 1
            
            parts = line.strip().split('\t')
            
            if len(parts) < 16:
                continue
            
            tax_id = parts[0]
            
            if tax_id != '9606':
                continue
            
            human_lines += 1
            
            gene_id = parts[1]
            
            # Correct column positions from gene2refseq README:
            # 0:tax_id 1:GeneID 2:status 3:RNA_acc 4:RNA_gi 5:protein_acc
            # 6:protein_gi 7:genomic_acc 8:genomic_gi 9:start_position_on_genomic
            # 10:end_position_on_genomic 11:orientation 12:assembly
            # 13:mature_peptide_acc 14:mature_peptide_gi 15:Symbol
            
            try:
                start = parts[9]
                end = parts[10]
                gene_symbol = parts[15] if len(parts) > 15 else ''
            except:
                continue
            
            if start == '-' or end == '-':
                continue
            
            try:
                start_int = int(start)
                end_int = int(end)
            except:
                continue
            
            # Sanity check: gene length should be < 5 million bp
            length = abs(end_int - start_int)
            if length > 5000000 or length < 1:
                skipped_bad_coords += 1
                continue
            
            if gene_id not in genes or length > genes[gene_id]['gene_length']:
                genes[gene_id] = {
                    'gene_id': gene_id,
                    'gene_symbol': gene_symbol,
                    'start_position': min(start_int, end_int),
                    'end_position': max(start_int, end_int),
                    'gene_length': length,
                    'source': 'ncbi_gene2refseq'
                }
            
            if lines_processed % 500000 == 0:
                logger.info(f"  Processed {lines_processed:,} lines | Human genes: {len(genes):,}")
    
    logger.info(f"Parsing complete")
    logger.info(f"  Total lines: {lines_processed:,}")
    logger.info(f"  Human lines: {human_lines:,}")
    logger.info(f"  Skipped (bad coordinates): {skipped_bad_coords:,}")
    logger.info(f"  Unique genes with coordinates: {len(genes):,}")
    
    return genes

def write_csv(genes):
    logger.info(f"Writing to CSV: {OUTPUT_CSV.name}")
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['gene_id', 'start_position', 'end_position', 'gene_length', 'source']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        for gene_data in genes.values():
            writer.writerow({
                'gene_id': gene_data['gene_id'],
                'start_position': gene_data['start_position'],
                'end_position': gene_data['end_position'],
                'gene_length': gene_data['gene_length'],
                'source': gene_data['source']
            })
    
    logger.info(f"CSV created successfully")
    logger.info(f"  Output: {OUTPUT_CSV}")
    logger.info(f"  Rows: {len(genes):,}")
    logger.info(f"  Size: {OUTPUT_CSV.stat().st_size / 1024:.1f} KB")

def show_sample(genes):
    logger.info("\nSample genes (first 10):")
    for i, gene in enumerate(list(genes.values())[:10], 1):
        symbol = gene.get('gene_symbol', 'N/A')
        logger.info(f"  {i:2}. gene_id={gene['gene_id']:8} symbol={symbol:10} "
                   f"{gene['start_position']:12,} - {gene['end_position']:12,} "
                   f"({gene['gene_length']:9,} bp)")

def main():
    print("\n" + "=" * 80)
    print("NCBI gene2refseq COORDINATE EXTRACTION")
    print("Source: ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz")
    print("=" * 80)
    print()
    
    download_if_missing(GENE2REFSEQ_URL, GENE2REFSEQ_GZ)
    
    genes = parse_gene2refseq()
    
    if len(genes) == 0:
        logger.error("No genes extracted!")
        return
    
    write_csv(genes)
    show_sample(genes)
    
    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)
    print(f"\nExtracted {len(genes):,} genes with coordinates")
    print(f"\nNext steps:")
    print(f"  1. Upload CSV to Databricks Unity Catalog")
    print(f"  2. Run merge script (17e4_merge_gene2refseq.py)")
    print("=" * 80)

if __name__ == "__main__":
    main()