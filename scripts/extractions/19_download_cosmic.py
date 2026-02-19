# ====================================================================
# Alternative Cancer Data Sources (Non-COSMIC)
# DNA Gene Mapping Project
# Author: Sharique Mohammad  
# Date: February 18, 2026
# ====================================================================
# FILE: scripts/extraction/19_download_cosmic.py
# ====================================================================

"""
Cancer Data from Public Sources (Alternative to COSMIC)

Sources:
1. TCGA (The Cancer Genome Atlas) - NIH, no registration
2. ICGC (International Cancer Genome Consortium) - public access
3. cBioPortal - aggregated cancer studies

Recommended: TCGA + ClinVar cancer variants
"""

import urllib.request
import pandas as pd
from pathlib import Path
import logging
import gzip

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# TCGA Pan-Cancer Summary (small file, public)
TCGA_URL = "https://api.gdc.cancer.gov/data/1c8cfe5f-e52d-41ba-94da-f15ea1337efc"  # mc3.v0.2.8.PUBLIC.maf.gz
# Alternative: Use cBioPortal API

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "cancer"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TCGA_FILE = OUTPUT_DIR / "tcga_pancancer.maf.gz"
OUTPUT_CSV = OUTPUT_DIR / "cancer_mutations.csv"

def download_tcga_if_missing():
    if TCGA_FILE.exists():
        logger.info(f"TCGA file exists ({TCGA_FILE.stat().st_size/1024/1024:.1f} MB), skipping")
        return True
    
    logger.info("Downloading TCGA Pan-Cancer data...")
    logger.info("This is a summary file (~30 MB), not full TCGA")
    logger.info(f"URL: {TCGA_URL}")
    
    try:
        def progress(block, block_size, total):
            if total > 0 and block % 50 == 0:
                pct = (block * block_size / total) * 100
                mb = (block * block_size) / (1024 * 1024)
                print(f"  {pct:.1f}% ({mb:.1f} MB)", end='\r')
        
        urllib.request.urlretrieve(TCGA_URL, TCGA_FILE, reporthook=progress)
        print()
        logger.info(f"Downloaded: {TCGA_FILE.name}")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        logger.info("Alternative: Use ClinVar cancer variants only")
        return False

def process_tcga_maf_chunked():
    """Process TCGA MAF file in chunks (memory efficient)."""
    if OUTPUT_CSV.exists():
        logger.info(f"Processed file exists: {OUTPUT_CSV.name}, skipping")
        return OUTPUT_CSV
    
    logger.info(f"Processing {TCGA_FILE.name} (chunked)...")
    
    try:
        chunk_size = 10000
        total_processed = 0
        total_kept = 0
        
        with gzip.open(TCGA_FILE, 'rt') as f:
            # Skip comments
            for line in f:
                if not line.startswith('#'):
                    header_line = line
                    break
            
            header = header_line.strip().split('\t')
            logger.info(f"MAF columns: {len(header)}")
            
            # Key columns to extract
            cols_to_keep = ['Hugo_Symbol', 'Chromosome', 'Start_Position', 
                           'Variant_Classification', 'Variant_Type', 
                           'Reference_Allele', 'Tumor_Seq_Allele2',
                           'Tumor_Sample_Barcode']
            
            col_indices = {}
            for col in cols_to_keep:
                if col in header:
                    col_indices[col] = header.index(col)
            
            records = []
            
            for line in f:
                total_processed += 1
                parts = line.strip().split('\t')
                
                if len(parts) < len(header):
                    continue
                
                # Extract key columns
                try:
                    record = {
                        'gene_symbol': parts[col_indices['Hugo_Symbol']],
                        'chromosome': parts[col_indices['Chromosome']],
                        'position': parts[col_indices['Start_Position']],
                        'variant_class': parts[col_indices['Variant_Classification']],
                        'variant_type': parts[col_indices['Variant_Type']],
                        'reference_allele': parts[col_indices['Reference_Allele']],
                        'alternate_allele': parts[col_indices['Tumor_Seq_Allele2']],
                        'tumor_sample': parts[col_indices['Tumor_Sample_Barcode']]
                    }
                    
                    records.append(record)
                    total_kept += 1
                except:
                    continue
                
                # Write chunk
                if len(records) >= chunk_size:
                    df_chunk = pd.DataFrame(records)
                    df_chunk.to_csv(OUTPUT_CSV, mode='a', 
                                   header=(total_kept == len(records)), 
                                   index=False)
                    logger.info(f"  Processed {total_processed:,}, kept {total_kept:,}")
                    records = []
            
            # Write remaining
            if records:
                df_chunk = pd.DataFrame(records)
                df_chunk.to_csv(OUTPUT_CSV, mode='a', 
                               header=(total_kept == len(records)), 
                               index=False)
        
        logger.info(f"Complete: {total_kept:,} cancer mutations")
        return OUTPUT_CSV
    
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return None

def main():
    print("\n" + "="*80)
    print("CANCER MUTATION DATA DOWNLOAD (TCGA)")
    print("="*80)
    print("Using TCGA Pan-Cancer instead of COSMIC (no registration)")
    print("="*80 + "\n")
    
    if not download_tcga_if_missing():
        print("\nFallback: Use ClinVar cancer variants")
        print("Filter: clinical_significance LIKE '%cancer%' OR '%oncogenic%'")
        return
    
    process_tcga_maf_chunked()
    
    print("\n" + "="*80)
    print("SUCCESS - Cancer data processed")
    print(f"Output: {OUTPUT_CSV}")
    print("Source: TCGA Pan-Cancer (public, no registration)")
    print("="*80)

if __name__ == "__main__":
    main()
