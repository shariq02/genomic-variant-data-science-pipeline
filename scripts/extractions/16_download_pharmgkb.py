# ====================================================================
# PharmGKB Data Download
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 18, 2026
# ====================================================================
# FILE: scripts/extraction/16_download_pharmgkb.py
# Purpose: Download PharmGKB drug-gene relationships
# ====================================================================

"""
PharmGKB Data Download - Direct TSV files
Source: https://www.pharmgkb.org/downloads

Available datasets:
- Drugs/Chemicals
- Genes
- Variants  
- Relationships (drug-gene, gene-variant)
- Drug Label Annotations
"""

import urllib.request
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Direct download URLs (updated Feb 2026)
FILES = {
    'drugs': 'https://s3.pgkb.org/data/drugs.zip',
    'genes': 'https://s3.pgkb.org/data/genes.zip', 
    'relationships': 'https://s3.pgkb.org/data/relationships.zip',
    'variants': 'https://s3.pgkb.org/data/variants.zip',
}

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "pharmgkb"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def download_if_missing(url, dest):
    if dest.exists():
        logger.info(f"{dest.name} already exists ({dest.stat().st_size/1024/1024:.1f} MB), skipping")
        return True
    
    logger.info(f"Downloading {dest.name}...")
    try:
        def progress(block, block_size, total):
            if total > 0 and block % 50 == 0:
                pct = (block * block_size / total) * 100
                print(f"  {pct:.1f}%", end='\r')
        
        urllib.request.urlretrieve(url, dest, reporthook=progress)
        print()
        logger.info(f"Downloaded: {dest.name} ({dest.stat().st_size/1024/1024:.1f} MB)")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False

def extract_and_process(name):
    import zipfile
    
    zip_file = OUTPUT_DIR / f"{name}.zip"
    if not zip_file.exists():
        return None
    
    output_csv = OUTPUT_DIR / f"pharmgkb_{name}.csv"
    if output_csv.exists():
        logger.info(f"Processed file exists: {output_csv.name}, skipping")
        return output_csv
    
    logger.info(f"Extracting {name}.zip...")
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(OUTPUT_DIR)
        
        # Find TSV file
        tsv_files = list(OUTPUT_DIR.glob(f"{name}.tsv"))
        if not tsv_files:
            logger.warning(f"No TSV found for {name}")
            return None
        
        df = pd.read_csv(tsv_files[0], sep='\t', low_memory=False)
        df.to_csv(output_csv, index=False)
        logger.info(f"Processed: {output_csv.name} ({len(df):,} rows)")
        
        # Cleanup
        zip_file.unlink()
        tsv_files[0].unlink()
        
        return output_csv
    except Exception as e:
        logger.error(f"Processing failed for {name}: {e}")
        return None

def main():
    print("\n" + "="*80)
    print("PHARMGKB DATA DOWNLOAD")
    print("="*80 + "\n")
    
    # Download all
    for name, url in FILES.items():
        dest = OUTPUT_DIR / f"{name}.zip"
        download_if_missing(url, dest)
    
    # Process all
    for name in FILES.keys():
        extract_and_process(name)
    
    print("\n" + "="*80)
    print("SUCCESS - PharmGKB data downloaded")
    print(f"Output: {OUTPUT_DIR}")
    print("="*80)

if __name__ == "__main__":
    main()
