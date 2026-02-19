# ====================================================================
# Allele ID Mapping - Critical Fix for Issue #1
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 15, 2026
# ====================================================================
# FILE: scripts/extraction/03_download_allele_mapping.py
# Purpose: Download VariationID to AlleleID mapping from ClinVar
# ====================================================================
# Allele ID Mapping - Critical Fix for Issue #1
# ====================================================================

import gzip
import urllib.request
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

VARIATION_ALLELE_FTP_URL = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variation_allele.txt.gz"
CHUNK_SIZE = 100000

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "variants"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Output directory: {OUTPUT_DIR}")

def download_variation_allele_file():
    local_file = OUTPUT_DIR / "variation_allele.txt.gz"
    logger.info("Downloading variation_allele from ClinVar FTP...")
    logger.info(f"URL: {VARIATION_ALLELE_FTP_URL}")
    
    try:
        def progress_hook(block_num, block_size, total_size):
            if total_size > 0 and block_num % 100 == 0:
                percent = (block_num * block_size / total_size) * 100
                print(f"  Progress: {percent:.1f}%", end='\r')
        
        urllib.request.urlretrieve(VARIATION_ALLELE_FTP_URL, local_file, reporthook=progress_hook)
        print()
        logger.info(f"Downloaded: {local_file}")
        return local_file
    except Exception as e:
        logger.error(f"Error: {e}")
        return None

def parse_variation_allele_file_chunked(variation_allele_file):
    logger.info("Starting parsing (skipping first 7 comment lines)...")
    
    output_file = OUTPUT_DIR / "allele_id_mapping.csv"
    if output_file.exists():
        output_file.unlink()
    
    try:
        total_mappings = 0
        chunk_num = 0
        
        with gzip.open(variation_allele_file, 'rt', encoding='utf-8') as f:
            for chunk in pd.read_csv(f, sep='\t', chunksize=CHUNK_SIZE, skiprows=7, engine='python'):
                chunk_num += 1
                
                # Clean column names (remove # prefix)
                chunk.columns = [col.strip('#').strip() for col in chunk.columns]
                
                if chunk_num == 1:
                    logger.info(f"Columns: {list(chunk.columns)}")
                
                # Create simple mapping
                if 'VariationID' in chunk.columns and 'AlleleID' in chunk.columns:
                    df_map = pd.DataFrame()
                    df_map['variation_id'] = pd.to_numeric(chunk['VariationID'], errors='coerce')
                    df_map['allele_id'] = pd.to_numeric(chunk['AlleleID'], errors='coerce')
                    df_map = df_map.dropna()
                    df_map['variation_id'] = df_map['variation_id'].astype(int)
                    df_map['allele_id'] = df_map['allele_id'].astype(int)
                    df_map = df_map.drop_duplicates(subset=['variation_id'], keep='first')
                    
                    if len(df_map) > 0:
                        df_map.to_csv(output_file, mode='a', header=(chunk_num == 1), index=False)
                        total_mappings += len(df_map)
                        logger.info(f"Chunk {chunk_num}: {len(df_map):,} mappings ({total_mappings:,} total)")
        
        logger.info(f"Complete! Total mappings: {total_mappings:,}")
        return output_file, total_mappings
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None, 0

def main():
    print("\n" + "="*70)
    print("ALLELE ID MAPPING EXTRACTION")
    print("="*70)
    
    # Download
    gz_file = download_variation_allele_file()
    if not gz_file:
        return
    
    # Parse
    output_file, total = parse_variation_allele_file_chunked(gz_file)
    
    if output_file and total > 0:
        print("\n" + "="*70)
        print("SUCCESS")
        print("="*70)
        print(f"Output: {output_file}")
        print(f"Total mappings: {total:,}")
        print("\nSample data:")
        df_sample = pd.read_csv(output_file, nrows=10)
        print(df_sample)
    else:
        print("\nFAILED - no mappings created")

if __name__ == "__main__":
    main()
