# ====================================================================
# GTEx Expression Data Download
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 18, 2026
# ====================================================================
# FILE: scripts/extraction/17_download_gtex.py
# ====================================================================

"""
GTEx Median TPM Data Download

Working URL (as of Feb 2026):
https://storage.googleapis.com/adult-gtex/bulk-gex/v8/rna-seq/GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_median_tpm.gct.gz

File size: ~60 MB (median TPM only, not full matrix)
Processing: Filters human genes only, removes low expression
"""

import gzip
import urllib.request
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Working GTEx URL (adult-gtex bucket)
GTEX_URL = "https://storage.googleapis.com/adult-gtex/bulk-gex/v8/rna-seq/GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_median_tpm.gct.gz"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "gtex"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GCT_GZ = OUTPUT_DIR / "gtex_median_tpm.gct.gz"
OUTPUT_CSV = OUTPUT_DIR / "gtex_tissue_expression.csv"

def download_if_missing():
    if GCT_GZ.exists():
        logger.info(f"GTEx file exists ({GCT_GZ.stat().st_size/1024/1024:.1f} MB), skipping")
        return True
    
    logger.info("Downloading GTEx median TPM (~60 MB, 2-3 minutes)...")
    logger.info(f"URL: {GTEX_URL}")
    
    try:
        def progress(block, block_size, total):
            if total > 0 and block % 50 == 0:
                pct = (block * block_size / total) * 100
                mb = (block * block_size) / (1024 * 1024)
                print(f"  {pct:.1f}% ({mb:.1f} MB)", end='\r')
        
        urllib.request.urlretrieve(GTEX_URL, GCT_GZ, reporthook=progress)
        print()
        logger.info(f"Downloaded: {GCT_GZ.name}")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        logger.error("Try manual download from: https://gtexportal.org/home/datasets")
        return False

def process_gct_chunked():
    """Process GCT file in memory-efficient chunks."""
    if OUTPUT_CSV.exists():
        logger.info(f"Processed file exists: {OUTPUT_CSV.name}, skipping")
        return OUTPUT_CSV
    
    logger.info(f"Processing {GCT_GZ.name} (chunked, memory efficient)...")
    
    try:
        with gzip.open(GCT_GZ, 'rt') as f:
            # Skip GCT header
            version = f.readline().strip()  # #1.2
            dims = f.readline().strip()  # dimensions
            logger.info(f"GCT version: {version}, dimensions: {dims}")
            
            # Read header
            header = f.readline().strip().split('\t')
            gene_id_col = header[0]
            gene_name_col = header[1]
            tissues = header[2:]  # Remaining columns are tissues
            
            logger.info(f"Tissues: {len(tissues)}")
            logger.info(f"Sample tissues: {tissues[:5]}")
            
            # Process in chunks
            chunk_size = 1000
            records = []
            genes_processed = 0
            total_pairs = 0
            
            while True:
                lines = []
                for _ in range(chunk_size):
                    line = f.readline()
                    if not line:
                        break
                    lines.append(line)
                
                if not lines:
                    break
                
                # Parse chunk
                for line in lines:
                    parts = line.strip().split('\t')
                    if len(parts) < len(tissues) + 2:
                        continue
                    
                    gene_id = parts[0]
                    gene_name = parts[1]
                    
                    # Only process human protein-coding genes (ENSG IDs)
                    if not gene_id.startswith('ENSG'):
                        continue
                    
                    # Process expression values
                    for i, tissue in enumerate(tissues):
                        try:
                            tpm = float(parts[i + 2])
                            
                            # Filter low expression (TPM < 0.1)
                            if tpm < 0.1:
                                continue
                            
                            # Categorize expression
                            if tpm < 1:
                                category = 'low'
                            elif tpm < 10:
                                category = 'medium'
                            else:
                                category = 'high'
                            
                            records.append({
                                'gene_id': gene_id,
                                'gene_name': gene_name,
                                'tissue_type': tissue,
                                'expression_tpm': tpm,
                                'expression_category': category
                            })
                            total_pairs += 1
                        except:
                            continue
                    
                    genes_processed += 1
                
                # Write chunk to CSV
                if records:
                    df_chunk = pd.DataFrame(records)
                    df_chunk.to_csv(OUTPUT_CSV, mode='a', 
                                   header=(genes_processed == len(lines)), 
                                   index=False)
                    logger.info(f"  Processed {genes_processed:,} genes, {total_pairs:,} gene-tissue pairs")
                    records = []
        
        logger.info(f"Complete: {genes_processed:,} genes, {total_pairs:,} filtered pairs")
        logger.info(f"Output: {OUTPUT_CSV}")
        return OUTPUT_CSV
    
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def main():
    print("\n" + "="*80)
    print("GTEX MEDIAN TPM DOWNLOAD & PROCESSING")
    print("="*80 + "\n")
    
    if not download_if_missing():
        return
    
    process_gct_chunked()
    
    print("\n" + "="*80)
    print("SUCCESS - GTEx data processed")
    print(f"Output: {OUTPUT_CSV}")
    print("Filtered: TPM >= 0.1, human genes only")
    print("="*80)

if __name__ == "__main__":
    main()
