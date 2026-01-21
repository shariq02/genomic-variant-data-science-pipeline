# ====================================================================
# SPLIT LARGE CSV FOR DATABRICKS UPLOAD
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 21, 2026
# ====================================================================
# Purpose: Split protein_cluster_members_raw.csv into smaller files for faster upload
# Output: Multiple smaller CSVs that can be uploaded separately
# ====================================================================

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "protein_clusters" / "protein_cluster_members_raw.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "protein_clusters" / "chunks"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Settings
ROWS_PER_CHUNK = 40000  # 40K rows per file (~15MB)
TOTAL_CHUNKS = 150

def split_csv_file():
    """Split large CSV into smaller chunks"""
    
    print("\n" + "="*70)
    print("SPLIT LARGE CSV FOR DATABRICKS UPLOAD")
    print("="*70)
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Rows per chunk: {ROWS_PER_CHUNK:,}")
    print("="*70 + "\n")
    
    if not INPUT_FILE.exists():
        logger.error(f"File not found: {INPUT_FILE}")
        return
    
    logger.info("Reading CSV in chunks...")
    
    chunk_num = 0
    total_rows = 0
    
    # Read and write in chunks
    for chunk in pd.read_csv(INPUT_FILE, chunksize=ROWS_PER_CHUNK):
        chunk_num += 1
        
        # Output filename
        output_file = OUTPUT_DIR / f"clinvar_variants_part_{chunk_num:02d}.csv"
        
        # Write chunk
        chunk.to_csv(output_file, index=False)
        
        total_rows += len(chunk)
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        logger.info(f"Created: {output_file.name} ({len(chunk):,} rows, {file_size_mb:.1f} MB)")
    
    print("\n" + "="*70)
    print("SPLITTING COMPLETE")
    print("="*70)
    print(f"Total rows processed: {total_rows:,}")
    print(f"Total chunks created: {chunk_num}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("\n" + "="*70)

if __name__ == "__main__":
    split_csv_file()
