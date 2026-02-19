# ====================================================================
# InterPro Protein Family Data Download
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 18, 2026
# ====================================================================
# FILE: scripts/extraction/18_download_interpro.py
# Purpose: Download InterPro protein family mappings
# ====================================================================

"""
InterPro Protein Family Download

Correct URL: https://ftp.ebi.ac.uk/pub/databases/interpro/current_release/protein2ipr.dat.gz
(Note: current/ changed to current_release/)
"""

import urllib.request
import gzip
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Correct URL (current_release, not current)
INTERPRO_URL = "https://ftp.ebi.ac.uk/pub/databases/interpro/current_release/protein2ipr.dat.gz"
CHUNK_SIZE = 10000000

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "interpro"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INTERPRO_GZ = OUTPUT_DIR / "protein2ipr.dat.gz"
OUTPUT_CSV = OUTPUT_DIR / "interpro_protein_families.csv"

def download_if_missing():
    if INTERPRO_GZ.exists():
        logger.info(f"InterPro file exists ({INTERPRO_GZ.stat().st_size/1024/1024:.1f} MB), skipping")
        return True
    
    logger.info("Downloading InterPro data (~200 MB, 5-10 minutes)...")
    logger.info(f"URL: {INTERPRO_URL}")
    
    try:
        def progress(block, block_size, total):
            if total > 0 and block % 100 == 0:
                pct = (block * block_size / total) * 100
                mb = (block * block_size) / (1024 * 1024)
                print(f"  {pct:.1f}% ({mb:.1f} MB)", end='\r')
        
        urllib.request.urlretrieve(INTERPRO_URL, INTERPRO_GZ, reporthook=progress)
        print()
        logger.info(f"Downloaded: {INTERPRO_GZ.name}")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False

def parse_interpro():
    if OUTPUT_CSV.exists():
        logger.info(f"Processed file exists: {OUTPUT_CSV.name}, skipping")
        return OUTPUT_CSV
    
    logger.info(f"Parsing {INTERPRO_GZ.name} (15 GB compressed)")
    logger.info("Using chunked extraction with temp files to save space")
    logger.info("Strategy: Extract chunk -> Filter human -> Append to final -> Delete chunk")
    
    # Load human UniProt IDs from existing data if available
    logger.info("Loading known human protein IDs from UniProt mapping...")
    
    # Try to get human proteins from your existing data
    human_proteins_file = PROJECT_ROOT / "data" / "raw" / "references" / "uniprot_swissprot_human.csv"
    known_human_ids = set()
    
    if human_proteins_file.exists():
        try:
            df_human = pd.read_csv(human_proteins_file)
            if 'uniprot_accession' in df_human.columns:
                known_human_ids = set(df_human['uniprot_accession'].dropna())
                logger.info(f"Loaded {len(known_human_ids):,} known human protein IDs")
        except:
            pass
    
    # If no mapping file, use stricter pattern matching
    if not known_human_ids:
        logger.info("Using pattern-based filtering (stricter)")
    
    try:
        TEMP_CSV = OUTPUT_DIR / "interpro_temp_chunk.csv"
        
        total_lines = 0
        total_human = 0
        chunk_num = 0
        records = []
        chunk_size = 10000000
        
        with gzip.open(INTERPRO_GZ, 'rt') as f:
            for line in f:
                total_lines += 1
                parts = line.strip().split('\t')
                
                if len(parts) < 3:
                    continue
                
                protein_id = parts[0]
                interpro_id = parts[1]
                family_name = parts[2] if len(parts) > 2 else ''
                
                records.append({
                    'protein_id': protein_id,
                    'interpro_id': interpro_id,
                    'family_name': family_name
                })
                
                # When chunk is full, process it
                if len(records) >= chunk_size:
                    chunk_num += 1
                    logger.info(f"  Processing chunk {chunk_num} ({total_lines:,} lines processed)...")
                    
                    # Step 1: Write chunk to temp CSV
                    df_chunk = pd.DataFrame(records)
                    df_chunk.to_csv(TEMP_CSV, index=False)
                    
                    # Step 2: Filter human proteins
                    if known_human_ids:
                        # Use known human IDs (most accurate)
                        df_filtered = df_chunk[df_chunk['protein_id'].isin(known_human_ids)].copy()
                    else:
                        # Use stricter pattern: Standard UniProt human format
                        # Human proteins: [OPQ][0-9][A-Z0-9]{3}[0-9] OR [A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]
                        df_filtered = df_chunk[
                            (df_chunk['protein_id'].str.match(r'^[OPQ][0-9][A-Z0-9]{3}[0-9]$', na=False)) |
                            (df_chunk['protein_id'].str.match(r'^[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]$', na=False))
                        ].copy()
                    
                    # Step 3: Append filtered data to final CSV
                    if len(df_filtered) > 0:
                        df_filtered.to_csv(OUTPUT_CSV, mode='a', 
                                          header=(chunk_num == 1), 
                                          index=False)
                        total_human += len(df_filtered)
                    
                    # Step 4: Delete temp CSV
                    if TEMP_CSV.exists():
                        TEMP_CSV.unlink()
                    
                    logger.info(f"    Chunk {chunk_num}: {len(df_filtered):,} human proteins kept (Total: {total_human:,})")
                    
                    # Clear memory
                    records = []
                    del df_chunk
                    del df_filtered
        
        # Process remaining records
        if records:
            chunk_num += 1
            logger.info(f"  Processing final chunk {chunk_num}...")
            
            df_chunk = pd.DataFrame(records)
            df_chunk.to_csv(TEMP_CSV, index=False)
            
            if known_human_ids:
                df_filtered = df_chunk[df_chunk['protein_id'].isin(known_human_ids)].copy()
            else:
                df_filtered = df_chunk[
                    (df_chunk['protein_id'].str.match(r'^[OPQ][0-9][A-Z0-9]{3}[0-9]$', na=False)) |
                    (df_chunk['protein_id'].str.match(r'^[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]$', na=False))
                ].copy()
            
            if len(df_filtered) > 0:
                df_filtered.to_csv(OUTPUT_CSV, mode='a', 
                                  header=(chunk_num == 1), 
                                  index=False)
                total_human += len(df_filtered)
            
            if TEMP_CSV.exists():
                TEMP_CSV.unlink()
            
            del df_chunk
            del df_filtered
        
        logger.info(f"\nComplete!")
        logger.info(f"  Total lines: {total_lines:,}")
        logger.info(f"  Human proteins: {total_human:,}")
        logger.info(f"  Filtered out: {100 - (total_human/total_lines*100):.1f}%")
        
        if OUTPUT_CSV.exists():
            logger.info(f"  Output size: {OUTPUT_CSV.stat().st_size / (1024*1024):.1f} MB")
        
        return OUTPUT_CSV
    
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def main():
    print("\n" + "="*80)
    print("INTERPRO PROTEIN FAMILY DOWNLOAD")
    print("="*80 + "\n")
    
    if not download_if_missing():
        return
    
    parse_interpro()
    
    print("\n" + "="*80)
    print("SUCCESS - InterPro data processed")
    print(f"Output: {OUTPUT_CSV}")
    print("="*80)

if __name__ == "__main__":
    main()
