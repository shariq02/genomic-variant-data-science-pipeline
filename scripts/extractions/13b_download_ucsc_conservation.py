"""
Download UCSC Conservation Scores (PhyloP + PhastCons)
Runs LOCALLY on your machine, NOT in Databricks

FREE public data, no license required
Downloads bigWig files and extracts scores for your specific variants

YOU NEED TO EXPORT VARIANT POSITIONS FROM DATABRICKS FIRST
Put the downloaded variant_positions.csv in your project root folder,
then run this script.

Author: Sharique Mohammad
Date: January 26, 2026
"""

import os
import sys
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# Check dependencies
try:
    import pyBigWig
except ImportError:
    print("ERROR: pyBigWig not installed")
    print("Install with: pip install pyBigWig")
    sys.exit(1)

# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "conservation"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "conservation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# UCSC bigWig files (FREE, no license)
PHYLOP_URL = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/phyloP100way/hg38.phyloP100way.bw"
PHASTCONS_URL = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/phastCons100way/hg38.phastCons100way.bw"

PHYLOP_FILE = RAW_DATA_DIR / "hg38.phyloP100way.bw"
PHASTCONS_FILE = RAW_DATA_DIR / "hg38.phastCons100way.bw"

OUTPUT_CSV = OUTPUT_DIR / "ucsc_conservation_scores.csv"

# ============================================================================
# DOWNLOAD FUNCTIONS
# ============================================================================

def download_file(url, dest_path):
    """Download file with progress bar and resume capability"""
    
    if dest_path.exists():
        existing_size = dest_path.stat().st_size
        print(f"Found existing file: {dest_path.name} ({existing_size / 1024**3:.2f} GB)")
        
        # Check if download is complete by getting expected size
        try:
            response = requests.head(url, timeout=10)
            total_size = int(response.headers.get('content-length', 0))
            
            if existing_size == total_size:
                print(f" File already complete, skipping download")
                return True
            elif existing_size > total_size:
                print(f" File corrupted (larger than expected), re-downloading...")
                dest_path.unlink()
                existing_size = 0
            else:
                print(f"Resuming download from {existing_size / 1024**3:.2f} GB...")
        except:
            print("Could not verify file, re-downloading...")
            dest_path.unlink()
            existing_size = 0
    else:
        existing_size = 0
    
    # Download with resume
    try:
        headers = {'Range': f'bytes={existing_size}-'} if existing_size > 0 else {}
        
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0)) + existing_size
        
        mode = 'ab' if existing_size > 0 else 'wb'
        
        with open(dest_path, mode) as f:
            with tqdm(
                total=total_size,
                initial=existing_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {dest_path.name}"
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        print(f" Downloaded: {dest_path.name}")
        return True
        
    except KeyboardInterrupt:
        print("\n\nDownload interrupted. Run again to resume.")
        return False
    except Exception as e:
        print(f"\n Download failed: {e}")
        return False

# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def load_variant_positions():
    """
    Load variant positions from Databricks export
    
    YOU NEED TO EXPORT THIS FROM DATABRICKS FIRST:
    Run in Databricks:
        df_variants = spark.table("workspace.silver.variants_ultra_enriched")
        df_positions = df_variants.select("variant_id", "chromosome", "position").toPandas()
        df_positions.to_csv("variant_positions.csv", index=False)
    
    Then download variant_positions.csv and put it in the project root
    """
    
    # Try multiple possible locations
    possible_paths = [
        PROJECT_ROOT / "variant_positions.csv",
        PROJECT_ROOT / "data" / "variant_positions.csv",
        Path.cwd() / "variant_positions.csv"
    ]
    
    for path in possible_paths:
        if path.exists():
            print(f" Found variant positions: {path}")
            df = pd.read_csv(path)
            print(f"  Loaded {len(df):,} variant positions")
            return df
      
    sys.exit(1)

def extract_conservation_scores(df_variants, phylop_file, phastcons_file):
    """Extract conservation scores for all variant positions"""
    
    print("\n" + "="*80)
    print("EXTRACTING CONSERVATION SCORES")
    print("="*80)
    
    # Open bigWig files
    print(f"\nOpening bigWig files...")
    bw_phylop = pyBigWig.open(str(phylop_file))
    bw_phastcons = pyBigWig.open(str(phastcons_file))
    
    print(f" PhyloP loaded")
    print(f" PhastCons loaded")
    
    # Extract scores
    results = []
    
    print(f"\nExtracting scores for {len(df_variants):,} variants...")
    
    for idx, row in tqdm(df_variants.iterrows(), total=len(df_variants), desc="Processing variants"):
        variant_id = row['variant_id']
        chromosome = str(row['chromosome'])
        position = int(row['position'])
        
        # bigWig uses "chr" prefix
        chr_name = f"chr{chromosome}"
        
        try:
            # Get PhyloP score (single position)
            phylop_scores = bw_phylop.values(chr_name, position-1, position)
            phylop_score = phylop_scores[0] if phylop_scores and len(phylop_scores) > 0 else None
            
            # Get PhastCons score (single position)
            phastcons_scores = bw_phastcons.values(chr_name, position-1, position)
            phastcons_score = phastcons_scores[0] if phastcons_scores and len(phastcons_scores) > 0 else None
            
            results.append({
                'variant_id': variant_id,
                'chromosome': chromosome,
                'position': position,
                'phylop_score_ucsc': phylop_score,
                'phastcons_score_ucsc': phastcons_score
            })
            
        except Exception as e:
            # Chromosome not in bigWig (e.g., MT, unplaced contigs)
            results.append({
                'variant_id': variant_id,
                'chromosome': chromosome,
                'position': position,
                'phylop_score_ucsc': None,
                'phastcons_score_ucsc': None
            })
    
    # Close files
    bw_phylop.close()
    bw_phastcons.close()
    
    # Create DataFrame
    df_results = pd.DataFrame(results)
    
    # Statistics
    print(f"\n" + "="*80)
    print("EXTRACTION COMPLETE")
    print("="*80)
    
    phylop_count = df_results['phylop_score_ucsc'].notna().sum()
    phastcons_count = df_results['phastcons_score_ucsc'].notna().sum()
    
    print(f"\nTotal variants: {len(df_results):,}")
    print(f"PhyloP scores: {phylop_count:,} ({phylop_count/len(df_results)*100:.1f}%)")
    print(f"PhastCons scores: {phastcons_count:,} ({phastcons_count/len(df_results)*100:.1f}%)")
    
    return df_results

# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Downloads PhyloP and PhastCons bigWig files from UCSC Genome Browser,
    extracts conservation scores for all variant positions, and saves the results
    to a CSV file.

    Parameters:
    None

    Returns:
    None

    Output:
    UCSC conservation scores for all variant positions in a CSV file
    """
    print("UCSC CONSERVATION SCORES DOWNLOADER")
    print("="*80)
    print("\nFREE public data from UCSC Genome Browser")
    print("No license required!")
    
    # Step 1: Download bigWig files
    print("STEP 1: DOWNLOAD BIGWIG FILES")
    print("="*80)
    
    print("\nDownloading PhyloP (100-way vertebrate conservation)...")
    print(f"Size: ~5-6 GB")
    success = download_file(PHYLOP_URL, PHYLOP_FILE)
    if not success:
        return
    
    print("\nDownloading PhastCons (conserved elements)...")
    print(f"Size: ~5-6 GB")
    success = download_file(PHASTCONS_URL, PHASTCONS_FILE)
    if not success:
        return
    
    # Step 2: Load variant positions
    print("STEP 2: LOAD VARIANT POSITIONS")
    print("="*80)
    
    df_variants = load_variant_positions()
    
    # Step 3: Extract scores
    print("STEP 3: EXTRACT CONSERVATION SCORES")
    print("="*80)
    
    df_results = extract_conservation_scores(df_variants, PHYLOP_FILE, PHASTCONS_FILE)
    
    # Step 4: Save results
    print("STEP 4: SAVE RESULTS")
    print("="*80)
    
    df_results.to_csv(OUTPUT_CSV, index=False)
    print(f"\n Saved: {OUTPUT_CSV}")
    print(f"  Size: {OUTPUT_CSV.stat().st_size / 1024**2:.1f} MB")
    
    # Next steps
    print("SUCCESS!")
    print("="*80)
    
    print(f"\nNext steps:")
    print(f" Upload {OUTPUT_CSV.name} to Databricks")
    
    print(f"\nSummary:")
    print(f"  - Downloaded: PhyloP + PhastCons bigWig files")
    print(f"  - Extracted: {len(df_results):,} variant scores")
    print(f"  - Coverage: ~{phylop_count/len(df_results)*100:.0f}%")
    print(f"  - Ready to upload!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Progress saved - run again to resume.")
    except Exception as e:
        print(f"\n\nERROR: {e}")
        import traceback
        traceback.print_exc()
