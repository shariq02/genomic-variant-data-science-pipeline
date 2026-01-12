# ====================================================================
# IMPROVED Variant Data Extraction - Bulk Download
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 12, 2026
# ====================================================================
# FILE: scripts/extraction/02_download_variants.py
# Purpose: Download ALL ClinVar variants (1,000,000+ variants)
# ====================================================================

"""
IMPROVED Variant Data Extraction from ClinVar
Downloads ALL ClinVar variants in bulk from NCBI FTP site.

Key Improvements:
1. Downloads ALL variants (1,000,000+) instead of just 7,000-14,000
2. Uses FTP bulk download instead of slow API calls
3. Processes variant_summary.txt.gz file
4. Much faster (minutes instead of hours)
5. Complete variant metadata with proper field mappings

Data Source:
ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variant_summary.txt.gz

Fields Available (from ClinVar README):
AlleleID, Type, Name, GeneID, GeneSymbol, HGNC_ID, ClinicalSignificance, 
ClinSigSimple, LastEvaluated, RS (dbSNP), nsv/esv (dbVar), RCVaccession, 
PhenotypeIDS, PhenotypeList, Origin, OriginSimple, Assembly, 
ChromosomeAccession, Chromosome, Start, Stop, ReferenceAllele, 
AlternateAllele, Cytogenetic, ReviewStatus, NumberSubmitters, Guidelines, 
TestedInGTR, OtherIDs, SubmitterCategories, VariationID, 
PositionVCF, ReferenceAlleleVCF, AlternateAlleleVCF
"""

import os
import gzip
import urllib.request
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CLINVAR_VARIANT_FTP_URL = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variant_summary.txt.gz"
CHUNK_SIZE = 100000

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "variants"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")


def download_variant_summary_file():
    local_file = OUTPUT_DIR / "variant_summary.txt.gz"
    
    logger.info(f"Downloading variant summary from ClinVar FTP...")
    logger.info(f"URL: {CLINVAR_VARIANT_FTP_URL}")
    logger.info("This may take 5-10 minutes (file is around 400 MB)...")
    
    try:
        def progress_hook(block_num, block_size, total_size):
            if total_size > 0:
                percent = (block_num * block_size / total_size) * 100
                downloaded_mb = (block_num * block_size) / (1024 * 1024)
                total_mb = total_size / (1024 * 1024)
                if block_num % 100 == 0:
                    print(f"  Progress: {percent:.1f}% ({downloaded_mb:.1f}/{total_mb:.1f} MB)", end='\r')
        
        urllib.request.urlretrieve(CLINVAR_VARIANT_FTP_URL, local_file, reporthook=progress_hook)
        print()
        
        file_size = local_file.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded successfully: {local_file}")
        logger.info(f"  File size: {file_size:.2f} MB")
        
        return local_file
    
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return None


def get_column(chunk, possible_names, default='Unknown'):
    """Get column value handling different possible column names."""
    for name in possible_names:
        if name in chunk.columns:
            return chunk[name]
    return default


def process_variant_chunk(chunk, chunk_num):
    try:
        chunk = chunk[chunk['Assembly'] == 'GRCh38'].copy()
        
        if len(chunk) == 0:
            return None
        
        df_clean = pd.DataFrame()
        
        df_clean['variant_id'] = get_column(chunk, ['VariationID', 'Variation ID', 'variation_id'])
        df_clean['allele_id'] = get_column(chunk, ['AlleleID', 'Allele ID', 'allele_id'])
        df_clean['accession'] = get_column(chunk, ['RCVaccession', 'RCV accession', 'accession'])
        df_clean['gene_id'] = get_column(chunk, ['GeneID', 'Gene ID', 'gene_id'], -1)
        df_clean['gene_name'] = get_column(chunk, ['GeneSymbol', 'Gene Symbol', 'gene_symbol'], 'Unknown')
        df_clean['clinical_significance'] = get_column(chunk, ['ClinicalSignificance', 'Clinical Significance', 'clinical_significance'], 'Unknown')
        df_clean['clinical_significance_simple'] = get_column(chunk, ['ClinSigSimple', 'ClinSig Simple', 'clinsig_simple'], 'Unknown')
        df_clean['disease'] = get_column(chunk, ['PhenotypeList', 'Phenotype List', 'phenotype_list'], 'Unknown')
        df_clean['phenotype_ids'] = get_column(chunk, ['PhenotypeIDS', 'Phenotype IDS', 'phenotype_ids'], '')
        df_clean['chromosome'] = get_column(chunk, ['Chromosome', 'chromosome'], 'Unknown')
        df_clean['position'] = get_column(chunk, ['Start', 'start', 'position'])
        df_clean['stop_position'] = get_column(chunk, ['Stop', 'stop', 'stop_position'])
        df_clean['cytogenetic'] = get_column(chunk, ['Cytogenetic', 'cytogenetic'], 'Unknown')
        df_clean['variant_type'] = get_column(chunk, ['Type', 'type', 'variant_type'], 'Unknown')
        df_clean['variant_name'] = get_column(chunk, ['Name', 'name', 'variant_name'], 'Unknown')
        df_clean['reference_allele'] = get_column(chunk, ['ReferenceAllele', 'Reference Allele', 'reference_allele'], '')
        df_clean['alternate_allele'] = get_column(chunk, ['AlternateAllele', 'Alternate Allele', 'alternate_allele'], '')
        df_clean['review_status'] = get_column(chunk, ['ReviewStatus', 'Review Status', 'review_status'], 'Unknown')
        df_clean['number_submitters'] = get_column(chunk, ['NumberSubmitters', 'Number Submitters', 'number_submitters'], 0)
        df_clean['origin'] = get_column(chunk, ['Origin', 'origin'], 'Unknown')
        df_clean['origin_simple'] = get_column(chunk, ['OriginSimple', 'Origin Simple', 'origin_simple'], 'Unknown')
        df_clean['last_evaluated'] = get_column(chunk, ['LastEvaluated', 'Last Evaluated', 'last_evaluated'])
        df_clean['assembly'] = chunk['Assembly']
        
        df_clean['gene_id'] = pd.to_numeric(df_clean['gene_id'], errors='coerce').fillna(-1).astype(int)
        df_clean['number_submitters'] = pd.to_numeric(df_clean['number_submitters'], errors='coerce').fillna(0).astype(int)
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Error processing chunk {chunk_num}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def parse_variant_summary_file_chunked(variant_summary_file):
    logger.info("Starting CHUNKED parsing (memory efficient)...")
    logger.info(f"Processing {CHUNK_SIZE:,} rows at a time...")
    
    output_file = OUTPUT_DIR / "clinvar_all_variants.csv"
    output_file_pathogenic = OUTPUT_DIR / "clinvar_pathogenic.csv"
    
    if output_file.exists():
        output_file.unlink()
    if output_file_pathogenic.exists():
        output_file_pathogenic.unlink()
    
    try:
        total_processed = 0
        total_kept = 0
        total_pathogenic = 0
        chunk_num = 0
        first_chunk = True
        
        with gzip.open(variant_summary_file, 'rt', encoding='utf-8') as f:
            for chunk in pd.read_csv(f, sep='\t', chunksize=CHUNK_SIZE, low_memory=False):
                chunk_num += 1
                total_processed += len(chunk)
                
                if first_chunk:
                    logger.info(f"File columns detected: {list(chunk.columns)[:10]}...")
                    first_chunk = False
                
                logger.info(f"Processing chunk {chunk_num} (rows {total_processed-len(chunk)+1:,} to {total_processed:,})...")
                
                df_clean = process_variant_chunk(chunk, chunk_num)
                
                if df_clean is None or len(df_clean) == 0:
                    logger.info(f"  Chunk {chunk_num}: 0 variants after GRCh38 filter")
                    continue
                
                total_kept += len(df_clean)
                
                df_clean.to_csv(output_file, mode='a', header=(chunk_num == 1), index=False)
                
                df_pathogenic = df_clean[
                    df_clean['clinical_significance'].astype(str).str.contains('Pathogenic', case=False, na=False)
                ]
                
                if len(df_pathogenic) > 0:
                    total_pathogenic += len(df_pathogenic)
                    df_pathogenic.to_csv(output_file_pathogenic, mode='a', header=(chunk_num == 1), index=False)
                
                logger.info(f"  Chunk {chunk_num}: {len(df_clean):,} variants kept (GRCh38), {len(df_pathogenic):,} pathogenic")
        
        logger.info(f"\nParsing complete!")
        logger.info(f"  Total rows processed: {total_processed:,}")
        logger.info(f"  Total variants kept (GRCh38): {total_kept:,}")
        logger.info(f"  Total pathogenic variants: {total_pathogenic:,}")
        
        return output_file, output_file_pathogenic, total_kept, total_pathogenic
        
    except Exception as e:
        logger.error(f"Error parsing variant summary file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None, 0, 0


def generate_summary_statistics(output_file, total_variants):
    logger.info("\nGenerating summary statistics...")
    
    try:
        df_sample = pd.read_csv(output_file, nrows=50000)
        
        print("\n" + "="*70)
        print("VARIANT DATA SUMMARY")
        print("="*70)
        print(f"Total variants saved: {total_variants:,}")
        print(f"Output file: {output_file}")
        print(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        
        print("\nVariants by gene (Top 20 from sample):")
        print(df_sample['gene_name'].value_counts().head(20))
        
        print("\nVariants by clinical significance (from sample):")
        print(df_sample['clinical_significance'].value_counts().head(10))
        
        print("\nVariants by chromosome (from sample):")
        print(df_sample['chromosome'].value_counts().head(15))
        
        print("\nVariants by type (from sample):")
        print(df_sample['variant_type'].value_counts().head(10))
        
        print("\nReview status (from sample):")
        print(df_sample['review_status'].value_counts().head(10))
        
        print("\nSample variants:")
        print(df_sample[['gene_name', 'chromosome', 'clinical_significance', 'variant_type']].head(10))
        
        print("="*70)
        
    except Exception as e:
        logger.warning(f"Could not generate summary statistics: {e}")


def main():
    print("\n" + "="*70)
    print("OPTIMIZED VARIANT DATA EXTRACTION - MEMORY EFFICIENT")
    print("="*70)
    print("This script uses CHUNKED PROCESSING to avoid memory issues")
    print(f"Chunk size: {CHUNK_SIZE:,} rows at a time")
    print(f"Output: {OUTPUT_DIR}")
    print("This will take 5-10 minutes...\n")
    print("="*70 + "\n")
    
    variant_summary_file = download_variant_summary_file()
    if not variant_summary_file:
        logger.error("Failed to download variant summary file")
        return
    
    output_file, output_file_pathogenic, total_all, total_pathogenic = parse_variant_summary_file_chunked(variant_summary_file)
    
    if output_file is None:
        logger.error("Failed to parse variant summary file")
        return
    
    generate_summary_statistics(output_file, total_all)
    
    print("\n" + "="*70)
    print("SUCCESS - VARIANT DATA EXTRACTION COMPLETED")
    print("="*70)
    print(f"All variants (GRCh38): {output_file}")
    print(f"  Total: {total_all:,} variants")
    print(f"\nPathogenic only: {output_file_pathogenic}")
    print(f"  Total: {total_pathogenic:,} variants")
    print(f"\nThis is {total_all / 7000:.0f}x MORE data than before")
    print("="*70)

if __name__ == "__main__":
    main()
