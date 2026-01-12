# ====================================================================
# IMPROVED Gene Data Extraction - Bulk Download
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 12, 2026
# ====================================================================
# FILE: scripts/extraction/01_download_genes.py
# Purpose: Download ALL human genes from NCBI FTP (20,000+ genes)
# ====================================================================

"""
IMPROVED Gene Data Extraction from NCBI
Downloads ALL human genes in bulk from NCBI FTP site.

Key Improvements:
1. Downloads ALL human genes (20,000+) instead of just 42
2. Uses FTP bulk download instead of slow API calls
3. Processes Homo_sapiens.gene_info.gz file
4. Much faster (minutes instead of hours)
5. Complete gene metadata with proper field mappings

Data Source:
ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz

Fields Available (from NCBI Gene README):
tax_id GeneID Symbol LocusTag Synonyms dbXrefs chromosome map_location 
description type_of_gene Symbol_from_nomenclature_authority 
Full_name_from_nomenclature_authority Nomenclature_status Other_designations 
Modification_date Feature_type
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
NCBI_GENE_FTP_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz"
CHUNK_SIZE = 10000  # Process 10k rows at a time (gene file is smaller)

# Paths
SCRIPT_DIR = Path(__file__).parent  # scripts/extraction/
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # genomic-variant-data-science-pipeline/
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "genes"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")


def download_gene_info_file():
    """Download the Homo_sapiens.gene_info.gz file from NCBI FTP."""
    local_file = OUTPUT_DIR / "Homo_sapiens.gene_info.gz"
    
    logger.info(f"Downloading gene info from NCBI FTP...")
    logger.info(f"URL: {NCBI_GENE_FTP_URL}")
    
    try:
        # Download
        urllib.request.urlretrieve(NCBI_GENE_FTP_URL, local_file)
        
        file_size = local_file.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded successfully: {local_file}")
        logger.info(f"  File size: {file_size:.2f} MB")
        
        return local_file
    
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return None


def process_gene_chunk(chunk):
    """Process a single chunk of gene data."""
    
    # Create cleaned dataframe
    df_clean = pd.DataFrame()
    
    # Basic fields
    df_clean['gene_id'] = chunk['gene_id']
    df_clean['gene_name'] = chunk['symbol']
    df_clean['official_symbol'] = chunk['symbol_from_nomenclature_authority'].fillna(chunk['symbol'])
    df_clean['description'] = chunk['description'].fillna('No description available')
    df_clean['chromosome'] = chunk['chromosome']
    df_clean['map_location'] = chunk['map_location'].fillna('Unknown')
    df_clean['gene_type'] = chunk['type_of_gene'].fillna('Unknown')
    
    # Synonyms and other names
    df_clean['other_aliases'] = chunk['synonyms'].fillna('')
    df_clean['other_designations'] = chunk['other_designations'].fillna('')
    
    # Full name
    df_clean['full_name'] = chunk['full_name_from_nomenclature_authority'].fillna(chunk['description'])
    
    # Nomenclature status
    df_clean['nomenclature_status'] = chunk['nomenclature_status'].fillna('Unknown')
    
    # Database cross-references
    df_clean['db_xrefs'] = chunk['db_xrefs'].fillna('')
    
    # Modification date
    df_clean['modification_date'] = chunk['modification_date']
    
    # Feature type
    df_clean['feature_type'] = chunk['feature_type'].fillna('Unknown')
    
    # Genomic positions (not available in gene_info)
    df_clean['start_position'] = None
    df_clean['end_position'] = None
    df_clean['strand'] = 'Unknown'
    df_clean['gene_length'] = None
    
    # Add download metadata
    df_clean['download_date'] = datetime.now().strftime('%Y-%m-%d')
    df_clean['data_source'] = 'NCBI Gene FTP'
    
    return df_clean


def parse_gene_info_file_chunked(gene_info_file):
    """Parse the gene_info file in CHUNKS."""
    
    logger.info("Starting CHUNKED parsing (memory efficient)...")
    logger.info(f"Processing {CHUNK_SIZE:,} rows at a time...")
    
    output_file_all = OUTPUT_DIR / "gene_metadata_all.csv"
    output_file_protein = OUTPUT_DIR / "gene_metadata.csv"
    
    # Delete old files if they exist
    if output_file_all.exists():
        output_file_all.unlink()
    if output_file_protein.exists():
        output_file_protein.unlink()
    
    try:
        # Column names from NCBI Gene README
        columns = [
            'tax_id', 'gene_id', 'symbol', 'locus_tag', 'synonyms', 'db_xrefs',
            'chromosome', 'map_location', 'description', 'type_of_gene',
            'symbol_from_nomenclature_authority', 'full_name_from_nomenclature_authority',
            'nomenclature_status', 'other_designations', 'modification_date', 'feature_type'
        ]
        
        total_processed = 0
        total_protein_coding = 0
        chunk_num = 0
        
        # Process file in chunks
        with gzip.open(gene_info_file, 'rt', encoding='utf-8') as f:
            for chunk in pd.read_csv(
                f,
                sep='\t',
                comment='#',
                names=columns,
                chunksize=CHUNK_SIZE,
                low_memory=False
            ):
                chunk_num += 1
                total_processed += len(chunk)
                
                logger.info(f"Processing chunk {chunk_num} (rows {total_processed-len(chunk)+1:,} to {total_processed:,})...")
                
                # Process this chunk
                df_clean = process_gene_chunk(chunk)
                
                # Save all genes
                df_clean.to_csv(
                    output_file_all,
                    mode='a',
                    header=(chunk_num == 1),
                    index=False
                )
                
                # Save protein-coding genes separately
                df_protein = df_clean[df_clean['gene_type'] == 'protein-coding'].copy()
                if len(df_protein) > 0:
                    total_protein_coding += len(df_protein)
                    df_protein.to_csv(
                        output_file_protein,
                        mode='a',
                        header=(chunk_num == 1),
                        index=False
                    )
                
                logger.info(f"  Chunk {chunk_num}: {len(df_clean):,} total genes, {len(df_protein):,} protein-coding")
        
        logger.info(f"\nParsing complete!")
        logger.info(f"  Total genes processed: {total_processed:,}")
        logger.info(f"  Protein-coding genes: {total_protein_coding:,}")
        
        return output_file_all, output_file_protein, total_processed, total_protein_coding
        
    except Exception as e:
        logger.error(f"Error parsing gene info file: {e}")
        return None, None, 0, 0


def generate_summary_statistics(output_file, total_genes):
    """Generate summary statistics from the output file."""
    
    logger.info("\nGenerating summary statistics...")
    
    try:
        # Read just the first 10,000 rows for statistics
        df_sample = pd.read_csv(output_file, nrows=10000)
        
        print("\n" + "="*70)
        print("GENE DATA SUMMARY")
        print("="*70)
        print(f"Total genes saved: {total_genes:,}")
        print(f"Output file: {output_file}")
        print(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        
        print("\nGenes by type (from sample):")
        print(df_sample['gene_type'].value_counts().head(10))
        
        print("\nGenes by chromosome (from sample):")
        print(df_sample['chromosome'].value_counts().head(15))
        
        print("\nNomenclature status (from sample):")
        print(df_sample['nomenclature_status'].value_counts())
        
        print("\nSample genes:")
        print(df_sample[['gene_name', 'chromosome', 'gene_type', 'description']].head(10))
        
        print("="*70)
        
    except Exception as e:
        logger.warning(f"Could not generate summary statistics: {e}")


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("OPTIMIZED GENE DATA EXTRACTION - MEMORY EFFICIENT")
    print("="*70)
    print("This script uses CHUNKED PROCESSING to avoid memory issues")
    print(f"Chunk size: {CHUNK_SIZE:,} rows at a time")
    print(f"Output: {OUTPUT_DIR}")
    print("This will take 2-5 minutes...\n")
    print("="*70 + "\n")
    
    # Step 1: Download gene_info file from NCBI FTP
    gene_info_file = download_gene_info_file()
    if not gene_info_file:
        logger.error("Failed to download gene info file")
        return
    
    # Step 2: Parse the gene_info file with CHUNKED processing
    output_file_all, output_file_protein, total_all, total_protein = parse_gene_info_file_chunked(
        gene_info_file
    )
    
    if output_file_all is None:
        logger.error("Failed to parse gene info file")
        return
    
    # Step 3: Generate summary statistics
    generate_summary_statistics(output_file_protein, total_protein)
    
    print("\n" + "="*70)
    print("SUCCESS - GENE DATA EXTRACTION COMPLETED")
    print("="*70)
    print(f"Protein-coding genes: {output_file_protein}")
    print(f"  Total: {total_protein:,} genes")
    print(f"\nAll genes: {output_file_all}")
    print(f"  Total: {total_all:,} genes")
    print(f"\nThis is {total_protein / 42:.0f}x MORE data than before")
    print("="*70)

if __name__ == "__main__":
    main()
