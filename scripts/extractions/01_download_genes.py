# ====================================================================
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 28 December 2025
# ====================================================================

# ====================================================================
# FILE 1: scripts/extraction/01_download_genes.py
# Purpose: Download gene metadata from NCBI
# ====================================================================

"""
Gene Data Extraction from NCBI
Downloads gene information for disease-related genes.
"""

import os
import sys
import time
import pandas as pd
from Bio import Entrez
from dotenv import load_dotenv
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SCRIPT_DIR = Path(__file__).parent  # scripts/extraction/
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # genomic-variant-data-science-pipeline/

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "genes"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")

# Configuration
Entrez.email = "sharique020709@gmail.com"
Entrez.api_key = os.getenv('NCBI_API_KEY')

# Extended list of disease-related genes (30+ genes for more data)
GENES_TO_DOWNLOAD = [
    # Breast/Ovarian Cancer
    "BRCA1", "BRCA2", "PALB2", "ATM", "CHEK2", "PTEN", "CDH1", "STK11",
    
    # Colorectal Cancer
    "APC", "MLH1", "MSH2", "MSH6", "PMS2", "EPCAM",
    
    # Tumor Suppressors
    "TP53", "RB1", "VHL", "NF1", "NF2", "TSC1", "TSC2",
    
    # Genetic Disorders
    "CFTR",     # Cystic Fibrosis
    "HBB",      # Sickle Cell Disease
    "HTT",      # Huntington's Disease
    "DMD",      # Muscular Dystrophy
    "F8", "F9", # Hemophilia
    "MTHFR",    # Folate metabolism
    
    # Alzheimer's & Neurodegenerative
    "APOE", "APP", "PSEN1", "PSEN2",
    
    # Cancer Oncogenes
    "EGFR", "KRAS", "BRAF", "MYC", "HER2",
    
    # Lynch Syndrome
    "MUTYH", "SMAD4",
    
    # Additional Important Genes
    "BRIP1", "RAD51C", "RAD51D"
]


def search_gene(gene_name):
    """Search for gene ID by gene name."""
    try:
        logger.info(f"Searching for gene: {gene_name}")
        
        search_handle = Entrez.esearch(
            db="gene",
            term=f"{gene_name}[Gene Name] AND Homo sapiens[Organism]",
            retmax=1
        )
        search_results = Entrez.read(search_handle, validate=False)
        search_handle.close()
        
        if search_results['IdList']:
            gene_id = search_results['IdList'][0]
            logger.info(f"Found gene ID: {gene_id} for {gene_name}")
            return gene_id
        else:
            logger.warning(f"Gene not found: {gene_name}")
            return None
            
    except Exception as e:
        logger.error(f"Error searching gene {gene_name}: {e}")
        return None


def fetch_gene_details_summary(gene_id, gene_name):
    """Fetch gene details using esummary (more reliable)."""
    try:
        logger.info(f"Fetching details for gene ID: {gene_id}")
        
        handle = Entrez.esummary(db="gene", id=gene_id, retmode="xml")
        records = Entrez.read(handle, validate=False)
        handle.close()
        
        if records and 'DocumentSummarySet' in records:
            record = records['DocumentSummarySet']['DocumentSummary'][0]
            
            # Extract gene details safely
            gene_details = {
                'gene_id': gene_id,
                'gene_name': gene_name,
                'official_symbol': safe_get(record, 'NomenclatureSymbol', gene_name),
                'description': safe_get(record, 'Description', 'No description available'),
                'chromosome': safe_get(record, 'Chromosome', 'Unknown'),
                'map_location': safe_get(record, 'MapLocation', 'Unknown'),
                'gene_type': safe_get(record, 'GeneType', 'Unknown'),
                'summary': safe_get(record, 'Summary', 'No summary available'),
                'other_aliases': safe_get(record, 'OtherAliases', ''),
                'other_designations': safe_get(record, 'OtherDesignations', '')
            }
            
            # Extract genomic positions
            try:
                genomic_info = record.get('GenomicInfo', [])
                if genomic_info and len(genomic_info) > 0:
                    gene_details['start_position'] = genomic_info[0].get('ChrStart', None)
                    gene_details['end_position'] = genomic_info[0].get('ChrStop', None)
                    gene_details['strand'] = genomic_info[0].get('ChrStrand', 'Unknown')
                    
                    # Calculate gene length
                    if gene_details['start_position'] and gene_details['end_position']:
                        gene_details['gene_length'] = abs(int(gene_details['end_position']) - 
                                                           int(gene_details['start_position']))
                else:
                    gene_details['start_position'] = None
                    gene_details['end_position'] = None
                    gene_details['strand'] = 'Unknown'
                    gene_details['gene_length'] = None
            except Exception as e:
                logger.warning(f"Could not extract genomic info: {e}")
                gene_details['start_position'] = None
                gene_details['end_position'] = None
                gene_details['strand'] = 'Unknown'
                gene_details['gene_length'] = None
            
            logger.info(f"✅ Successfully fetched details for gene: {gene_name}")
            return gene_details
        else:
            logger.error(f"No records found for gene ID: {gene_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching gene details for ID {gene_id}: {e}")
        return None


def safe_get(dictionary, key, default='Unknown'):
    """Safely get value from dictionary or object."""
    try:
        if hasattr(dictionary, 'get'):
            value = dictionary.get(key, default)
            return value if value else default
        elif hasattr(dictionary, key):
            value = getattr(dictionary, key)
            return value if value else default
        else:
            return default
    except:
        return default


def download_all_genes():
    """Download all genes in the list."""
    gene_data_list = []
    total_genes = len(GENES_TO_DOWNLOAD)
    
    logger.info(f"Starting download of {total_genes} genes...")
    
    for idx, gene_name in enumerate(GENES_TO_DOWNLOAD, 1):
        try:
            logger.info(f"\n[{idx}/{total_genes}] Processing: {gene_name}")
            
            # Search for gene
            gene_id = search_gene(gene_name)
            
            if gene_id:
                # Fetch details
                gene_details = fetch_gene_details_summary(gene_id, gene_name)
                
                if gene_details:
                    gene_data_list.append(gene_details)
                    logger.info(f"✅ [{idx}/{total_genes}] Successfully processed: {gene_name}")
                else:
                    logger.warning(f"⚠️ [{idx}/{total_genes}] Could not fetch details for: {gene_name}")
            else:
                logger.warning(f"⚠️ [{idx}/{total_genes}] Could not find gene ID for: {gene_name}")
            
            # Rate limiting
            time.sleep(0.4)  # 2.5 requests/second with API key
            
        except Exception as e:
            logger.error(f"❌ Error processing {gene_name}: {e}")
            continue
    
    # Create DataFrame
    if gene_data_list:
        df_genes = pd.DataFrame(gene_data_list)
        logger.info(f"\n📊 Downloaded {len(df_genes)} genes successfully out of {total_genes} attempted")
        return df_genes
    else:
        logger.error("❌ No genes downloaded")
        return pd.DataFrame()


def save_gene_data(df_genes):
    """Save gene data to CSV."""
    output_path = OUTPUT_DIR / "gene_metadata.csv"
    
    try:
        df_genes.to_csv(output_path, index=False)
        logger.info(f"✅ Gene data saved to: {output_path}")
        
        # Display comprehensive summary
        print("\n" + "="*70)
        print("GENE DATA DOWNLOAD SUMMARY")
        print("="*70)
        print(f"Total genes downloaded: {len(df_genes)}")
        print(f"Output file: {output_path}")
        
        print("\n📍 Genes by chromosome:")
        print(df_genes['chromosome'].value_counts().head(15))
        
        print("\n🧬 Genes by type:")
        print(df_genes['gene_type'].value_counts())
        
        print("\n📏 Gene length statistics (bp):")
        if df_genes['gene_length'].notna().any():
            print(df_genes['gene_length'].describe())
        
        print("\n📋 Sample genes:")
        print(df_genes[['gene_name', 'chromosome', 'gene_type', 'gene_length']].head(15))
        
        print("\n💾 All columns saved:")
        print(df_genes.columns.tolist())
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Error saving gene data: {e}")


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("GENE DATA EXTRACTION FROM NCBI")
    print("="*70)
    print(f"Downloading {len(GENES_TO_DOWNLOAD)} disease-related genes...")
    print(f"Output: {OUTPUT_DIR}")
    print("This may take 3-5 minutes...\n")
    
    if not Entrez.api_key:
        logger.warning("⚠️ No NCBI API key found. Using rate-limited access.")
        logger.warning("Get API key from: https://www.ncbi.nlm.nih.gov/account/")
    
    # Download genes
    df_genes = download_all_genes()
    
    # Save results
    if not df_genes.empty:
        save_gene_data(df_genes)
        print(f"\n✅ Gene data extraction completed successfully!")
        print(f"📁 Check file: {OUTPUT_DIR / 'gene_metadata.csv'}")
    else:
        print("\n❌ Gene data extraction failed!")


if __name__ == "__main__":
    main()