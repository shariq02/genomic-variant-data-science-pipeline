# ====================================================================
# Variant Data Extraction
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 28 December 2025
# ====================================================================
# FILE 2: scripts/extraction/02_download_variants.py
# Purpose: Download variant data from ClinVar
# ====================================================================

"""
Variant Data Extraction from ClinVar
Downloads pathogenic variants for specified genes.
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

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "variants"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Output directory: {OUTPUT_DIR}")

# Configuration
Entrez.email = "sharique020709@gmail.com"
Entrez.api_key = os.getenv('NCBI_API_KEY')

# Extended gene list (matches gene download script)
GENES_FOR_VARIANTS = [
    # High-priority genes with many variants
    "BRCA1", "BRCA2", "TP53", "CFTR", "HBB", "APOE", "HTT", "DMD",
    "F8", "F9", "MTHFR", "EGFR", "KRAS", "BRAF",
    
    # Additional cancer genes
    "PALB2", "ATM", "CHEK2", "PTEN", "APC", "MLH1", "MSH2", "MSH6",
    
    # More genetic disorder genes
    "VHL", "NF1", "NF2", "RB1", "CDH1", "STK11"
]


def search_clinvar_variants(gene_name, max_results=500):
    """
    Search ClinVar for pathogenic variants of a specific gene.
    INCREASED: max_results from 100 to 500 for more data.
    """
    try:
        logger.info(f"Searching ClinVar variants for gene: {gene_name}")
        
        # Search for ALL clinical significance (not just pathogenic)
        # to get more variants for analysis
        search_query = f"{gene_name}[Gene Name]"
        
        search_handle = Entrez.esearch(
            db="clinvar",
            term=search_query,
            retmax=max_results,
            usehistory="y"
        )
        search_results = Entrez.read(search_handle, validate=False)
        search_handle.close()
        
        variant_ids = search_results['IdList']
        logger.info(f"Found {len(variant_ids)} variants for {gene_name}")
        
        return variant_ids
        
    except Exception as e:
        logger.error(f"Error searching variants for {gene_name}: {e}")
        return []


def fetch_variant_details(variant_ids):
    """
    Fetch detailed information for variant IDs.
    Optimized batch processing for more data.
    """
    variant_details = []
    
    try:
        # Process in batches of 20
        batch_size = 20
        total_batches = (len(variant_ids) + batch_size - 1) // batch_size
        
        for batch_num, i in enumerate(range(0, len(variant_ids), batch_size), 1):
            batch = variant_ids[i:i+batch_size]
            logger.info(f"  Fetching batch {batch_num}/{total_batches} "
                       f"(variants {i+1} to {min(i+batch_size, len(variant_ids))})")
            
            try:
                fetch_handle = Entrez.esummary(
                    db="clinvar",
                    id=",".join(batch),
                    retmode="xml"
                )
                
                records = Entrez.read(fetch_handle, validate=False)
                fetch_handle.close()
                
                # Parse records
                if 'DocumentSummarySet' in records:
                    summaries = records['DocumentSummarySet'].get('DocumentSummary', [])
                    
                    for record in summaries:
                        try:
                            variant_info = parse_variant_record(record)
                            if variant_info:
                                variant_details.append(variant_info)
                        except Exception as e:
                            logger.debug(f"Error parsing variant: {e}")
                            continue
                
                # Rate limiting
                time.sleep(0.4)
                
            except Exception as e:
                logger.warning(f"Error fetching batch {batch_num}: {e}")
                continue
            
    except Exception as e:
        logger.error(f"Error in fetch_variant_details: {e}")
    
    return variant_details


def parse_variant_record(record):
    """Parse a single variant record safely with more fields."""
    try:
        # Extract clinical significance
        clin_sig = extract_clinical_significance(record)
        
        # Extract more detailed information
        variant_info = {
            'variant_id': safe_get_attr(record, 'uid', 'Unknown'),
            'accession': safe_get_attr(record, 'accession', 'Unknown'),
            'gene_name': extract_gene_name_safe(record),
            'clinical_significance': clin_sig,
            'disease': extract_disease_safe(record),
            'chromosome': safe_get_attr(record, 'chr_sort', 'Unknown'),
            'position': safe_get_attr(record, 'chr_start', None),
            'stop_position': safe_get_attr(record, 'chr_stop', None),
            'variant_type': safe_get_attr(record, 'variation_set_type', 'Unknown'),
            'molecular_consequence': safe_get_attr(record, 'molecular_consequence', 'Unknown'),
            'protein_change': safe_get_attr(record, 'protein_change', 'Unknown'),
            'allele_id': safe_get_attr(record, 'variation_set_id', 'Unknown'),
            'review_status': safe_get_attr(record, 'clinical_significance_review_status', 'Unknown'),
            'assembly': safe_get_attr(record, 'genome_assembly', 'GRCh38'),
            'cytogenetic': safe_get_attr(record, 'cytogenetic', 'Unknown')
        }
        
        return variant_info
        
    except Exception as e:
        logger.debug(f"Error parsing variant record: {e}")
        return None


def safe_get_attr(obj, attr, default='Unknown'):
    """Safely get attribute from object or dict."""
    try:
        if hasattr(obj, 'get'):
            value = obj.get(attr, default)
            return value if value else default
        elif hasattr(obj, attr):
            value = getattr(obj, attr)
            return value if value else default
        else:
            return default
    except:
        return default


def extract_gene_name_safe(record):
    """Extract gene name safely from variant record."""
    try:
        genes = safe_get_attr(record, 'genes', [])
        if genes and len(genes) > 0:
            if isinstance(genes, list):
                gene = genes[0]
                if hasattr(gene, 'get'):
                    return gene.get('symbol', 'Unknown')
                elif hasattr(gene, 'symbol'):
                    return gene.symbol
        
        gene_sort = safe_get_attr(record, 'gene_sort', None)
        if gene_sort and gene_sort != 'Unknown':
            return gene_sort
        
        return 'Unknown'
    except:
        return 'Unknown'


def extract_clinical_significance(record):
    """Extract clinical significance safely."""
    try:
        clin_sig = safe_get_attr(record, 'clinical_significance', {})
        if clin_sig and clin_sig != 'Unknown':
            if hasattr(clin_sig, 'get'):
                return clin_sig.get('description', 'Unknown')
            elif hasattr(clin_sig, 'description'):
                return clin_sig.description
        return 'Unknown'
    except:
        return 'Unknown'


def extract_disease_safe(record):
    """Extract disease/condition safely."""
    try:
        title = safe_get_attr(record, 'title', None)
        if title and title != 'Unknown':
            if ':' in title:
                disease = title.split(':', 1)[1].strip()
                return disease if disease else title
            return title
        
        trait_set = safe_get_attr(record, 'trait_set', [])
        if trait_set and len(trait_set) > 0:
            trait = trait_set[0]
            if hasattr(trait, 'get'):
                trait_name = trait.get('trait_name', 'Unknown')
                if trait_name != 'Unknown':
                    return trait_name
        
        return 'Unknown'
    except:
        return 'Unknown'


def download_all_variants():
    """Download variants for all genes."""
    all_variants = []
    total_genes = len(GENES_FOR_VARIANTS)
    
    logger.info(f"Starting download of variants for {total_genes} genes...")
    
    for idx, gene_name in enumerate(GENES_FOR_VARIANTS, 1):
        try:
            logger.info(f"\n{'='*70}")
            logger.info(f"[{idx}/{total_genes}] Processing gene: {gene_name}")
            logger.info(f"{'='*70}")
            
            # Search for variants (up to 500 per gene)
            variant_ids = search_clinvar_variants(gene_name, max_results=500)
            
            if variant_ids:
                # Fetch variant details
                variant_details = fetch_variant_details(variant_ids)
                all_variants.extend(variant_details)
                logger.info(f"✅ [{idx}/{total_genes}] Downloaded {len(variant_details)} variants for {gene_name}")
            else:
                logger.warning(f"⚠️ [{idx}/{total_genes}] No variants found for {gene_name}")
            
            # Rate limiting between genes
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Error processing {gene_name}: {e}")
            continue
    
    # Create DataFrame
    if all_variants:
        df_variants = pd.DataFrame(all_variants)
        
        # Clean up data
        df_variants = df_variants[df_variants['gene_name'] != 'Unknown']
        
        logger.info(f"\n📊 Downloaded {len(df_variants)} total variants from {total_genes} genes")
        return df_variants
    else:
        logger.error("❌ No variants downloaded")
        return pd.DataFrame()


def save_variant_data(df_variants):
    """Save variant data to CSV."""
    output_path = OUTPUT_DIR / "clinvar_pathogenic.csv"
    
    try:
        df_variants.to_csv(output_path, index=False)
        logger.info(f"✅ Variant data saved to: {output_path}")
        
        # Display comprehensive summary
        print("\n" + "="*70)
        print("VARIANT DATA DOWNLOAD SUMMARY")
        print("="*70)
        print(f"Total variants downloaded: {len(df_variants)}")
        print(f"Output file: {output_path}")
        
        print("\n🧬 Variants by gene (Top 15):")
        print(df_variants['gene_name'].value_counts().head(15))
        
        print("\n⚕️ Variants by clinical significance:")
        print(df_variants['clinical_significance'].value_counts())
        
        print("\n📍 Variants by chromosome (Top 15):")
        print(df_variants['chromosome'].value_counts().head(15))
        
        print("\n🔬 Variants by type:")
        print(df_variants['variant_type'].value_counts().head(10))
        
        print("\n📋 Sample variants:")
        print(df_variants[['gene_name', 'chromosome', 'clinical_significance', 
                          'variant_type']].head(15))
        
        print("\n💾 All columns saved:")
        print(df_variants.columns.tolist())
        print("="*70)
        
        # Additional statistics
        pathogenic_count = len(df_variants[df_variants['clinical_significance'].str.contains(
            'Pathogenic', case=False, na=False)])
        print(f"\n🎯 Pathogenic/Likely pathogenic variants: {pathogenic_count}")
        print(f"📊 Unique genes with variants: {df_variants['gene_name'].nunique()}")
        print(f"🧬 Unique diseases/conditions: {df_variants['disease'].nunique()}")
        
    except Exception as e:
        logger.error(f"❌ Error saving variant data: {e}")


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("VARIANT DATA EXTRACTION FROM CLINVAR")
    print("="*70)
    print(f"Downloading variants for {len(GENES_FOR_VARIANTS)} genes...")
    print(f"Target: ~500 variants per gene = ~5000-10000 total variants")
    print(f"Output: {OUTPUT_DIR}")
    print("This may take 10-15 minutes...\n")
    
    if not Entrez.api_key:
        logger.warning("⚠️ No NCBI API key found. Download will be slower.")
        logger.warning("Get API key from: https://www.ncbi.nlm.nih.gov/account/")
    
    # Download variants
    df_variants = download_all_variants()
    
    # Save results
    if not df_variants.empty:
        save_variant_data(df_variants)
        print(f"\n✅ Variant data extraction completed successfully!")
        print(f"📁 Check file: {OUTPUT_DIR / 'clinvar_pathogenic.csv'}")
    else:
        print("\n❌ Variant data extraction failed!")


if __name__ == "__main__":
    main()