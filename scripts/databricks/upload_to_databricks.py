# ====================================================================
# UPLOAD TO DATABRICKS
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 31 December 2025
# ====================================================================
# FILE 5: scripts/databricks/upload_to_databricks.py
# Purpose: Upload CSV files to Databricks DBFS
# ====================================================================


"""
Upload Data to Databricks DBFS
Uploads gene and variant CSV files to Databricks File System.
"""

import os
import sys
from pathlib import Path
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Data paths
GENES_CSV = PROJECT_ROOT / "data" / "raw" / "genes" / "gene_metadata.csv"
VARIANTS_CSV = PROJECT_ROOT / "data" / "raw" / "variants" / "clinvar_pathogenic.csv"


def check_databricks_cli():
    """Check if Databricks CLI is installed."""
    try:
        import subprocess
        result = subprocess.run(['databricks', '--version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"✅ Databricks CLI found: {result.stdout.strip()}")
            return True
        else:
            logger.warning("⚠️ Databricks CLI not found")
            return False
    except FileNotFoundError:
        logger.warning("⚠️ Databricks CLI not installed")
        return False


def upload_file_manual():
    """
    Manual upload instructions (for Databricks Community Edition).
    Community Edition doesn't support API uploads, must use UI.
    """
    print("\n" + "="*70)
    print("DATABRICKS COMMUNITY EDITION - MANUAL UPLOAD INSTRUCTIONS")
    print("="*70)
    print("\nDatabricks Community Edition requires manual file upload via UI.")
    print("\n📋 STEP-BY-STEP INSTRUCTIONS:")
    print("\n1️⃣  LOGIN TO DATABRICKS:")
    print("   - Go to: https://community.cloud.databricks.com/")
    print("   - Login with your credentials")
    
    print("\n2️⃣  NAVIGATE TO DATA:")
    print("   - Click 'Data' in the left sidebar")
    print("   - Click 'Add' button (top right)")
    print("   - Select 'Upload File'")
    
    print("\n3️⃣  UPLOAD GENES FILE:")
    print(f"   - Click 'Browse' or drag and drop")
    print(f"   - Select file: {GENES_CSV}")
    print(f"   - Upload location: /FileStore/tables/genes/gene_metadata.csv")
    print(f"   - Click 'Upload'")
    print(f"   - Wait for upload to complete ✅")
    
    print("\n4️⃣  UPLOAD VARIANTS FILE:")
    print(f"   - Repeat process for variants")
    print(f"   - Select file: {VARIANTS_CSV}")
    print(f"   - Upload location: /FileStore/tables/variants/clinvar_pathogenic.csv")
    print(f"   - Click 'Upload'")
    print(f"   - Wait for upload to complete ✅")
    
    print("\n5️⃣  VERIFY UPLOADS:")
    print("   - In Databricks, click 'Data'")
    print("   - Navigate to: DBFS → FileStore → tables")
    print("   - You should see folders: genes/ and variants/")
    print("   - Click to verify files are there")
    
    print("\n6️⃣  ALTERNATIVE - UPLOAD IN NOTEBOOK:")
    print("   Create a notebook and run:")
    print("   ```python")
    print("   # Upload from local (only works in Databricks)")
    print("   dbutils.fs.put('/FileStore/tables/genes/gene_metadata.csv', open('gene_metadata.csv').read())")
    print("   ```")
    
    print("\n" + "="*70)
    print("📁 FILES TO UPLOAD:")
    print("="*70)
    print(f"1. Genes:    {GENES_CSV}")
    print(f"   → Upload to: /FileStore/tables/genes/gene_metadata.csv")
    print(f"\n2. Variants: {VARIANTS_CSV}")
    print(f"   → Upload to: /FileStore/tables/variants/clinvar_pathogenic.csv")
    print("="*70)
    
    # Check if files exist
    if GENES_CSV.exists():
        file_size = GENES_CSV.stat().st_size / (1024 * 1024)
        print(f"\n✅ Genes file exists: {file_size:.2f} MB")
    else:
        print(f"\n❌ Genes file not found: {GENES_CSV}")
        
    if VARIANTS_CSV.exists():
        file_size = VARIANTS_CSV.stat().st_size / (1024 * 1024)
        print(f"✅ Variants file exists: {file_size:.2f} MB")
    else:
        print(f"❌ Variants file not found: {VARIANTS_CSV}")
    
    print("\n" + "="*70)
    print("⏱️  ESTIMATED UPLOAD TIME:")
    print("   - Genes: 1-2 minutes")
    print("   - Variants: 2-3 minutes")
    print("   - Total: ~5 minutes")
    print("="*70)


def create_upload_notebook_code():
    """
    Create Python code for Databricks notebook to verify uploads.
    """
    notebook_code = """
# ====================================================================
# DATABRICKS NOTEBOOK: Verify Uploaded Data
# Run this in Databricks after uploading files
# ====================================================================

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

print("="*70)
print("VERIFY UPLOADED DATA IN DATABRICKS")
print("="*70)

# Initialize Spark
spark = SparkSession.builder.appName("VerifyUploads").getOrCreate()

# ====================================================================
# VERIFY GENES FILE
# ====================================================================

try:
    print("\\n1. Checking genes file...")
    
    # Read genes CSV
    df_genes = spark.read.csv(
        "/FileStore/tables/genes/gene_metadata.csv",
        header=True,
        inferSchema=True
    )
    
    # Display info
    print(f"   SUCCESS: Genes file found!")
    print(f"   Total genes: {df_genes.count()}")
    print(f"   Columns: {len(df_genes.columns)}")
    
    # Show schema
    print("\\n   Schema:")
    df_genes.printSchema()
    
    # Show sample data
    print("\\n   Sample data:")
    df_genes.show(5, truncate=False)
    
    # Basic statistics
    print("\\n   Genes by chromosome:")
    df_genes.groupBy("chromosome").count().orderBy("chromosome").show()
    
except Exception as e:
    print(f"   ERROR reading genes file: {e}")


# ====================================================================
# VERIFY VARIANTS FILE
# ====================================================================

try:
    print("\\n2. Checking variants file...")
    
    # Read variants CSV
    df_variants = spark.read.csv(
        "/FileStore/tables/variants/clinvar_pathogenic.csv",
        header=True,
        inferSchema=True
    )
    
    # Display info
    print(f"   SUCCESS: Variants file found!")
    print(f"   Total variants: {df_variants.count()}")
    print(f"   Columns: {len(df_variants.columns)}")
    
    # Show schema
    print("\\n   Schema:")
    df_variants.printSchema()
    
    # Show sample data
    print("\\n   Sample data:")
    df_variants.show(5, truncate=False)
    
    # Basic statistics
    print("\\n   Variants by gene (top 10):")
    df_variants.groupBy("gene_name").count() \\
        .orderBy(col("count").desc()) \\
        .show(10)
    
    print("\\n   Variants by clinical significance:")
    df_variants.groupBy("clinical_significance").count().show()
    
except Exception as e:
    print(f"   ERROR reading variants file: {e}")


# ====================================================================
# SUMMARY
# ====================================================================

print("\\n" + "="*70)
print("VERIFICATION COMPLETE")
print("="*70)
print("SUCCESS: If you see data above, uploads were successful!")
print("READY: You're ready for Week 3: PySpark data processing")
print("="*70)
"""
    
    # Save to file with UTF-8 encoding (FIX HERE)
    notebook_file = PROJECT_ROOT / "databricks_notebooks" / "00_verify_uploads.py"
    notebook_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(notebook_file, 'w', encoding='utf-8') as f:  # ← Added encoding='utf-8'
        f.write(notebook_code)
    
    logger.info(f"Created verification notebook: {notebook_file}")
    
    return notebook_code

def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("UPLOAD DATA TO DATABRICKS")
    print("="*70)
    
    # Check if files exist
    if not GENES_CSV.exists():
        print(f"❌ Genes file not found: {GENES_CSV}")
        print("   Run Week 1 scripts first to download data!")
        return
    
    if not VARIANTS_CSV.exists():
        print(f"❌ Variants file not found: {VARIANTS_CSV}")
        print("   Run Week 1 scripts first to download data!")
        return
    
    # Check Databricks CLI (optional)
    has_cli = check_databricks_cli()
    
    if not has_cli:
        print("\n📝 Note: Databricks CLI not installed (optional for Community Edition)")
    
    # Show manual upload instructions
    upload_file_manual()
    
    # Create verification notebook
    print("\n" + "="*70)
    print("CREATING VERIFICATION NOTEBOOK")
    print("="*70)
    create_upload_notebook_code()
    print("\n✅ Verification notebook created!")
    print(f"📁 Location: databricks_notebooks/00_verify_uploads.py")
    print("\n📋 After uploading files to Databricks:")
    print("   1. Create new notebook in Databricks")
    print("   2. Copy code from: databricks_notebooks/00_verify_uploads.py")
    print("   3. Run the notebook to verify uploads")
    print("="*70)


if __name__ == "__main__":
    main()
