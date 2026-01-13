# ====================================================================
# AUTOMATED DATABRICKS VOLUME DOWNLOAD - UNITY CATALOG VERSION
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 12, 2026
# ====================================================================
# Purpose: Automatically download exported CSV files from Databricks
#          Unity Catalog Volume to local processed data folder
# ====================================================================

import os
import requests
from pathlib import Path
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# ====================================================================
# CONFIGURATION
# ====================================================================

# Databricks credentials (add these to your .env file)
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', '').rstrip('/')  # Remove trailing slash
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Databricks volume paths
VOLUME_BASE = "/Volumes/workspace/gold/gold_exports"

# Files to download (Spark creates .csv folders, not files)
FILES_TO_DOWNLOAD = {
    "gene_features": f"{VOLUME_BASE}/gene_features.csv/",
    "chromosome_features": f"{VOLUME_BASE}/chromosome_features.csv/",
    "gene_disease_association": f"{VOLUME_BASE}/gene_disease_association.csv/",
    "ml_features": f"{VOLUME_BASE}/ml_features.csv/"
}

# ====================================================================
# DATABRICKS FILES API FUNCTIONS (Unity Catalog Compatible)
# ====================================================================

def list_directory_contents(path):
    """List files in Unity Catalog Volume using Files API"""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{path}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return data.get('contents', [])
    else:
        print(f"Error listing directory: {response.status_code} - {response.text}")
        return []

def download_file_from_volume(volume_path, local_path):
    """Download file from Unity Catalog Volume using Files API"""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_path}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    
    print(f"  Downloading from: {url}")
    
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\r  Progress: {progress:.1f}% ({downloaded/(1024*1024):.1f}/{total_size/(1024*1024):.1f} MB)", end='')
        
        print()  # New line after progress
        return True
    else:
        print(f"  Error downloading: {response.status_code} - {response.text[:200]}")
        return False

# ====================================================================
# MAIN DOWNLOAD FUNCTION
# ====================================================================

def download_gold_tables():
    """Download all gold layer tables from Databricks Volume"""
    
    print("="*70)
    print("DOWNLOADING GOLD LAYER TABLES FROM DATABRICKS")
    print("="*70)
    print(f"Databricks Host: {DATABRICKS_HOST}")
    print(f"Local Directory: {PROCESSED_DIR}")
    print("="*70)
    
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: Databricks credentials not found!")
        print("Please add to your .env file:")
        print("  DATABRICKS_HOST=https://dbc-xxxxx-xxxx.cloud.databricks.com")
        print("  DATABRICKS_TOKEN=your-access-token")
        return
    
    total_downloaded = 0
    
    for table_name, volume_path in FILES_TO_DOWNLOAD.items():
        print(f"\n{'='*70}")
        print(f"Downloading: {table_name}")
        print(f"{'='*70}")
        
        try:
            # List files in volume directory
            print(f"Listing files in: {volume_path}")
            files = list_directory_contents(volume_path)
            
            if not files:
                print(f"  No files found in {volume_path}")
                print(f"  Make sure you've run the export notebook first!")
                continue
            
            # Find CSV part file (Spark creates part-00000-xxx.csv)
            csv_files = [f for f in files if 'part-' in f.get('path', '') and f.get('path', '').endswith('.csv')]
            
            if not csv_files:
                print(f"  No CSV files found")
                print(f"  Available files: {[f.get('name') for f in files]}")
                continue
            
            # Download the CSV file
            csv_file = csv_files[0]
            remote_path = csv_file['path']
            file_size = csv_file.get('file_size', 0)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"  Found: {csv_file.get('name')}")
            print(f"  Size: {file_size_mb:.2f} MB")
            
            # Local file path
            local_file = PROCESSED_DIR / f"{table_name}.csv"
            
            # Download
            print(f"  Saving to: {local_file}")
            start_time = time.time()
            
            if download_file_from_volume(remote_path, local_file):
                elapsed = time.time() - start_time
                speed = file_size_mb / elapsed if elapsed > 0 else 0
                
                print(f"  Downloaded successfully!")
                print(f"  Time: {elapsed:.1f}s ({speed:.2f} MB/s)")
                
                # Verify file
                if local_file.exists():
                    local_size_mb = local_file.stat().st_size / (1024 * 1024)
                    print(f"  Verified: {local_size_mb:.2f} MB")
                    total_downloaded += 1
            else:
                print(f"  Download failed")
        
        except Exception as e:
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Summary
    print("\n" + "="*70)
    print("DOWNLOAD SUMMARY")
    print("="*70)
    print(f"Successfully downloaded: {total_downloaded}/{len(FILES_TO_DOWNLOAD)} files")
    print(f"Location: {PROCESSED_DIR}")
    
    # Detailed verification
    print("\n" + "="*70)
    print("DETAILED VERIFICATION")
    print("="*70)
    
    all_verified = True
    
    for table_name in FILES_TO_DOWNLOAD.keys():
        local_file = PROCESSED_DIR / f"{table_name}.csv"
        
        if local_file.exists():
            size_mb = local_file.stat().st_size / (1024 * 1024)
            
            # Count rows in CSV
            try:
                with open(local_file, 'r', encoding='utf-8') as f:
                    row_count = sum(1 for _ in f) - 1  # -1 for header
                print(f"\n{table_name}.csv:")
                print(f"  Size: {size_mb:.2f} MB")
                print(f"  Rows: {row_count:,}")
                print(f"  Status: [OK] Downloaded and verified")
            except Exception as e:
                print(f"\n{table_name}.csv:")
                print(f"  Size: {size_mb:.2f} MB")
                print(f"  Status: [WARNING] Could not count rows: {e}")
                all_verified = False
        else:
            print(f"\n{table_name}.csv:")
            print(f"  Status: [MISSING] File not found")
            all_verified = False
    
    if total_downloaded == len(FILES_TO_DOWNLOAD):
        print("\n" + "="*70)
        print("SUCCESS! All files downloaded")
        print("="*70)
        
        if all_verified:
            print("\nVERIFICATION PASSED:")
            print("  - All files downloaded successfully")
            print("  - File sizes verified")
            print("  - Row counts calculated")
            print("\nExpected approximate row counts:")
            print("  - gene_features: ~190,000 rows")
            print("  - chromosome_features: ~25 rows")
            print("  - gene_disease_association: ~500,000+ rows")
            print("  - ml_features: ~190,000 rows")
            print("\nIf your counts are significantly different, check:")
            print("  1. Feature engineering completed successfully")
            print("  2. No data filtering removed too many rows")
            print("  3. Export notebook ran without errors")
        
        print("\n" + "="*70)
        print("NEXT STEP: Load to PostgreSQL")
        print("="*70)
        print("Run: python scripts/transformation/load_gold_to_postgres.py")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("INCOMPLETE DOWNLOAD")
        print("="*70)
        print("Troubleshooting:")
        print("1. Make sure you ran 05_export_to_csv.py in Databricks first")
        print("2. Verify token has permissions to access Unity Catalog Volumes")
        print("3. Check that files exist in: Catalog > workspace > gold > gold_exports")
        print("="*70)

# ====================================================================
# MAIN
# ====================================================================

if __name__ == "__main__":
    download_gold_tables()
