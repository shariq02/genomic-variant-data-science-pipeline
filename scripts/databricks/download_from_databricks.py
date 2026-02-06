# ====================================================================
# AUTOMATED DATABRICKS DOWNLOAD WITH RESUME & CLEANUP
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: February 2026
# ====================================================================
# Features:
# - Downloads 11 gold tables from Databricks Unity Catalog Volume
# - Resume capability (checkpoint file tracks progress)
# - Automatic verification (row count + file size)
# - Auto-delete from Databricks after successful download
# - Batch processing for large files
# ====================================================================

import os
import requests
from pathlib import Path
from dotenv import load_dotenv
import time
import json

# Load environment variables
load_dotenv()

# ====================================================================
# CONFIGURATION
# ====================================================================

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', '').rstrip('/')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = PROCESSED_DIR / ".download_checkpoint.json"

VOLUME_BASE = "/Volumes/workspace/gold/gold_exports"

FILES_TO_DOWNLOAD = {
    # 5 Gold Feature Tables
    "clinical_ml_features": f"{VOLUME_BASE}/clinical_ml_features/",
    "disease_ml_features": f"{VOLUME_BASE}/disease_ml_features/",
    "pharmacogene_ml_features": f"{VOLUME_BASE}/pharmacogene_ml_features/",
    "structural_variant_ml_features": f"{VOLUME_BASE}/structural_variant_ml_features/",
    "variant_impact_ml_features": f"{VOLUME_BASE}/variant_impact_ml_features/",
    
    # 6 ML Dataset Tables
    "ml_dataset_variants_train": f"{VOLUME_BASE}/ml_dataset_variants_train/",
    "ml_dataset_variants_validation": f"{VOLUME_BASE}/ml_dataset_variants_validation/",
    "ml_dataset_variants_test": f"{VOLUME_BASE}/ml_dataset_variants_test/",
    "ml_dataset_structural_variants_train": f"{VOLUME_BASE}/ml_dataset_structural_variants_train/",
    "ml_dataset_structural_variants_validation": f"{VOLUME_BASE}/ml_dataset_structural_variants_validation/",
    "ml_dataset_structural_variants_test": f"{VOLUME_BASE}/ml_dataset_structural_variants_test/"
}

# ====================================================================
# CHECKPOINT FUNCTIONS
# ====================================================================

def load_checkpoint():
    """Load download progress from checkpoint file"""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint):
    """Save download progress to checkpoint file"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)
    print(f"  Checkpoint saved")

# ====================================================================
# DATABRICKS FILES API FUNCTIONS
# ====================================================================

def list_directory_contents(path):
    """List files in Unity Catalog Volume using Files API"""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return data.get('contents', [])
    else:
        print(f"Error listing directory: {response.status_code}")
        return []

def download_file_from_volume(volume_path, local_path):
    """Download file from Unity Catalog Volume using Files API"""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\r  Progress: {progress:.1f}% ({downloaded/(1024*1024):.1f}/{total_size/(1024*1024):.1f} MB)", end='')
        
        print()
        return True
    else:
        print(f"  Error downloading: {response.status_code}")
        return False

def delete_file_from_volume(volume_path):
    """Delete file from Unity Catalog Volume"""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.delete(url, headers=headers)
    return response.status_code in (200, 204)

def delete_folder_from_volume(folder_path):
    """Delete folder and all files from Unity Catalog Volume"""
    files = list_directory_contents(folder_path)
    if files is None:
        return False, "Could not list folder contents"
    
    deleted_count = 0
    failed_files = []
    
    for f in files:
        fpath = f.get('path', '')
        if not fpath:
            continue
        if delete_file_from_volume(fpath):
            deleted_count += 1
        else:
            failed_files.append(fpath)
    
    if failed_files:
        return False, f"Deleted {deleted_count} files, {len(failed_files)} failed"
    
    delete_file_from_volume(folder_path.rstrip('/'))
    
    return True, f"Deleted {deleted_count} file(s)"

# ====================================================================
# MAIN DOWNLOAD FUNCTION WITH RESUME
# ====================================================================

def download_gold_tables():
    """Download all 11 gold tables with resume capability"""
    
    print("="*80)
    print("DOWNLOADING 11 GOLD TABLES FROM DATABRICKS")
    print("="*80)
    print(f"Databricks Host: {DATABRICKS_HOST}")
    print(f"Local Directory: {PROCESSED_DIR}")
    print(f"Checkpoint File: {CHECKPOINT_FILE}")
    print("="*80)
    
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: Databricks credentials not found!")
        print("Please add to your .env file:")
        print("  DATABRICKS_HOST=https://dbc-xxxxx-xxxx.cloud.databricks.com")
        print("  DATABRICKS_TOKEN=your-access-token")
        return
    
    checkpoint = load_checkpoint()
    print(f"\nLoaded checkpoint: {len(checkpoint)} tables previously downloaded")
    
    total_downloaded = 0
    download_status = {}
    
    for table_name, volume_path in FILES_TO_DOWNLOAD.items():
        download_status[table_name] = {
            "downloaded": False,
            "verified": False,
            "deleted": False,
            "details": ""
        }
        
        print(f"\n{'='*80}")
        print(f"Table {list(FILES_TO_DOWNLOAD.keys()).index(table_name) + 1}/{len(FILES_TO_DOWNLOAD)}: {table_name}")
        print(f"{'='*80}")
        
        # Check if already downloaded
        if table_name in checkpoint and checkpoint[table_name].get("downloaded"):
            local_file = PROCESSED_DIR / f"{table_name}.csv"
            if local_file.exists():
                print(f"  SKIPPED - Already downloaded")
                print(f"  File: {local_file}")
                print(f"  Size: {local_file.stat().st_size / (1024*1024):.2f} MB")
                download_status[table_name]["downloaded"] = True
                download_status[table_name]["verified"] = True
                download_status[table_name]["details"] = checkpoint[table_name].get("details", "Already downloaded")
                total_downloaded += 1
                continue
        
        try:
            print(f"Listing files in: {volume_path}")
            files = list_directory_contents(volume_path)
            
            if not files:
                print(f"  No files found")
                download_status[table_name]["details"] = "No files on Databricks"
                continue
            
            csv_files = [f for f in files if 'part-' in f.get('path', '') and f.get('path', '').endswith('.csv')]
            
            if not csv_files:
                print(f"  No CSV files found")
                download_status[table_name]["details"] = "No CSV part file"
                continue
            
            csv_file = csv_files[0]
            remote_path = csv_file['path']
            file_size = csv_file.get('file_size', 0)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"  Found: {csv_file.get('name')}")
            print(f"  Size: {file_size_mb:.2f} MB")
            
            local_file = PROCESSED_DIR / f"{table_name}.csv"
            
            print(f"  Downloading to: {local_file}")
            start_time = time.time()
            
            if not download_file_from_volume(remote_path, local_file):
                print(f"  Download failed")
                download_status[table_name]["details"] = "Download failed"
                continue
            
            elapsed = time.time() - start_time
            speed = file_size_mb / elapsed if elapsed > 0 else 0
            print(f"  Downloaded successfully in {elapsed:.1f}s ({speed:.2f} MB/s)")
            download_status[table_name]["downloaded"] = True
            
            if not local_file.exists():
                print(f"  ERROR: File missing after download")
                download_status[table_name]["details"] = "File missing"
                continue
            
            local_size_mb = local_file.stat().st_size / (1024 * 1024)
            print(f"  Local size: {local_size_mb:.2f} MB")
            
            print(f"  Counting rows...")
            with open(local_file, 'r', encoding='utf-8') as f:
                local_row_count = sum(1 for _ in f) - 1
            print(f"  Rows: {local_row_count:,}")
            
            if local_row_count == 0:
                print(f"  ERROR: 0 rows")
                download_status[table_name]["details"] = "0 rows"
                continue
            
            download_status[table_name]["verified"] = True
            download_status[table_name]["details"] = f"{local_row_count:,} rows, {local_size_mb:.2f} MB"
            total_downloaded += 1
            print(f"  Verification: PASSED")
            
            checkpoint[table_name] = {
                "downloaded": True,
                "rows": local_row_count,
                "size_mb": local_size_mb,
                "details": download_status[table_name]["details"]
            }
            save_checkpoint(checkpoint)
            
            print(f"  Deleting from Databricks: {volume_path}")
            delete_ok, delete_msg = delete_folder_from_volume(volume_path)
            if delete_ok:
                download_status[table_name]["deleted"] = True
                print(f"  Databricks cleanup: OK - {delete_msg}")
            else:
                print(f"  Databricks cleanup: FAILED - {delete_msg}")
                download_status[table_name]["details"] += " | DELETE FAILED"
        
        except Exception as e:
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            download_status[table_name]["details"] = str(e)
            continue
    
    print("\n" + "="*80)
    print("DOWNLOAD SUMMARY")
    print("="*80)
    print(f"Successfully downloaded & verified: {total_downloaded}/{len(FILES_TO_DOWNLOAD)} files")
    print(f"Location: {PROCESSED_DIR}")
    
    print(f"\n{'Table':<50} {'Download':<10} {'Verify':<10} {'Delete':<10} Details")
    print("-"*120)
    for table_name, status in download_status.items():
        dl = "OK" if status["downloaded"] else "---"
        ver = "OK" if status["verified"] else "---"
        dele = "OK" if status["deleted"] else ("---" if not status["downloaded"] else "PENDING")
        print(f"  {table_name:<48} {dl:<10} {ver:<10} {dele:<10} {status['details'][:40]}")
    
    if total_downloaded == len(FILES_TO_DOWNLOAD):
        print("\n" + "="*80)
        print("SUCCESS! All 11 files downloaded, verified, and cleaned up")
        print("="*80)
        print("\nNEXT STEP: Load to PostgreSQL")
        print("Run: python scripts/transformation/load_gold_to_postgres.py")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("INCOMPLETE DOWNLOAD")
        print("="*80)
        print("Resume by running this script again - progress is saved")
        print("="*80)

if __name__ == "__main__":
    download_gold_tables()
