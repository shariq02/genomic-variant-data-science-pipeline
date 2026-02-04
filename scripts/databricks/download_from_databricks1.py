# ====================================================================
# AUTOMATED DATABRICKS VOLUME DOWNLOAD - UNITY CATALOG VERSION
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: January 2026
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
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', '').rstrip('/')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Databricks volume paths
VOLUME_BASE = "/Volumes/workspace/gold/gold_exports"

# All 5 gold layer ML feature tables
FILES_TO_DOWNLOAD = {
    "clinical_ml_features": f"{VOLUME_BASE}/clinical_ml_features/",
    "disease_ml_features": f"{VOLUME_BASE}/disease_ml_features/",
    "pharmacogene_ml_features": f"{VOLUME_BASE}/pharmacogene_ml_features/",
    "structural_variant_ml_features": f"{VOLUME_BASE}/structural_variant_ml_features/",
    "variant_impact_ml_features": f"{VOLUME_BASE}/variant_impact_ml_features/"
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

def delete_file_from_volume(volume_path):
    """Delete a single file from Unity Catalog Volume using Files API"""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_path}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    response = requests.delete(url, headers=headers)
    return response.status_code in (200, 204)


def delete_folder_from_volume(folder_path):
    """Delete all files inside a folder, then the folder itself.
    Returns (success: bool, details: str)"""
    # List everything in the folder
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
        return False, f"Deleted {deleted_count} files, but {len(failed_files)} failed: {failed_files}"

    # Folder itself disappears once empty in Unity Catalog Volumes,
    # but attempt explicit delete in case the API requires it
    delete_file_from_volume(folder_path.rstrip('/'))

    return True, f"Deleted {deleted_count} file(s) from {folder_path}"


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
    download_status = {}  # table -> {downloaded, verified, deleted, details}

    for table_name, volume_path in FILES_TO_DOWNLOAD.items():
        download_status[table_name] = {"downloaded": False, "verified": False, "deleted": False, "details": ""}

        print(f"\n{'='*70}")
        print(f"Downloading: {table_name}")
        print(f"{'='*70}")

        try:
            # List files in volume directory
            print(f"Listing files in: {volume_path}")
            files = list_directory_contents(volume_path)

            if not files:
                print(f"  No files found in {volume_path}")
                print(f"  Make sure you ran 18_export_to_csv.py in Databricks first!")
                download_status[table_name]["details"] = "No files on Databricks"
                continue

            # Find CSV part file (Spark creates part-00000-xxx.csv)
            csv_files = [f for f in files if 'part-' in f.get('path', '') and f.get('path', '').endswith('.csv')]

            if not csv_files:
                print(f"  No CSV files found")
                print(f"  Available files: {[f.get('name') for f in files]}")
                download_status[table_name]["details"] = "No CSV part file found"
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

            if not download_file_from_volume(remote_path, local_file):
                print(f"  Download failed")
                download_status[table_name]["details"] = "Download failed"
                continue

            elapsed = time.time() - start_time
            speed = file_size_mb / elapsed if elapsed > 0 else 0
            print(f"  Downloaded successfully!")
            print(f"  Time: {elapsed:.1f}s ({speed:.2f} MB/s)")
            download_status[table_name]["downloaded"] = True

            # --- Verify: size + row count ---
            if not local_file.exists():
                print(f"  ERROR: Local file missing after download")
                download_status[table_name]["details"] = "File missing after download"
                continue

            local_size_mb = local_file.stat().st_size / (1024 * 1024)
            print(f"  Local size: {local_size_mb:.2f} MB")

            print(f"  Counting rows (this may take a moment for large files)...")
            with open(local_file, 'r', encoding='utf-8') as f:
                local_row_count = sum(1 for _ in f) - 1  # -1 for header
            print(f"  Rows: {local_row_count:,}")

            if local_row_count == 0:
                print(f"  ERROR: Downloaded file has 0 data rows")
                download_status[table_name]["details"] = "0 rows after download"
                continue

            download_status[table_name]["verified"] = True
            download_status[table_name]["details"] = f"{local_row_count:,} rows, {local_size_mb:.2f} MB"
            total_downloaded += 1
            print(f"  Verification: PASSED")

            # --- Delete from Databricks after successful verification ---
            print(f"  Deleting from Databricks: {volume_path}")
            delete_ok, delete_msg = delete_folder_from_volume(volume_path)
            if delete_ok:
                download_status[table_name]["deleted"] = True
                print(f"  Databricks cleanup: OK — {delete_msg}")
            else:
                print(f"  Databricks cleanup: FAILED — {delete_msg}")
                print(f"  (Data is safe locally; you can delete manually later)")
                download_status[table_name]["details"] += " | DELETE FAILED"

        except Exception as e:
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            download_status[table_name]["details"] = str(e)
            continue

    # ================================================================
    # SUMMARY
    # ================================================================
    print("\n" + "="*70)
    print("DOWNLOAD SUMMARY")
    print("="*70)
    print(f"Successfully downloaded & verified: {total_downloaded}/{len(FILES_TO_DOWNLOAD)} files")
    print(f"Location: {PROCESSED_DIR}")

    print(f"\n{'Table':<42} {'Download':<10} {'Verify':<10} {'Delete':<10} Details")
    print("-"*100)
    for table_name, status in download_status.items():
        dl  = "OK"    if status["downloaded"] else "---"
        ver = "OK"    if status["verified"]   else "---"
        dele = "OK"   if status["deleted"]    else ("---" if not status["downloaded"] else "PENDING")
        print(f"  {table_name:<40} {dl:<10} {ver:<10} {dele:<10} {status['details']}")

    if total_downloaded == len(FILES_TO_DOWNLOAD):
        print("\n" + "="*70)
        print("SUCCESS! All files downloaded, verified, and cleaned up")
        print("="*70)
        print("\nNEXT STEP: Load to PostgreSQL")
        print("Run: python scripts/transformation/load_gold_to_postgres.py")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("INCOMPLETE DOWNLOAD")
        print("="*70)
        print("Troubleshooting:")
        print("1. Make sure you ran 18_export_to_csv.py in Databricks first")
        print("2. Verify token has permissions to access Unity Catalog Volumes")
        print("3. Check that files exist in: Catalog > workspace > gold > gold_exports")
        print("="*70)

# ====================================================================
# MAIN
# ====================================================================

if __name__ == "__main__":
    download_gold_tables()
