"""
SMART DOWNLOAD - CHANGED TABLES ONLY
Downloads only tables that have been updated in Databricks
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import os
import requests
from pathlib import Path
from dotenv import load_dotenv
import json

load_dotenv()

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', '').rstrip('/')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

PROJECT_ROOT = Path(__file__).parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_CHECKPOINT = PROCESSED_DIR / ".download_checkpoint.json"
VOLUME_BASE = "/Volumes/workspace/gold/gold_exports"
REMOTE_METADATA = f"{VOLUME_BASE}/.export_metadata.json"

def get_file_content(file_path):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    return None

def list_directory(path):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('contents', [])
    return []

def download_file(remote_path, local_path):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{remote_path}"
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
                        print(f"\r  Progress: {progress:.1f}%", end='')
        print()
        return True
    return False

def load_local_checkpoint():
    if LOCAL_CHECKPOINT.exists():
        with open(LOCAL_CHECKPOINT, 'r') as f:
            return json.load(f)
    return {}

def save_local_checkpoint(data):
    with open(LOCAL_CHECKPOINT, 'w') as f:
        json.dump(data, f, indent=2)

def main():
    print("="*80)
    print("SMART DOWNLOAD - CHANGED TABLES ONLY")
    print("="*80)
    
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: Databricks credentials not found in .env")
        return
    
    print("\nLoading remote export metadata...")
    remote_metadata_content = get_file_content(REMOTE_METADATA)
    
    if not remote_metadata_content:
        print("ERROR: Could not load remote metadata")
        print("Make sure 18_export_changed_tables_only.py ran successfully in Databricks")
        return
    
    remote_metadata = json.loads(remote_metadata_content)
    print(f"Remote metadata: {len(remote_metadata)} tables")
    
    print("\nLoading local checkpoint...")
    local_checkpoint = load_local_checkpoint()
    print(f"Local checkpoint: {len(local_checkpoint)} tables")
    
    print("\nIdentifying tables to download...")
    print("="*80)
    
    tables_to_download = []
    
    for table_name, remote_info in remote_metadata.items():
        if table_name not in local_checkpoint:
            print(f"{table_name}: DOWNLOAD (not downloaded yet)")
            tables_to_download.append(table_name)
            continue
        
        local_info = local_checkpoint[table_name]
        
        if remote_info.get("rows") != local_info.get("rows"):
            print(f"{table_name}: DOWNLOAD (rows changed: {local_info.get('rows'):,} -> {remote_info.get('rows'):,})")
            tables_to_download.append(table_name)
            continue
        
        if remote_info.get("columns") != local_info.get("columns"):
            print(f"{table_name}: DOWNLOAD (columns changed)")
            tables_to_download.append(table_name)
            continue
        
        print(f"{table_name}: SKIP (up to date)")
    
    if len(tables_to_download) == 0:
        print("\nNo tables need download - all up to date")
        return
    
    print(f"\nTables to download: {len(tables_to_download)}")
    print("="*80)
    
    download_results = {}
    
    for table_name in tables_to_download:
        print(f"\n{table_name}:")
        
        folder_path = f"{VOLUME_BASE}/{table_name}/"
        files = list_directory(folder_path)
        
        if not files:
            print("  ERROR: No files in folder")
            download_results[table_name] = {"success": False}
            continue
        
        csv_files = [f for f in files if f.get('path', '').endswith('.csv')]
        
        if not csv_files:
            print("  ERROR: No CSV file found")
            download_results[table_name] = {"success": False}
            continue
        
        csv_file = csv_files[0]
        remote_path = csv_file['path']
        file_size_mb = csv_file.get('file_size', 0) / (1024 * 1024)
        
        print(f"  File: {csv_file.get('name')}")
        print(f"  Size: {file_size_mb:.2f} MB")
        
        local_file = PROCESSED_DIR / f"{table_name}.csv"
        print(f"  Downloading to: {local_file}")
        
        if download_file(remote_path, local_file):
            print("  Download: OK")
            
            with open(local_file, 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1
            
            print(f"  Rows: {row_count:,}")
            
            download_results[table_name] = {
                "success": True,
                "rows": row_count,
                "size_mb": file_size_mb
            }
            
            local_checkpoint[table_name] = remote_metadata[table_name].copy()
            save_local_checkpoint(local_checkpoint)
            
        else:
            print("  Download: FAILED")
            download_results[table_name] = {"success": False}
    
    print("\n" + "="*80)
    print("DOWNLOAD SUMMARY")
    print("="*80)
    
    successful = [t for t in tables_to_download if download_results.get(t, {}).get("success")]
    failed = [t for t in tables_to_download if not download_results.get(t, {}).get("success")]
    
    print(f"\nSuccessful: {len(successful)}/{len(tables_to_download)}")
    for table in successful:
        print(f"  - {table}")
    
    if failed:
        print(f"\nFailed: {len(failed)}")
        for table in failed:
            print(f"  - {table}")
    
    print("\n" + "="*80)
    print("NEXT STEP: Run load_changed_tables_to_postgres.py")
    print("="*80)

if __name__ == "__main__":
    main()
