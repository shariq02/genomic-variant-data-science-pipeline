"""
Download Schemas and Sample Data from Databricks Volumes
WITH AUTOMATIC VOLUME CLEANUP
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import os
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', '').rstrip('/')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DOCS_DIR = PROJECT_ROOT / "documents"
SCHEMAS_DIR = DOCS_DIR / "schemas"
SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)

def list_directory(path):
    """List contents of a Databricks directory."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('contents', [])
    return []

def download_file(remote_path, local_path):
    """Download a file from Databricks."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{remote_path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        return True
    return False

def delete_volume_contents(volume_path):
    """Delete all contents of a volume (files and folders)."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/delete"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    
    try:
        # Get all items in volume
        items = list_directory(volume_path)
        deleted = 0
        
        for item in items:
            item_path = item.get('path')
            data = {"path": item_path, "recursive": True}
            response = requests.delete(url, headers=headers, json=data)
            
            if response.status_code == 200:
                deleted += 1
        
        return deleted, len(items)
    except Exception as e:
        return 0, 0

def delete_volume(catalog, schema, volume):
    """Delete a volume in Databricks using Workspace API."""
    # Try Unity Catalog API first
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/volumes/{catalog}/{schema}/{volume}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.delete(url, headers=headers)
    
    if response.status_code == 200:
        return True, "deleted"
    
    # If Unity Catalog fails, try deleting contents instead
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    deleted, total = delete_volume_contents(volume_path)
    
    if deleted > 0:
        return True, f"cleaned ({deleted}/{total} items)"
    
    return False, f"failed (status: {response.status_code})"

def main():
    print("="*80)
    print("DOWNLOADING SCHEMAS AND SAMPLE DATA")
    print("="*80)
    
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: Databricks credentials not found in .env")
        return
    
    schemas = []
    catalog = "workspace"
    
    print("\nAuto-discovering schemas...")
    catalog_url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/schemas"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    params = {"catalog_name": catalog}
    
    response = requests.get(catalog_url, headers=headers, params=params)
    if response.status_code == 200:
        schema_list = response.json().get('schemas', [])
        schemas = [s['name'] for s in schema_list if s['name'] not in ['information_schema']]
        print(f"Found schemas: {', '.join(schemas)}")
    else:
        schemas = ["default", "silver", "reference", "gold"]
        print(f"Using default schemas: {', '.join(schemas)}")
    
    download_summary = {}
    
    for schema in schemas:
        print(f"\n{schema.upper()} SCHEMA")
        print("="*80)
        
        sample_dir = SCHEMAS_DIR / schema
        sample_dir.mkdir(exist_ok=True)
        
        volume_name = f"{schema}_exports"
        volume_base = f"/Volumes/{catalog}/{schema}/{volume_name}"
        
        folders = list_directory(volume_base)
        
        if not folders:
            print(f"  No exports found")
            download_summary[schema] = {"downloaded": 0, "volume_existed": False}
            continue
        
        download_summary[schema] = {"downloaded": 0, "volume_existed": True}
        
        for folder_info in folders:
            folder_path = folder_info.get('path')
            folder_name = folder_info.get('name')
            
            # Only process schema file and sample files
            if not (folder_name == f"{schema}_schema" or folder_name.endswith("_sample")):
                continue
            
            files = list_directory(folder_path)
            
            if not files:
                continue
            
            csv_files = [f for f in files if f.get('path', '').endswith('.csv')]
            
            if not csv_files:
                continue
            
            csv_file = csv_files[0]
            remote_path = csv_file['path']
            
            if folder_name == f"{schema}_schema":
                local_file = SCHEMAS_DIR / f"{schema}_schema.csv"
                print(f"  Schema: {schema}_schema.csv", end=' ')
            else:
                local_file = sample_dir / f"{folder_name}.csv"
                print(f"  Sample: {folder_name}.csv", end=' ')
            
            if download_file(remote_path, local_file):
                print("OK")
                download_summary[schema]["downloaded"] += 1
            else:
                print("FAILED")
    
    print("\n" + "="*80)
    print("DOWNLOAD COMPLETE")
    print("="*80)
    print(f"\nLocation: {SCHEMAS_DIR}")
    
    total_downloaded = sum(s["downloaded"] for s in download_summary.values())
    print(f"Total files downloaded: {total_downloaded}")
    
    if total_downloaded > 0:
        print("\n" + "="*80)
        print("CLEANING UP SCHEMA EXPORT VOLUMES")
        print("="*80)
        
        cleanup_count = 0
        for schema in schemas:
            if download_summary.get(schema, {}).get("volume_existed"):
                volume_name = f"{schema}_exports"
                
                print(f"\nCleaning volume: {catalog}.{schema}.{volume_name}", end=' ')
                
                success, message = delete_volume(catalog, schema, volume_name)
                
                if success:
                    print(f"OK ({message})")
                    cleanup_count += 1
                else:
                    print(f"FAILED ({message})")
        
        print(f"\nVolumes cleaned up: {cleanup_count}/{len([s for s in schemas if download_summary.get(s, {}).get('volume_existed')])}")
    
    print("\n" + "="*80)
    print("PROCESS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
