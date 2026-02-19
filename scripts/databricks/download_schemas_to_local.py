"""
Download Schemas and Sample Data from Databricks Volumes
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
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        return True
    return False

def main():
    print("="*80)
    print("DOWNLOADING SCHEMAS AND SAMPLE DATA")
    print("="*80)
    
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: Databricks credentials not found in .env")
        return
    
    schemas = ["default", "silver", "reference", "gold"]
    
    for schema in schemas:
        print(f"\n{schema.upper()} SCHEMA")
        print("="*80)
        
        sample_dir = SCHEMAS_DIR / schema
        sample_dir.mkdir(exist_ok=True)
        
        volume_base = f"/Volumes/workspace/{schema}/{schema}_exports"
        
        folders = list_directory(volume_base)
        
        if not folders:
            print(f"  No exports found")
            continue
        
        for folder_info in folders:
            folder_path = folder_info.get('path')
            folder_name = folder_info.get('name')
            
            files = list_directory(folder_path)
            
            if not files:
                continue
            
            csv_files = [f for f in files if f.get('path', '').endswith('.csv')]
            
            if not csv_files:
                continue
            
            csv_file = csv_files[0]
            remote_path = csv_file['path']
            file_name = csv_file.get('name')
            
            if folder_name == f"{schema}_schema":
                local_file = SCHEMAS_DIR / f"{schema}_schema.csv"
                print(f"  Schema: {schema}_schema.csv")
            else:
                local_file = sample_dir / f"{folder_name}.csv"
                print(f"  Sample: {folder_name}.csv")
            
            if download_file(remote_path, local_file):
                print(f"Downloaded successfully: {local_file}")
            else:
                print(f"FAILED")
    
    print("\n" + "="*80)
    print("DOWNLOAD COMPLETE")
    print("="*80)
    print(f"\nLocation: {DOCS_DIR / 'schemas'}")
    print("\nStructure:")
    print("  schemas/default_schema.csv  - Default schema")
    print("  schemas/default/            - Default sample tables")
    print("  schemas/silver_schema.csv   - Silver schema")
    print("  schemas/silver/             - Silver sample tables")
    print("="*80)

if __name__ == "__main__":
    main()