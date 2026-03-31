"""
SMART DOWNLOAD - AUTO-DISCOVER CHANGED TABLES
Downloads only tables that have been updated in Databricks
WITH AUTOMATIC VOLUME CLEANUP
DNA Gene Mapping Project
Author: Sharique Mohammad
Date: February 2026
"""

import os
import requests
import time
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

CATALOG = "workspace"
SCHEMA = "gold"
VOLUME_NAME = "gold_exports"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
REMOTE_METADATA = f"{VOLUME_BASE}/.export_metadata.json"

# --- Retry / timeout settings ---
MAX_RETRIES     = 5               # attempts per file before giving up
RETRY_WAIT      = 5               # seconds before first retry (doubles each time)
CHUNK_SIZE      = 4 * 1024 * 1024 # 4 MB per write
CONNECT_TIMEOUT = 30              # seconds to establish connection
READ_TIMEOUT    = 120             # seconds of silence before aborting a stalled chunk


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def get_file_content(file_path):
    """Fetch a small text file from Databricks volume."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.get(url, headers=headers,
                            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if response.status_code == 200:
        return response.text
    return None


def list_directory(path):
    """List contents of a Databricks volume directory."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.get(url, headers=headers,
                            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if response.status_code == 200:
        return response.json().get('contents', [])
    return []


# ---------------------------------------------------------------------------
# Core download: resume + retry + full exception trap
# ---------------------------------------------------------------------------

def download_file(remote_path, local_path):
    """
    Download a file from Databricks with:
      - HTTP Range resume : continues a partial file from its current size
      - Retry with back-off: up to MAX_RETRIES on ANY exception
      - Read timeout      : drops stalled connections after READ_TIMEOUT seconds
      - Full exception trap: ChunkedEncodingError / ProtocolError / etc. caught
    """
    local_path = Path(local_path)
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{remote_path}"

    for attempt in range(1, MAX_RETRIES + 1):

        # Determine how many bytes we already have
        resume_from = local_path.stat().st_size if local_path.exists() else 0

        headers   = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        file_mode = 'wb'

        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"
            file_mode = 'ab'
            print(f"  Resuming from {resume_from / (1024*1024):.1f} MB "
                  f"(attempt {attempt}/{MAX_RETRIES})")
        elif attempt > 1:
            print(f"  Retrying from scratch (attempt {attempt}/{MAX_RETRIES})")

        try:
            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )

            # 206 = server honoured Range header (partial content)
            # 200 = server sent full file (ignores Range)
            if response.status_code == 200 and resume_from > 0:
                # Server does not support resume — start over
                print("  Server does not support Range — restarting from 0")
                local_path.unlink(missing_ok=True)
                resume_from = 0
                file_mode   = 'wb'

            elif response.status_code not in (200, 206):
                print(f"  HTTP {response.status_code} — cannot download")
                return False

            # content-length is only the remaining bytes when resuming
            remaining  = int(response.headers.get('content-length', 0))
            total_size = resume_from + remaining
            downloaded = resume_from

            with open(local_path, file_mode) as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            pct      = downloaded / total_size * 100
                            done_mb  = downloaded  / (1024 * 1024)
                            total_mb = total_size  / (1024 * 1024)
                            print(
                                f"\r  Progress: {pct:.1f}%  "
                                f"({done_mb:.0f} MB / {total_mb:.0f} MB)",
                                end='', flush=True
                            )
            print()  # newline after progress bar

            # Sanity check
            if total_size > 0 and downloaded < total_size:
                raise Exception(
                    f"Incomplete: got {downloaded:,} bytes, "
                    f"expected {total_size:,}"
                )

            return True  # success

        except Exception as exc:
            # Catches ChunkedEncodingError, ProtocolError, IncompleteRead,
            # ReadTimeout, ConnectionError, and any other network failure
            print(f"\n  Attempt {attempt}/{MAX_RETRIES} failed: "
                  f"{type(exc).__name__}: {str(exc)[:150]}")

            if attempt < MAX_RETRIES:
                wait = RETRY_WAIT * (2 ** (attempt - 1))  # 5s 10s 20s 40s
                print(f"  Waiting {wait}s before next attempt...")
                time.sleep(wait)
            else:
                print(f"  All {MAX_RETRIES} attempts exhausted — skipping file")
                return False

    return False  # unreachable, satisfies linter


# ---------------------------------------------------------------------------
# Volume cleanup helpers (unchanged from original)
# ---------------------------------------------------------------------------

def delete_volume_contents(volume_path):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/delete"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    try:
        items = list_directory(volume_path)
        deleted = 0
        for item in items:
            data = {"path": item.get('path'), "recursive": True}
            r = requests.delete(url, headers=headers, json=data,
                                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if r.status_code == 200:
                deleted += 1
        return deleted, len(items)
    except Exception:
        return 0, 0


def delete_volume(catalog, schema, volume):
    url = (f"{DATABRICKS_HOST}/api/2.1/unity-catalog/volumes/"
           f"{catalog}/{schema}/{volume}")
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    r = requests.delete(url, headers=headers,
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if r.status_code == 200:
        return True, "deleted"
    deleted, total = delete_volume_contents(
        f"/Volumes/{catalog}/{schema}/{volume}")
    if deleted > 0:
        return True, f"cleaned ({deleted}/{total} items)"
    return False, f"failed (HTTP {r.status_code})"


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_local_checkpoint():
    if LOCAL_CHECKPOINT.exists():
        with open(LOCAL_CHECKPOINT, 'r') as f:
            return json.load(f)
    return {}


def save_local_checkpoint(data):
    with open(LOCAL_CHECKPOINT, 'w') as f:
        json.dump(data, f, indent=2)


def count_rows(local_file):
    """Count CSV data rows (excludes header) without loading into memory."""
    count = 0
    with open(local_file, 'r', encoding='utf-8') as f:
        for _ in f:
            count += 1
    return max(count - 1, 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 80)
    print("SMART DOWNLOAD - AUTO-DISCOVER CHANGED TABLES")
    print("=" * 80)
    print(f"Retry: {MAX_RETRIES} attempts | back-off: {RETRY_WAIT}s base | "
          f"chunks: {CHUNK_SIZE // (1024*1024)}MB | "
          f"timeouts: connect={CONNECT_TIMEOUT}s read={READ_TIMEOUT}s")

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: Databricks credentials not found in .env")
        return

    print(f"\nTarget: {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

    # Load remote metadata
    print("\nLoading remote export metadata...")
    raw = get_file_content(REMOTE_METADATA)
    if not raw:
        print("ERROR: Could not load remote metadata")
        print("Make sure 18_export_gold_tables.py ran successfully in Databricks")
        return
    remote_metadata = json.loads(raw)
    print(f"Remote metadata: {len(remote_metadata)} tables")

    # Load local checkpoint
    print("\nLoading local checkpoint...")
    local_checkpoint = load_local_checkpoint()
    print(f"Local checkpoint: {len(local_checkpoint)} tables")

    # Decide what to download
    print("\nIdentifying tables to download...")
    print("=" * 80)

    tables_to_download = []

    for table_name, remote_info in remote_metadata.items():
        local_file = PROCESSED_DIR / f"{table_name}.csv"

        # Partial file from a previous crash — queue for resume
        if local_file.exists() and table_name not in local_checkpoint:
            size_mb = local_file.stat().st_size / (1024 * 1024)
            print(f"{table_name}: RESUME  (partial {size_mb:.0f} MB on disk)")
            tables_to_download.append(table_name)
            continue

        if table_name not in local_checkpoint:
            print(f"{table_name}: DOWNLOAD (new table)")
            tables_to_download.append(table_name)
            continue

        local_info = local_checkpoint[table_name]

        if remote_info.get("rows") != local_info.get("rows"):
            print(f"{table_name}: DOWNLOAD "
                  f"(rows: {local_info.get('rows'):,} -> {remote_info.get('rows'):,})")
            tables_to_download.append(table_name)
            continue

        if remote_info.get("columns") != local_info.get("columns"):
            print(f"{table_name}: DOWNLOAD (columns changed)")
            tables_to_download.append(table_name)
            continue

        print(f"{table_name}: SKIP (up to date)")

    if not tables_to_download:
        print("\nNo tables need download — all up to date")
        return

    print(f"\nTables to download: {len(tables_to_download)}")
    print("=" * 80)

    # Download loop
    download_results = {}

    for i, table_name in enumerate(tables_to_download, 1):
        print(f"\n[{i}/{len(tables_to_download)}] {table_name}:")

        folder_path = f"{VOLUME_BASE}/{table_name}/"
        files       = list_directory(folder_path)
        csv_files   = [f for f in files if f.get('path', '').endswith('.csv')]

        if not csv_files:
            print("  ERROR: No CSV file found in volume folder")
            download_results[table_name] = {"success": False}
            continue

        csv_file    = csv_files[0]
        remote_path = csv_file['path']
        size_mb     = csv_file.get('file_size', 0) / (1024 * 1024)
        local_file  = PROCESSED_DIR / f"{table_name}.csv"

        print(f"  File : {csv_file.get('name')}")
        print(f"  Size : {size_mb:.2f} MB")
        print(f"  Local: {local_file}")

        if download_file(remote_path, local_file):
            row_count = count_rows(local_file)
            print(f"  Rows : {row_count:,}")
            print(f"  Status: OK")

            download_results[table_name] = {
                "success": True,
                "rows":    row_count,
                "size_mb": size_mb,
            }

            # Save checkpoint immediately after each file
            local_checkpoint[table_name] = remote_metadata[table_name].copy()
            save_local_checkpoint(local_checkpoint)

        else:
            partial_mb = (local_file.stat().st_size / (1024 * 1024)
                          if local_file.exists() else 0)
            print(f"  Status: FAILED  "
                  f"(partial {partial_mb:.0f} MB kept — re-run to resume)")
            download_results[table_name] = {"success": False}

    # Summary
    print("\n" + "=" * 80)
    print("DOWNLOAD SUMMARY")
    print("=" * 80)

    successful = [t for t in tables_to_download
                  if download_results.get(t, {}).get("success")]
    failed     = [t for t in tables_to_download
                  if not download_results.get(t, {}).get("success")]

    total_mb = sum(download_results[t]["size_mb"] for t in successful)
    print(f"\nSuccessful: {len(successful)}/{len(tables_to_download)} ({total_mb:.0f} MB)")
    for t in successful:
        print(f"  - {t}  ({download_results[t]['size_mb']:.0f} MB, "
              f"{download_results[t]['rows']:,} rows)")

    if failed:
        print(f"\nFailed ({len(failed)}) — partial files kept, re-run to resume:")
        for t in failed:
            lf = PROCESSED_DIR / f"{t}.csv"
            partial_mb = lf.stat().st_size / (1024 * 1024) if lf.exists() else 0
            print(f"  - {t}  ({partial_mb:.0f} MB on disk)")

    # Volume cleanup — only when everything succeeded
    if not failed and successful:
        print("\n" + "=" * 80)
        print("CLEANING UP VOLUME")
        print("=" * 80)
        print(f"\nCleaning {CATALOG}.{SCHEMA}.{VOLUME_NAME} ...", end=' ', flush=True)
        ok, msg = delete_volume(CATALOG, SCHEMA, VOLUME_NAME)
        print(f"{'OK' if ok else 'FAILED'} ({msg})")
        if not ok:
            print("Note: clean up manually in Databricks if needed")
    elif failed:
        print("\nVolume NOT cleaned — finish remaining files first, then re-run")

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)

    if failed:
        print(f"\n{len(failed)} file(s) still pending.")
        print("Just re-run this script — it will resume each partial file automatically.")
    else:
        print("\nNEXT STEP: Run load_postgres_hybrid_psql.py")

    print("=" * 80)


if __name__ == "__main__":
    main()
