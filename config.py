"""
Configuration settings for DNA Gene Mapping Project.
ALL SENSITIVE DATA IN .env FILE (NOT IN GIT)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables FIRST
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
ML_DATA_DIR = DATA_DIR / "ml"

# Create directories if they don't exist
for directory in [RAW_DATA_DIR / "genes", RAW_DATA_DIR / "variants", 
                  PROCESSED_DATA_DIR, ML_DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ====================================================================
# DATABASE CONFIGURATION - ALL FROM .env (SECURE)
# ====================================================================
DATABASE_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'genome_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD')  # NO DEFAULT - MUST BE IN .env
}

# Helper function to get connection string
def get_database_url():
    """Get PostgreSQL connection URL from env variables."""
    return (f"postgresql://{DATABASE_CONFIG['user']}:"
            f"{DATABASE_CONFIG['password']}@"
            f"{DATABASE_CONFIG['host']}:"
            f"{DATABASE_CONFIG['port']}/"
            f"{DATABASE_CONFIG['database']}")


# ====================================================================
# NCBI API CONFIGURATION - FROM .env
# ====================================================================
NCBI_EMAIL = os.getenv('NCBI_EMAIL', 'sharique020709@gmail.com')
NCBI_API_KEY = os.getenv('NCBI_API_KEY')  # Get from NCBI account

# ====================================================================
# DATABRICKS CONFIGURATION - ALL FROM .env (NO DEFAULTS FOR SECURITY)
# ====================================================================
# Note: For Databricks Community Edition:
# - No cluster needed initially (use notebook compute)
# - Host URL is your workspace URL
# - Token is generated from User Settings
DATABRICKS_CONFIG = {
    'host': os.getenv('DATABRICKS_HOST'),           # e.g., https://community.cloud.databricks.com
    'token': os.getenv('DATABRICKS_TOKEN'),         # Personal Access Token
    'workspace_id': os.getenv('DATABRICKS_WORKSPACE_ID')  # Your workspace ID
}

# ====================================================================
# PROJECT CONFIGURATION
# ====================================================================
# Genes to download
DISEASE_GENES = [
    "BRCA1", "BRCA2", "TP53", "CFTR", "HBB", 
    "APOE", "HTT", "DMD", "F8", "F9", "MTHFR", "EGFR"
]

# Model configuration
MODEL_CONFIG = {
    'random_state': 42,
    'test_size': 0.15,
    'val_size': 0.15
}
