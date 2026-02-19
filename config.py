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

# ====================================================================
# DIRECTORY STRUCTURE
# ====================================================================
# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
ML_DATA_DIR = DATA_DIR / "ml"
ANALYTICAL_DIR = DATA_DIR / "analytical"
FIGURES_DIR = ANALYTICAL_DIR / "figures"
REPORTS_DIR = ANALYTICAL_DIR / "reports"

# Model directories
MODEL_DIR = PROJECT_ROOT / "models"
METADATA_DIR = MODEL_DIR / "metadata"

# Script directories
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
NOTEBOOKS_DIR = PROJECT_ROOT / "ml_phase"

# Documentation
DOCS_DIR = PROJECT_ROOT / "docs"

# Create directories if they don't exist
for directory in [
    RAW_DATA_DIR / "genes", 
    RAW_DATA_DIR / "variants",
    PROCESSED_DATA_DIR, 
    ML_DATA_DIR,
    ANALYTICAL_DIR,
    FIGURES_DIR / "phase1",
    FIGURES_DIR / "phase2", 
    FIGURES_DIR / "phase3",
    REPORTS_DIR,
    MODEL_DIR,
    METADATA_DIR,
    DOCS_DIR / "models",
    DOCS_DIR / "deployment"
]:
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
    if not DATABASE_CONFIG['password']:
        raise ValueError("POSTGRES_PASSWORD must be set in .env file")
    
    return (f"postgresql://{DATABASE_CONFIG['user']}:"
            f"{DATABASE_CONFIG['password']}@"
            f"{DATABASE_CONFIG['host']}:"
            f"{DATABASE_CONFIG['port']}/"
            f"{DATABASE_CONFIG['database']}")


# ====================================================================
# NCBI API CONFIGURATION - FROM .env
# ====================================================================
NCBI_EMAIL = os.getenv('NCBI_EMAIL')
NCBI_API_KEY = os.getenv('NCBI_API_KEY')  # Get from NCBI account

# ====================================================================
# DATABRICKS CONFIGURATION - ALL FROM .env (NO DEFAULTS FOR SECURITY)
# ====================================================================
DATABRICKS_CONFIG = {
    'host': os.getenv('DATABRICKS_HOST'),
    'token': os.getenv('DATABRICKS_TOKEN'),
    'workspace_id': os.getenv('DATABRICKS_WORKSPACE_ID')
}

# ====================================================================
# MODEL CONFIGURATION
# ====================================================================
MODEL_CONFIG = {
    'random_state': 42,
    'test_size': 0.15,
    'val_size': 0.15,
    'smote_strategy': 0.5,  # Oversample minority to 50% of majority
    'cv_folds': 3,
    'n_iter': 20  # RandomizedSearchCV iterations
}

# Model file paths
MODEL_FILES = {
    'variant_model': MODEL_DIR / 'ensemble_xgb_variants.pkl',
    'variant_scaler': MODEL_DIR / 'variant_scaler.pkl',
    'sv_model': MODEL_DIR / 'sv_raw_features_best.pkl',
    'variant_metadata': METADATA_DIR / 'variant_model_metadata.json',
    'sv_metadata': METADATA_DIR / 'sv_model_metadata.json'
}

# ====================================================================
# FEATURE CONFIGURATION
# ====================================================================
FEATURE_CONFIG = {
    'variant_features': 75,
    'sv_features': 8,
    'variant_feature_file': DOCS_DIR / 'FEATURE_DESCRIPTIONS.txt',
    'sv_feature_file': DOCS_DIR / 'FEATURE_DESCRIPTIONS.txt'
}

# ====================================================================
# PROJECT CONFIGURATION
# ====================================================================
# Genes to download (for initial testing)
DISEASE_GENES = [
    "BRCA1", "BRCA2", "TP53", "CFTR", "HBB", 
    "APOE", "HTT", "DMD", "F8", "F9", "MTHFR", "EGFR"
]

# Data sources
DATA_SOURCES = {
    'ncbi_gene': 'https://ftp.ncbi.nlm.nih.gov/gene/DATA/',
    'clinvar': 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/',
    'pharmgkb': 'https://api.pharmgkb.org/',
    'omim': 'https://www.omim.org/',
    'ucsc_genome': 'https://genome.ucsc.edu/'
}

# ====================================================================
# API CONFIGURATION (Future Phase 8/12)
# ====================================================================
API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', 8000)),
    'debug': os.getenv('API_DEBUG', 'False').lower() == 'true',
    'reload': os.getenv('API_RELOAD', 'False').lower() == 'true'
}

# JWT Secret (for future authentication)
JWT_SECRET = os.getenv('JWT_SECRET')  # MUST be in .env for production
JWT_ALGORITHM = 'HS256'
JWT_EXPIRATION_HOURS = 24

# ====================================================================
# LOGGING CONFIGURATION
# ====================================================================
LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': PROJECT_ROOT / 'logs' / 'app.log'
}

# Create logs directory
(PROJECT_ROOT / 'logs').mkdir(exist_ok=True)

# ====================================================================
# DEPLOYMENT CONFIGURATION (Future)
# ====================================================================
DEPLOYMENT_CONFIG = {
    'environment': os.getenv('ENVIRONMENT', 'development'),  # development, staging, production
    'max_workers': int(os.getenv('MAX_WORKERS', 4)),
    'timeout': int(os.getenv('TIMEOUT', 30))
}

# ====================================================================
# VALIDATION HELPERS
# ====================================================================
def validate_config():
    """Validate critical configuration settings."""
    errors = []
    
    # Check database password
    if not DATABASE_CONFIG['password']:
        errors.append("POSTGRES_PASSWORD not set in .env")
    
    # Check NCBI email
    if not NCBI_EMAIL or '@' not in NCBI_EMAIL:
        errors.append("Valid NCBI_EMAIL not set")
    
    # Check model files exist (for inference)
    if not MODEL_FILES['variant_model'].exists():
        errors.append(f"Variant model not found: {MODEL_FILES['variant_model']}")
    
    if errors:
        print("Configuration Errors:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    return True

# ====================================================================
# EXAMPLE .env FILE TEMPLATE
# ====================================================================
ENV_TEMPLATE = """
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=genome_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password_here

# NCBI API
NCBI_EMAIL=your_email@example.com
NCBI_API_KEY=your_ncbi_api_key_here

# Databricks (if using)
DATABRICKS_HOST=https://community.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_token_here
DATABRICKS_WORKSPACE_ID=your_workspace_id

# API Configuration (Phase 8/12)
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=False
JWT_SECRET=your_random_secret_key_here

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO
"""

def create_env_template():
    """Create .env.example template file."""
    env_example = PROJECT_ROOT / '.env.example'
    if not env_example.exists():
        with open(env_example, 'w') as f:
            f.write(ENV_TEMPLATE.strip())
        print(f"Created .env.example template at {env_example}")

if __name__ == '__main__':
    print("Configuration loaded successfully")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Database: {DATABASE_CONFIG['database']}")
    print(f"Model directory: {MODEL_DIR}")
    
    # Validate config
    if validate_config():
        print("\nConfiguration validation: PASSED")
    else:
        print("\nConfiguration validation: FAILED")
    
    # Create .env.example if it doesn't exist
    create_env_template()
