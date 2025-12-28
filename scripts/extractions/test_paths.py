# ====================================================================
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 28 December 2025
# ====================================================================

from pathlib import Path

# Get project structure
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent

print("="*70)
print("PROJECT PATH VERIFICATION")
print("="*70)
print(f"Script location: {Path(__file__)}")
print(f"Script directory: {script_dir}")
print(f"Project root: {project_root}")
print(f"\nExpected data directories:")
print(f"  Genes: {project_root / 'data' / 'raw' / 'genes'}")
print(f"  Variants: {project_root / 'data' / 'raw' / 'variants'}")
print("="*70)

# Verify directories exist
genes_dir = project_root / "data" / "raw" / "genes"
variants_dir = project_root / "data" / "raw" / "variants"

genes_dir.mkdir(parents=True, exist_ok=True)
variants_dir.mkdir(parents=True, exist_ok=True)

print("\n✅ Directories created/verified successfully!")
print(f"✅ Genes directory exists: {genes_dir.exists()}")
print(f"✅ Variants directory exists: {variants_dir.exists()}")