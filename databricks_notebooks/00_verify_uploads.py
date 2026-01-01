# ====================================================================
# UPLOAD TO DATABRICKS
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 1 January 2026
# ====================================================================
# DATABRICKS NOTEBOOK: Verify Uploaded Data
# Run this in Databricks after uploading files
# ====================================================================

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

print("="*70)
print("VERIFY UPLOADED DATA IN DATABRICKS")
print("="*70)

# Initialize Spark
spark = SparkSession.builder.appName("VerifyUploads").getOrCreate()

# Track success
genes_success = False
variants_success = False

# ====================================================================
# VERIFY GENES TABLE
# ====================================================================

try:
    print("\n1. Checking genes table...")
    print("   Reading: default.gene_metadata")
    
    # Read from catalog table (Unity Catalog way)
    df_genes = spark.table("default.gene_metadata")
    
    # Count rows
    gene_count = df_genes.count()
    
    # Success!
    print(f"   SUCCESS: Genes table found and readable!")
    print(f"   Total genes: {gene_count}")
    print(f"   Columns: {len(df_genes.columns)}")
    
    # Show schema
    print("\n   Schema:")
    df_genes.printSchema()
    
    # Show sample data
    print("\n   Sample data:")
    df_genes.show(5, truncate=False)
    
    # Basic statistics
    print("\n   Genes by chromosome:")
    df_genes.groupBy("chromosome").count().orderBy("chromosome").show()
    
    genes_success = True
    
except Exception as e:
    print(f"   ERROR: Could not read genes table!")
    print(f"   Reason: {e}")
    print("\n   Troubleshooting:")
    print("   - Table not created yet?")
    print("   - Try: Catalog > default > Tables")
    print("   - Or upload via: Create > Table")


# ====================================================================
# VERIFY VARIANTS TABLE
# ====================================================================

try:
    print("\n2. Checking variants table...")
    print("   Reading: default.clinvar_pathogenic")
    
    # Read from catalog table
    df_variants = spark.table("default.clinvar_pathogenic")
    
    # Count rows
    variant_count = df_variants.count()
    
    # Success!
    print(f"   SUCCESS: Variants table found and readable!")
    print(f"   Total variants: {variant_count}")
    print(f"   Columns: {len(df_variants.columns)}")
    
    # Show schema
    print("\n   Schema:")
    df_variants.printSchema()
    
    # Show sample data
    print("\n   Sample data:")
    df_variants.show(5, truncate=False)
    
    # Basic statistics
    print("\n   Variants by gene (top 10):")
    df_variants.groupBy("gene_name").count() \
        .orderBy(col("count").desc()) \
        .show(10)
    
    print("\n   Variants by clinical significance:")
    df_variants.groupBy("clinical_significance").count().show()
    
    variants_success = True
    
except Exception as e:
    print(f"   ERROR: Could not read variants table!")
    print(f"   Reason: {e}")
    print("\n   Troubleshooting:")
    print("   - Table not created yet?")
    print("   - Try: Catalog > default > Tables")
    print("   - Or upload via: Create > Table")


# ====================================================================
# SUMMARY
# ====================================================================

print("\n" + "="*70)
print("VERIFICATION SUMMARY")
print("="*70)

if genes_success and variants_success:
    print("SUCCESS: All tables uploaded and verified!")
elif genes_success or variants_success:
    print("PARTIAL: Some tables are missing")
    if not genes_success:
        print("Genes table: NOT FOUND")
    if not variants_success:
        print("Variants table: NOT FOUND")
else:
    print("FAILED: No tables found")
    print("ACTION: Upload files via Create > Table")
    
print("="*70)