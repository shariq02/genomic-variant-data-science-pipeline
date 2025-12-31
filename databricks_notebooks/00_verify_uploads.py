
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

# ====================================================================
# VERIFY GENES FILE
# ====================================================================

try:
    print("\n1. Checking genes file...")
    
    # Read genes CSV
    df_genes = spark.read.csv(
        "/FileStore/tables/genes/gene_metadata.csv",
        header=True,
        inferSchema=True
    )
    
    # Display info
    print(f"   SUCCESS: Genes file found!")
    print(f"   Total genes: {df_genes.count()}")
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
    
except Exception as e:
    print(f"   ERROR reading genes file: {e}")


# ====================================================================
# VERIFY VARIANTS FILE
# ====================================================================

try:
    print("\n2. Checking variants file...")
    
    # Read variants CSV
    df_variants = spark.read.csv(
        "/FileStore/tables/variants/clinvar_pathogenic.csv",
        header=True,
        inferSchema=True
    )
    
    # Display info
    print(f"   SUCCESS: Variants file found!")
    print(f"   Total variants: {df_variants.count()}")
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
    
except Exception as e:
    print(f"   ERROR reading variants file: {e}")


# ====================================================================
# SUMMARY
# ====================================================================

print("\n" + "="*70)
print("VERIFICATION COMPLETE")
print("="*70)
print("SUCCESS: If you see data above, uploads were successful!")
print("READY: You're ready for Week 3: PySpark data processing")
print("="*70)
