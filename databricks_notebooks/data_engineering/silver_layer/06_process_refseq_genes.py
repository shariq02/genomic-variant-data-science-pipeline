# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, expr
)

# COMMAND ----------

# DBTITLE 1,Initialize Spark
spark = SparkSession.builder.getOrCreate()
print("Spark Initialized")

# COMMAND ----------

# DBTITLE 1,Configuration
catalog_name = "workspace"
spark.sql(f"USE CATALOG {catalog_name}")

print(f"Catalog: {catalog_name}")
print("Processing RefSeq genes and transcripts with genomic coordinates")

# COMMAND ----------

# DBTITLE 1,Read Raw RefSeq Data
df_refseq_genes_raw = spark.table(f"{catalog_name}.default.refseq_genes_grch_38")
df_refseq_transcripts_raw = spark.table(f"{catalog_name}.default.refseq_transcripts_grch_38")

print(f"Loaded RefSeq genes: {df_refseq_genes_raw.count():,}")
print(f"Loaded RefSeq transcripts: {df_refseq_transcripts_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Inspect Schemas
print("RefSeq Genes Schema:")
df_refseq_genes_raw.printSchema()

print("\nRefSeq Transcripts Schema:")
df_refseq_transcripts_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Sample Raw Data
print("RefSeq Genes Sample:")
df_refseq_genes_raw.show(3, truncate=60)

print("\nRefSeq Transcripts Sample:")
df_refseq_transcripts_raw.show(3, truncate=60)

# COMMAND ----------

print("RefSeq Genes Columns:")
print(df_refseq_genes_raw.columns)
df_refseq_genes_raw.show(5, truncate=False)

print("\nRefSeq Transcripts Columns:")
print(df_refseq_transcripts_raw.columns)
df_refseq_transcripts_raw.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,STEP 1: Clean RefSeq Genes
df_refseq_clean = (
    df_refseq_genes_raw
    .withColumn("gene_symbol", upper(trim(col("gene_id"))))
    .withColumn("gene_name", trim(col("gene_name")))
    .withColumn("chromosome", 
                when(col("chromosome").startswith("chr"), col("chromosome").substr(4, 10))
                .otherwise(col("chromosome")))
    .withColumn("strand", trim(col("strand")))
    
    # Filter valid genes
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    .filter(col("gene_name").isNotNull())
    .filter(col("gene_name") != "")
    .filter(col("chromosome").isNotNull())
    .filter(col("chromosome") != "")
    
    # Valid chromosomes only
    .filter(
        col("chromosome").isin(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "X", "Y", "M", "MT"
        )
    )
)

print(f"Clean RefSeq genes: {df_refseq_clean.count():,}")
df_refseq_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 2: Create RefSeq Genes Table
df_genes_refseq = (
    df_refseq_clean
    .select(
        "gene_symbol",
        "gene_name",
        "chromosome",
        "strand"
    )
    .dropDuplicates(["gene_symbol"])
)

print(f"Unique RefSeq genes: {df_genes_refseq.count():,}")
df_genes_refseq.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save RefSeq Genes to Silver
df_genes_refseq.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.genes_refseq")

print(f"Saved table: {catalog_name}.silver.genes_refseq")

# COMMAND ----------

# DBTITLE 1,STEP 3: Clean RefSeq Transcripts
df_transcripts_clean = (
    df_refseq_transcripts_raw
    .withColumn("transcript_id", trim(col("transcript_id")))
    .withColumn("gene_symbol", upper(trim(col("gene_id"))))
    .withColumn("gene_name", trim(col("gene_name")))
    .withColumn("chromosome", 
                when(col("chromosome").startswith("chr"), col("chromosome").substr(4, 10))
                .otherwise(col("chromosome")))
    .withColumn("start", col("start").cast("long"))
    .withColumn("end", col("end").cast("long"))
    .withColumn("strand", trim(col("strand")))
    
    # Filter valid transcripts
    .filter(col("transcript_id").isNotNull())
    .filter(col("transcript_id") != "")
    .filter(col("gene_symbol").isNotNull())
    .filter(col("gene_symbol") != "")
    .filter(col("chromosome").isNotNull())
    .filter(col("chromosome") != "")
    
    # Valid chromosomes only
    .filter(
        col("chromosome").isin(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "X", "Y", "M", "MT"
        )
    )
    
    # Fix position order
    .withColumn("start_fixed", when(col("start") > col("end"), col("end")).otherwise(col("start")))
    .withColumn("end_fixed", when(col("start") > col("end"), col("start")).otherwise(col("end")))
    .drop("start", "end")
    .withColumnRenamed("start_fixed", "start")
    .withColumnRenamed("end_fixed", "stop")
)

print(f"Clean transcripts: {df_transcripts_clean.count():,}")
df_transcripts_clean.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,STEP 4: Create Transcripts Table
df_transcripts = (
    df_transcripts_clean
    .select(
        "transcript_id",
        "gene_symbol",
        "gene_name",
        "chromosome",
        "start",
        "stop",
        "strand"
    )
    .dropDuplicates(["transcript_id"])
)

print(f"Unique transcripts: {df_transcripts.count():,}")
df_transcripts.show(3, truncate=60)

# COMMAND ----------

# DBTITLE 1,Save Transcripts to Silver
df_transcripts.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.transcripts")

print(f"Saved table: {catalog_name}.silver.transcripts")

# COMMAND ----------

# DBTITLE 1,Final Validation
print("VALIDATION SUMMARY")
print("=" * 70)

genes_refseq_count = spark.table(f"{catalog_name}.silver.genes_refseq").count()
transcripts_count = spark.table(f"{catalog_name}.silver.transcripts").count()

print(f"\nsilver.genes_refseq: {genes_refseq_count:,} rows")
print(f"silver.transcripts: {transcripts_count:,} rows")

# Chromosome distribution for genes
print("\nGenes - Chromosome Distribution:")
spark.table(f"{catalog_name}.silver.genes_refseq") \
    .groupBy("chromosome") \
    .count() \
    .orderBy("chromosome") \
    .show(30)

# Strand distribution for genes
print("\nGenes - Strand Distribution:")
spark.table(f"{catalog_name}.silver.genes_refseq") \
    .groupBy("strand") \
    .count() \
    .show()

# Sample genes
print("\nSample Genes:")
spark.table(f"{catalog_name}.silver.genes_refseq").show(10, truncate=60)

# Sample transcripts
print("\nSample Transcripts:")
spark.table(f"{catalog_name}.silver.transcripts").show(10, truncate=60)

print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
