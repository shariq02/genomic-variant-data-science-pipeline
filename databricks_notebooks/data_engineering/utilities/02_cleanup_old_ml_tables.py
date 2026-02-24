# Databricks notebook source
# MAGIC %md
# MAGIC #### CLEANUP - DELETE OLD ML DATASET TABLES
# MAGIC ##### Run this BEFORE executing notebooks 30-40
# MAGIC
# MAGIC **DNA Gene Mapping Project**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** February 23, 2026

# COMMAND ----------

# DBTITLE 1,Initialize
spark.sql("USE CATALOG workspace")

catalog_name = "workspace"
schema_name = "gold"

print("CLEANUP - DELETING OLD ML DATASET TABLES")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Define Old Tables to Delete
old_tables = [
    # From old combined notebook (prepare_combined_ml_dataset.py)
    "ml_dataset_variants_train",
    "ml_dataset_variants_validation",
    "ml_dataset_variants_test",
    "ml_dataset_structural_variants_train",
    "ml_dataset_structural_variants_validation",
    "ml_dataset_structural_variants_test",

    # From old 31_ml_dataset_cancer.py
    "ml_dataset_cancer_train",
    "ml_dataset_cancer_validation",
    "ml_dataset_cancer_test",

    # From old 30_ml_dataset_drug_response.py
    "ml_dataset_drug_response_train",
    "ml_dataset_drug_response_validation",
    "ml_dataset_drug_response_test",

    # From old 32_ml_dataset_pharmacogene.py
    "ml_dataset_gene_pharmacogene_train",
    "ml_dataset_gene_pharmacogene_validation",
    "ml_dataset_gene_pharmacogene_test",

    # From old 33_ml_dataset_expression.py
    "ml_dataset_expression_train",
    "ml_dataset_expression_validation",
    "ml_dataset_expression_test",

    # From old 34_ml_dataset_carrier_screening.py
    "ml_dataset_carrier_screening_train",
    "ml_dataset_carrier_screening_validation",
    "ml_dataset_carrier_screening_test",
]

print(f"Tables to delete: {len(old_tables)}")

# COMMAND ----------

# DBTITLE 1,Delete Old Tables
print("\nDELETING OLD TABLES")
print("=" * 80)

deleted = []
not_found = []

for table in old_tables:
    full_name = f"{catalog_name}.{schema_name}.{table}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        deleted.append(full_name)
        print(f"DELETED: {full_name}")
    except Exception as e:
        not_found.append(full_name)
        print(f"SKIPPED: {full_name} - {str(e)}")

# COMMAND ----------

# DBTITLE 1,Verify Deletion
print("\nVERIFICATION SUMMARY")
print("=" * 80)
print(f"Deleted:   {len(deleted)}")
print(f"Skipped:   {len(not_found)}")

print("\nRemaining ml_dataset tables in gold schema:")
spark.sql(f"""
    SHOW TABLES IN {catalog_name}.{schema_name}
""").filter("tableName LIKE 'ml_dataset%'").show(50, truncate=False)

print("\nCLEANUP COMPLETE - Ready to run notebooks 30-40")
