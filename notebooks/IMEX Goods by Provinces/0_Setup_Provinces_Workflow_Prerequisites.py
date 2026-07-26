# Databricks notebook source
# DBTITLE 1,Setup Provinces Workflow - Overview
# MAGIC %md
# MAGIC # Step 0: Provinces Monthly Workflow Prerequisites
# MAGIC
# MAGIC Creates the Unity Catalog objects the **IMEX Goods by Provinces** workflow
# MAGIC needs. Safe to re-run: every statement is `CREATE ... IF NOT EXISTS`.
# MAGIC
# MAGIC ## What this workflow covers
# MAGIC
# MAGIC Vietnam Customs publishes a monthly report titled
# MAGIC *"Xuất khẩu, nhập khẩu chia theo tỉnh/thành phố"* (Biểu số 019.T/BCB-TC).
# MAGIC
# MAGIC Its grain is **province x trade flow x month**, valued in **USD only**. There
# MAGIC is no product breakdown and no quantity - unlike every other workstream here.
# MAGIC Each PDF holds both flows side by side:
# MAGIC
# MAGIC | TỈNH/THÀNH PHỐ | Tháng N (XK) | N tháng (XK) | Tháng N (NK) | N tháng (NK) |
# MAGIC |----------------|--------------|--------------|--------------|--------------|
# MAGIC | An Giang       | 235,578,472  | 765,507,712  | 121,618,306  | 432,070,307  |
# MAGIC
# MAGIC So one document is registered **once** (`sub_category = 'Provinces IMEX'`),
# MAGIC downloaded once and parsed once; extraction emits two rows per province.
# MAGIC
# MAGIC ## Pipeline
# MAGIC
# MAGIC 1. `0_Update_Provinces_URL_Table` - discover monthly report URLs from the Customs API
# MAGIC 2. `1_Download_Provinces_Documents` - download PDFs into the volume
# MAGIC 3. `2_Parse_Provinces_Documents` - `ai_parse_document()` into raw JSON
# MAGIC 4. `3_Extract_Provinces_Statistics` - structured rows into the fact table
# MAGIC
# MAGIC URL discovery is owned by `0_Update_Provinces_URL_Table`. This notebook never
# MAGIC writes URL rows - an earlier version loaded them from `.md` files with
# MAGIC `write.mode("overwrite")`, which would silently wipe every discovered URL.

# COMMAND ----------

# DBTITLE 1,Create Volume for Downloaded PDFs
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS market_data.customs.provinces_imex_docs
# MAGIC COMMENT 'Raw monthly province-split PDFs (one file per month, both flows)'

# COMMAND ----------

# DBTITLE 1,Create provinces_customs_documents_url
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.provinces_customs_documents_url (
# MAGIC   url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL COMMENT 'Always "Provinces IMEX" - one document covers both flows',
# MAGIC   created_at TIMESTAMP,
# MAGIC   report_month STRING COMMENT 'Format: YYYY-MM'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Provinces monthly - discovered source document URLs'

# COMMAND ----------

# DBTITLE 1,Create provinces_document_processing_log
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.provinces_document_processing_log (
# MAGIC   document_id STRING NOT NULL,
# MAGIC   document_url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL,
# MAGIC   filename STRING,
# MAGIC
# MAGIC   download_status STRING,
# MAGIC   download_timestamp TIMESTAMP,
# MAGIC   download_attempts INT,
# MAGIC   download_error_message STRING,
# MAGIC
# MAGIC   parse_status STRING,
# MAGIC   parse_timestamp TIMESTAMP,
# MAGIC   parse_error_message STRING,
# MAGIC
# MAGIC   extraction_status STRING,
# MAGIC   extraction_timestamp TIMESTAMP,
# MAGIC   extraction_error_message STRING,
# MAGIC   extraction_rows_inserted INT,
# MAGIC
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP,
# MAGIC
# MAGIC   PRIMARY KEY (document_id)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Provinces monthly - document processing status tracking'

# COMMAND ----------

# DBTITLE 1,Create provinces_parsed_documents_raw
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.provinces_parsed_documents_raw (
# MAGIC   document_id STRING NOT NULL,
# MAGIC   document_url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL,
# MAGIC   filename STRING,
# MAGIC   volume_path STRING,
# MAGIC
# MAGIC   parsed_json STRING,
# MAGIC
# MAGIC   report_period STRING,
# MAGIC   report_start_date DATE,
# MAGIC   report_end_date DATE,
# MAGIC
# MAGIC   parsed_timestamp TIMESTAMP,
# MAGIC
# MAGIC   PRIMARY KEY (document_id)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Provinces monthly - raw ai_parse_document() results'

# COMMAND ----------

# DBTITLE 1,Create provinces_trade_statistics
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.provinces_trade_statistics (
# MAGIC   document_id STRING NOT NULL,
# MAGIC
# MAGIC   report_period STRING COMMENT 'Source label, e.g. "Tháng 4 năm 2026"',
# MAGIC   report_month STRING COMMENT 'Format: YYYY-MM',
# MAGIC   report_start_date DATE,
# MAGIC   report_end_date DATE,
# MAGIC
# MAGIC   row_number INT NOT NULL,
# MAGIC
# MAGIC   province_name STRING COMMENT 'Raw province/city label as printed in the report',
# MAGIC   trade_flow STRING COMMENT '"export" or "import"',
# MAGIC
# MAGIC   period_value_usd DECIMAL(20,3) COMMENT 'Reporting-month value in USD',
# MAGIC   cumulative_value_usd DECIMAL(20,3) COMMENT 'Year-to-date value in USD',
# MAGIC
# MAGIC   parsed_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Provinces monthly - province x trade flow x month, USD only (no product dimension)'

# COMMAND ----------

# DBTITLE 1,Verify Objects Exist
# MAGIC %sql
# MAGIC SELECT table_name, comment
# MAGIC FROM market_data.information_schema.tables
# MAGIC WHERE table_schema = 'customs'
# MAGIC   AND table_name LIKE 'provinces%'
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Row Counts
display(spark.sql("""
    SELECT 'provinces_customs_documents_url' AS table_name, COUNT(*) AS rows
    FROM market_data.customs.provinces_customs_documents_url
    UNION ALL
    SELECT 'provinces_document_processing_log', COUNT(*)
    FROM market_data.customs.provinces_document_processing_log
    UNION ALL
    SELECT 'provinces_parsed_documents_raw', COUNT(*)
    FROM market_data.customs.provinces_parsed_documents_raw
    UNION ALL
    SELECT 'provinces_trade_statistics', COUNT(*)
    FROM market_data.customs.provinces_trade_statistics
    ORDER BY table_name
"""))

print("\n✓ Provinces workflow prerequisites are in place.")
print("  Next: run 0_Update_Provinces_URL_Table with dry_run=false to discover URLs.")
