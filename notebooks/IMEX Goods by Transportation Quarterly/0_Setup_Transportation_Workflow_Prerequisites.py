# Databricks notebook source
# DBTITLE 1,Setup Transportation Workflow - Overview
# MAGIC %md
# MAGIC # Step 0: Transportation Quarterly Workflow Prerequisites
# MAGIC
# MAGIC Creates the Unity Catalog objects the **IMEX Goods by Transportation Quarterly**
# MAGIC workflow needs. Safe to re-run: every statement is `CREATE ... IF NOT EXISTS`.
# MAGIC
# MAGIC ## What this workflow covers
# MAGIC
# MAGIC Vietnam Customs publishes quarterly reports titled
# MAGIC *"Xuất/Nhập khẩu hàng hóa theo phương thức vận tải"* (Biểu số 1X/1N-PTVT).
# MAGIC The grain is **product group x mode of transport**, reported per quarter with
# MAGIC period and cumulative figures.
# MAGIC
# MAGIC Transport modes in the source: `Đường bộ` (road), `Đường không` (air),
# MAGIC `Đường thủy` (water), `Loại khác` (other). `Cộng` rows are subtotals and are
# MAGIC dropped during extraction to avoid double counting.
# MAGIC
# MAGIC **These reports carry no province dimension.** Earlier versions of this folder
# MAGIC were copied from the provinces workstream and wrote into the `provinces_*`
# MAGIC namespace; that is fixed - everything here uses `transportation_*`.
# MAGIC
# MAGIC ## Pipeline
# MAGIC
# MAGIC 1. `0_Update_Transportation_URL_Table` - discover quarterly report URLs from the Customs API
# MAGIC 2. `1_Download_Transportation_Documents` - download PDFs into the volume
# MAGIC 3. `2_Parse_Transportation_Documents` - `ai_parse_document()` into raw JSON
# MAGIC 4. `3_Extract_Transportation_Statistics` - structured rows into the fact table
# MAGIC
# MAGIC URL discovery is owned by `0_Update_Transportation_URL_Table`. This notebook
# MAGIC never writes URL rows.

# COMMAND ----------

# DBTITLE 1,Create Volume for Downloaded PDFs
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS market_data.customs.transportation_download_docs
# MAGIC COMMENT 'Raw quarterly transport-mode PDFs, one subdirectory per trade flow'

# COMMAND ----------

# DBTITLE 1,Create transportation_customs_documents_url
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.transportation_customs_documents_url (
# MAGIC   url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL COMMENT '"Import by Transportation" or "Export by Transportation"',
# MAGIC   created_at TIMESTAMP,
# MAGIC   report_quarter STRING COMMENT 'Format: YYYY-QN (e.g., 2026-Q1)'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Transportation quarterly - discovered source document URLs'

# COMMAND ----------

# DBTITLE 1,Create transportation_document_processing_log
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.transportation_document_processing_log (
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
# MAGIC COMMENT 'Transportation quarterly - document processing status tracking'

# COMMAND ----------

# DBTITLE 1,Create transportation_parsed_documents_raw
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.transportation_parsed_documents_raw (
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
# MAGIC COMMENT 'Transportation quarterly - raw ai_parse_document() results'

# COMMAND ----------

# DBTITLE 1,Create transportation_trade_statistics
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.transportation_trade_statistics (
# MAGIC   sub_category STRING NOT NULL COMMENT '"Import by Transportation" or "Export by Transportation"',
# MAGIC   document_id STRING NOT NULL,
# MAGIC
# MAGIC   report_period STRING,
# MAGIC   report_quarter STRING COMMENT 'Format: YYYY-QN (e.g., 2026-Q1)',
# MAGIC   report_start_date DATE,
# MAGIC   report_end_date DATE,
# MAGIC
# MAGIC   row_number INT NOT NULL,
# MAGIC
# MAGIC   product_category STRING,
# MAGIC   parent_category STRING,
# MAGIC   vehicle_type STRING COMMENT 'Normalized transport mode: Đường bộ / Đường không / Đường thủy / Loại khác',
# MAGIC
# MAGIC   unit STRING COMMENT 'Quantity unit from the source table (USD or Tấn)',
# MAGIC   quantity DECIMAL(20,3) COMMENT 'Reporting-period quantity',
# MAGIC   value_usd DECIMAL(20,3) COMMENT 'Reporting-period value in USD',
# MAGIC   cumulative_quantity DECIMAL(20,3) COMMENT 'Year-to-date quantity (Lũy kế)',
# MAGIC   cumulative_value_usd DECIMAL(20,3) COMMENT 'Year-to-date value in USD (Lũy kế)',
# MAGIC
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Transportation quarterly - product group x transport mode trade statistics'

# COMMAND ----------

# DBTITLE 1,Verify Objects Exist
# MAGIC %sql
# MAGIC SELECT table_name, comment
# MAGIC FROM market_data.information_schema.tables
# MAGIC WHERE table_schema = 'customs'
# MAGIC   AND table_name LIKE 'transportation%'
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Row Counts
row_counts = spark.sql("""
    SELECT 'transportation_customs_documents_url' AS table_name, COUNT(*) AS rows
    FROM market_data.customs.transportation_customs_documents_url
    UNION ALL
    SELECT 'transportation_document_processing_log', COUNT(*)
    FROM market_data.customs.transportation_document_processing_log
    UNION ALL
    SELECT 'transportation_parsed_documents_raw', COUNT(*)
    FROM market_data.customs.transportation_parsed_documents_raw
    UNION ALL
    SELECT 'transportation_trade_statistics', COUNT(*)
    FROM market_data.customs.transportation_trade_statistics
    ORDER BY table_name
""")

display(row_counts)

print("\n✓ Transportation workflow prerequisites are in place.")
print("  Next: run 0_Update_Transportation_URL_Table with dry_run=false to discover URLs.")
