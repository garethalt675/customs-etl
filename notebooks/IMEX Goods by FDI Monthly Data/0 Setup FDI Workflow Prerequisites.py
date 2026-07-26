# Databricks notebook source
# DBTITLE 1,FDI Workflow Setup - Overview
# MAGIC %md
# MAGIC # FDI Import/Export Workflow - Setup Guide
# MAGIC
# MAGIC This notebook documents the setup steps required before running the FDI customs data ETL workflow.
# MAGIC
# MAGIC ## Workflow Overview
# MAGIC
# MAGIC The FDI workflow mirrors the regular IMEX workflow with **3N/3X** URL patterns:
# MAGIC
# MAGIC 1. **[1_Download_FDI_Documents](#notebook/125734742720941)**: Download PDFs from URLs to FDI volumes
# MAGIC 2. **[2_Parse_FDI_Documents](#notebook/125734742720942)**: Parse PDFs using AI to extract tables
# MAGIC 3. **[3_Extract_FDI_Statistics](#notebook/125734742720943)**: Transform parsed data into structured statistics
# MAGIC
# MAGIC ## Key Differences from Regular IMEX
# MAGIC
# MAGIC | Aspect | Regular IMEX | FDI |
# MAGIC | --- | --- | --- |
# MAGIC | **URL Pattern** | 2N (import), 2X (export) | 3N (import), 3X (export) |
# MAGIC | **Tables** | `customs_documents_url`, `document_processing_log`, etc. | `fdi_customs_documents_url`, `fdi_document_processing_log`, etc. |
# MAGIC | **Volumes** | `download_docs/import_goods`, `download_docs/export_goods` | `fdi_import_goods`, `fdi_export_goods` |
# MAGIC | **Sub-category** | "Import Goods", "Export Goods" | Same ("Import Goods", "Export Goods") |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC ### ✅ Already Created
# MAGIC * Volumes: `market_data.customs.fdi_import_goods` and `market_data.customs.fdi_export_goods`
# MAGIC * URL files: `import_url.md` and `export_url.md` in this directory
# MAGIC
# MAGIC ### ⚠ Still Needed
# MAGIC
# MAGIC 1. **URL Table**: Load URLs from .md files into `market_data.customs.fdi_customs_documents_url`
# MAGIC 2. **Tracking Tables**: Create FDI versions of processing, parsed, and statistics tables
# MAGIC
# MAGIC Follow the setup steps below to complete the prerequisites.

# COMMAND ----------

# DBTITLE 1,Step 1: Create FDI URL Table
# MAGIC %md
# MAGIC ## Step 1: Create FDI URL Table
# MAGIC
# MAGIC The FDI workflow needs URLs in a table format. We'll load them from the existing .md files.
# MAGIC
# MAGIC ### Schema
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.fdi_customs_documents_url (
# MAGIC   url STRING,
# MAGIC   sub_category STRING,
# MAGIC   created_at TIMESTAMP DEFAULT current_timestamp()
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Load Data
# MAGIC
# MAGIC You'll need to:
# MAGIC 1. Read URLs from [import_url.md](#file/125734742720937) and [export_url.md](#file/125734742720936)
# MAGIC 2. Parse each line as a URL
# MAGIC 3. Insert with appropriate `sub_category` ("Import Goods" or "Export Goods")
# MAGIC
# MAGIC **Run the SQL cell below** to create the table, then **run the Python cell** to load URLs.

# COMMAND ----------

# DBTITLE 1,Create fdi_customs_documents_url table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE market_data.customs.fdi_customs_documents_url (
# MAGIC   url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'FDI customs document URLs (3N/3X pattern) for Import and Export goods'

# COMMAND ----------

# DBTITLE 1,Load URLs from .md files
import re
from datetime import datetime

# Read import URLs
import_urls = []
with open('/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/1. Customs/IMEX Goods from FDI Monthly Data ETL/import_url.md', 'r') as f:
    for line in f:
        url = line.strip()
        # Skip empty lines and lines that don't start with http
        if url and url.startswith('http'):
            import_urls.append((url, 'Import Goods'))

print(f"Found {len(import_urls)} import URLs")

# Read export URLs
export_urls = []
with open('/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/1. Customs/IMEX Goods from FDI Monthly Data ETL/export_url.md', 'r') as f:
    for line in f:
        url = line.strip()
        if url and url.startswith('http'):
            export_urls.append((url, 'Export Goods'))

print(f"Found {len(export_urls)} export URLs")

# Combine and create DataFrame
all_urls = import_urls + export_urls
url_df = spark.createDataFrame(all_urls, ["url", "sub_category"])

# Insert into table
url_df.write.mode("append").saveAsTable("market_data.customs.fdi_customs_documents_url")

print(f"\n✓ Loaded {len(all_urls)} total URLs into fdi_customs_documents_url")

# Verify
display(spark.sql("SELECT sub_category, COUNT(*) as count FROM market_data.customs.fdi_customs_documents_url GROUP BY sub_category"))

# COMMAND ----------

# DBTITLE 1,Step 2: Create FDI Processing Tables
# MAGIC %md
# MAGIC ## Step 2: Create FDI Processing Tables
# MAGIC
# MAGIC We need three tables that mirror the original IMEX tables but with `fdi_` prefix.
# MAGIC
# MAGIC ### Tables to Create
# MAGIC
# MAGIC 1. **`fdi_document_processing_log`**: Tracks download, parse, and extraction status
# MAGIC 2. **`fdi_parsed_documents_raw`**: Stores raw AI parsing results (JSON)
# MAGIC 3. **`fdi_trade_statistics`**: Final structured trade statistics
# MAGIC
# MAGIC **Run the SQL cells below** to create these tables with the same schema as the originals.

# COMMAND ----------

# DBTITLE 1,Create fdi_document_processing_log
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.fdi_document_processing_log (
# MAGIC   document_id STRING NOT NULL,
# MAGIC   document_url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL,
# MAGIC   filename STRING,
# MAGIC   
# MAGIC   -- Download tracking
# MAGIC   download_status STRING,
# MAGIC   download_timestamp TIMESTAMP,
# MAGIC   download_attempts INT,
# MAGIC   download_error_message STRING,
# MAGIC   
# MAGIC   -- Parse tracking
# MAGIC   parse_status STRING,
# MAGIC   parse_timestamp TIMESTAMP,
# MAGIC   parse_error_message STRING,
# MAGIC   
# MAGIC   -- Extraction tracking
# MAGIC   extraction_status STRING,
# MAGIC   extraction_timestamp TIMESTAMP,
# MAGIC   extraction_error_message STRING,
# MAGIC   extraction_rows_inserted INT,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   created_at TIMESTAMP ,
# MAGIC   updated_at TIMESTAMP ,
# MAGIC   
# MAGIC   PRIMARY KEY (document_id)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'FDI document processing log - tracks download, parse, and extraction status'

# COMMAND ----------

# DBTITLE 1,Create fdi_parsed_documents_raw
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.fdi_parsed_documents_raw (
# MAGIC   document_id STRING NOT NULL,
# MAGIC   document_url STRING NOT NULL,
# MAGIC   sub_category STRING NOT NULL,
# MAGIC   filename STRING,
# MAGIC   volume_path STRING,
# MAGIC   
# MAGIC   -- Parsed content
# MAGIC   parsed_json STRING,
# MAGIC   
# MAGIC   -- Metadata extracted from document
# MAGIC   report_period STRING,
# MAGIC   report_start_date DATE,
# MAGIC   report_end_date DATE,
# MAGIC   
# MAGIC   -- Timestamps
# MAGIC   parsed_timestamp TIMESTAMP,
# MAGIC   
# MAGIC   PRIMARY KEY (document_id)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'FDI parsed documents - raw AI parsing results in JSON format'

# COMMAND ----------

# DBTITLE 1,Create fdi_trade_statistics
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.fdi_trade_statistics (
# MAGIC   sub_category STRING NOT NULL,
# MAGIC   document_id STRING NOT NULL,
# MAGIC   
# MAGIC   -- Report metadata
# MAGIC   report_period STRING,
# MAGIC   report_month STRING,
# MAGIC   report_start_date DATE,
# MAGIC   report_end_date DATE,
# MAGIC   
# MAGIC   -- Row identification
# MAGIC   row_number INT NOT NULL,
# MAGIC   
# MAGIC   -- Product information
# MAGIC   product_category STRING,
# MAGIC   parent_category STRING,
# MAGIC   unit STRING,
# MAGIC   
# MAGIC   -- Trade values
# MAGIC   period_quantity DECIMAL(20, 3),
# MAGIC   period_value_usd DECIMAL(20, 3),
# MAGIC   cumulative_quantity DECIMAL(20, 3),
# MAGIC   cumulative_value_usd DECIMAL(20, 3),
# MAGIC   
# MAGIC   -- Timestamp
# MAGIC   parsed_timestamp TIMESTAMP,
# MAGIC   
# MAGIC   PRIMARY KEY (sub_category, document_id, row_number)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'FDI trade statistics - structured import/export data by product category'

# COMMAND ----------

# DBTITLE 1,Step 3: Run the Workflow
# MAGIC %md
# MAGIC ## Step 3: Run the FDI Workflow
# MAGIC
# MAGIC Once the tables are created and URLs are loaded, run the notebooks in order:
# MAGIC
# MAGIC ### Execution Order
# MAGIC
# MAGIC 1. **[1_Download_FDI_Documents](#notebook/125734742720941)**
# MAGIC    - Downloads PDFs from FDI URLs (3N/3X pattern)
# MAGIC    - Saves to volumes: `fdi_import_goods` and `fdi_export_goods`
# MAGIC    - Updates: `fdi_document_processing_log`
# MAGIC
# MAGIC 2. **[2_Parse_FDI_Documents](#notebook/125734742720942)**
# MAGIC    - Parses PDFs using `ai_parse_document()`
# MAGIC    - Extracts tables and text from documents
# MAGIC    - Updates: `fdi_parsed_documents_raw` and `fdi_document_processing_log`
# MAGIC
# MAGIC 3. **[3_Extract_FDI_Statistics](#notebook/125734742720943)**
# MAGIC    - Transforms parsed JSON into structured records
# MAGIC    - Detects hierarchical categories
# MAGIC    - Updates: `fdi_trade_statistics` and `fdi_document_processing_log`
# MAGIC
# MAGIC ### Monitoring
# MAGIC
# MAGIC Check progress with:
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   sub_category,
# MAGIC   download_status,
# MAGIC   parse_status,
# MAGIC   extraction_status,
# MAGIC   COUNT(*) as count
# MAGIC FROM market_data.customs.fdi_document_processing_log
# MAGIC GROUP BY sub_category, download_status, parse_status, extraction_status
# MAGIC ORDER BY sub_category;
# MAGIC ```
# MAGIC
# MAGIC ### Verification
# MAGIC
# MAGIC Verify final data:
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   sub_category,
# MAGIC   report_month,
# MAGIC   COUNT(DISTINCT document_id) as documents,
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(period_value_usd) as total_value_usd
# MAGIC FROM market_data.customs.fdi_trade_statistics
# MAGIC GROUP BY sub_category, report_month
# MAGIC ORDER BY report_month DESC, sub_category;
# MAGIC ```