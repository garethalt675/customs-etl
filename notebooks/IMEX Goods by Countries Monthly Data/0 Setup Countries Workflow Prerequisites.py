# Databricks notebook source
# DBTITLE 1,Countries Workflow Setup Overview
# MAGIC %md
# MAGIC # Countries IMEX Workflow - Setup Prerequisites
# MAGIC
# MAGIC This notebook sets up the necessary tables, volumes, and URLs for the **Countries IMEX Goods Monthly Data ETL** workflow.
# MAGIC
# MAGIC ## Workflow Purpose
# MAGIC Extract trade statistics from Vietnamese customs PDFs, organized by **partner country** (origin for imports, destination for exports).
# MAGIC
# MAGIC ## URL Patterns
# MAGIC - **Import (5N)**: Trade by country of origin
# MAGIC - **Export (5X)**: Trade by country of destination
# MAGIC
# MAGIC **Note**: The `import_url.md` file contains 5X patterns (typically export). Verify this is intentional before loading.
# MAGIC
# MAGIC ## Components
# MAGIC 1. **URL Table**: `countries_customs_documents_url` - Source URLs
# MAGIC 2. **Processing Log**: `countries_document_processing_log` - Download/parse/extract tracking
# MAGIC 3. **Parsed Raw**: `countries_parsed_documents_raw` - JSON storage
# MAGIC 4. **Statistics**: `countries_trade_statistics` - Final structured data with country dimension
# MAGIC
# MAGIC ## Volumes
# MAGIC - `market_data.customs.countries_import_goods`
# MAGIC - `market_data.customs.countries_export_goods`
# MAGIC
# MAGIC ## Execution Order
# MAGIC 1. Run this setup notebook **once**
# MAGIC 2. `1_Download_Countries_Documents`
# MAGIC 3. `2_Parse_Countries_Documents`
# MAGIC 4. `3_Extract_Countries_Statistics`

# COMMAND ----------

# DBTITLE 1,Create URL Table
# MAGIC %sql
# MAGIC -- Table 1: URL source table
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.countries_customs_documents_url (
# MAGIC   url STRING COMMENT 'URL to download the PDF document',
# MAGIC   sub_category STRING COMMENT 'Category: Import by Origin or Export by Destination',
# MAGIC   created_at TIMESTAMP COMMENT 'Timestamp when URL was added'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'List of URLs for Countries customs documents to be processed'
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# DBTITLE 1,Create Processing Log Table
# MAGIC %sql
# MAGIC -- Table 2: Processing log for tracking status
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.countries_document_processing_log (
# MAGIC   document_id STRING COMMENT 'Unique document identifier (hash of URL)',
# MAGIC   document_url STRING COMMENT 'Original PDF URL',
# MAGIC   sub_category STRING COMMENT 'Category: Import by Origin or Export by Destination',
# MAGIC   
# MAGIC   -- Download tracking
# MAGIC   download_status STRING COMMENT 'Status: pending, success, failed',
# MAGIC   download_timestamp TIMESTAMP COMMENT 'When download completed',
# MAGIC   download_error_message STRING COMMENT 'Error message if download failed',
# MAGIC   downloaded_file_path STRING COMMENT 'Path to downloaded PDF in volume',
# MAGIC   
# MAGIC   -- Parse tracking
# MAGIC   parse_status STRING COMMENT 'Status: pending, success, failed',
# MAGIC   parse_timestamp TIMESTAMP COMMENT 'When parsing completed',
# MAGIC   parse_error_message STRING COMMENT 'Error message if parsing failed',
# MAGIC   parsed_tables_count INT COMMENT 'Number of tables extracted',
# MAGIC   
# MAGIC   -- Extraction tracking
# MAGIC   extraction_status STRING COMMENT 'Status: pending, success, failed',
# MAGIC   extraction_timestamp TIMESTAMP COMMENT 'When extraction completed',
# MAGIC   extraction_error_message STRING COMMENT 'Error message if extraction failed',
# MAGIC   extraction_rows_inserted INT COMMENT 'Number of rows inserted into statistics table',
# MAGIC   
# MAGIC   created_at TIMESTAMP COMMENT 'When record was created',
# MAGIC   updated_at TIMESTAMP COMMENT 'Last update timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Processing log for Countries customs documents workflow'
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# DBTITLE 1,Create Parsed Raw Table
# MAGIC %sql
# MAGIC -- Table 3: Raw parsed data storage
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.countries_parsed_documents_raw (
# MAGIC   document_id STRING COMMENT 'Unique document identifier (hash of URL)',
# MAGIC   document_url STRING COMMENT 'Original PDF URL',
# MAGIC   sub_category STRING COMMENT 'Category: Import by Origin or Export by Destination',
# MAGIC   parsed_json STRING COMMENT 'Raw JSON output from ai_parse_document()',
# MAGIC   parsed_timestamp TIMESTAMP COMMENT 'When document was parsed',
# MAGIC   
# MAGIC   PRIMARY KEY (document_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Raw parsed JSON data from Countries customs PDFs'
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# DBTITLE 1,Create Statistics Table
# MAGIC %sql
# MAGIC -- Table 4: Final structured statistics table
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.countries_trade_statistics (
# MAGIC   sub_category STRING COMMENT 'Category: Import by Origin or Export by Destination',
# MAGIC   document_id STRING COMMENT 'Source document identifier',
# MAGIC   report_period STRING COMMENT 'Report period text (e.g., Tháng 01/2025)',
# MAGIC   report_month STRING COMMENT 'Report month (e.g., 01/2025)',
# MAGIC   report_start_date DATE COMMENT 'Report period start date',
# MAGIC   report_end_date DATE COMMENT 'Report period end date',
# MAGIC   row_number INT COMMENT 'Row number in source document',
# MAGIC   country_name STRING COMMENT 'Partner country name (origin for imports, destination for exports)',
# MAGIC   unit STRING COMMENT 'Measurement unit',
# MAGIC   period_quantity DECIMAL(20, 3) COMMENT 'Quantity for current period',
# MAGIC   period_value_usd DECIMAL(20, 3) COMMENT 'Value in USD for current period',
# MAGIC   cumulative_quantity DECIMAL(20, 3) COMMENT 'Cumulative quantity',
# MAGIC   cumulative_value_usd DECIMAL(20, 3) COMMENT 'Cumulative value in USD',
# MAGIC   parsed_timestamp TIMESTAMP COMMENT 'When data was extracted',
# MAGIC   
# MAGIC   PRIMARY KEY (sub_category, document_id, row_number)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Structured trade statistics by country from Countries customs documents'
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# DBTITLE 1,Create Volumes
# Create volumes for storing downloaded PDFs
print("Creating volumes for Countries workflow...\n")

try:
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS market_data.customs.countries_import_goods
        COMMENT 'Storage for Countries import customs PDF documents'
    """)
    print("✓ Created volume: market_data.customs.countries_import_goods")
except Exception as e:
    print(f"⚠ Volume creation note: {str(e)}")

try:
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS market_data.customs.countries_export_goods
        COMMENT 'Storage for Countries export customs PDF documents'
    """)
    print("✓ Created volume: market_data.customs.countries_export_goods")
except Exception as e:
    print(f"⚠ Volume creation note: {str(e)}")

print("\nVolume setup complete!")

# COMMAND ----------

# DBTITLE 1,Load Import URLs
# Load URLs from import_url.md file
import os
import re
from datetime import datetime

print("Loading import URLs...\n")

# Path to import_url.md in this workflow directory
import_url_file = "/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/1. Customs/IMEX Goods to Countries Monthly Data ETL/import_url.md"

try:
    with open(import_url_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract URLs (lines starting with http)
    urls = [line.strip() for line in content.split('\n') if line.strip().startswith('http')]
    
    print(f"Found {len(urls)} URLs in import_url.md")
    
    # Check for URL patterns
    pattern_5n = sum(1 for url in urls if '/5N' in url)
    pattern_5x = sum(1 for url in urls if '/5X' in url)
    
    print(f"  Pattern 5N (import by origin): {pattern_5n}")
    print(f"  Pattern 5X (export by destination): {pattern_5x}")
    
    if pattern_5x > 0:
        print("\n⚠ WARNING: import_url.md contains 5X patterns (typically export).")
        print("  Please verify this is intentional before loading.")
    
    if len(urls) > 0:
        # Prepare data for insertion
        import_data = [(url, "Import by Origin", datetime.now()) for url in urls]
        import_df = spark.createDataFrame(import_data, ["url", "sub_category", "created_at"])
        
        # Insert into table
        import_df.write.mode("append").saveAsTable("market_data.customs.countries_customs_documents_url")
        print(f"\n✓ Loaded {len(urls)} import URLs into countries_customs_documents_url table")
    else:
        print("\n⚠ No URLs found in import_url.md")
        
except FileNotFoundError:
    print(f"✗ File not found: {import_url_file}")
    print("  Please ensure import_url.md exists in the workflow directory")
except Exception as e:
    print(f"✗ Error loading import URLs: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Load Export URLs
# Load URLs from export_url.md file
print("Loading export URLs...\n")

# Path to export_url.md in this workflow directory
export_url_file = "/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/1. Customs/IMEX Goods to Countries Monthly Data ETL/export_url.md"

try:
    with open(export_url_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract URLs (lines starting with http)
    urls = [line.strip() for line in content.split('\n') if line.strip().startswith('http')]
    
    print(f"Found {len(urls)} URLs in export_url.md")
    
    # Check for URL patterns
    pattern_5n = sum(1 for url in urls if '/5N' in url)
    pattern_5x = sum(1 for url in urls if '/5X' in url)
    
    print(f"  Pattern 5N (import by origin): {pattern_5n}")
    print(f"  Pattern 5X (export by destination): {pattern_5x}")
    
    if pattern_5n > 0:
        print("\n⚠ WARNING: export_url.md contains 5N patterns (typically import).")
        print("  Please verify this is intentional before loading.")
    
    if len(urls) > 0:
        # Prepare data for insertion
        export_data = [(url, "Export by Destination", datetime.now()) for url in urls]
        export_df = spark.createDataFrame(export_data, ["url", "sub_category", "created_at"])
        
        # Insert into table
        export_df.write.mode("append").saveAsTable("market_data.customs.countries_customs_documents_url")
        print(f"\n✓ Loaded {len(urls)} export URLs into countries_customs_documents_url table")
    else:
        print("\n⚠ No URLs found in export_url.md")
        
except FileNotFoundError:
    print(f"✗ File not found: {export_url_file}")
    print("  Please ensure export_url.md exists in the workflow directory")
except Exception as e:
    print(f"✗ Error loading export URLs: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Verify Setup
# MAGIC %sql
# MAGIC -- Verify all components are created
# MAGIC SELECT 
# MAGIC   'URL Table' as component,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT sub_category) as categories
# MAGIC FROM market_data.customs.countries_customs_documents_url
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Processing Log' as component,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT sub_category) as categories
# MAGIC FROM market_data.customs.countries_document_processing_log
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Parsed Raw' as component,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT sub_category) as categories
# MAGIC FROM market_data.customs.countries_parsed_documents_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Statistics' as component,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT sub_category) as categories
# MAGIC FROM market_data.customs.countries_trade_statistics;

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ### Execution Order
# MAGIC 1. ✓ **Run this setup notebook once** (you just completed this)
# MAGIC 2. Run [1_Download_Countries_Documents](#notebook-1855633332224046) to download PDFs
# MAGIC 3. Run [2_Parse_Countries_Documents](#notebook-1855633332224047) to extract tables using AI
# MAGIC 4. Run [3_Extract_Countries_Statistics](#notebook-1855633332224048) to transform into structured data
# MAGIC
# MAGIC ### Key Differences from Other Workflows
# MAGIC - **URL Pattern**: 5N (import by origin), 5X (export by destination)
# MAGIC - **Dimension**: Country name instead of product category
# MAGIC - **Schema**: Includes `country_name` field
# MAGIC - **Function**: Uses `process_countries_table_rows()` instead of `process_table_rows()`
# MAGIC
# MAGIC ### Monitoring
# MAGIC - Check processing status: `SELECT * FROM market_data.customs.countries_document_processing_log`
# MAGIC - View statistics: `SELECT * FROM market_data.customs.countries_trade_statistics LIMIT 10`
# MAGIC
# MAGIC ---
# MAGIC **Setup complete! Ready to run the workflow.**