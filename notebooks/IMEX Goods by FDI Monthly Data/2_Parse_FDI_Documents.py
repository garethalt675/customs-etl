# Databricks notebook source
# DBTITLE 1,Parse FDI Documents - Overview
# MAGIC %md
# MAGIC # Step 2: Parse FDI Customs Documents with AI
# MAGIC
# MAGIC This notebook uses `ai_parse_document()` to extract text and tables from FDI PDF files (3N/3X pattern) stored in Unity Catalog volumes.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read PDFs from volumes**: Access files from FDI-specific volumes:
# MAGIC    - `/Volumes/market_data/customs/fdi_import_goods/`
# MAGIC    - `/Volumes/market_data/customs/fdi_export_goods/`
# MAGIC 2. **Extract sub_category**: Identify category from file path
# MAGIC 3. **Parse with AI**: Use `ai_parse_document()` to extract structured content
# MAGIC 4. **Store raw results**: Save full JSON output to `market_data.customs.fdi_parsed_documents_raw`
# MAGIC 5. **Update tracking**: Mark parse status in `market_data.customs.fdi_document_processing_log`
# MAGIC
# MAGIC ## AI Parsing
# MAGIC
# MAGIC The `ai_parse_document()` function extracts:
# MAGIC * **Text**: Full document text content
# MAGIC * **Tables**: Structured data tables from the PDF
# MAGIC * **Metadata**: Document properties and structure
# MAGIC
# MAGIC ## FDI vs. Regular IMEX
# MAGIC
# MAGIC * **Same parsing logic**: PDF structures are identical
# MAGIC * **Different tables**: All data goes to `fdi_` prefixed tables
# MAGIC * **Same sub_category values**: "Import Goods" and "Export Goods"
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The raw table (`market_data.customs.fdi_parsed_documents_raw`) stores:
# MAGIC * Complete JSON from `ai_parse_document()`
# MAGIC * `sub_category`: "Import Goods" or "Export Goods"
# MAGIC * Parsing timestamp
# MAGIC
# MAGIC The processing log is updated with:
# MAGIC * `parse_status`: 'success' or 'failed'
# MAGIC * `parse_timestamp`: When parsing completed
# MAGIC * `extraction_status`: Set to 'pending' for next step

# COMMAND ----------

# DBTITLE 1,Configuration
VOLUME_BASE_PATH = '/Volumes/market_data/customs/'
CATEGORIES = ["Import Goods", "Export Goods"]

# COMMAND ----------

# DBTITLE 1,Parse FDI Documents with ai_parse_document()
# Parse all successfully downloaded FDI documents from both Import and Export volumes
for category in CATEGORIES:
    print("="*80)
    print(f"PARSING CATEGORY: {category}")
    print("="*80)
    
    # Determine volume path
    if category == "Import Goods":
        volume_path = f"{VOLUME_BASE_PATH}fdi_import_goods/"
    else:  # Export Goods
        volume_path = f"{VOLUME_BASE_PATH}fdi_export_goods/"
    
    print(f"Volume path: {volume_path}\n")
    
    spark.sql(f"""
    MERGE INTO market_data.customs.fdi_parsed_documents_raw AS target
    USING (
      SELECT
        log.document_id,
        log.document_url,
        log.sub_category,
        log.filename,
        files.path AS volume_path,
        
        -- Parse the PDF using AI
        ai_parse_document(
          files.content,
          map('version', '2.0')
        ) AS parsed_json,
        
        -- Metadata will be extracted in next step
        NULL AS report_period,
        NULL AS report_start_date,
        NULL AS report_end_date,
        
        current_timestamp() AS parsed_timestamp
        
      FROM read_files(
        '{volume_path}',
        format => 'binaryFile'
      ) AS files
      
      INNER JOIN market_data.customs.fdi_document_processing_log AS log
        ON regexp_extract(files.path, '([^/]+)$', 1) = log.filename
      
      WHERE log.download_status = 'success'
        AND log.sub_category = '{category}'
        AND (log.parse_status IS NULL OR log.parse_status IN ('pending', 'failed'))
        
    ) AS source
    ON target.document_id = source.document_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✓ {category} parsing complete\n")

# COMMAND ----------

# DBTITLE 1,Update Processing Log with Parse Status
from pyspark.sql.functions import col, current_timestamp, lit

print("="*80)
print("UPDATING PROCESSING LOG")
print("="*80)

# Get recently parsed documents
recently_parsed = spark.sql("""
    SELECT document_id, sub_category, parsed_timestamp
    FROM market_data.customs.fdi_parsed_documents_raw
    WHERE parsed_timestamp >= current_timestamp() - INTERVAL 1 HOUR
""")

parse_count = recently_parsed.count()
print(f"\nDocuments parsed in this run: {parse_count}")

if parse_count > 0:
    # Update processing log
    spark.sql("""
        MERGE INTO market_data.customs.fdi_document_processing_log AS target
        USING (
            SELECT
                document_id,
                'success' AS parse_status,
                parsed_timestamp AS parse_timestamp,
                CAST(NULL AS STRING) AS parse_error_message,
                'pending' AS extraction_status,
                current_timestamp() AS updated_at
            FROM market_data.customs.fdi_parsed_documents_raw
            WHERE parsed_timestamp >= current_timestamp() - INTERVAL 1 HOUR
        ) AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET
            parse_status = source.parse_status,
            parse_timestamp = source.parse_timestamp,
            parse_error_message = source.parse_error_message,
            extraction_status = source.extraction_status,
            updated_at = source.updated_at
    """)
    
    print("✓ Processing log updated successfully")
    
    # Show summary by category
    summary = spark.sql("""
        SELECT
            sub_category,
            parse_status,
            COUNT(*) as count
        FROM market_data.customs.fdi_document_processing_log
        WHERE parse_timestamp IS NOT NULL
        GROUP BY sub_category, parse_status
        ORDER BY sub_category, parse_status
    """)
    
    print("\nParsing Summary by Category:")
    display(summary)
else:
    print("\n⚠ No documents were parsed in this run")

print("\n" + "="*80)
print("FDI Parsing workflow complete!")
print("="*80)