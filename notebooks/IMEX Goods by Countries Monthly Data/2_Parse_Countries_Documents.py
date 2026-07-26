# Databricks notebook source
# DBTITLE 1,Parse Countries Documents - Overview
# MAGIC %md   
# MAGIC # Step 2: Parse IMEX to Countries Documents with AI
# MAGIC
# MAGIC This notebook uses `ai_parse_document()` to extract text and tables from IMEX to Countries PDF files (5N/5X pattern) stored in Unity Catalog volumes.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read PDFs from volumes**: Access files from countries-specific volumes:
# MAGIC    - `/Volumes/market_data/customs/countries_import_goods/`
# MAGIC    - `/Volumes/market_data/customs/countries_export_goods/`
# MAGIC 2. **Match with processing log**: Join files with their metadata using `downloaded_file_path`
# MAGIC 3. **Parse with AI**: Use `ai_parse_document()` to extract structured content
# MAGIC 4. **Store raw results**: Save full JSON output to `market_data.customs.countries_parsed_documents_raw`
# MAGIC 5. **Update tracking**: Mark parse status in `market_data.customs.countries_document_processing_log`
# MAGIC
# MAGIC ## AI Parsing
# MAGIC
# MAGIC The `ai_parse_document()` function extracts:
# MAGIC * **Text**: Full document text content
# MAGIC * **Tables**: Structured data tables from the PDF
# MAGIC * **Metadata**: Document properties and structure
# MAGIC
# MAGIC ## Data Type: IMEX by Countries
# MAGIC
# MAGIC * **Same parsing logic**: PDF structure is consistent across all IMEX variants
# MAGIC * **Different tables**: All data goes to `countries_` prefixed tables
# MAGIC * **Category values**: "Import by Origin" (5N) and "Export by Destination" (5X)
# MAGIC * **Additional dimension**: Country-level breakdown in the parsed tables
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The raw table (`market_data.customs.countries_parsed_documents_raw`) stores:
# MAGIC * Complete JSON from `ai_parse_document()`
# MAGIC * `sub_category`: "Import by Origin" or "Export by Destination"
# MAGIC * Parsing timestamp
# MAGIC
# MAGIC The processing log is updated with:
# MAGIC * `parse_status`: 'success' or 'failed'
# MAGIC * `parse_timestamp`: When parsing completed
# MAGIC * `extraction_status`: Set to 'pending' for next step

# COMMAND ----------

# DBTITLE 1,Configuration
VOLUME_BASE_PATH = '/Volumes/market_data/customs/'
CATEGORIES = ["Import by Origin", "Export by Destination"]

# COMMAND ----------

# DBTITLE 1,Parse Countries Documents with ai_parse_document()
# Parse all successfully downloaded Countries documents from both Import and Export volumes in batches of 10
for category in CATEGORIES:
    print("="*80)
    print(f"PARSING CATEGORY: {category}")
    print("="*80)
    
    # Determine volume path
    if category == "Import by Origin":
        volume_path = f"{VOLUME_BASE_PATH}countries_import_goods/"
    else:  # Export by Destination
        volume_path = f"{VOLUME_BASE_PATH}countries_export_goods/"
    
    print(f"Volume path: {volume_path}\n")

    # read_files() raises on a missing path and cannot infer a schema from an
    # empty one - both normal when nothing new has been downloaded.
    try:
        entries = [f for f in dbutils.fs.ls(volume_path) if f.size > 0]
    except Exception:
        entries = []
    if not entries:
        print(f"⊘ No files at {volume_path}, skipping {category}")
        continue

    # Get document paths to process in batches of 10
    docs_to_parse = spark.sql(f"""
        SELECT log.document_id, log.document_url, log.sub_category, files.path AS volume_path, files.content
        FROM read_files('{volume_path}', format => 'binaryFile') AS files
        INNER JOIN market_data.customs.countries_document_processing_log AS log
            ON files.path = log.downloaded_file_path
        WHERE log.download_status = 'success'
            AND log.sub_category = '{category}'
            AND (log.parse_status IS NULL OR log.parse_status IN ('pending', 'failed'))
    """)
    
    doc_count = docs_to_parse.count()
    print(f"Total documents to parse: {doc_count}")
    
    if doc_count == 0:
        print(f"⚠ No documents to parse for category: {category}\n")
        continue
    
    for start in range(0, doc_count, 10):
        batch = docs_to_parse.offset(start).limit(10)
        batch.createOrReplaceTempView("batch_docs")
        
        spark.sql("""
        MERGE INTO market_data.customs.countries_parsed_documents_raw AS target
        USING (
          SELECT
            document_id,
            document_url,
            sub_category,
            volume_path,
            ai_parse_document(
              content,
              map('version', '2.0')
            ) AS parsed_json,
            current_timestamp() AS parsed_timestamp
          FROM batch_docs
        ) AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
        
        print(f"✓ Parsed batch {start+1}-{min(start+10, doc_count)} for {category}")
    
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
    FROM market_data.customs.countries_parsed_documents_raw
    WHERE parsed_timestamp <= current_timestamp()
""")

parse_count = recently_parsed.count()
print(f"\nDocuments parsed in this run: {parse_count}")

if parse_count > 0:
    # Update processing log
    spark.sql("""
        MERGE INTO market_data.customs.countries_document_processing_log AS target
        USING (
            SELECT
                document_id,
                'success' AS parse_status,
                parsed_timestamp AS parse_timestamp,
                CAST(NULL AS STRING) AS parse_error_message,
                'pending' AS extraction_status,
                current_timestamp() AS updated_at
            FROM market_data.customs.countries_parsed_documents_raw
            WHERE parsed_timestamp <= current_timestamp()
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
        FROM market_data.customs.countries_document_processing_log
        WHERE parse_timestamp IS NOT NULL
        GROUP BY sub_category, parse_status
        ORDER BY sub_category, parse_status
    """)
    
    print("\nParsing Summary by Category:")
    display(summary)
else:
    print("\n⚠ No documents were parsed in this run")

print("\n" + "="*80)
print("Countries Parsing workflow complete!")
print("="*80)