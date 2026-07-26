# Databricks notebook source
# DBTITLE 1,Parse Transportation Documents - Overview
# MAGIC %md
# MAGIC # Step 2: Parse Customs Documents with AI - Transportation Quarterly
# MAGIC
# MAGIC This notebook uses `ai_parse_document()` to extract text and tables from quarterly PDF files stored in the Unity Catalog volume for both Import and Export by mode of transport.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read PDFs from volume**: Access files from both subdirectories:
# MAGIC    - `/Volumes/market_data/customs/transportation_download_docs/import_by_transportation/`
# MAGIC    - `/Volumes/market_data/customs/transportation_download_docs/export_by_transportation/`
# MAGIC 2. **Extract sub_category**: Identify category from file path
# MAGIC 3. **Parse with AI**: Use `ai_parse_document()` to extract structured content
# MAGIC 4. **Store raw results**: Save full JSON output with `sub_category` to `market_data.customs.transportation_parsed_documents_raw`
# MAGIC 5. **Update tracking**: Mark parse status in processing log
# MAGIC
# MAGIC ## AI Parsing
# MAGIC
# MAGIC The `ai_parse_document()` function extracts:
# MAGIC * **Text**: Full document text content
# MAGIC * **Tables**: Structured data tables from the PDF (product group x transport mode)
# MAGIC * **Metadata**: Document properties and structure
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The raw table stores:
# MAGIC * Complete JSON from `ai_parse_document()`
# MAGIC * `sub_category`: "Import by Transportation" or "Export by Transportation"
# MAGIC * Basic metadata extracted from headers
# MAGIC * Parsing timestamp
# MAGIC
# MAGIC The processing log is updated with:
# MAGIC * `parse_status`: 'success' or 'failed'
# MAGIC * `parse_timestamp`: When parsing completed
# MAGIC * `extraction_status`: Set to 'pending' for next step

# COMMAND ----------

# DBTITLE 1,Configure Transportation Volume Path
VOLUME_BASE_PATH = '/Volumes/market_data/customs/transportation_download_docs/'
CATEGORIES = ["Import by Transportation", "Export by Transportation"]

for category in CATEGORIES:
    category_subdir = category.lower().replace(' ', '_')
    volume_path = f"{VOLUME_BASE_PATH}{category_subdir}/"
    print(f"{category}: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Parse Transportation Documents with ai_parse_document()
# Parse all successfully downloaded documents from both Import and Export subdirectories
for category in CATEGORIES:
    category_subdir = category.lower().replace(' ', '_')
    volume_path = f"{VOLUME_BASE_PATH}{category_subdir}/"

    print(f"\nProcessing {category}...")
    print(f"Volume path: {volume_path}")

    # read_files() raises on a missing path, and cannot infer a schema from an
    # empty one - both are normal for a flow with nothing new downloaded.
    try:
        entries = [f for f in dbutils.fs.ls(volume_path) if f.size > 0]
    except Exception:
        entries = []
    if not entries:
        print(f"⊘ No files at {volume_path}, skipping {category}")
        continue

    spark.sql(f"""
    MERGE INTO market_data.customs.transportation_parsed_documents_raw AS target
    USING (
      SELECT
        log.document_id,
        log.document_url,
        log.sub_category,
        log.filename,
        files.path AS volume_path,
        
        ai_parse_document(
          files.content,
          map('version', '2.0')
        ) AS parsed_json,
        
        NULL AS report_period,
        NULL AS report_start_date,
        NULL AS report_end_date,
        
        current_timestamp() AS parsed_timestamp
        
      FROM read_files(
        '{volume_path}',
        format => 'binaryFile'
      ) AS files
      
      INNER JOIN market_data.customs.transportation_document_processing_log AS log
        ON regexp_extract(files.path, '([^/]+)$', 1) = log.filename
      
      WHERE log.download_status = 'success'
        AND log.sub_category = '{category}'
        AND (log.parse_status IS NULL OR log.parse_status IN ('pending', 'failed'))
        
    ) AS source
    ON target.document_id = source.document_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✓ {category} parsing complete")

# COMMAND ----------

# DBTITLE 1,Update Processing Log with Parse Status
from pyspark.sql.functions import col, current_timestamp, lit

print("="*80)
print("UPDATING PROCESSING LOG")
print("="*80)

recently_parsed = spark.sql("""
    SELECT document_id, sub_category, parsed_timestamp
    FROM market_data.customs.transportation_parsed_documents_raw
    WHERE parsed_timestamp >= current_timestamp() - INTERVAL 1 HOUR
""")

parse_count = recently_parsed.count()
print(f"\nDocuments parsed in this run: {parse_count}")

if parse_count > 0:
    spark.sql("""
        MERGE INTO market_data.customs.transportation_document_processing_log AS target
        USING (
            SELECT
                document_id,
                'success' AS parse_status,
                parsed_timestamp AS parse_timestamp,
                CAST(NULL AS STRING) AS parse_error_message,
                'pending' AS extraction_status,
                current_timestamp() AS updated_at
            FROM market_data.customs.transportation_parsed_documents_raw
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
    
    summary = spark.sql("""
        SELECT
            sub_category,
            parse_status,
            COUNT(*) as count
        FROM market_data.customs.transportation_document_processing_log
        WHERE parse_timestamp IS NOT NULL
        GROUP BY sub_category, parse_status
        ORDER BY sub_category, parse_status
    """)
    
    print("\nParsing Summary by Category:")
    display(summary)
else:
    print("\n⚠ No documents were parsed in this run")

print("\n" + "="*80)
print("Parsing workflow complete!")
print("="*80)