# Databricks notebook source
# DBTITLE 1,Download Provinces Documents - Overview
# MAGIC %md
# MAGIC # Step 1: Download Provinces Customs Documents
# MAGIC
# MAGIC This notebook downloads PDF documents for **Provincial Import/Export goods** (4 pattern) from URLs and saves them to Unity Catalog volumes.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read URL table**: Get all document URLs from `market_data.customs.provinces_customs_documents_url` filtered by `sub_category`
# MAGIC 2. **Process by category**: Loop through each sub_category independently
# MAGIC 3. **Check processing log**: Identify documents that haven't been downloaded or failed previously
# MAGIC 4. **Download with retry**: Fetch each PDF with up to 3 retry attempts
# MAGIC 5. **Save to volume**: Store files in Provinces-specific volumes:
# MAGIC    - Import: `/Volumes/market_data/customs/provinces_import_goods/`
# MAGIC    - Export: `/Volumes/market_data/customs/provinces_export_goods/`
# MAGIC 6. **Update tracking**: Log success/failure status in `market_data.customs.provinces_document_processing_log`
# MAGIC
# MAGIC ## Provincial Patterns
# MAGIC
# MAGIC * **URL Pattern**: Provincial documents use `4(vn-sb)`, `4(VN-DC)`, `4(VN-CT)` in filenames
# MAGIC * **Data type**: Provincial-level import/export trade statistics
# MAGIC * **Separate tables**: All Provinces data uses `provinces_` prefixed tables
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC * **Category-level isolation**: If Import processing fails, Export still runs
# MAGIC * **Network errors**: Retry up to 3 times with exponential backoff
# MAGIC * **HTTP errors**: Log specific error codes (404, 403, 500, etc.)
# MAGIC * **Summary report**: Display per-category statistics at the end
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The processing log (`market_data.customs.provinces_document_processing_log`) tracks:
# MAGIC * `sub_category`: "Import Goods" or "Export Goods"
# MAGIC * `download_status`: 'success' or 'failed'
# MAGIC * `download_timestamp`: When the download was attempted
# MAGIC * `download_attempts`: Number of retry attempts
# MAGIC * `download_error_message`: Error details if failed

# COMMAND ----------

# DBTITLE 1,Download Provinces Documents from URL Table
import requests
import time
from datetime import datetime
from urllib.parse import unquote, urlparse
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import hashlib

# Configuration for Provinces documents.
# The monthly province report is a single PDF holding both trade flows, so there
# is one category and one volume rather than an import/export split.
VOLUME_BASE_PATH = '/Volumes/market_data/customs/'
CATEGORIES = ["Provinces IMEX"]
VOLUME_BY_CATEGORY = {"Provinces IMEX": "provinces_imex_docs"}
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_TIMEOUT = 60

print("="*80)
print("PROVINCES CUSTOMS DOCUMENTS DOWNLOAD WORKFLOW")
print("Processing Provincial Import/Export goods")
print("="*80)

def generate_document_id(url):
    """Generate unique document ID from URL hash."""
    return hashlib.md5(url.encode()).hexdigest()[:16]

def download_document_with_retry(url, file_path, max_retries=MAX_RETRIES):
    """
    Download document with retry logic.
    
    Returns:
        tuple: (success: bool, file_path: str or None, error_message: str or None, attempts: int)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://www.customs.gov.vn/'
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Attempt {attempt}/{max_retries}: Downloading...")
            
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            if len(response.content) == 0:
                raise ValueError("Downloaded file is empty")
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            file_size_kb = len(response.content) / 1024
            print(f"  ✓ Downloaded successfully: {file_size_kb:.2f} KB")
            
            return (True, file_path, None, attempt)
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP {e.response.status_code}"
            print(f"  ✗ Attempt {attempt} failed: {error_msg}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
            else:
                return (False, None, error_msg, attempt)
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error: {str(e)[:100]}"
            print(f"  ✗ Attempt {attempt} failed: {error_msg}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
            else:
                return (False, None, error_msg, attempt)
                
        except Exception as e:
            error_msg = str(e)[:200]
            print(f"  ✗ Attempt {attempt} failed: {error_msg}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
            else:
                return (False, None, error_msg, attempt)
    
    return (False, None, "Max retries exceeded", max_retries)

# Load URL table and processing log
print("\nLoading URL table and processing log...")
all_urls_df = spark.table("market_data.customs.provinces_customs_documents_url")
processing_log_df = spark.table("market_data.customs.provinces_document_processing_log")

category_summaries = {}
all_results = []

# Define schema for results
schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("document_url", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("filename", StringType(), True),
    StructField("download_status", StringType(), True),
    StructField("download_timestamp", TimestampType(), True),
    StructField("download_attempts", IntegerType(), True),
    StructField("download_error_message", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

print("✓ Setup complete\n")

# COMMAND ----------

# DBTITLE 1,Process Each Category
# Main loop: Process each category
for category in CATEGORIES:
    print("\n" + "="*80)
    print(f"PROCESSING CATEGORY: {category}")
    print("="*80)
    
    try:
        # Determine volume path based on category
        volume_path = f"{VOLUME_BASE_PATH}{VOLUME_BY_CATEGORY[category]}/"

        print(f"\nVolume path: {volume_path}")

        # A fresh volume has no directory entry until something is written.
        dbutils.fs.mkdirs(volume_path)
        
        # Step 1: Get URLs for this category
        print(f"\nStep 1: Loading URLs for {category}...")
        print("-" * 80)
        
        category_urls_df = all_urls_df.filter(col("sub_category") == category)
        total_category_urls = category_urls_df.count()
        print(f"Total URLs in source table for {category}: {total_category_urls}")
        
        if total_category_urls == 0:
            print(f"⚠ No URLs found for {category}, skipping...")
            category_summaries[category] = {"processed": 0, "success": 0, "failed": 0, "error": "No URLs found"}
            continue
        
        # Filter out successfully downloaded documents
        if processing_log_df.count() > 0:
            processed_urls = processing_log_df.filter(
                (col("sub_category") == category) & 
                (col("download_status") == "success")
            ).select("document_url").distinct()
            
            urls_to_process = category_urls_df.join(processed_urls, category_urls_df.url == processed_urls.document_url, "left_anti")
        else:
            urls_to_process = category_urls_df
        
        urls_list = urls_to_process.select("url").distinct().collect()
        print(f"URLs to download for {category}: {len(urls_list)}")
        
        if len(urls_list) == 0:
            print(f"\n✓ No new documents to download for {category}!")
            category_summaries[category] = {"processed": 0, "success": 0, "failed": 0}
            continue
        
        # Step 2: Download each document
        print(f"\nStep 2: Downloading documents...")
        print("-" * 80)
        
        success_count = 0
        failed_count = 0
        
        for idx, row in enumerate(urls_list, 1):
            url = row['url']
            doc_id = generate_document_id(url)
            
            # Extract filename from URL
            parsed_url = urlparse(url)
            filename = unquote(parsed_url.path.split('/')[-1])
            
            if not filename or filename == '':
                filename = f"{doc_id}.pdf"
            
            print(f"\n[{idx}/{len(urls_list)}] Processing: {filename}")
            print(f"  URL: {url}")
            print(f"  Document ID: {doc_id}")
            
            # Full file path in volume
            file_path = f"{volume_path}{filename}"
            
            # Download with retry
            success, result_path, error_msg, attempts = download_document_with_retry(url, file_path)
            
            # Record result
            now = datetime.now()
            result = {
                "document_id": doc_id,
                "document_url": url,
                "sub_category": category,
                "filename": filename,
                "download_status": "success" if success else "failed",
                "download_timestamp": now,
                "download_attempts": attempts,
                "download_error_message": error_msg,
                "created_at": now,
                "updated_at": now
            }
            
            all_results.append(result)
            
            if success:
                success_count += 1
            else:
                failed_count += 1
            
            # Small delay between downloads to be polite
            if idx < len(urls_list):
                time.sleep(1)
        
        category_summaries[category] = {
            "processed": len(urls_list),
            "success": success_count,
            "failed": failed_count
        }
        
        print(f"\n✓ {category} processing complete")
        print(f"  Success: {success_count}/{len(urls_list)}")
        print(f"  Failed: {failed_count}/{len(urls_list)}")
        
    except Exception as e:
        print(f"\n✗ Error processing {category}: {str(e)}")
        category_summaries[category] = {"processed": 0, "success": 0, "failed": 0, "error": str(e)}
        continue

print("\n" + "="*80)
print("All categories processed")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Update Processing Log and Summary
# Step 3: Update processing log
if len(all_results) > 0:
    print("\n" + "="*80)
    print("UPDATING PROCESSING LOG")
    print("="*80)
    
    results_df = spark.createDataFrame(all_results, schema=schema)
    results_df.createOrReplaceTempView("new_results")
    
    # Columns are listed explicitly: `UPDATE SET *` needs the source to carry every
    # target column, and this source only has the download-stage ones. Naming them
    # also stops a re-download from clearing parse/extraction status.
    spark.sql("""
        MERGE INTO market_data.customs.provinces_document_processing_log AS target
        USING new_results AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET
            target.document_url = source.document_url,
            target.sub_category = source.sub_category,
            target.filename = source.filename,
            target.download_status = source.download_status,
            target.download_timestamp = source.download_timestamp,
            target.download_attempts = source.download_attempts,
            target.download_error_message = source.download_error_message,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            document_id, document_url, sub_category, filename,
            download_status, download_timestamp, download_attempts,
            download_error_message, created_at, updated_at
        ) VALUES (
            source.document_id, source.document_url, source.sub_category, source.filename,
            source.download_status, source.download_timestamp, source.download_attempts,
            source.download_error_message, source.created_at, source.updated_at
        )
    """)
    
    print("✓ Processing log updated")
else:
    print("\n⚠ No results to update in processing log")

# Step 4: Summary report
print("\n" + "="*80)
print("DOWNLOAD SUMMARY - ALL CATEGORIES")
print("="*80)

for category, summary in category_summaries.items():
    print(f"\n{category}:")
    print(f"  Processed: {summary['processed']}")
    print(f"  ✓ Successful: {summary['success']}")
    print(f"  ✗ Failed: {summary['failed']}")
    if 'error' in summary:
        print(f"  Error: {summary['error']}")

total_failed = sum(summary['failed'] for summary in category_summaries.values())
if total_failed > 0:
    print("\nFailed downloads:")
    for r in all_results:
        if r['download_status'] == 'failed':
            print(f"  • [{r['sub_category']}] {r['document_url']}")
            print(f"    Error: {r['download_error_message']}")

print("\n" + "="*80)
print("Provinces Download workflow complete!")
print("="*80)