# Databricks notebook source
# DBTITLE 1,Download FDI Documents - Overview
# MAGIC %md
# MAGIC # Step 1: Download FDI Customs Documents
# MAGIC
# MAGIC This notebook downloads PDF documents for **FDI Import/Export goods** (3N/3X pattern) from URLs and saves them to Unity Catalog volumes.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read URL table**: Get all document URLs from `market_data.customs.fdi_customs_documents_url` filtered by `sub_category`
# MAGIC 2. **Process by category**: Loop through each sub_category independently
# MAGIC 3. **Check processing log**: Identify documents that haven't been downloaded or failed previously
# MAGIC 4. **Download with retry**: Fetch each PDF with up to 3 retry attempts
# MAGIC 5. **Save to volume**: Store files in FDI-specific volumes:
# MAGIC    - Import: `/Volumes/market_data/customs/fdi_import_goods/`
# MAGIC    - Export: `/Volumes/market_data/customs/fdi_export_goods/`
# MAGIC 6. **Update tracking**: Log success/failure status in `market_data.customs.fdi_document_processing_log`
# MAGIC
# MAGIC ## FDI vs. Regular IMEX
# MAGIC
# MAGIC * **URL Pattern**: FDI documents use `3N` (import) and `3X` (export) in filenames
# MAGIC * **Regular IMEX**: Uses `2N` (import) and `2X` (export)
# MAGIC * **Separate tables**: All FDI data uses `fdi_` prefixed tables
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
# MAGIC The processing log (`market_data.customs.fdi_document_processing_log`) tracks:
# MAGIC * `sub_category`: "Import Goods" or "Export Goods"
# MAGIC * `download_status`: 'success' or 'failed'
# MAGIC * `download_timestamp`: When the download was attempted
# MAGIC * `download_attempts`: Number of retry attempts
# MAGIC * `download_error_message`: Error details if failed

# COMMAND ----------

# DBTITLE 1,Download FDI Documents from URL Table
import requests
import time
from datetime import datetime
from urllib.parse import unquote, urlparse
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import hashlib

# Configuration for FDI documents
VOLUME_BASE_PATH = '/Volumes/market_data/customs/'
CATEGORIES = ["Import Goods", "Export Goods"]
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_TIMEOUT = 60

print("="*80)
print("FDI CUSTOMS DOCUMENTS DOWNLOAD WORKFLOW")
print("Processing FDI Import (3N) and Export (3X) goods")
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
            error_msg = f"HTTP {e.response.status_code}: {e.response.reason}"
            print(f"  ✗ HTTP error: {error_msg}")
            if e.response.status_code in [404, 403, 410]:
                return (False, None, error_msg, attempt)
                
        except requests.exceptions.Timeout:
            error_msg = f"Timeout after {REQUEST_TIMEOUT} seconds"
            print(f"  ✗ {error_msg}")
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {str(e)}"
            print(f"  ✗ {error_msg}")
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            print(f"  ✗ {error_msg}")
        
        if attempt < max_retries:
            delay = RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
            print(f"  Waiting {delay} seconds before retry...")
            time.sleep(delay)
    
    final_error = f"Failed after {max_retries} attempts: {error_msg}"
    return (False, None, final_error, max_retries)

# Define schema
schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("document_url", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("filename", StringType(), True),
    StructField("download_status", StringType(), True),
    StructField("download_timestamp", TimestampType(), True),
    StructField("download_attempts", IntegerType(), True),
    StructField("download_error_message", StringType(), True),
    StructField("parse_status", StringType(), True),
    StructField("parse_timestamp", TimestampType(), True),
    StructField("parse_error_message", StringType(), True),
    StructField("extraction_status", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("extraction_error_message", StringType(), True),
    StructField("extraction_rows_inserted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Read FDI URLs and processing log
all_urls_df = spark.table("market_data.customs.fdi_customs_documents_url")
processing_log_df = spark.table("market_data.customs.fdi_document_processing_log")

all_results = []
category_summaries = {}

# COMMAND ----------

# DBTITLE 1,Process Each Category
# Main loop: Process each category
for category in CATEGORIES:
    print("\n" + "="*80)
    print(f"PROCESSING CATEGORY: {category}")
    print("="*80)
    
    try:
        # Determine volume path based on category
        if category == "Import Goods":
            volume_path = f"{VOLUME_BASE_PATH}fdi_import_goods/"
        else:  # Export Goods
            volume_path = f"{VOLUME_BASE_PATH}fdi_export_goods/"
        
        print(f"\nVolume path: {volume_path}")
        
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
        
        # Step 2: Download documents
        print(f"\nStep 2: Downloading documents for {category}...")
        print("-" * 80)
        
        category_results = []
        
        for idx, row in enumerate(urls_list, 1):
            url = row['url']
            
            print(f"\n[{idx}/{len(urls_list)}] Processing: {url}")
            
            doc_id = generate_document_id(url)
            parsed_url = urlparse(url)
            original_filename = unquote(parsed_url.path.split('/')[-1])
            
            date_prefix = datetime.now().strftime("%Y%m%d")
            filename = f"{date_prefix}_{original_filename}"
            file_path = f"{volume_path}{filename}"
            
            success, returned_path, error_msg, attempts = download_document_with_retry(url, file_path)
            
            category_results.append({
                'document_id': doc_id,
                'document_url': url,
                'sub_category': category,
                'filename': filename,
                'download_status': 'success' if success else 'failed',
                'download_timestamp': datetime.now(),
                'download_attempts': attempts,
                'download_error_message': error_msg if error_msg else '',
                'parse_status': 'pending' if success else '',
                'parse_timestamp': None,
                'parse_error_message': '',
                'extraction_status': '',
                'extraction_timestamp': None,
                'extraction_error_message': '',
                'extraction_rows_inserted': 0,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
        
        all_results.extend(category_results)
        
        success_count = sum(1 for r in category_results if r['download_status'] == 'success')
        failed_count = len(category_results) - success_count
        
        category_summaries[category] = {
            "processed": len(category_results),
            "success": success_count,
            "failed": failed_count
        }
        
        print(f"\n✓ {category} processing complete: {success_count} success, {failed_count} failed")
        
    except Exception as e:
        error_msg = f"Category-level error: {str(e)}"
        print(f"\n✗ {error_msg}")
        category_summaries[category] = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "error": error_msg
        }
        continue

# COMMAND ----------

# DBTITLE 1,Update Processing Log and Summary
# Step 3: Update processing log
if len(all_results) > 0:
    print("\n" + "="*80)
    print("UPDATING PROCESSING LOG")
    print("="*80)
    
    results_df = spark.createDataFrame(all_results, schema=schema)
    results_df.createOrReplaceTempView("new_results")
    
    spark.sql("""
        MERGE INTO market_data.customs.fdi_document_processing_log AS target
        USING new_results AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print("✓ Processing log updated")

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
print("FDI Download workflow complete!")
print("="*80)