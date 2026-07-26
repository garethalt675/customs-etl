# Databricks notebook source
# DBTITLE 1,Download Documents - Overview
# MAGIC %md
# MAGIC # Step 1: Download Customs Documents
# MAGIC
# MAGIC This notebook downloads PDF documents from URLs stored in `market_data.customs.customs_documents_url` and saves them to the Unity Catalog volume.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read URL table**: Get all document URLs filtered by `sub_category` ("Import Goods" or "Export Goods")
# MAGIC 2. **Process by category**: Loop through each sub_category independently
# MAGIC 3. **Check processing log**: Identify documents that haven't been downloaded or failed previously
# MAGIC 4. **Download with retry**: Fetch each PDF with up to 3 retry attempts
# MAGIC 5. **Save to volume**: Store files in category-specific subdirectories:
# MAGIC    - Import: `/Volumes/market_data/customs/download_docs/import_goods/`
# MAGIC    - Export: `/Volumes/market_data/customs/download_docs/export_goods/`
# MAGIC 6. **Update tracking**: Log success/failure status with `sub_category` for each document
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC * **Category-level isolation**: If Import processing fails, Export still runs
# MAGIC * **Network errors**: Retry up to 3 times with exponential backoff
# MAGIC * **HTTP errors**: Log specific error codes (404, 403, 500, etc.)
# MAGIC * **File errors**: Track corrupted or empty downloads
# MAGIC * **Summary report**: Display per-category statistics at the end
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The processing log (`market_data.customs.document_processing_log`) is updated with:
# MAGIC * `sub_category`: "Import Goods" or "Export Goods"
# MAGIC * `download_status`: 'success' or 'failed'
# MAGIC * `download_timestamp`: When the download was attempted
# MAGIC * `download_attempts`: Number of retry attempts
# MAGIC * `download_error_message`: Error details if failed

# COMMAND ----------

# DBTITLE 1,Download Documents from URL Table
import requests
import time
from datetime import datetime
from urllib.parse import unquote, urlparse
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import hashlib

# Configuration
VOLUME_BASE_PATH = '/Volumes/market_data/customs/'
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5  # Base delay, will increase exponentially
REQUEST_TIMEOUT = 60

print("="*80)
print("CUSTOMS DOCUMENTS DOWNLOAD WORKFLOW")
print("Processing both Import and Export goods")
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
            
            # Check if content is valid
            if len(response.content) == 0:
                raise ValueError("Downloaded file is empty")
            
            # Save to volume
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            file_size_kb = len(response.content) / 1024
            print(f"  ✓ Downloaded successfully: {file_size_kb:.2f} KB")
            
            return (True, file_path, None, attempt)
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.reason}"
            print(f"  ✗ HTTP error: {error_msg}")
            if e.response.status_code in [404, 403, 410]:  # Don't retry for these
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
        
        # Wait before retry (exponential backoff)
        if attempt < max_retries:
            delay = RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
            print(f"  Waiting {delay} seconds before retry...")
            time.sleep(delay)
    
    # All retries failed
    final_error = f"Failed after {max_retries} attempts: {error_msg}"
    return (False, None, final_error, max_retries)

# Define explicit schema to avoid type inference issues
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

# Read all URLs with sub_category
all_urls_df = spark.table("market_data.customs.customs_documents_url")
processing_log_df = spark.table("market_data.customs.document_processing_log")

# Get distinct categories dynamically from the source table
CATEGORIES = [row['sub_category'] for row in all_urls_df.select('sub_category').distinct().collect()]
print(f"Auto-detected categories: {CATEGORIES}")

# Collect all results across categories
all_results = []
category_summaries = {}

# ========================================
# MAIN LOOP: Process each category
# ========================================
for category in CATEGORIES:
    print("\n" + "="*80)
    print(f"PROCESSING CATEGORY: {category}")
    print("="*80)
    
    # Category-level error handling
    try:
        # Create subdirectory path
        category_subdir = category.lower().replace(' ', '_')
        volume_path = f"{VOLUME_BASE_PATH}{category_subdir}/"
        
        print(f"\nVolume path: {volume_path}")
        
        # ========================================
        # Step 1: Get URLs for this category
        # ========================================
        print(f"\nStep 1: Loading URLs for {category}...")
        print("-" * 80)
        
        # Filter URLs by sub_category
        category_urls_df = all_urls_df.filter(col("sub_category") == category)
        total_category_urls = category_urls_df.count()
        print(f"Total URLs in source table for {category}: {total_category_urls}")
        
        if total_category_urls == 0:
            print(f"⚠ No URLs found for {category}, skipping...")
            category_summaries[category] = {"processed": 0, "success": 0, "failed": 0, "error": "No URLs found"}
            continue
        
        # Filter out successfully downloaded documents for this category
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
        
        # ========================================
        # Step 2: Download documents
        # ========================================
        print(f"\nStep 2: Downloading documents for {category}...")
        print("-" * 80)
        
        category_results = []
        
        for idx, row in enumerate(urls_list, 1):
            url = row['url']
            
            print(f"\n[{idx}/{len(urls_list)}] Processing: {url}")
            
            # Generate document ID and filename
            doc_id = generate_document_id(url)
            parsed_url = urlparse(url)
            original_filename = unquote(parsed_url.path.split('/')[-1])
            
            # Add date prefix to filename
            date_prefix = datetime.now().strftime("%Y%m%d")
            filename = f"{date_prefix}_{original_filename}"
            file_path = f"{volume_path}{filename}"
            
            # Download with retry
            success, returned_path, error_msg, attempts = download_document_with_retry(url, file_path)
            
            # Record result
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
        
        # Add to overall results
        all_results.extend(category_results)
        
        # Calculate category summary
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
        # Continue to next category
        continue

# ========================================
# Step 3: Update processing log
# ========================================
if len(all_results) > 0:
    print("\n" + "="*80)
    print("UPDATING PROCESSING LOG")
    print("="*80)
    
    results_df = spark.createDataFrame(all_results, schema=schema)
    results_df.createOrReplaceTempView("new_results")
    
    spark.sql("""
        MERGE INTO market_data.customs.document_processing_log AS target
        USING new_results AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print("✓ Processing log updated")

# ========================================
# Step 4: Summary report
# ========================================
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

# Show failed downloads if any
total_failed = sum(summary['failed'] for summary in category_summaries.values())
if total_failed > 0:
    print("\nFailed downloads:")
    for r in all_results:
        if r['download_status'] == 'failed':
            print(f"  • [{r['sub_category']}] {r['document_url']}")
            print(f"    Error: {r['download_error_message']}")

print("\n" + "="*80)
print("Download workflow complete!")
print("="*80)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market_data.customs.document_processing_log

# COMMAND ----------

