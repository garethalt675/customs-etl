# Databricks notebook source
# DBTITLE 1,Download Countries Documents - Overview
# MAGIC %md   
# MAGIC # Step 1: Download IMEX to Countries Customs Documents
# MAGIC
# MAGIC This notebook downloads PDF documents for **IMEX trade by destination/origin countries** (5N/5X pattern) from URLs and saves them to Unity Catalog volumes.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read URL table**: Get all document URLs from `market_data.customs.countries_customs_documents_url` filtered by `sub_category`
# MAGIC 2. **Process by category**: Loop through each sub_category independently
# MAGIC 3. **Check processing log**: Identify documents that haven't been downloaded or failed previously
# MAGIC 4. **Download with retry**: Fetch each PDF with up to 3 retry attempts
# MAGIC 5. **Save to volume**: Store files in countries-specific volumes:
# MAGIC    - Import: `/Volumes/market_data/customs/countries_import_goods/`
# MAGIC    - Export: `/Volumes/market_data/customs/countries_export_goods/`
# MAGIC 6. **Update tracking**: Log success/failure status in `market_data.customs.countries_document_processing_log`
# MAGIC
# MAGIC ## Data Type: IMEX by Countries
# MAGIC
# MAGIC * **URL Pattern**: Documents use `5N` (import by origin country) and `5X` (export by destination country)
# MAGIC * **Content**: Trade statistics broken down by partner countries
# MAGIC * **Separate tables**: All data uses `countries_` prefixed tables
# MAGIC
# MAGIC ## Comparison
# MAGIC
# MAGIC | Type | Pattern | Content |
# MAGIC | --- | --- | --- |
# MAGIC | **Regular IMEX** | 2N/2X | Trade by product categories |
# MAGIC | **FDI IMEX** | 3N/3X | Trade from FDI enterprises |
# MAGIC | **IMEX to Countries** | 5N/5X | Trade by partner countries |
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
# MAGIC The processing log (`market_data.customs.countries_document_processing_log`) tracks:
# MAGIC * `sub_category`: "Import by Origin" or "Export by Destination"
# MAGIC * `download_status`: 'success' or 'failed'
# MAGIC * `download_timestamp`: When the download was attempted
# MAGIC * `download_attempts`: Number of retry attempts
# MAGIC * `download_error_message`: Error details if failed

# COMMAND ----------

# DBTITLE 1,Download Countries Documents from URL Table
import requests
import time
from datetime import datetime
from urllib.parse import unquote, urlparse
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import hashlib

# Configuration for Countries documents
VOLUME_BASE_PATH = '/Volumes/market_data/customs/'
CATEGORIES = ["Import by Origin", "Export by Destination"]
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_TIMEOUT = 60

print("="*80)
print("IMEX TO COUNTRIES DOCUMENTS DOWNLOAD WORKFLOW")
print("Processing import by origin (5N) and export by destination (5X)")
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

# Define schema - must match the processing log table structure
schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("document_url", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("download_status", StringType(), True),
    StructField("download_timestamp", TimestampType(), True),
    StructField("download_error_message", StringType(), True),
    StructField("downloaded_file_path", StringType(), True),
    StructField("parse_status", StringType(), True),
    StructField("parse_timestamp", TimestampType(), True),
    StructField("parse_error_message", StringType(), True),
    StructField("parsed_tables_count", IntegerType(), True),
    StructField("extraction_status", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("extraction_error_message", StringType(), True),
    StructField("extraction_rows_inserted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Read Countries URLs and processing log
all_urls_df = spark.table("market_data.customs.countries_customs_documents_url")
processing_log_df = spark.table("market_data.customs.countries_document_processing_log")

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
        if category == "Import by Origin":
            volume_path = f"{VOLUME_BASE_PATH}countries_import_goods/"
        else:  # Export by Destination
            volume_path = f"{VOLUME_BASE_PATH}countries_export_goods/"
        
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
                'download_status': 'success' if success else 'failed',
                'download_timestamp': datetime.now(),
                'download_error_message': error_msg if error_msg else None,
                'downloaded_file_path': file_path if success else None,
                'parse_status': 'pending' if success else None,
                'parse_timestamp': None,
                'parse_error_message': None,
                'parsed_tables_count': None,
                'extraction_status': None,
                'extraction_timestamp': None,
                'extraction_error_message': None,
                'extraction_rows_inserted': None,
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

# DBTITLE 1,Update Processing Log & Summary
# Update Processing Log from Downloaded Files
import hashlib
import urllib.parse
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col

print("\n" + "="*80)
print("UPDATING PROCESSING LOG")
print("="*80)

# Define schema matching the processing log table
log_schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("document_url", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("download_status", StringType(), True),
    StructField("download_timestamp", TimestampType(), True),
    StructField("download_error_message", StringType(), True),
    StructField("downloaded_file_path", StringType(), True),
    StructField("parse_status", StringType(), True),
    StructField("parse_timestamp", TimestampType(), True),
    StructField("parse_error_message", StringType(), True),
    StructField("parsed_tables_count", IntegerType(), True),
    StructField("extraction_status", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("extraction_error_message", StringType(), True),
    StructField("extraction_rows_inserted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Build lookup: decoded_filename -> URL info (with proper URL decoding)
print("\nBuilding URL lookup table with decoded filenames...")
url_by_filename = {}
for row in all_urls_df.collect():
    url = row['url']
    doc_id = hashlib.md5(url.encode()).hexdigest()[:16]
    
    # Decode the URL path to get the actual filename
    parsed = urllib.parse.urlparse(url)
    decoded_filename = urllib.parse.unquote(parsed.path.split('/')[-1])
    
    url_by_filename[decoded_filename] = {
        'doc_id': doc_id,
        'url': url,
        'sub_category': row['sub_category']
    }

print(f"  ✓ Built lookup for {len(url_by_filename)} URLs")

# Get already logged document IDs to avoid duplicates
logged_ids = set([row['document_id'] for row in 
                  spark.table("market_data.customs.countries_document_processing_log")
                  .select("document_id").collect()])

print(f"  ✓ Found {len(logged_ids)} already logged documents\n")

# Scan volumes and create log entries
volumes = [
    ('/Volumes/market_data/customs/countries_import_goods/', 'Import by Origin'),
    ('/Volumes/market_data/customs/countries_export_goods/', 'Export by Destination')
]

new_log_entries = []
category_stats = {}

for volume_path, category in volumes:
    print(f"Scanning {category}: {volume_path}")
    
    try:
        files = dbutils.fs.ls(volume_path)
        pdf_files = [f for f in files if f.name.endswith('.pdf')]
        
        matched = 0
        skipped = 0
        unmatched = 0
        
        for file_info in pdf_files:
            filename = file_info.name
            
            # Remove date prefix (format: YYYYMMDD_originalname.pdf)
            if '_' in filename and filename[:8].isdigit():
                original_name = '_'.join(filename.split('_')[1:])
            else:
                original_name = filename
            
            # Look up by decoded filename
            if original_name in url_by_filename:
                info = url_by_filename[original_name]
                
                # Verify category matches
                if info['sub_category'] == category:
                    doc_id = info['doc_id']
                    
                    # Skip if already logged
                    if doc_id in logged_ids:
                        skipped += 1
                        continue
                    
                    # Create log entry
                    new_log_entries.append({
                        'document_id': doc_id,
                        'document_url': info['url'],
                        'sub_category': category,
                        'download_status': 'success',
                        'download_timestamp': datetime.now(),
                        'download_error_message': None,
                        'downloaded_file_path': file_info.path,
                        'parse_status': 'pending',
                        'parse_timestamp': None,
                        'parse_error_message': None,
                        'parsed_tables_count': None,
                        'extraction_status': None,
                        'extraction_timestamp': None,
                        'extraction_error_message': None,
                        'extraction_rows_inserted': None,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })
                    matched += 1
                else:
                    # File in wrong category folder
                    unmatched += 1
            else:
                # No URL found for this file
                unmatched += 1
        
        category_stats[category] = {
            'total_files': len(pdf_files),
            'matched': matched,
            'skipped': skipped,
            'unmatched': unmatched
        }
        
        print(f"  Total files: {len(pdf_files)}")
        print(f"  New matches: {matched}")
        print(f"  Already logged: {skipped}")
        if unmatched > 0:
            print(f"  ⚠ Unmatched: {unmatched}")
        print()
    
    except Exception as e:
        print(f"  ✗ Error scanning {category}: {str(e)}\n")
        category_stats[category] = {'error': str(e)}

# Insert new entries into processing log
if len(new_log_entries) > 0:
    print(f"Inserting {len(new_log_entries)} new log entries...")
    new_log_df = spark.createDataFrame(new_log_entries, schema=log_schema)
    new_log_df.write.mode("append").saveAsTable("market_data.customs.countries_document_processing_log")
    print("  ✓ Processing log updated successfully!\n")
else:
    print("  ✓ All files already logged - nothing to add\n")

# Display final summary
print("="*80)
print("DOWNLOAD & LOGGING SUMMARY")
print("="*80)

# Get final counts from processing log
final_counts = spark.sql("""
    SELECT 
        sub_category,
        COUNT(*) as total_logged,
        SUM(CASE WHEN download_status = 'success' THEN 1 ELSE 0 END) as successful,
        SUM(CASE WHEN parse_status = 'pending' THEN 1 ELSE 0 END) as ready_for_parsing
    FROM market_data.customs.countries_document_processing_log
    GROUP BY sub_category
    ORDER BY sub_category
""").collect()

for row in final_counts:
    print(f"\n{row['sub_category']}:")
    print(f"  Total logged: {row['total_logged']}")
    print(f"  ✓ Successful downloads: {row['successful']}")
    print(f"  ⚡ Ready for parsing: {row['ready_for_parsing']}")

total_logged = sum(row['total_logged'] for row in final_counts)
total_ready = sum(row['ready_for_parsing'] for row in final_counts)

print(f"\n{'='*80}")
print(f"TOTAL: {total_logged} documents logged, {total_ready} ready for next step")
print(f"{'='*80}")
print("\n✓ Countries Download workflow complete!")
print("\nNext step: Run 2_Parse_Countries_Documents to extract tables from PDFs")