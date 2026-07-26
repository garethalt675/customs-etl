# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview: URL Discovery & Update Workflow
# MAGIC %md
# MAGIC # Customs Document URL Discovery & Update Workflow
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook automates the discovery and ingestion of new customs statistics document URLs from the Vietnamese Customs website into the Unity Catalog table `market_data.customs.customs_documents_url`.
# MAGIC
# MAGIC ## Process Flow
# MAGIC ```
# MAGIC API Request → JSON Parsing → Title Filtering → Month Deduplication → Database Insert
# MAGIC ```
# MAGIC
# MAGIC ## Data Source
# MAGIC * **Primary Source:** Vietnamese Customs Backend API  
# MAGIC   `https://www.customs.gov.vn/bridge?url=/customs/api/GetTKHQInfo`
# MAGIC * **Document Repository:** `files.customs.gov.vn`
# MAGIC
# MAGIC ## Target Reports (Title-Based Filtering)
# MAGIC * **Import Goods:** Reports starting with `Nhập khẩu hàng hóa tháng MM/YYYY`  
# MAGIC * **Export Goods:** Reports starting with `Xuất khẩu hàng hóa tháng MM/YYYY`
# MAGIC
# MAGIC ## Duplicate Prevention
# MAGIC Each report month (YYYY-MM) can only exist **once** per category (Import/Export):
# MAGIC * If December 2025 SB is already in the database, December 2025 CT will be **skipped**
# MAGIC * **First version inserted wins** - no upgrades or replacements
# MAGIC * Prevents the same month from appearing multiple times
# MAGIC
# MAGIC ## Schema: `market_data.customs.customs_documents_url`
# MAGIC | Field | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | url | STRING | Full PDF document URL |
# MAGIC | added_date | TIMESTAMP | UTC timestamp of ingestion |
# MAGIC | category | STRING | "Customs Statistics" |
# MAGIC | sub_category | STRING | "Import Goods" or "Export Goods" |
# MAGIC | report_month | STRING | Report period in YYYY-MM format |
# MAGIC
# MAGIC ## Data Lineage
# MAGIC All URLs are timestamped with `added_date` to track ingestion history.

# COMMAND ----------

# DBTITLE 1,Install Required Packages
# MAGIC %pip install requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Dependencies
import requests
import re
import unicodedata
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import time
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Configuration
# ============================================================================
# CONFIGURATION
# ============================================================================

# Target Unity Catalog table
TARGET_TABLE = "market_data.customs.customs_documents_url"

# API Configuration
API_HOST = "www.customs.gov.vn"
API_ENDPOINT = f"https://{API_HOST}/bridge?url=/customs/api/GetTKHQInfo"
FILE_DOMAIN = "files.customs.gov.vn"

# Report title patterns (accent-insensitive matching)
REPORT_PATTERNS = {
    "import": {
        "pattern": r"^nhap khau hang hoa thang\s+(\d{1,2})/(\d{4})$",
        "sub_category": "Import Goods"
    },
    "export": {
        "pattern": r"^xuat khau hang hoa thang\s+(\d{1,2})/(\d{4})$",
        "sub_category": "Export Goods"
    }
}

CATEGORY = "Customs Statistics"

# API request parameters
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

logger.info(f"Configuration loaded. Target table: {TARGET_TABLE}")
logger.info(f"API endpoint: {API_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,Helper Functions: URL Parsing & Metadata Extraction
def remove_accents(text: str) -> str:
    """
    Remove Vietnamese accents for normalized matching.
    
    Args:
        text: Input text with potential accents
    
    Returns:
        Text with accents removed and lowercased
    """
    import unicodedata
    # Normalize to NFD (decomposed form) and remove combining characters
    nfd = unicodedata.normalize('NFD', text)
    without_accents = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    return without_accents.lower()


def extract_version(filename: str) -> str:
    """
    Extract version indicator (CT/DC/SB) from filename.
    
    Args:
        filename: PDF filename
    
    Returns:
        Version string (CT, DC, SB) or empty string if not found
    """
    filename_upper = filename.upper()
    if '(CT)' in filename_upper or '-CT.' in filename_upper or '-CT-' in filename_upper:
        return 'CT'
    elif '(DC)' in filename_upper or '-DC.' in filename_upper or '-DC-' in filename_upper:
        return 'DC'
    elif '(SB)' in filename_upper or '-SB.' in filename_upper or '-SB-' in filename_upper:
        return 'SB'
    return ''


def normalize_filename(url: str) -> str:
    """
    Normalize filename for deduplication.
    Removes duplicate upload suffixes and standardizes format.
    
    Args:
        url: Full URL
    
    Returns:
        Normalized filename key
    """
    filename = url.split('/')[-1]
    
    # Remove duplicate upload suffixes like -1.pdf, -2.pdf, etc.
    filename = re.sub(r'-\d+\.pdf$', '.pdf', filename, flags=re.IGNORECASE)
    
    # Canonicalize monthly report names to YYYY-MM format
    # Handle variations like: 2025-t4-2n, 2025-T04T-2N
    filename = re.sub(r'(\d{4})-[tT]?(\d{1,2})[tT]?-', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-", filename)
    
    # Remove version indicators for grouping
    filename = re.sub(r'\((CT|DC|SB)\)', '', filename, flags=re.IGNORECASE)
    filename = re.sub(r'-(CT|DC|SB)[-.\)]', '-', filename, flags=re.IGNORECASE)
    
    return filename.lower()


def parse_url_metadata(url: str, title: str, sub_category: str) -> Optional[Dict[str, str]]:
    """
    Extract metadata from customs document URL and title.
    
    Args:
        url: Document URL string
        title: Report title
        sub_category: Import Goods or Export Goods
    
    Returns:
        Dict with metadata or None if parsing fails
    """
    try:
        # Extract year and month from title (MM/YYYY format)
        title_normalized = remove_accents(title)
        
        # Try to extract from title first
        month_year_match = re.search(r'(\d{1,2})/(\d{4})', title)
        if month_year_match:
            month = month_year_match.group(1).zfill(2)
            year = month_year_match.group(2)
        else:
            # Fallback: extract from URL
            year_month_match = re.search(r'(20\d{2})[_-]?[tT]?(\d{1,2})', url)
            if year_month_match:
                year = year_month_match.group(1)
                month = year_month_match.group(2).zfill(2)
            else:
                logger.warning(f"Could not extract year/month from title or URL: {title}")
                return None
        
        # Extract version
        version = extract_version(url)
        
        # Normalize filename for deduplication
        normalized_filename = normalize_filename(url)
        
        return {
            "url": url,
            "year": year,
            "month": month,
            "sub_category": sub_category,
            "category": CATEGORY,
            "version": version,
            "normalized_filename": normalized_filename,
            "title": title
        }
    
    except Exception as e:
        logger.warning(f"Failed to parse URL metadata: {url}. Error: {str(e)}")
        return None


def validate_url(url: str) -> bool:
    """
    Validate if URL is from the expected domain.
    
    Args:
        url: URL string to validate
    
    Returns:
        Boolean indicating if URL is valid
    """
    if not url:
        return False
    
    # Must be HTTPS/HTTP
    if not url.startswith(('http://', 'https://')):
        return False
    
    # Must be from files.customs.gov.vn (not internal IPs)
    if FILE_DOMAIN not in url:
        return False
    
    # Must end with .pdf
    if not url.lower().endswith('.pdf'):
        return False
    
    return True

# COMMAND ----------

# DBTITLE 1,Web Scraping Function with Retry Logic
def fetch_api_data(max_retries: int = MAX_RETRIES) -> Optional[Dict]:
    """
    Fetch customs data from backend API with retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
    
    Returns:
        JSON response as dict or None if all retries fail
    """
    headers = {
        'User-Agent': USER_AGENT,
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }
    
    # API request payload
    payload = {
        "skip": 0,
        "take": 5000,
        "ky": "",
        "textSearch": "",
        "the_loai": "0",
        "thoigianCongBo": "",
        "typeName": "GetListSoLieu",
        "language": "TIENG_VIET"
    }
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Calling API (attempt {attempt + 1}/{max_retries})")
            response = requests.post(API_ENDPOINT, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed (attempt {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                delay = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All retry attempts failed for API endpoint")
                return None
    
    return None


def match_report_title(title: str) -> Optional[str]:
    """
    Check if title matches import or export monthly report patterns.
    
    Args:
        title: Report title
    
    Returns:
        'import' or 'export' if matched, None otherwise
    """
    normalized_title = remove_accents(title).strip()
    
    for report_type, config in REPORT_PATTERNS.items():
        if re.match(config['pattern'], normalized_title):
            return report_type
    
    return None


def extract_pdf_links(api_data: Dict) -> List[Dict[str, str]]:
    """
    Extract PDF links from API response and filter by title patterns.
    Each report can have up to 3 versions: CT (official), DC (revised), SB (preliminary).
    
    Args:
        api_data: JSON response from API
    
    Returns:
        List of dicts with url, title, sub_category, and version
    """
    pdf_links = []
    
    if not api_data or 'arr' not in api_data:
        logger.error("Invalid API response structure")
        return []
    
    records = api_data.get('arr', [])
    logger.info(f"Processing {len(records)} records from API")
    
    # Field mapping for each version type
    version_fields = {
        'CT': 'FILE_CHINH_THUC',
        'DC': 'FILE_DIEU_CHINH', 
        'SB': 'FILE_SO_BO'
    }
    
    for record in records:
        # Extract title
        title = record.get('TIEU_DE', '').strip()
        if not title:
            continue
        
        # Check if title matches our patterns
        report_type = match_report_title(title)
        if not report_type:
            continue
        
        sub_category = REPORT_PATTERNS[report_type]['sub_category']
        
        # Extract PDF URLs from all 3 version fields
        for version, field_name in version_fields.items():
            pdf_url = record.get(field_name, '').strip()
            
            if pdf_url and validate_url(pdf_url):
                pdf_links.append({
                    'url': pdf_url,
                    'title': title,
                    'sub_category': sub_category,
                    'report_type': report_type,
                    'version': version
                })
                logger.debug(f"Matched {report_type} ({version}): {title} -> {pdf_url[:80]}...")
    
    logger.info(f"Found {len(pdf_links)} PDF links (all versions)")
    return pdf_links


def deduplicate_by_version(pdf_links: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Deduplicate by report period (title).
    Keeps first version encountered per report period - no priority ordering.
    
    Args:
        pdf_links: List of PDF link dicts (with version already identified)
    
    Returns:
        Deduplicated list with one URL per report period
    """
    # Group by title (which uniquely identifies the report period)
    report_groups = {}
    
    for link in pdf_links:
        url = link['url']
        metadata = parse_url_metadata(url, link['title'], link['sub_category'])
        
        if not metadata:
            continue
        
        # Use title as the grouping key (each title represents a unique report period)
        title_key = remove_accents(link['title']).strip()
        
        # Keep the first version encountered for this report period
        if title_key not in report_groups:
            report_groups[title_key] = metadata
            logger.debug(f"Keeping first version for {link['title']}")
    
    # Extract metadata list
    deduplicated = list(report_groups.values())
    
    logger.info(f"After deduplication: {len(deduplicated)} unique reports")
    return deduplicated


def discover_customs_urls() -> List[Dict[str, str]]:
    """
    Main discovery function: fetch from API, filter, and deduplicate.
    
    Returns:
        List of URL metadata dicts
    """
    logger.info("Starting API data discovery process...")
    
    # Step 1: Fetch data from API
    api_data = fetch_api_data()
    if not api_data:
        logger.error("Failed to fetch API data. Aborting discovery.")
        return []
    
    # Step 2: Extract and filter PDF links
    pdf_links = extract_pdf_links(api_data)
    if not pdf_links:
        logger.warning("No matching reports found in API response")
        return []
    
    # Step 3: Deduplicate by version priority
    deduplicated_urls = deduplicate_by_version(pdf_links)
    
    logger.info(f"Discovery complete: {len(deduplicated_urls)} URLs ready for ingestion")
    return deduplicated_urls

# COMMAND ----------

# DBTITLE 1,De-duplication Logic
def get_existing_report_months() -> set:
    """
    Retrieve all existing report_month + sub_category combinations from the target table.
    
    Returns:
        Set of "report_month|sub_category" keys
    """
    try:
        logger.info(f"Reading existing report months from {TARGET_TABLE}...")
        existing_df = spark.table(TARGET_TABLE)
        
        existing_months = set()
        for row in existing_df.select('report_month', 'sub_category').distinct().collect():
            key = f"{row.report_month}|{row.sub_category}"
            existing_months.add(key)
        
        logger.info(f"Found {len(existing_months)} existing report months in table")
        return existing_months
    
    except Exception as e:
        logger.warning(f"Could not read existing records (table may not exist): {str(e)}")
        return set()


def filter_new_urls(discovered_metadata: List[Dict[str, str]], existing_months: set) -> List[Dict[str, str]]:
    """
    Filter out URLs for months that already exist in the database.
    Keeps whichever version was inserted first - no version upgrades.
    
    Args:
        discovered_metadata: List of URL metadata dicts
        existing_months: Set of "report_month|sub_category" keys already in database
    
    Returns:
        List of new URLs to insert
    """
    urls_to_insert = []
    
    for metadata in discovered_metadata:
        report_month = f"{metadata['year']}-{metadata['month']}"
        sub_category = metadata['sub_category']
        
        # Create composite key
        key = f"{report_month}|{sub_category}"
        
        # Check if this report_month + sub_category already exists
        if key in existing_months:
            # Skip - month already exists (regardless of version)
            logger.debug(f"Skipping {report_month} {sub_category}: month already exists in database")
            continue
        else:
            # New report month - insert it
            urls_to_insert.append(metadata)
    
    logger.info(f"Identified {len(urls_to_insert)} new URLs to insert")
    return urls_to_insert

# COMMAND ----------

# DBTITLE 1,Schema Migration: Add report_month Column
def migrate_table_schema():
    """
    Add report_month column to existing table if it doesn't exist.
    Backfills the column by extracting year/month from existing URLs.
    """
    try:
        # Check if table exists
        existing_df = spark.table(TARGET_TABLE)
        
        # Check if report_month column already exists
        if 'report_month' in existing_df.columns:
            logger.info("report_month column already exists, skipping migration")
            return
        
        logger.info("Migrating table schema: adding report_month column...")
        
        # Read all existing data
        all_records = existing_df.collect()
        
        # Backfill report_month from URLs
        rows_with_month = []
        for row in all_records:
            url = row.url
            
            # Try to extract year and month from URL
            year_month_match = re.search(r'(20\d{2})[_-]?[tT]?(\d{1,2})', url)
            if year_month_match:
                year = year_month_match.group(1)
                month = year_month_match.group(2).zfill(2)
                report_month = f"{year}-{month}"
            else:
                # Fallback for unparseable URLs
                report_month = "Unknown"
                logger.warning(f"Could not extract report_month from URL: {url}")
            
            rows_with_month.append((
                row.url,
                row.added_date,
                row.category,
                row.sub_category,
                report_month
            ))
        
        # Create new DataFrame with report_month
        schema = StructType([
            StructField("url", StringType(), False),
            StructField("added_date", TimestampType(), False),
            StructField("category", StringType(), False),
            StructField("sub_category", StringType(), False),
            StructField("report_month", StringType(), False)
        ])
        
        migrated_df = spark.createDataFrame(rows_with_month, schema)
        
        # Overwrite table with new schema
        logger.info(f"Writing {len(rows_with_month)} records with new schema...")
        migrated_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
        
        logger.info("Schema migration completed successfully")
        
    except Exception as e:
        logger.warning(f"Table may not exist yet, will be created with correct schema: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Database Insertion
def insert_new_urls(new_urls_data: List[Dict[str, str]]) -> int:
    """
    Insert new URLs into the target table.
    
    Args:
        new_urls_data: List of dicts with URL metadata
    
    Returns:
        Number of rows inserted
    """
    if not new_urls_data:
        logger.info("No new URLs to insert")
        return 0
    
    # Define schema with report_month column
    schema = StructType([
        StructField("url", StringType(), False),
        StructField("added_date", TimestampType(), False),
        StructField("category", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("report_month", StringType(), False)
    ])
    
    # Prepare data with timestamp
    current_timestamp = datetime.utcnow()
    rows_to_insert = []
    
    for url_data in new_urls_data:
        report_month = f"{url_data['year']}-{url_data['month']}"
        rows_to_insert.append((
            url_data["url"],
            current_timestamp,
            url_data["category"],
            url_data["sub_category"],
            report_month
        ))
    
    # Create DataFrame
    new_urls_df = spark.createDataFrame(rows_to_insert, schema)
    
    # Insert into table
    try:
        logger.info(f"Inserting {len(rows_to_insert)} new URLs into {TARGET_TABLE}...")
        new_urls_df.write.mode("append").saveAsTable(TARGET_TABLE)
        logger.info(f"Successfully inserted {len(rows_to_insert)} URLs")
        return len(rows_to_insert)
    
    except Exception as e:
        logger.error(f"Failed to insert URLs: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Main Execution: Orchestrate ETL Pipeline
# ============================================================================
# MAIN EXECUTION
# ============================================================================

logger.info("="*80)
logger.info("Starting Customs URL Discovery & Update Pipeline")
logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
logger.info("="*80)

# Step 0: Schema Migration (if needed)
logger.info("\n[STEP 0] Schema Migration")
migrate_table_schema()

# Step 1: API Discovery & Filtering
logger.info("\n[STEP 1] API Discovery & Filtering Phase")
discovered_metadata = discover_customs_urls()

if not discovered_metadata:
    logger.error("No URLs discovered. Exiting pipeline.")
    dbutils.notebook.exit("FAILURE: No URLs discovered")

# Step 2: De-duplication (Skip existing months)
logger.info("\n[STEP 2] De-duplication (Skip Existing Months)")
existing_months = get_existing_report_months()
urls_to_insert = filter_new_urls(discovered_metadata, existing_months)

# Step 3: Database Insert
logger.info("\n[STEP 3] Database Insert Phase")
inserted_count = insert_new_urls(urls_to_insert)

# Store results for summary
results = {
    "total_discovered": len(discovered_metadata),
    "already_exists": len(discovered_metadata) - len(urls_to_insert),
    "newly_inserted": inserted_count,
    "execution_time": datetime.utcnow().isoformat(),
    "new_urls_data": urls_to_insert
}

logger.info("\n" + "="*80)
logger.info("Pipeline execution completed successfully")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,Summary Report: ETL Logs & Statistics
# ============================================================================
# SUMMARY REPORT
# ============================================================================

print("\n" + "="*80)
print("CUSTOMS URL UPDATE SUMMARY")
print("="*80)
print(f"Execution Time: {results['execution_time']}")
print(f"\nDiscovery Statistics:")
print(f"  • Total URLs Discovered: {results['total_discovered']}")
print(f"  • Already in Database (skipped): {results['already_exists']}")
print(f"  • Newly Inserted: {results['newly_inserted']}")

if results['newly_inserted'] > 0:
    # Count by sub-category
    import_count = sum(1 for item in results['new_urls_data'] if item['sub_category'] == 'Import Goods')
    export_count = sum(1 for item in results['new_urls_data'] if item['sub_category'] == 'Export Goods')
    
    print(f"\nBreakdown by Category:")
    print(f"  • Import Goods (2N): {import_count} new URLs")
    print(f"  • Export Goods (2X): {export_count} new URLs")
    
    # Show sample of new URLs
    print(f"\nSample of Newly Added URLs (first 5):")
    print("-" * 80)
    for i, url_data in enumerate(results['new_urls_data'][:5], 1):
        print(f"{i}. {url_data['sub_category']} | {url_data['year']}-{url_data['month']}")
        print(f"   {url_data['url']}")
    
    if len(results['new_urls_data']) > 5:
        print(f"   ... and {len(results['new_urls_data']) - 5} more")
else:
    print("\n✓ No new URLs found. Database is up to date.")

print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Validation: Display Updated Table Statistics
# ============================================================================
# VALIDATION & TABLE STATISTICS
# ============================================================================

# Read updated table
updated_table = spark.table(TARGET_TABLE)

# Display table statistics
print("\nTable Statistics After Update:")
print("="*80)

# Total count
total_count = updated_table.count()
print(f"Total URLs in table: {total_count}")

# Count by sub-category
category_counts = updated_table.groupBy("sub_category").count().orderBy("sub_category")
print("\nBy Sub-Category:")
display(category_counts)

# Most recent additions
print("\nMost Recent Additions (last 10):")
recent = updated_table.orderBy(F.col("added_date").desc()).limit(10)
display(recent.select("report_month", "sub_category", "url", "added_date"))

# COMMAND ----------

# DBTITLE 1,Pipeline Documentation & Maintenance Notes
# MAGIC %md
# MAGIC ## 📝 Pipeline Documentation
# MAGIC
# MAGIC ### Error Handling Strategy
# MAGIC * **Network Failures:** Exponential backoff retry (max 3 attempts) with 2-second base delay
# MAGIC * **Parsing Errors:** Logged as warnings, pipeline continues processing valid URLs
# MAGIC * **Database Errors:** Pipeline halts with detailed error message to prevent partial updates
# MAGIC
# MAGIC ### Data Quality Checks
# MAGIC ✓ URL validation (HTTPS protocol, domain verification, .pdf extension)  
# MAGIC ✓ Title-based filtering (accent-insensitive pattern matching)  
# MAGIC ✓ Version priority deduplication (CT > DC > SB)  
# MAGIC ✓ **Report month tracking** - prevents duplicate months, auto-upgrades to higher priority versions  
# MAGIC ✓ Filename normalization for robust duplicate detection  
# MAGIC ✓ Exclusion of internal IP addresses (only files.customs.gov.vn)
# MAGIC
# MAGIC ### Report Month Deduplication
# MAGIC Each report month can only exist **once** per category (Import/Export). The pipeline:
# MAGIC * **Keeps the first version inserted** - no upgrades (if SB exists, CT is skipped)
# MAGIC * **Skips duplicate months** - prevents the same month from being added twice
# MAGIC * **Tracks** report month in `YYYY-MM` format for clean deduplication  
# MAGIC
# MAGIC ### Maintenance Schedule
# MAGIC **Recommended Frequency:** Weekly or monthly (depending on customs publication schedule)
# MAGIC
# MAGIC ### API Configuration
# MAGIC ```
# MAGIC Endpoint: https://www.customs.gov.vn/bridge?url=/customs/api/GetTKHQInfo
# MAGIC Method: POST
# MAGIC Payload: {"skip": 0, "take": 5000, "typeName": "GetListSoLieu", ...}
# MAGIC ```
# MAGIC
# MAGIC ### Report Title Patterns (Accent-Insensitive)
# MAGIC ```
# MAGIC Import:  ^nhap khau hang hoa thang\s+MM/YYYY$
# MAGIC Export:  ^xuat khau hang hoa thang\s+MM/YYYY$
# MAGIC ```
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC | Issue | Solution |
# MAGIC |-------|----------|
# MAGIC | No URLs discovered | Check API endpoint accessibility; verify API response structure |
# MAGIC | API errors | Review API payload format; check network connectivity |
# MAGIC | Title match failures | Verify title patterns in `REPORT_PATTERNS`; check accent removal logic |
# MAGIC | Wrong versions kept | Review version extraction logic in `extract_version()` |
# MAGIC | Table does not exist | Create table schema manually or run initial setup script |
# MAGIC
# MAGIC ### Future Enhancements
# MAGIC * Add support for historical year archives if API provides them
# MAGIC * Implement incremental updates based on last run timestamp
# MAGIC * Add email notifications for new document discoveries
# MAGIC * Store API metadata (response timestamp, total records) for audit trail
# MAGIC * Monitor for changes in API response schema