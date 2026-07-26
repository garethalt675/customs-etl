# Databricks notebook source
# DBTITLE 1,Extract Countries Statistics v2 - Comparison Build
# MAGIC %md
# MAGIC # Step 3: Extract Structured IMEX to Countries Trade Statistics
# MAGIC
# MAGIC This notebook transforms raw parsed JSON from **IMEX to Countries** (5N/5X pattern) into structured trade statistics by partner countries.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read raw parsed data**: Load JSON from `market_data.customs.countries_parsed_documents_raw`
# MAGIC 2. **Extract document metadata**: Parse report period/month and dates from document headers
# MAGIC 3. **Extract tables**: Parse trade statistics tables from JSON
# MAGIC 4. **Preprocess HTML**: Split cells containing `<br>` tags into separate rows (malformed HTML fix)
# MAGIC 5. **Extract country dimension**: Identify country names from table rows
# MAGIC 6. **Transform to rows**: Convert table data to structured records
# MAGIC 7. **Insert to target**: Write to `market_data.customs.countries_trade_statistics`
# MAGIC 8. **Update tracking**: Mark extraction status in `market_data.customs.countries_document_processing_log`
# MAGIC
# MAGIC ## Data Structure: Trade by Countries
# MAGIC
# MAGIC These documents contain trade statistics broken down by **partner countries**:
# MAGIC
# MAGIC * **Import (5N)**: Shows imports from each origin country
# MAGIC * **Export (5X)**: Shows exports to each destination country
# MAGIC
# MAGIC ### Example Table Structure
# MAGIC ```
# MAGIC STT | Country/Territory | Quantity | Value (USD) | ...
# MAGIC 1   | China             | 1,234.5  | 5,678,901   | ...
# MAGIC 2   | United States     | 890.2    | 3,456,789   | ...
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC ## Schema Output
# MAGIC
# MAGIC Key columns in `countries_trade_statistics` table:
# MAGIC * **sub_category** (STRING): "Import by Origin" or "Export by Destination"
# MAGIC * **report_month** (STRING): Month in YYYY-MM format
# MAGIC * **country_name** (STRING): Partner country name (normalized to English)
# MAGIC * **country_name_vi** (STRING): Vietnamese country name as it appears in source
# MAGIC * **data_quality_flag** (STRING): Normalization status (english_mapped, unmapped_country_name, etc.)
# MAGIC * **product_category** (STRING): Product name (if broken down by product)
# MAGIC * **period_quantity** / **period_value_usd**: Trade values for the period
# MAGIC * **cumulative_quantity** / **cumulative_value_usd**: Year-to-date totals
# MAGIC
# MAGIC ## Key Features
# MAGIC
# MAGIC ### HTML Preprocessing (`<br>` Tag Fix)
# MAGIC **Issue**: Some source PDFs contain malformed HTML where multiple logical rows are compressed into a single table row with `<br>` tags separating values:
# MAGIC ```html
# MAGIC <td>HÀN QUỐC<br>Product1<br>Product2</td>
# MAGIC ```
# MAGIC
# MAGIC **Solution**: The `preprocess_html_split_br_tags()` function splits these cells into separate table rows before parsing:
# MAGIC ```html
# MAGIC <tr><td>HÀN QUỐC</td>...</tr>
# MAGIC <tr><td>Product1</td>...</tr>
# MAGIC <tr><td>Product2</td>...</tr>
# MAGIC ```
# MAGIC
# MAGIC This ensures country rows are properly recognized and products are correctly associated with their countries.
# MAGIC
# MAGIC ### Country Name Normalization
# MAGIC - Vietnamese country names (e.g., "HOA KỲ", "TRUNG QUỐC") are normalized to English ("United States", "China")
# MAGIC - 150+ known aliases with fuzzy matching fallback
# MAGIC - Preserves original Vietnamese name in `country_name_vi` for audit trail
# MAGIC
# MAGIC ### Data Validation
# MAGIC - Malformed row detection (concatenated values, astronomical numbers)
# MAGIC - Product-to-country validation (products must sum to <= country total)
# MAGIC - Duplicate product detection under same country
# MAGIC - Value threshold: $100B max per row
# MAGIC
# MAGIC ## Comparison with Other IMEX Types
# MAGIC
# MAGIC | Type | Dimension | Example Rows |
# MAGIC | --- | --- | --- |
# MAGIC | **Regular IMEX (2N/2X)** | Product categories | "Rice", "Steel", "Electronics" |
# MAGIC | **FDI IMEX (3N/3X)** | Product categories (FDI only) | Same as regular |
# MAGIC | **IMEX to Countries (5N/5X)** | **Partner countries** | "China", "USA", "Japan" |

# COMMAND ----------

# DBTITLE 1,Configuration & Parameters
# ============================================================================
# ETL Configuration
# ============================================================================

# Source tables
SOURCE_RAW_TABLE = "market_data.customs.countries_parsed_documents_raw"
SOURCE_LOG_TABLE = "market_data.customs.countries_document_processing_log"

# Target table (production)
TARGET_TABLE = "market_data.customs.countries_trade_statistics"

# Processing mode
PROCESS_ALL_SUCCESSFUL_PARSED_DOCS = True  # Incremental mode: Only process new documents
WRITE_MODE = "append"
USE_MERGE_UPSERT = True  # Use MERGE to prevent duplicates
ENABLE_COUNTRY_NORMALIZATION = True
UPDATE_PROCESSING_LOG = True

# Processing parameters
MAX_PRODUCT_NAME_LENGTH = 100
MAX_UNIT_LENGTH = 10
MAX_PRODUCT_VALUE = 100e9
MAX_TABLE_DISTANCE = 999
MAX_PRODUCT_TO_COUNTRY_RATIO = 1.5

# Debug mode
DEBUG_MODE = False

print("Configuration loaded")
print(f"  Source: {SOURCE_RAW_TABLE}")
print(f"  Target: {TARGET_TABLE}")
print(f"  Process all parsed docs: {PROCESS_ALL_SUCCESSFUL_PARSED_DOCS}")
print(f"  Write mode: {WRITE_MODE}")
print(f"  Use MERGE (prevent duplicates): {USE_MERGE_UPSERT}")
print(f"  Country normalization: {ENABLE_COUNTRY_NORMALIZATION}")
print(f"  Update processing log: {UPDATE_PROCESSING_LOG}")

# COMMAND ----------

# DBTITLE 1,Load Country Mapping Table
# ============================================================================
# Load Country Name Mapping Table
# ============================================================================
# Load the mapping table built by the user and convert to dictionary

print("="*80)
print("LOADING COUNTRY NAME MAPPING")
print("="*80)

mapping_df = spark.table("market_data.customs.countries_mapping_table")
mapping_count = mapping_df.count()

# Convert to dictionary for fast lookups
COUNTRY_MAPPING = {}
for row in mapping_df.collect():
    raw_name = row['country_name_raw']
    english_name = row['english_name']
    if raw_name and english_name:
        # Store with cleaned key for robust matching
        clean_key = raw_name.strip().upper()
        COUNTRY_MAPPING[clean_key] = english_name

print(f"✓ Loaded {mapping_count} country mappings")
print(f"✓ Dictionary ready with {len(COUNTRY_MAPPING)} entries")

# COMMAND ----------

# DBTITLE 1,Reset Extraction Status in Log Table
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Reset Extraction Status
# MAGIC -- ============================================================================
# MAGIC -- Clear extraction status fields to allow re-processing of all documents
# MAGIC -- WARNING: This will reset extraction tracking for ALL documents
# MAGIC
# MAGIC
# MAGIC DELETE from market_data.customs.countries_trade_statistics;
# MAGIC
# MAGIC UPDATE market_data.customs.countries_document_processing_log
# MAGIC SET 
# MAGIC     extraction_status = NULL,
# MAGIC     extraction_timestamp = NULL,
# MAGIC     extraction_rows_inserted = NULL,
# MAGIC     extraction_error_message = NULL,
# MAGIC     updated_at = current_timestamp()
# MAGIC WHERE extraction_status IS NOT NULL;
# MAGIC
# MAGIC -- Show reset summary
# MAGIC SELECT 
# MAGIC     'Documents reset for re-extraction' AS status,
# MAGIC     COUNT(*) AS documents_affected
# MAGIC FROM market_data.customs.countries_document_processing_log
# MAGIC WHERE parse_status = 'success';
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Extract and Transform Countries Statistics
# ============================================================================
# Imports & Dependencies
# ============================================================================

import json
import re
import unicodedata
from difflib import get_close_matches
from datetime import datetime
from calendar import monthrange
from pyspark.sql.functions import col, when, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType, DoubleType
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html.parser import HTMLParser

print("="*80)
print("COUNTRIES STATISTICS EXTRACTION - UTILITIES")
print("="*80)

# ============================================================================
# HTML Table Parser
# ============================================================================

class TableHTMLParser(HTMLParser):
    """Parse HTML table into structured data."""
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = []
        self.current_row = []
        self.current_cell = ""
        self.current_colspan = 1
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        
    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
            self.current_table = []
        elif tag == 'tr' and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ['td', 'th'] and self.in_row:
            self.in_cell = True
            self.current_cell = ""
            self.current_colspan = 1
            for attr_name, attr_value in attrs:
                if attr_name == 'colspan':
                    try:
                        self.current_colspan = int(attr_value)
                    except (ValueError, TypeError):
                        self.current_colspan = 1
    
    def handle_endtag(self, tag):
        if tag == 'table':
            if self.current_table:
                self.tables.append(self.current_table)
            self.in_table = False
        elif tag == 'tr' and self.in_row:
            if self.current_row:
                self.current_table.append(self.current_row)
            self.in_row = False
        elif tag in ['td', 'th'] and self.in_cell:
            for _ in range(self.current_colspan - 1):
                self.current_row.append("")
            self.current_row.append(self.current_cell.strip())
            self.in_cell = False
            self.current_colspan = 1
    
    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

print("✓ HTML Parser loaded")

# COMMAND ----------

# DBTITLE 1,Data Extraction Logic
# ============================================================================
# Main Data Extraction Function
# ============================================================================

def process_countries_table_rows(table_data, start_row_number=0, initial_country=None, initial_country_table=None, table_idx=0, initial_country_total=None, initial_product_sum=None, initial_seen_products=None):
    """Process hierarchical table: Extract BOTH country totals AND product details.
    
    ENHANCED WITH STRICT VALIDATION + COLSPAN FIX:
    - Track country total value (from product_category IS NULL row)
    - Track cumulative product values assigned to that country
    - Stop assigning products when sum exceeds country_total * MAX_PRODUCT_TO_COUNTRY_RATIO
    - Detect duplicate product categories under same country
    - Prevent orphaned products from being assigned to wrong countries
    - Handle HTML colspan=\"2\" in country headers (shifts name to row[1])
    
    Table structure:
    Row 0: Headers ['Nước/Mặt hàng chủ yếu', 'DVT', '', 'Số liệu tháng báo cáo', '', 'Cộng dồn...']
    Row 1: Sub-headers ['', '', 'Lượng', 'Trị giá (USD)', 'Lượng', 'Trị giá (USD)']
    Row 2+: Data
      - Normal country row: ['CHINA', '', '', '5,000,000', '', '15,000,000']
      - Colspan country row: ['', 'CHINA', '', '5,000,000', '', '15,000,000'] <- Name shifted to [1]
      - Product rows: ['Computers', 'USD', '100', '2,000,000', '500', '8,000,000']
    
    CRITICAL: Values are ALWAYS in row[2], row[3], row[4], row[5] regardless of colspan!
    The colspan only shifts the country name from row[0] to row[1].
    """
    rows = []
    skipped_malformed = 0
    skipped_validation = 0
    
    if not table_data or len(table_data) < 3:
        return rows, initial_country, initial_country_table, start_row_number, initial_country_total, initial_product_sum, initial_seen_products, skipped_malformed, skipped_validation
    
    # Check if this is a new table starting with a country row
    if table_starts_with_country(table_data):
        # New country section - reset context
        current_country = None
        current_country_table = None
        current_country_total = None
        product_sum = Decimal('0')
        seen_products = set()
    else:
        # Continuation table - carry context from previous table
        if initial_country and initial_country_table is not None:
            table_distance = table_idx - initial_country_table
            if table_distance > MAX_TABLE_DISTANCE:
                current_country = None
                current_country_table = None
                current_country_total = None
                product_sum = Decimal('0')
                seen_products = set()
            else:
                current_country = initial_country
                current_country_table = initial_country_table
                current_country_total = initial_country_total or Decimal('0')
                product_sum = initial_product_sum or Decimal('0')
                seen_products = initial_seen_products or set()
        else:
            current_country = initial_country
            current_country_table = initial_country_table
            current_country_total = initial_country_total or Decimal('0')
            product_sum = initial_product_sum or Decimal('0')
            seen_products = initial_seen_products or set()
    
    # Skip first 2 header rows
    data_rows = table_data[2:]
    row_counter = start_row_number
    
    for row in data_rows:
        if not isinstance(row, list) or len(row) < 4:
            continue
        
        try:
            # =====================================================================
            # COLSPAN FIX: Check both row[0] and row[1] for country name
            # =====================================================================
            # HTML colspan=\"2\" creates empty cell, shifting name to row[1]:
            #   <td colspan=\"2\"><b>CHINA</b></td> → ['', 'CHINA', '', '5000000', ...]
            # =====================================================================
            name_field_0 = str(row[0]).strip() if len(row) > 0 else ""
            name_field_1 = str(row[1]).strip() if len(row) > 1 else ""
            
            # Determine name and unit position
            if not name_field_0 and name_field_1:
                # Colspan case: name shifted to row[1], unit to row[2]
                name_field = name_field_1
                unit_field = str(row[2]).strip() if len(row) > 2 else ""
            else:
                # Normal case: name in row[0], unit in row[1]
                name_field = name_field_0
                unit_field = str(row[1]).strip() if len(row) > 1 else ""
            
            # Skip empty rows
            if not name_field or name_field == '':
                continue
            
            # Skip header-like rows
            if name_field.upper() in ['NƯỚC/MẶT HÀNG CHỦ YẾU', 'COUNTRY/TERRITORY', 'DVT', 'STT']:
                continue
            
            # Skip aggregate rows
            if 'TỔNG' in name_field.upper() or 'Trong đó:' in name_field:
                continue
            
            # Determine if this is a COUNTRY or PRODUCT row
            name_no_spaces = name_field.replace(' ', '')
            is_country_row = (name_field == name_field.upper() and 
                            name_no_spaces.isalpha() and 
                            len(name_field) > 2)
            
            row_counter += 1
            
            if is_country_row:
                # This is a COUNTRY total row
                current_country = name_field
                current_country_table = table_idx
                
                # CRITICAL: Values are ALWAYS in row[3] and row[5], regardless of colspan!
                period_val = safe_decimal(row[3]) if len(row) > 3 else None
                cumul_val = safe_decimal(row[5]) if len(row) > 5 else None
                
                # Validate country total
                is_bad, reason = is_malformed_row(name_field, '', period_val)
                if is_bad:
                    if DEBUG_MODE:
                        print(f"  ⚠ Skipping malformed country row {row_counter}: {reason}")
                    skipped_malformed += 1
                    current_country = None
                    current_country_table = None
                    current_country_total = None
                    continue
                
                # Store country total for validation
                current_country_total = period_val or Decimal('0')
                product_sum = Decimal('0')
                seen_products = set()
                
                rows.append({
                    'row_number': row_counter,
                    'country_name': current_country,
                    'product_category': None,  # NULL for country totals
                    'unit': 'USD',
                    'period_quantity': None,
                    'period_value_usd': period_val,
                    'cumulative_quantity': None,
                    'cumulative_value_usd': cumul_val
                })
                
            else:
                # This is a PRODUCT row under the current country
                if current_country is None:
                    continue
                
                product_name = name_field
                
                # Values are ALWAYS in standard positions
                if unit_field and unit_field != 'USD':
                    period_qty = safe_decimal(row[2]) if len(row) > 2 else None
                    period_val = safe_decimal(row[3]) if len(row) > 3 else None
                    cumul_qty = safe_decimal(row[4]) if len(row) > 4 else None
                    cumul_val = safe_decimal(row[5]) if len(row) > 5 else None
                    unit = unit_field
                else:
                    period_qty = None
                    period_val = safe_decimal(row[3]) if len(row) > 3 else None
                    cumul_qty = None
                    cumul_val = safe_decimal(row[5]) if len(row) > 5 else None
                    unit = 'USD'
                
                # VALIDATE: Check if this is a malformed row
                is_bad, reason = is_malformed_row(product_name, unit, period_val)
                if is_bad:
                    if DEBUG_MODE:
                        print(f"  ⚠ Skipping malformed product row {row_counter} (table {table_idx}): {reason}")
                        print(f"    Country: {current_country}, Product: {product_name[:60]}...")
                    skipped_malformed += 1
                    continue
                
                # VALIDATE: Check for duplicate products
                if product_name in seen_products:
                    if DEBUG_MODE:
                        print(f"  ⚠ Duplicate product detected at row {row_counter}: '{product_name}'")
                        print(f"    Under country: {current_country}")
                        print(f"    This suggests a missing country header. Stopping product assignment.")
                    skipped_validation += 1
                    # Stop assigning products to this country
                    current_country = None
                    current_country_table = None
                    current_country_total = None
                    product_sum = Decimal('0')
                    seen_products = set()
                    continue
                
                # VALIDATE: Check if product sum exceeds country total
                if current_country_total and period_val:
                    new_product_sum = product_sum + period_val
                    ratio = float(new_product_sum) / float(current_country_total) if current_country_total > 0 else 0
                    
                    if ratio > MAX_PRODUCT_TO_COUNTRY_RATIO:
                        if DEBUG_MODE:
                            print(f"  ⚠ VALIDATION FAILURE at row {row_counter} (table {table_idx})")
                            print(f"    Country: {current_country} (total: ${current_country_total:,.0f})")
                            print(f"    Product sum so far: ${product_sum:,.0f}")
                            print(f"    This product: '{product_name}' (${period_val:,.0f})")
                            print(f"    New ratio would be {ratio:.1f}x (threshold: {MAX_PRODUCT_TO_COUNTRY_RATIO}x)")
                            print(f"    Likely missing country header. Stopping product assignment.")
                        skipped_validation += 1
                        # Stop assigning products to this country
                        current_country = None
                        current_country_table = None
                        current_country_total = None
                        product_sum = Decimal('0')
                        seen_products = set()
                        continue
                
                # Valid product - add to results
                product_sum += (period_val or Decimal('0'))
                seen_products.add(product_name)
                
                rows.append({
                    'row_number': row_counter,
                    'country_name': current_country,
                    'product_category': product_name,
                    'unit': unit,
                    'period_quantity': period_qty,
                    'period_value_usd': period_val,
                    'cumulative_quantity': cumul_qty,
                    'cumulative_value_usd': cumul_val
                })
                
        except Exception as e:
            if DEBUG_MODE:
                print(f"  ⚠ Error processing row {row_counter}: {e}")
            continue
    
    return rows, current_country, current_country_table, row_counter, current_country_total, product_sum, seen_products, skipped_malformed, skipped_validation

print("✓ Extraction logic loaded with colspan fix and strict validation")

# COMMAND ----------

# DBTITLE 1,Utility Functions
# ============================================================================
# Utility Functions
# ============================================================================

def preprocess_html_split_br_tags(html_content):
    """Preprocess HTML to split cells with <br> tags into multiple rows.
    
    FIXES MALFORMED HTML ISSUE:
    Source PDFs sometimes contain tables where multiple logical rows are
    compressed into a single HTML <tr> with <br> tags separating values:
    
    Before:
    <tr>
      <td>HÀN QUỐC<br>Product1<br>Product2</td>
      <td>USD<br>USD<br>USD</td>
      <td>100<br>200<br>300</td>
    </tr>
    
    After:
    <tr><td>HÀN QUỐC</td><td></td><td>100</td></tr>
    <tr><td>Product1</td><td>USD</td><td>200</td></tr>
    <tr><td>Product2</td><td>USD</td><td>300</td></tr>
    
    This ensures the country row is properly recognized and products are
    correctly associated with their country.
    """
    import re
    
    # Pattern to find table rows
    row_pattern = re.compile(r'<tr>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
    
    def process_row(row_content):
        # Extract all cells
        cell_pattern = re.compile(r'<(td|th)(.*?)>(.*?)</\1>', re.DOTALL | re.IGNORECASE)
        cells = list(cell_pattern.finditer(row_content))
        
        if not cells:
            return f'<tr>{row_content}</tr>'
        
        # Split each cell by <br> tags (case-insensitive)
        cell_values = []
        max_splits = 0
        
        for cell_match in cells:
            tag_name = cell_match.group(1)
            tag_attrs = cell_match.group(2)
            cell_content = cell_match.group(3)
            
            # Split on <br>, <br/>, <br />, or <BR> (case-insensitive)
            parts = re.split(r'<br\s*/?>', cell_content, flags=re.IGNORECASE)
            cell_values.append((tag_name, tag_attrs, parts))
            max_splits = max(max_splits, len(parts))
        
        # If no <br> tags found, return original row
        if max_splits <= 1:
            return f'<tr>{row_content}</tr>'
        
        # Create multiple rows, one for each <br>-separated value
        new_rows = []
        for i in range(max_splits):
            new_cells = []
            for tag_name, tag_attrs, parts in cell_values:
                # Get the i-th part, or empty string if this cell has fewer parts
                value = parts[i].strip() if i < len(parts) else ''
                new_cells.append(f'<{tag_name}{tag_attrs}>{value}</{tag_name}>')
            new_rows.append(f'<tr>{"".join(new_cells)}</tr>')
        
        return ''.join(new_rows)
    
    # Process each row
    def replace_row(match):
        return process_row(match.group(1))
    
    processed_html = row_pattern.sub(replace_row, html_content)
    return processed_html

def extract_report_metadata(parsed_text):
    """Extract report period/month and date range from document header."""
    report_period = None
    report_month = None
    start_date = None
    end_date = None
    
    try:
        month_match = re.search(r'Tháng\s+(\d+)\s+năm\s+(\d{4})', parsed_text)
        if month_match:
            month_num = int(month_match.group(1))
            year_num = int(month_match.group(2))
            report_month = f"{year_num:04d}-{month_num:02d}"
            report_period = f"Tháng {month_num} năm {year_num}"
            start_date = datetime(year_num, month_num, 1).date()
            last_day = monthrange(year_num, month_num)[1]
            end_date = datetime(year_num, month_num, last_day).date()
            if DEBUG_MODE:
                print(f"  Format: Monthly report")
                print(f"  Month: {report_month}")
        else:
            period_match = re.search(r'(Kỳ\s+[IVX]+\s+tháng\s+(\d+)\s+năm\s+(\d{4}))', parsed_text)
            if period_match:
                report_period = period_match.group(1)
                month_num = int(period_match.group(2))
                year_num = int(period_match.group(3))
                report_month = f"{year_num:04d}-{month_num:02d}"
                if DEBUG_MODE:
                    print(f"  Format: Periodic report")
                    print(f"  Period: {report_period}")
                    print(f"  Month: {report_month}")
            
            date_match = re.search(r'Từ ngày (\d{2}/\d{2}/\d{4}) đến hết ngày (\d{2}/\d{2}/\d{4})', parsed_text)
            if date_match:
                start_str, end_str = date_match.groups()
                start_date = datetime.strptime(start_str, '%d/%m/%Y').date()
                end_date = datetime.strptime(end_str, '%d/%m/%Y').date()
    except Exception as e:
        print(f"  ⚠ Could not extract metadata: {e}")
    
    return report_period, report_month, start_date, end_date

# Target columns are DECIMAL(20,3): 17 integer digits plus 3 decimal places.
# A mis-parsed cell can produce a value beyond that, which raises
# decimal.InvalidOperation inside createDataFrame() and kills the whole run.
DECIMAL_QUANTUM = Decimal('0.001')
DECIMAL_MAX = Decimal(10) ** 17

def fit_decimal(value, default=None):
    """Fit a parsed Decimal to the DECIMAL(20,3) domain, or drop it."""
    if value is None:
        return default
    try:
        if not value.is_finite() or abs(value) >= DECIMAL_MAX:
            print(f"  ⚠ Value outside DECIMAL(20,3), dropped: {value}")
            return default
        return value.quantize(DECIMAL_QUANTUM, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        print(f"  ⚠ Value could not be quantized, dropped: {value}")
        return default

def safe_decimal(value, default=None):
    """Safely convert Vietnamese number format to Decimal."""
    if value is None or value == '':
        return default
    try:
        clean_value = str(value).replace('.', '').replace(' ', '')
        clean_value = clean_value.replace(',', '.')
        clean_value = clean_value.replace('−', '-')
        return fit_decimal(Decimal(clean_value), default)
    except (InvalidOperation, ValueError):
        return default

def extract_tables_from_json(parsed_json_str):
    """Extract table data from ai_parse_document() v2.0 JSON result.
    
    ENHANCEMENT: Preprocesses HTML to split <br>-separated cells into multiple rows.
    """
    try:
        if isinstance(parsed_json_str, str):
            parsed_data = json.loads(parsed_json_str)
        else:
            parsed_data = parsed_json_str
        
        elements = parsed_data.get('document', {}).get('elements', [])
        
        html_tables = []
        for elem in elements:
            if elem.get('type') == 'table':
                html_content = elem.get('content', '')
                if html_content:
                    # CRITICAL FIX: Preprocess to split <br> tags into separate rows
                    processed_html = preprocess_html_split_br_tags(html_content)
                    html_tables.append(processed_html)
        
        text_parts = []
        for elem in elements:
            if elem.get('type') in ['text', 'title', 'page_header', 'section_header']:
                content = elem.get('content', '')
                if content:
                    text_parts.append(content)
        
        full_text = '\n'.join(text_parts)
        
        structured_tables = []
        for html_table in html_tables:
            parser = TableHTMLParser()
            parser.feed(html_table)
            structured_tables.extend(parser.tables)
        
        return structured_tables, full_text
        
    except Exception as e:
        print(f"  ⚠ Error parsing JSON: {e}")
        import traceback
        traceback.print_exc()
        return [], ""

def is_malformed_row(name_field, unit_field, period_value):
    """Detect malformed rows from bad HTML parsing.
    
    Returns (is_malformed, reason)
    """
    # Check 1: Abnormally long product name (likely concatenated)
    if len(name_field) > MAX_PRODUCT_NAME_LENGTH:
        return True, f"Product name too long ({len(name_field)} chars)"
    
    # Check 2: Concatenated units (multiple units in one field)
    if unit_field and len(unit_field) > MAX_UNIT_LENGTH:
        return True, f"Unit field too long ({len(unit_field)} chars)"
    
    if unit_field and unit_field.count('USD') > 1:
        return True, "Multiple USD in unit field"
    
    # Check 3: Astronomical values (> configured threshold)
    if period_value and period_value > Decimal(str(MAX_PRODUCT_VALUE)):
        return True, f"Value too large (${period_value:,.0f})"
    
    # Check 4: Product name contains multiple capital words with no spaces
    # This catches "Hạt điềuHàng hóa khác" pattern
    if re.search(r'[a-zà-ỹ]{3,}[A-ZÀ-Ỹ][a-zà-ỹ]{3,}', name_field):
        return True, "Concatenated product names detected"
    
    return False, None

def table_starts_with_country(table_data):
    """Check if table starts with a country row (not a continuation)."""
    if not table_data or len(table_data) < 3:
        return False
    
    # Skip header rows and check first data row
    data_rows = table_data[2:]
    if not data_rows:
        return False
    
    first_row = data_rows[0]
    if not isinstance(first_row, list) or len(first_row) < 1:
        return False
    
    name_field = str(first_row[0]).strip()
    if not name_field or len(name_field) < 3:
        return False
    
    # Check if it's a country row (ALL CAPS, alphabetic)
    name_no_spaces = name_field.replace(' ', '')
    is_country = (name_field == name_field.upper() and 
                  name_no_spaces.isalpha() and 
                  len(name_field) > 2)
    
    return is_country

print("✓ Utility functions loaded")
print("✓ HTML preprocessing enabled (<br> tag splitting)")

# ============================================================================
# Country Name Normalization (Using Mapping Table)
# ============================================================================
# Simple lookup from the user-built mapping table loaded above.
# No fuzzy matching, no AI queries - just clean lookups.

def remove_accents(text):
    """Remove Vietnamese accent marks for matching against mapping table."""
    if text is None:
        return None
    # Replace Đ/đ first (special case)
    text = str(text).replace('Đ', 'D').replace('đ', 'd')
    # Normalize unicode and remove combining marks
    text = unicodedata.normalize('NFD', text)
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
    return text

def clean_country_name(country_name):
    """Normalize whitespace and punctuation without changing identity."""
    if country_name is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(country_name).strip().upper())
    cleaned = cleaned.replace("–", "-").replace("—", "-")
    cleaned = re.sub(r"\s*-\s*", "-", cleaned)
    return cleaned

def normalize_for_mapping(country_name):
    """Transform country name to match mapping table format (no accents, clean)."""
    if country_name is None:
        return None
    # First clean whitespace/punctuation
    cleaned = clean_country_name(country_name)
    # Then remove accents to match mapping table
    return remove_accents(cleaned)

def country_name_to_english(country_name):
    """Map Vietnamese country name to English using mapping table.
    
    Returns (english_name, quality_flag)
    """
    cleaned = clean_country_name(country_name)
    
    if not ENABLE_COUNTRY_NORMALIZATION or cleaned is None:
        return cleaned, "missing_country" if cleaned is None else "not_normalized"
    
    # Transform to match mapping table format (no accents)
    lookup_key = normalize_for_mapping(country_name)
    
    # Lookup in mapping table
    if lookup_key and lookup_key in COUNTRY_MAPPING:
        return COUNTRY_MAPPING[lookup_key], "mapped"
    
    # Not found in mapping - preserve original and flag it
    return cleaned, "unmapped_country_name"

print(f"✓ Country normalization loaded (using mapping table with {len(COUNTRY_MAPPING)} entries)")
print("✓ Accent removal enabled for mapping table lookups")

# COMMAND ----------

# DBTITLE 1,Load Raw Parsed Data
# ============================================================================
# STEP 1: Load Raw Parsed Data
# ============================================================================
# Load documents that have been parsed but not yet extracted into structured format

print("="*80)
print("STEP 1: LOADING RAW PARSED DATA")
print("="*80)

# Determine filter condition
if PROCESS_ALL_SUCCESSFUL_PARSED_DOCS:
    # Full refresh mode - process all successfully parsed documents
    filter_condition = "log.parse_status = 'success'"
    mode_desc = "all successfully parsed documents (full refresh mode)"
else:
    # Incremental mode - process only new documents (not yet extracted)
    filter_condition = "log.parse_status = 'success' AND log.extraction_status IS NULL"
    mode_desc = "new documents only (incremental mode)"

raw_df = spark.sql(f"""
    SELECT
        raw.document_id,
        raw.document_url,
        raw.sub_category,
        raw.parsed_json,
        log.parse_status
    FROM {SOURCE_RAW_TABLE} AS raw
    INNER JOIN {SOURCE_LOG_TABLE} AS log
        ON raw.document_id = log.document_id
    WHERE {filter_condition}
""")

docs_to_process = raw_df.count()

print(f"\nDocuments to process: {docs_to_process} ({mode_desc})")

if docs_to_process == 0:
    print("\n✓ No documents to extract!")
    dbutils.notebook.exit("No documents to process")
else:
    print(f"✓ Loaded {docs_to_process} documents ready for extraction")

# COMMAND ----------

# DBTITLE 1,Extract and Transform Data
# ============================================================================
# STEP 2: Extract and Transform Data
# ============================================================================
# Parse JSON documents and extract structured country-product statistics

print("\n" + "="*80)
print("STEP 2: EXTRACTING STRUCTURED DATA")
print("="*80)

all_statistics = []
errors = []

for row in raw_df.collect():
    doc_id = row['document_id']
    doc_url = row['document_url']
    sub_category = row['sub_category']
    parsed_json = row['parsed_json']
    
    print(f"\nProcessing document: {doc_id} ({sub_category})")
    
    try:
        # Extract tables and metadata from JSON
        tables, text = extract_tables_from_json(parsed_json)
        report_period, report_month, start_date, end_date = extract_report_metadata(text)
        
        print(f"  Period: {report_period}")
        print(f"  Dates: {start_date} to {end_date}")
        print(f"  Tables found: {len(tables)}")
        
        # Process each table with validation tracking
        row_count = 0
        country_count = 0
        product_count = 0
        doc_row_number = 0
        current_country = None
        current_country_table = None
        current_country_total = None
        product_sum = Decimal('0')
        seen_products = set()
        total_skipped_malformed = 0
        total_skipped_validation = 0
        
        for table_idx, table in enumerate(tables):
            if DEBUG_MODE:
                print(f"  Processing table {table_idx + 1}: {len(table)} rows")
            
            table_rows, current_country, current_country_table, doc_row_number, current_country_total, product_sum, seen_products, skipped_malformed, skipped_validation = process_countries_table_rows(
                table,
                start_row_number=doc_row_number,
                initial_country=current_country,
                initial_country_table=current_country_table,
                table_idx=table_idx + 1,
                initial_country_total=current_country_total,
                initial_product_sum=product_sum,
                initial_seen_products=seen_products
            )
            
            total_skipped_malformed += skipped_malformed
            total_skipped_validation += skipped_validation
            
            # Add document metadata to each row
            for row_data in table_rows:
                all_statistics.append({
                    'sub_category': sub_category,
                    'document_id': doc_id,
                    'report_period': report_period,
                    'report_month': report_month,
                    'report_start_date': start_date,
                    'report_end_date': end_date,
                    'row_number': row_data['row_number'],
                    'country_name_raw': clean_country_name(row_data['country_name']),
                    'country_name_vi': clean_country_name(row_data['country_name']),
                    'country_name': country_name_to_english(row_data['country_name'])[0],
                    'data_quality_flag': country_name_to_english(row_data['country_name'])[1],
                    'product_category': row_data['product_category'],
                    'unit': row_data['unit'],
                    'period_quantity': row_data['period_quantity'],
                    'period_value_usd': row_data['period_value_usd'],
                    'cumulative_quantity': row_data['cumulative_quantity'],
                    'cumulative_value_usd': row_data['cumulative_value_usd'],
                    'parsed_timestamp': datetime.now()
                })
                row_count += 1
                
                # Count countries vs products
                if row_data['product_category'] is None:
                    country_count += 1
                else:
                    product_count += 1
        
        if total_skipped_malformed > 0:
            print(f"  ⚠ Skipped {total_skipped_malformed} malformed rows")
        if total_skipped_validation > 0:
            print(f"  ⚠ Stopped {total_skipped_validation} products due to validation failures")
        print(f"  ✓ Extracted {row_count} rows: {country_count} countries + {product_count} products")
        
    except Exception as e:
        error_msg = f"Error processing {doc_id}: {str(e)}"
        print(f"  ✗ {error_msg}")
        if DEBUG_MODE:
            import traceback
            traceback.print_exc()
        errors.append({'document_id': doc_id, 'error': error_msg})

print(f"\n{'='*80}")
print(f"Total rows extracted: {len(all_statistics)}")
if errors:
    print(f"⚠ Errors encountered: {len(errors)}")
else:
    print("✓ All documents processed successfully")

# COMMAND ----------

# DBTITLE 1,Insert into Target Table
# ============================================================================
# STEP 3: Insert into Target Table
# ============================================================================
# Write extracted statistics to the target table and update processing log

if len(all_statistics) == 0:
    print("\n" + "="*80)
    print("No data to insert - all documents failed extraction")
    print("="*80)
else:
    print("\n" + "="*80)
    print("STEP 3: INSERTING INTO TARGET TABLE")
    print("="*80)
    
    # =========================================================================
    # IMPORTANT: Keep BOTH country totals AND product details
    # =========================================================================
    # Country-level rows (product_category IS NULL) contain official totals
    # from source documents. These are authoritative values that often exceed
    # the sum of listed products (unlisted products, aggregation differences).
    # We store BOTH to enable validation and preserve official statistics.
    # =========================================================================
    country_totals = [stat for stat in all_statistics if stat.get('product_category') is None]
    product_details = [stat for stat in all_statistics if stat.get('product_category') is not None]
    
    print(f"\nData composition:")
    print(f"  Total rows extracted: {len(all_statistics):,}")
    print(f"  Country totals: {len(country_totals):,}")
    print(f"  Product details: {len(product_details):,}")
    print(f"  Both will be inserted to preserve official country totals")
    
    if len(all_statistics) == 0:
        print("\n⚠ No data to insert")
    else:
        # Convert numeric values to Decimal with 3 decimal places to match target table schema
        for stat in all_statistics:
            for key in ['period_quantity', 'period_value_usd', 'cumulative_quantity', 'cumulative_value_usd']:
                if stat[key] is not None:
                    try:
                        if not isinstance(stat[key], Decimal):
                            stat[key] = Decimal(str(stat[key]))
                        # Check if value is too large for DECIMAL(20,3)
                        if abs(stat[key]) >= Decimal('1E17'):
                            stat[key] = None
                        else:
                            stat[key] = stat[key].quantize(Decimal('0.001'))
                    except (InvalidOperation, ValueError):
                        stat[key] = None
        
        # Define schema (matching target table - 15 columns)
        schema = StructType([
            StructField("sub_category", StringType(), False),
            StructField("document_id", StringType(), False),
            StructField("report_period", StringType(), True),
            StructField("report_month", StringType(), True),
            StructField("report_start_date", DateType(), True),
            StructField("report_end_date", DateType(), True),
            StructField("row_number", IntegerType(), False),
            StructField("country_name", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("period_quantity", DecimalType(20, 3), True),
            StructField("period_value_usd", DecimalType(20, 3), True),
            StructField("cumulative_quantity", DecimalType(20, 3), True),
            StructField("cumulative_value_usd", DecimalType(20, 3), True),
            StructField("parsed_timestamp", TimestampType(), True),
            StructField("product_category", StringType(), True)
        ])
        
        # Select only the 15 columns that exist in target table
        filtered_statistics = [
            {
                'sub_category': stat['sub_category'],
                'document_id': stat['document_id'],
                'report_period': stat['report_period'],
                'report_month': stat['report_month'],
                'report_start_date': stat['report_start_date'],
                'report_end_date': stat['report_end_date'],
                'row_number': stat['row_number'],
                'country_name': stat['country_name'],
                'unit': stat['unit'],
                'period_quantity': stat['period_quantity'],
                'period_value_usd': stat['period_value_usd'],
                'cumulative_quantity': stat['cumulative_quantity'],
                'cumulative_value_usd': stat['cumulative_value_usd'],
                'parsed_timestamp': stat['parsed_timestamp'],
                'product_category': stat['product_category']
            }
            for stat in all_statistics  # Use all_statistics, not product_statistics
        ]
        
        # Create DataFrame
        stats_df = spark.createDataFrame(filtered_statistics, schema=schema)
        
        # Use MERGE to prevent duplicates (if enabled)
        if USE_MERGE_UPSERT:
            print("\nUsing MERGE to prevent duplicates...")
            stats_df.createOrReplaceTempView("temp_new_statistics")
            
            merge_query = f"""
            MERGE INTO {TARGET_TABLE} AS target
            USING temp_new_statistics AS source
            ON target.document_id = source.document_id
               AND target.row_number = source.row_number
               AND target.sub_category = source.sub_category
               AND COALESCE(target.product_category, '') = COALESCE(source.product_category, '')
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            
            spark.sql(merge_query)
            rows_affected = len(filtered_statistics)
            print(f"\n✓ MERGE completed: {rows_affected:,} rows processed (upserted)")
        else:
            # Fallback to append mode (original behavior)
            stats_df.write.mode(WRITE_MODE).saveAsTable(TARGET_TABLE)
            rows_affected = len(filtered_statistics)
            print(f"\n✓ Wrote {rows_affected:,} rows to {TARGET_TABLE}")
        
        # Update processing log
        if UPDATE_PROCESSING_LOG:
            print("\nUpdating processing log...")
            
            doc_stats = {}
            for stat in filtered_statistics:
                doc_id = stat['document_id']
                if doc_id not in doc_stats:
                    doc_stats[doc_id] = 0
                doc_stats[doc_id] += 1
            
            for doc_id, count in doc_stats.items():
                spark.sql(f"""
                    UPDATE {SOURCE_LOG_TABLE}
                    SET
                        extraction_status = 'success',
                        extraction_timestamp = current_timestamp(),
                        extraction_rows_inserted = {count},
                        extraction_error_message = NULL
                    WHERE document_id = '{doc_id}'
                """)
            
            print(f"✓ Updated processing log for {len(doc_stats)} documents")
        
        # Track failed extractions
        if len(errors) > 0:
            print(f"\nUpdating log for {len(errors)} failed documents...")
            for err in errors:
                error_msg = str(err['error']).replace("'", "''")
                spark.sql(f"""
                    UPDATE {SOURCE_LOG_TABLE}
                    SET
                        extraction_status = 'failed',
                        extraction_timestamp = current_timestamp(),
                        extraction_rows_inserted = 0,
                        extraction_error_message = '{error_msg}'
                    WHERE document_id = '{err['document_id']}'
                """)
            print(f"✓ Marked {len(errors)} documents as failed")