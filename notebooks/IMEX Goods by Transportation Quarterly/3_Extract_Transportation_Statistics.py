# Databricks notebook source
# DBTITLE 1,Extract Provinces Statistics - Overview
# MAGIC %md
# MAGIC # Step 3: Extract Structured Trade Statistics - Transportation Quarterly
# MAGIC
# MAGIC This notebook transforms raw parsed JSON into structured trade statistics for **quarterly reports by transportation method**.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read raw parsed data**: Load JSON from `market_data.customs.transportation_parsed_documents_raw`
# MAGIC 2. **Extract document metadata**: Parse report period (Q1/Q2/Q3/Q4), year, and dates
# MAGIC 3. **Extract tables**: Parse trade statistics tables from JSON
# MAGIC 4. **Detect hierarchy**: Identify parent-child relationships (categories and sub-categories)
# MAGIC 5. **Extract vehicle breakdown**: Capture transportation method (Road, Air, Sea, Other) for each product
# MAGIC 6. **Transform to rows**: Convert table data to structured records
# MAGIC 7. **Insert to target**: Write to `market_data.customs.transportation_trade_statistics`
# MAGIC 8. **Update tracking**: Mark extraction status in processing log
# MAGIC
# MAGIC ## Supported Report Formats
# MAGIC
# MAGIC The notebook handles **quarterly reports** from Vietnamese customs:
# MAGIC
# MAGIC ### Quarterly Reports (THEO QUÝ)
# MAGIC - **Header format**: "Quý 1 năm 2026" or "Q1/2026"
# MAGIC - **Date range**: Start of quarter to end of quarter
# MAGIC   * Q1: January 1 - March 31
# MAGIC   * Q2: April 1 - June 30
# MAGIC   * Q3: July 1 - September 30
# MAGIC   * Q4: October 1 - December 31
# MAGIC - **Transportation Methods**: Data broken down by vehicle type (Đường bộ, Đường không, Đường thủy, Loại khác)
# MAGIC
# MAGIC ## Key Features
# MAGIC
# MAGIC * **Reporting Period**: Quarterly (Q1-Q4)
# MAGIC * **Metadata Extraction**: Recognize "Quý" patterns and quarter numbers
# MAGIC * **Date Calculation**: Derive 3-month date ranges from quarter
# MAGIC * **Vehicle Dimension**: Tables have transportation methods (road/air/sea/other) as rows
# MAGIC * **Total Handling**: Excludes "Cộng" (Total) rows to prevent double-counting
# MAGIC * **Target Table**: `transportation_trade_statistics` with `vehicle_type` column
# MAGIC
# MAGIC ## Schema Output
# MAGIC
# MAGIC Key columns in `transportation_trade_statistics` table:
# MAGIC * **sub_category** (STRING): "Import Transportation" or "Export Transportation"
# MAGIC * **report_period** (STRING): "Q1 2026" format
# MAGIC * **report_quarter** (STRING): Quarter in YYYY-QN format (e.g., "2026-Q1")
# MAGIC * **report_start_date** / **report_end_date** (DATE): Date range for the quarter
# MAGIC * **product_category** (STRING): Product name
# MAGIC * **parent_category** (STRING): Parent category for hierarchical items
# MAGIC * **vehicle_type** (STRING): Transportation method (Đường bộ/Đường không/Đường thủy/Loại khác)
# MAGIC * **quantity** / **value_usd**: Trade values for the quarter
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC * **Partial failures**: Process all documents, track which failed
# MAGIC * **Invalid data**: Log errors but don't stop the workflow  
# MAGIC * **Summary report**: Display per-category statistics on success/failure rates

# COMMAND ----------

# DBTITLE 1,Helper Functions for Transportation Extraction
import json
import re
import unicodedata
from datetime import datetime, date
from calendar import monthrange
from pyspark.sql.functions import col, when, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html.parser import HTMLParser

print("="*80)
print("TRANSPORTATION STATISTICS EXTRACTION WORKFLOW - QUARTERLY")
print("="*80)

class TableHTMLParser(HTMLParser):
    """Parse HTML table into structured data with colspan handling."""
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

def extract_report_metadata_quarterly(parsed_text):
    """
    Extract quarterly report metadata from document header.
    
    Handles formats like:
    - "Quý 1 năm 2026"
    - "QUÝ IV-NĂM 2025" (with hyphen)
    - "Quý IV/2019" (with slash separator)
    - "Q1/2026"
    - "Quý I năm 2026"
    
    Returns:
        tuple: (report_period, report_quarter, start_date, end_date)
    """
    report_period = None
    report_quarter = None
    start_date = None
    end_date = None
    
    try:
        # Try quarter format with flexible separators (slash, hyphen, or spaces)
        # Pattern 1: "Quý IV/2019" or "Quý III/2019" (slash separator)
        quarter_match = re.search(r'Qu[ýy]\s+([IVX1-4]+)[\s/-]*(\d{4})', parsed_text, re.IGNORECASE)
        
        if not quarter_match:
            # Pattern 2: "Quý 1 năm 2026" or "QUÝ IV-NĂM 2025" (with "năm")
            quarter_match = re.search(r'Qu[ýy]\s+([IVX1-4]+)[\s-]*n[ăa]m[\s-]*(\d{4})', parsed_text, re.IGNORECASE)
        
        if not quarter_match:
            # Pattern 3: "Q1/2026" or "Q1 2026" (short format)
            quarter_match = re.search(r'Q([1-4])[/\s-]+(\d{4})', parsed_text, re.IGNORECASE)
        
        if quarter_match:
            quarter_str = quarter_match.group(1)
            year_num = int(quarter_match.group(2))
            
            # Convert roman numerals or numbers to quarter number
            roman_to_num = {'I': 1, 'II': 2, 'III': 3, 'IV': 4}
            if quarter_str.upper() in roman_to_num:
                quarter_num = roman_to_num[quarter_str.upper()]
            else:
                quarter_num = int(quarter_str)
            
            # Format report_quarter as YYYY-QN
            report_quarter = f"{year_num:04d}-Q{quarter_num}"
            report_period = f"Q{quarter_num} {year_num}"
            
            # Calculate start and end dates for the quarter
            quarter_months = {
                1: (1, 3),   # Q1: Jan-Mar
                2: (4, 6),   # Q2: Apr-Jun
                3: (7, 9),   # Q3: Jul-Sep
                4: (10, 12)  # Q4: Oct-Dec
            }
            
            start_month, end_month = quarter_months[quarter_num]
            start_date = date(year_num, start_month, 1)
            last_day = monthrange(year_num, end_month)[1]
            end_date = date(year_num, end_month, last_day)
            
            print(f"  Format: Quarterly report")
            print(f"  Quarter: {report_quarter}")
            print(f"  Period: {start_date} to {end_date}")
        else:
            print(f"  ⚠ Could not extract quarterly metadata")
            
    except Exception as e:
        print(f"  ⚠ Error extracting metadata: {e}")
    
    return report_period, report_quarter, start_date, end_date

# The source PDFs are OCR'd, so the four transport modes arrive with accent and
# case damage (Đường / Dường / Dương, thủy / Thủy / thuy) and occasionally a stray
# number or "-" where the column split failed. Fold everything back to the four
# canonical labels; anything that does not map is not a transport mode at all.
VEHICLE_TYPE_CANONICAL = {
    'duong bo': 'Đường bộ',
    'duong khong': 'Đường không',
    'duong thuy': 'Đường thủy',
    'loai khac': 'Loại khác',
}

def normalize_vehicle_type(value):
    """Map an OCR'd transport-mode label to its canonical form, or None."""
    if not value:
        return None
    # NFD does not decompose Đ/đ (it is a distinct letter), so strip it explicitly.
    key = unicodedata.normalize('NFD', str(value))
    key = ''.join(c for c in key if unicodedata.category(c) != 'Mn')
    key = key.lower().replace('đ', 'd').replace('-', ' ')
    key = re.sub(r'\s+', ' ', key).strip()
    return VEHICLE_TYPE_CANONICAL.get(key)

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
    """Convert number string to Decimal, handling both US and Vietnamese formats.
    
    US/International format: 1,234,567.89 (comma for thousands, dot for decimal)
    Vietnamese format: 1.234.567,89 (dot for thousands, comma for decimal)
    """
    if value is None or value == '':
        return default
    try:
        clean_value = str(value).replace(' ', '').replace('−', '-')
        
        # Detect format by checking pattern
        # If we have both dots and commas, determine which is thousands vs decimal
        has_dot = '.' in clean_value
        has_comma = ',' in clean_value
        
        if has_dot and has_comma:
            # Check which appears last (that's the decimal separator)
            last_dot_pos = clean_value.rfind('.')
            last_comma_pos = clean_value.rfind(',')
            
            if last_dot_pos > last_comma_pos:
                # US format: 1,234,567.89
                clean_value = clean_value.replace(',', '')
            else:
                # Vietnamese format: 1.234.567,89
                clean_value = clean_value.replace('.', '').replace(',', '.')
        elif has_comma and not has_dot:
            # Check if comma is thousands or decimal
            # If comma appears multiple times or has 3 digits after it, it's thousands
            comma_count = clean_value.count(',')
            if comma_count > 1:
                # Multiple commas = thousands separator (e.g., 1,234,567)
                clean_value = clean_value.replace(',', '')
            else:
                # Single comma - check digits after it
                parts = clean_value.split(',')
                if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
                    # Pattern like 1,234 - likely thousands
                    clean_value = clean_value.replace(',', '')
                else:
                    # Likely decimal separator
                    clean_value = clean_value.replace(',', '.')
        elif has_dot and not has_comma:
            # Only dots - could be thousands or decimal
            dot_count = clean_value.count('.')
            if dot_count > 1:
                # Multiple dots = thousands separator (e.g., 1.234.567)
                clean_value = clean_value.replace('.', '')
            # else: single dot is decimal separator, leave as is
        
        return fit_decimal(Decimal(clean_value), default)
    except (InvalidOperation, ValueError, AttributeError):
        return default

def extract_tables_from_json(parsed_json_str):
    """Extract table data from ai_parse_document() v2.0 JSON result."""
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
                    html_tables.append(html_content)
        
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
        return [], ""

print("✓ Helper functions loaded")

# COMMAND ----------

# DBTITLE 1,Load and Process Provinces Documents
# Load raw parsed data
print("\nStep 1: Loading raw parsed data...")
print("-" * 80)

raw_df = spark.sql("""
    SELECT
        raw.document_id,
        raw.document_url,
        raw.sub_category,
        raw.parsed_json,
        log.parse_status,
        -- Fallback quarter: some scanned PDFs carry no readable quarter in their
        -- body text, but URL discovery already derived one from the API title.
        url.report_quarter AS url_report_quarter
    FROM market_data.customs.transportation_parsed_documents_raw AS raw
    INNER JOIN market_data.customs.transportation_document_processing_log AS log
        ON raw.document_id = log.document_id
    LEFT JOIN market_data.customs.transportation_customs_documents_url AS url
        ON raw.document_url = url.url
       AND raw.sub_category = url.sub_category
    WHERE log.parse_status = 'success'
      AND (log.extraction_status IS NULL
           OR log.extraction_status = 'failed'
           OR log.extraction_status = 'pending')
""")

docs_to_process = raw_df.count()
print(f"Documents to process: {docs_to_process}")

if docs_to_process == 0:
    print("\n✓ No new documents to extract!")
    dbutils.notebook.exit("No documents to process")

print("\nStep 2: Extracting structured data with vehicle breakdown...")
print("-" * 80)

# Map sub_category names
sub_category_mapping = {
    'Import Provinces': 'Import by Transportation',
    'Export Provinces': 'Export by Transportation'
}

all_statistics = []
errors = []
skipped_docs = []
unmapped_vehicles = []

for row in raw_df.collect():
    doc_id = row['document_id']
    raw_sub_category = row['sub_category']
    parsed_json = row['parsed_json']

    # Apply sub_category mapping
    sub_category = sub_category_mapping.get(raw_sub_category, raw_sub_category)

    print(f"\nProcessing: {doc_id} ({sub_category})")

    try:
        tables, text = extract_tables_from_json(parsed_json)
        report_period, report_quarter, start_date, end_date = extract_report_metadata_quarterly(text)
        print(f"  Tables found: {len(tables)}")

        # Older scans often lose the quarter heading to OCR. URL discovery derived
        # one from the API report title, which is authoritative, so prefer that over
        # discarding the document.
        if not report_quarter and row['url_report_quarter']:
            report_quarter = row['url_report_quarter']
            year_num = int(report_quarter[:4])
            quarter_num = int(report_quarter[-1])
            report_period = f"Q{quarter_num} {year_num}"
            start_month, end_month = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}[quarter_num]
            start_date = date(year_num, start_month, 1)
            end_date = date(year_num, end_month, monthrange(year_num, end_month)[1])
            print(f"  Quarter from URL table: {report_quarter}")

        # This workstream is quarterly. Annual PTVT reports (e.g. the 2015 full-year
        # and 2016 review files) parse fine but have no quarter, and mixing them in
        # double-counts every quarter they span.
        if not report_quarter:
            print(f"  ⊘ No quarter in document - not a quarterly report, skipping")
            skipped_docs.append(doc_id)
            spark.sql(f"""
                UPDATE market_data.customs.transportation_document_processing_log
                SET extraction_status = 'skipped_not_quarterly',
                    extraction_timestamp = current_timestamp(),
                    extraction_rows_inserted = 0,
                    updated_at = current_timestamp()
                WHERE document_id = '{doc_id}'
            """)
            continue

        row_count = 0
        for table in tables:
            if len(table) < 2:
                continue
            
            # Track current product category across rows (handles rowspan)
            current_product = None
            
            # Skip header rows (usually first 2 rows)
            for row_data in table[2:]:
                if len(row_data) < 4:  # Need at least 4 columns
                    continue
                
                # Column 1: Product category (empty if rowspan from previous row)
                product_col = row_data[1].strip() if len(row_data) > 1 and row_data[1] else None
                
                # Update current product if column 1 has a value
                if product_col:
                    current_product = product_col
                
                # Column 2: Vehicle type (Đường bộ, Đường không, Đường thủy, Loại khác)
                vehicle_raw = row_data[2].strip() if len(row_data) > 2 and row_data[2] else None

                # Skip "Cộng" (Total) rows to prevent double-counting
                if vehicle_raw and 'cộng' in vehicle_raw.lower():
                    continue

                # Skip if both product and vehicle are empty
                if not current_product and not vehicle_raw:
                    continue

                # The grain is product x transport mode, so a row whose mode cannot
                # be recognised is a column-split failure rather than a data row.
                vehicle_type = normalize_vehicle_type(vehicle_raw)
                if vehicle_type is None:
                    unmapped_vehicles.append(vehicle_raw)
                    continue

                # Column 3: Đơn vị tính - USD rows carry no quantity, Tấn rows do.
                unit = row_data[3].strip() if len(row_data) > 3 and row_data[3] else None
                
                # Extract from PERIOD columns (4-5), NOT cumulative columns (6-7)
                # Table structure:
                #   Columns 0-3: Row num, Product, Vehicle, Currency
                #   Columns 4-5: Period (Kỳ báo cáo) - Quantity, Value USD
                #   Columns 6-7: Cumulative (Lũy kế) - Quantity, Value USD
                quantity = None
                value_usd = None
                cumulative_quantity = None
                cumulative_value_usd = None

                if len(row_data) >= 6:
                    # Column 5: Period value_usd (NOT column 7 which is cumulative!)
                    value_usd = safe_decimal(row_data[5])
                    # Column 4: Period quantity
                    quantity = safe_decimal(row_data[4])
                    # Columns 6-7: Lũy kế (year-to-date) quantity and value
                    if len(row_data) > 6:
                        cumulative_quantity = safe_decimal(row_data[6])
                    if len(row_data) > 7:
                        cumulative_value_usd = safe_decimal(row_data[7])
                else:
                    # Fallback for tables with different structure
                    for col_idx in range(len(row_data) - 1, max(2, len(row_data) - 3), -1):
                        val = safe_decimal(row_data[col_idx])
                        if val is not None:
                            if value_usd is None:
                                value_usd = val
                            elif quantity is None:
                                quantity = val
                                break
                
                # Validate: reject unrealistically large values
                MAX_VALUE = Decimal('1000000000000')
                
                if value_usd and abs(value_usd) > MAX_VALUE:
                    value_usd = None
                
                if quantity and abs(quantity) > MAX_VALUE:
                    quantity = None
                
                # Only add row if we have product/vehicle + value
                if (current_product or vehicle_type) and value_usd:
                    all_statistics.append({
                        'sub_category': sub_category,
                        'document_id': doc_id,
                        'report_period': report_period,
                        'report_quarter': report_quarter,
                        'report_start_date': start_date,
                        'report_end_date': end_date,
                        'row_number': row_count + 1,
                        'product_category': current_product,
                        'parent_category': None,
                        'vehicle_type': vehicle_type,
                        'unit': unit,
                        'quantity': quantity,
                        'value_usd': value_usd,
                        'cumulative_quantity': cumulative_quantity,
                        'cumulative_value_usd': cumulative_value_usd
                    })
                    row_count += 1
        
        print(f"  ✓ Extracted {row_count} rows (period values, excluding 'Cộng' totals)")
        spark.sql(f"""
            UPDATE market_data.customs.transportation_document_processing_log
            SET extraction_status = 'success',
                extraction_timestamp = current_timestamp(),
                extraction_rows_inserted = {row_count},
                updated_at = current_timestamp()
            WHERE document_id = '{doc_id}'
        """)
        
    except Exception as e:
        error_msg = str(e)[:500]
        print(f"  ✗ Error: {error_msg}")
        errors.append({'document_id': doc_id, 'error': error_msg})
        spark.sql(f"""
            UPDATE market_data.customs.transportation_document_processing_log
            SET extraction_status = 'failed',
                extraction_error_message = '{error_msg}',
                updated_at = current_timestamp()
            WHERE document_id = '{doc_id}'
        """)

print(f"\n✓ Processed {len(all_statistics)} rows from {docs_to_process} documents")

# Step 3: Insert into table
if len(all_statistics) > 0:
    print("\nStep 3: Inserting into transportation_trade_statistics...")
    print("-" * 80)
    
    # safe_decimal() already fits every metric to DECIMAL(20,3).
    schema = StructType([
        StructField("sub_category", StringType(), False),
        StructField("document_id", StringType(), False),
        StructField("report_period", StringType(), True),
        StructField("report_quarter", StringType(), True),
        StructField("report_start_date", DateType(), True),
        StructField("report_end_date", DateType(), True),
        StructField("row_number", IntegerType(), False),
        StructField("product_category", StringType(), True),
        StructField("parent_category", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("quantity", DecimalType(20, 3), True),
        StructField("value_usd", DecimalType(20, 3), True),
        StructField("cumulative_quantity", DecimalType(20, 3), True),
        StructField("cumulative_value_usd", DecimalType(20, 3), True)
    ])

    stats_df = spark.createDataFrame(all_statistics, schema=schema)
    stats_df.createOrReplaceTempView("new_transportation_statistics")

    # MERGE rather than append so re-extracting a document replaces its rows
    # instead of duplicating them.
    spark.sql("""
        MERGE INTO market_data.customs.transportation_trade_statistics AS target
        USING (
            SELECT *, current_timestamp() AS created_at FROM new_transportation_statistics
        ) AS source
        ON target.sub_category = source.sub_category
           AND target.document_id = source.document_id
           AND target.row_number = source.row_number
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Merged {len(all_statistics)} rows")
else:
    print("\n⚠ No statistics to insert")

# COMMAND ----------

# DBTITLE 1,Extraction Summary
print("\n" + "="*80)
print("EXTRACTION SUMMARY")
print("="*80)

if skipped_docs:
    print(f"\n⊘ Skipped {len(skipped_docs)} non-quarterly document(s): {', '.join(skipped_docs)}")

if unmapped_vehicles:
    from collections import Counter
    print(f"\n⚠ Dropped {len(unmapped_vehicles)} row(s) whose transport mode did not map.")
    print("  Top unmapped values (check for a new OCR variant worth adding):")
    for value, count in Counter(unmapped_vehicles).most_common(10):
        print(f"    {count:5}x  {value!r}")

summary = spark.sql("""
    SELECT
        sub_category,
        extraction_status,
        COUNT(*) as document_count,
        SUM(extraction_rows_inserted) as total_rows
    FROM market_data.customs.transportation_document_processing_log
    WHERE extraction_status IS NOT NULL
    GROUP BY sub_category, extraction_status
    ORDER BY sub_category, extraction_status
""")

print("\nExtraction Status by Category:")
display(summary)

if len(errors) > 0:
    print(f"\n⚠ {len(errors)} documents failed:")
    for err in errors:
        print(f"  • {err['document_id']}: {err['error'][:100]}")

print("\n" + "="*80)
print("Extraction workflow complete!")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-extracting history
# MAGIC
# MAGIC This notebook previously ended with a cell that ran
# MAGIC `DELETE FROM transportation_trade_statistics WHERE TRUE` and reset every
# MAGIC `success` document back to `pending`. As the last cell of a job task it wiped
# MAGIC the fact table on **every run**, which is why the table kept coming back empty.
# MAGIC
# MAGIC To deliberately rebuild the whole history, run this by hand - never as part
# MAGIC of the job. The extraction step MERGEs on
# MAGIC `(sub_category, document_id, row_number)`, so re-extraction overwrites rows
# MAGIC in place and a wipe is not normally needed.
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE market_data.customs.transportation_document_processing_log
# MAGIC SET extraction_status = 'pending',
# MAGIC     extraction_rows_inserted = NULL,
# MAGIC     extraction_timestamp = NULL,
# MAGIC     updated_at = current_timestamp()
# MAGIC WHERE parse_status = 'success';
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC select right(report_period,4),sub_category,sum(value_usd)/1e9 as amount
# MAGIC from market_data.customs.transportation_trade_statistics
# MAGIC group by all