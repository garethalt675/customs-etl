# Databricks notebook source
# DBTITLE 1,Extract Statistics - Overview
# MAGIC %md
# MAGIC # Step 3: Extract Structured Trade Statistics
# MAGIC
# MAGIC This notebook transforms raw parsed JSON into structured trade statistics with proper hierarchy and data types for both Import and Export goods.
# MAGIC
# MAGIC ## Process Flow
# MAGIC
# MAGIC 1. **Read raw parsed data**: Load JSON from `market_data.customs.parsed_documents_raw` with `sub_category`
# MAGIC 2. **Extract document metadata**: Parse report period/month and dates from document headers
# MAGIC 3. **Extract tables**: Parse trade statistics tables from JSON
# MAGIC 4. **Detect hierarchy**: Identify parent-child relationships (main categories vs. sub-categories)
# MAGIC 5. **Transform to rows**: Convert table data to structured records
# MAGIC 6. **Insert to target**: Write to `market_data.customs.trade_statistics` with `sub_category`
# MAGIC 7. **Update tracking**: Mark extraction status in processing log
# MAGIC
# MAGIC ## Supported Report Formats
# MAGIC
# MAGIC The notebook handles two types of Vietnamese customs reports:
# MAGIC
# MAGIC ### 1. Monthly Reports (THEO THÁNG)
# MAGIC - **Header format**: "Tháng 9 năm 2024"
# MAGIC - **Columns**: Monthly amount + comparison to previous month + cumulative + YoY comparison
# MAGIC - **report_month**: Extracted as "YYYY-MM" (e.g., "2024-09")
# MAGIC - **Dates**: Derived from month (1st to last day of month)
# MAGIC
# MAGIC ### 2. Periodic Reports (THEO KỲ)
# MAGIC - **Header format**: "Kỳ I tháng 4 năm 2026\nTừ ngày 01/04/2026 đến hết ngày 15/04/2026"
# MAGIC - **Columns**: Period amount + cumulative to date
# MAGIC - **report_month**: Extracted from the month mentioned (e.g., "2026-04")
# MAGIC - **Dates**: Explicitly provided in the document
# MAGIC
# MAGIC ## Hierarchy Detection
# MAGIC
# MAGIC The Vietnamese customs documents have hierarchical structure:
# MAGIC * **Top-level categories**: Thịt và các sản phẩm từ thịt (main product groups)
# MAGIC * **Sub-categories**: - Thịt lợn và các sản phẩm từ thịt lợn (indented items)
# MAGIC
# MAGIC We detect this by:
# MAGIC * Lines starting with "-" are sub-categories
# MAGIC * Their parent is the most recent non-indented category
# MAGIC
# MAGIC ## Schema Output
# MAGIC
# MAGIC Key columns in `trade_statistics` table:
# MAGIC * **sub_category** (STRING): "Import Goods" or "Export Goods"
# MAGIC * **report_month** (STRING): Month in YYYY-MM format - **USE THIS FOR FILTERING/GROUPING BY MONTH**
# MAGIC * **report_period** (STRING): Original period description from document
# MAGIC * **report_start_date** / **report_end_date** (DATE): Date range covered
# MAGIC * **product_category** (STRING): Product name
# MAGIC * **parent_category** (STRING): Parent category for hierarchical items
# MAGIC * **period_quantity** / **period_value_usd**: Values for the reporting period
# MAGIC * **cumulative_quantity** / **cumulative_value_usd**: Year-to-date totals
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC * **Partial failures**: Process all documents, track which failed
# MAGIC * **Invalid data**: Log errors but don't stop the workflow
# MAGIC * **Summary report**: Display per-category statistics on success/failure rates

# COMMAND ----------

# DBTITLE 1,Extract and Transform Import Statistics
import json
import re
from datetime import datetime
from calendar import monthrange
from pyspark.sql.functions import col, when, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html.parser import HTMLParser

print("="*80)
print("IMPORT STATISTICS EXTRACTION WORKFLOW")
print("="*80)

class TableHTMLParser(HTMLParser):
    """Parse HTML table into structured data.
    
    Handles colspan attributes by expanding them into multiple cells.
    Empty cells are inserted BEFORE the cell value to match the column structure.
    """
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = []
        self.current_row = []
        self.current_cell = ""
        self.current_colspan = 1  # Track colspan for current cell
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
            # Check for colspan attribute
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
            # If colspan > 1, append (colspan-1) empty cells BEFORE the value
            for _ in range(self.current_colspan - 1):
                self.current_row.append("")
            # Then append the cell value
            self.current_row.append(self.current_cell.strip())
            self.in_cell = False
            self.current_colspan = 1
    
    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

def extract_report_metadata(parsed_text):
    """
    Extract report period/month and date range from document header.
    
    Handles two formats:
    1. Periodic: "Kỳ I tháng 4 năm 2026\nTừ ngày 01/04/2026 đến hết ngày 15/04/2026"
    2. Monthly: "Tháng 9 năm 2024"
    
    Returns:
        tuple: (report_period, report_month, start_date, end_date)
    """
    report_period = None
    report_month = None
    start_date = None
    end_date = None
    
    try:
        # Try monthly format first: "Tháng 9 năm 2024"
        month_match = re.search(r'Tháng\s+(\d+)\s+năm\s+(\d{4})', parsed_text)
        if month_match:
            month_num = int(month_match.group(1))
            year_num = int(month_match.group(2))
            
            # Format as YYYY-MM
            report_month = f"{year_num:04d}-{month_num:02d}"
            report_period = f"Tháng {month_num} năm {year_num}"
            
            # Derive start and end dates for the month
            start_date = datetime(year_num, month_num, 1).date()
            last_day = monthrange(year_num, month_num)[1]
            end_date = datetime(year_num, month_num, last_day).date()
            
            print(f"  Format: Monthly report")
            print(f"  Month: {report_month}")
        else:
            # Try periodic format: "Kỳ I tháng 4 năm 2026"
            period_match = re.search(r'(Kỳ\s+[IVX]+\s+tháng\s+(\d+)\s+năm\s+(\d{4}))', parsed_text)
            if period_match:
                report_period = period_match.group(1)
                month_num = int(period_match.group(2))
                year_num = int(period_match.group(3))
                
                # For periodic reports, extract month for report_month column
                report_month = f"{year_num:04d}-{month_num:02d}"
                
                print(f"  Format: Periodic report")
                print(f"  Period: {report_period}")
                print(f"  Month: {report_month}")
            
            # Extract explicit date range if provided
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
    """Safely convert value to Decimal, handling Vietnamese number format.
    
    Vietnamese format:
    - . (dot) = thousands separator
    - , (comma) = decimal separator
    
    Example: 133.438.152 = 133,438,152 (one hundred thirty-three million)
    """
    if value is None or value == '':
        return default
    
    try:
        # Remove thousands separators (dots) and spaces
        clean_value = str(value).replace('.', '').replace(' ', '')
        # Convert comma (decimal separator) to dot for Decimal parsing
        clean_value = clean_value.replace(',', '.')
        # Handle negative percentages
        clean_value = clean_value.replace('−', '-')  # Replace unicode minus
        return fit_decimal(Decimal(clean_value), default)
    except (InvalidOperation, ValueError):
        return default

def extract_tables_from_json(parsed_json_str):
    """
    Extract table data from ai_parse_document() v2.0 JSON result.
    
    v2.0 format: {"document": {"elements": [{"type": "table", "content": "<table>...</table>"}, ...]}}
    """
    try:
        if isinstance(parsed_json_str, str):
            parsed_data = json.loads(parsed_json_str)
        else:
            parsed_data = parsed_json_str
        
        elements = parsed_data.get('document', {}).get('elements', [])
        
        # Extract tables (HTML format)
        html_tables = []
        for elem in elements:
            if elem.get('type') == 'table':
                html_content = elem.get('content', '')
                if html_content:
                    html_tables.append(html_content)
        
        # Extract text content for metadata - UPDATED to include 'section_header'
        text_parts = []
        for elem in elements:
            if elem.get('type') in ['text', 'title', 'page_header', 'section_header']:
                content = elem.get('content', '')
                if content:
                    text_parts.append(content)
        
        full_text = '\n'.join(text_parts)
        
        # Parse HTML tables into structured data
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

def process_table_rows(table_data):
    """
    Process table rows and detect hierarchy.
    
    Returns list of dicts with product data and parent_category.
    Only extracts rows with valid STT numbers - skips sub-categories without STT.
    
    After colspan expansion, all rows have 11 columns:
    - Columns 0-2: STT, Product, Unit
    - Tấn/Chiếc rows: [STT, Product, Unit, Qty, Val, %Qty, %Val, CumQty, CumVal, YoY%Qty, YoY%Val]
    - USD rows: [STT, Product, USD, EMPTY, Val, EMPTY, %Val, EMPTY, CumVal, EMPTY, YoY%Val]
    """
    rows = []
    current_parent = None
    
    if not table_data or len(table_data) < 2:
        return rows
    
    # Skip header rows (first 1-2 rows typically)
    data_rows = table_data[2:] if len(table_data) > 2 else table_data
    
    for row in data_rows:
        if not isinstance(row, list) or len(row) < 3:
            continue
        
        try:
            # Parse row columns
            # Expected format: [STT, Product Name, Unit, ...values...]
            row_num_str = str(row[0]).strip() if len(row) > 0 else ""
            product = str(row[1]).strip() if len(row) > 1 else ""
            unit = str(row[2]).strip() if len(row) > 2 else ""
            
            # Skip empty rows or header rows
            if not product or product == '' or product.upper() in ['STT', 'NHÓM/MẶT HÀNG CHỦ YẾU']:
                continue
            
            # Skip aggregate rows (TỔNG TRỊ GIÁ, Trong đó:...)
            if 'TỔNG' in product.upper() or product.startswith('Trong đó:'):
                continue
            
            # SKIP rows without valid STT number (sub-categories)
            if not row_num_str or not row_num_str.isdigit():
                continue
            
            product_str = product
            
            # Detect if this is a sub-category (starts with "-" or indented)
            is_subcategory = product_str.startswith('-') or product_str.startswith('•')
            
            if is_subcategory:
                # This is a sub-category
                parent = current_parent
                # Clean the product name
                product_str = product_str.lstrip('-•').strip()
            else:
                # This is a top-level category
                parent = None
                current_parent = product_str
            
            # Extract numeric values based on unit type
            # After colspan expansion, all rows have 11 columns
            unit_lower = unit.lower() if unit else ""
            
            if unit_lower == 'usd':
                # USD rows: normalized to 11 columns by parser
                # [STT, Product, USD, EMPTY, Period_Val, EMPTY, %Val, EMPTY, Cumul_Val, EMPTY, YoY%]
                period_qty = None
                period_val = safe_decimal(row[4]) if len(row) > 4 else None
                cumul_qty = None
                cumul_val = safe_decimal(row[8]) if len(row) > 8 else None
            else:
                # Tấn/Chiếc rows: has quantity and values at all positions
                # [STT, Product, Unit, Period_Qty, Period_Val, %Qty, %Val, Cumul_Qty, Cumul_Val, YoY%Qty, YoY%Val]
                period_qty = safe_decimal(row[3]) if len(row) > 3 else None
                period_val = safe_decimal(row[4]) if len(row) > 4 else None
                cumul_qty = safe_decimal(row[7]) if len(row) > 7 else None
                cumul_val = safe_decimal(row[8]) if len(row) > 8 else None
            
            # Use the STT number directly
            row_number = int(row_num_str)
            
            rows.append({
                'row_number': row_number,
                'product_category': product_str,
                'parent_category': parent,
                'unit': unit if unit else None,
                'period_quantity': period_qty,
                'period_value_usd': period_val,
                'cumulative_quantity': cumul_qty,
                'cumulative_value_usd': cumul_val
            })
            
        except Exception as e:
            print(f"  ⚠ Error processing row: {e}")
            continue
    
    return rows

# COMMAND ----------


# ========================================
# Step 1: Load raw parsed data
# ========================================
print("\nStep 1: Loading raw parsed data...")
print("-" * 80)

raw_df = spark.sql("""
    SELECT
        raw.document_id,
        raw.document_url,
        raw.sub_category,
        raw.parsed_json,
        log.parse_status
    FROM market_data.customs.parsed_documents_raw AS raw
    INNER JOIN market_data.customs.document_processing_log AS log
        ON raw.document_id = log.document_id
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


# COMMAND ----------

# ========================================
# Step 2: Extract and transform data
# ========================================
print("\nStep 2: Extracting structured data...")
print("-" * 80)

all_statistics = []
errors = []

for row in raw_df.collect():
    doc_id = row['document_id']
    doc_url = row['document_url']
    sub_category = row['sub_category']
    parsed_json = row['parsed_json']
    
    print(f"\nProcessing document: {doc_id} ({sub_category})")
    
    try:
        # Extract tables and text
        tables, text = extract_tables_from_json(parsed_json)
        
        # Extract metadata from text (now returns 4 values)
        report_period, report_month, start_date, end_date = extract_report_metadata(text)
        
        print(f"  Dates: {start_date} to {end_date}")
        print(f"  Tables found: {len(tables)}")
        
        # Process each table
        row_count = 0
        for table_idx, table in enumerate(tables):
            print(f"  Processing table {table_idx + 1}: {len(table)} rows")
            table_rows = process_table_rows(table)
            
            for row_data in table_rows:
                all_statistics.append({
                    'sub_category': sub_category,
                    'document_id': doc_id,
                    'report_period': report_period,
                    'report_month': report_month,
                    'report_start_date': start_date,
                    'report_end_date': end_date,
                    'row_number': row_data['row_number'],
                    'product_category': row_data['product_category'],
                    'parent_category': row_data['parent_category'],
                    'unit': row_data['unit'],
                    'period_quantity': row_data['period_quantity'],
                    'period_value_usd': row_data['period_value_usd'],
                    'cumulative_quantity': row_data['cumulative_quantity'],
                    'cumulative_value_usd': row_data['cumulative_value_usd'],
                    'parsed_timestamp': datetime.now()
                })
                row_count += 1
        
        print(f"  ✓ Extracted {row_count} rows")
        
    except Exception as e:
        error_msg = f"Error processing {doc_id}: {str(e)}"
        print(f"  ✗ {error_msg}")
        import traceback
        traceback.print_exc()
        errors.append({'document_id': doc_id, 'error': error_msg})

print(f"\nTotal rows extracted: {len(all_statistics)}")


# COMMAND ----------


# ========================================
# Step 3: Insert into target table
# ========================================
if len(all_statistics) > 0:
    print("\nStep 3: Inserting into trade_statistics table...")
    print("-" * 80)
    
    # Define explicit schema to avoid type inference issues
    schema = StructType([
        StructField("sub_category", StringType(), False),
        StructField("document_id", StringType(), False),
        StructField("report_period", StringType(), True),
        StructField("report_month", StringType(), True),
        StructField("report_start_date", DateType(), True),
        StructField("report_end_date", DateType(), True),
        StructField("row_number", IntegerType(), False),
        StructField("product_category", StringType(), True),
        StructField("parent_category", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("period_quantity", DecimalType(20, 3), True),
        StructField("period_value_usd", DecimalType(20, 3), True),
        StructField("cumulative_quantity", DecimalType(20, 3), True),
        StructField("cumulative_value_usd", DecimalType(20, 3), True),
        StructField("parsed_timestamp", TimestampType(), True)
    ])
    
    stats_df = spark.createDataFrame(all_statistics, schema=schema)
    stats_df.createOrReplaceTempView("new_statistics")
    
    # Delete existing data for the months being updated to avoid duplicates
    print("  Identifying months to update...")
    months_to_update = spark.sql("""
        SELECT DISTINCT sub_category, report_month 
        FROM new_statistics 
        WHERE report_month IS NOT NULL
    """).collect()
    
    if len(months_to_update) > 0:
        print(f"  Deleting existing data for {len(months_to_update)} month(s)...")
        for row in months_to_update:
            sub_cat = row.sub_category
            month = row.report_month
            print(f"    - {sub_cat}: {month}")
            spark.sql(f"""
                DELETE FROM market_data.customs.trade_statistics
                WHERE sub_category = '{sub_cat}'
                  AND report_month = '{month}'
            """)
    
    # Insert new data (MERGE becomes simple insert after deletion)
    print("  Inserting new data...")
    spark.sql("""
        MERGE INTO market_data.customs.trade_statistics AS target
        USING new_statistics AS source
        ON target.sub_category = source.sub_category
           AND target.document_id = source.document_id
           AND target.row_number = source.row_number
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✓ Inserted {len(all_statistics)} rows")
    
    # Update processing log - success
    success_docs = [s['document_id'] for s in all_statistics]
    success_df = spark.createDataFrame(
        [{'document_id': doc_id, 'row_count': len([s for s in all_statistics if s['document_id'] == doc_id])} 
         for doc_id in set(success_docs)]
    )
    success_df.createOrReplaceTempView("successful_extractions")
    
    spark.sql("""
        MERGE INTO market_data.customs.document_processing_log AS target
        USING successful_extractions AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET
            extraction_status = 'success',
            extraction_timestamp = current_timestamp(),
            extraction_error_message = NULL,
            extraction_rows_inserted = source.row_count,
            updated_at = current_timestamp()
    """)
else:
    print("\n⚠ No rows extracted")

# Update processing log - failures
if len(errors) > 0:
    print("\nUpdating failed extractions...")
    
    errors_df = spark.createDataFrame(errors)
    errors_df.createOrReplaceTempView("failed_extractions")
    
    spark.sql("""
        MERGE INTO market_data.customs.document_processing_log AS target
        USING failed_extractions AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET
            extraction_status = 'failed',
            extraction_timestamp = current_timestamp(),
            extraction_error_message = source.error,
            updated_at = current_timestamp()
    """)


# COMMAND ----------


# ========================================
# Step 4: Summary report
# ========================================
print("\n" + "="*80)
print("EXTRACTION SUMMARY")
print("="*80)

summary = spark.sql("""
    SELECT
        sub_category,
        extraction_status,
        COUNT(*) as document_count,
        SUM(extraction_rows_inserted) as total_rows
    FROM market_data.customs.document_processing_log
    WHERE extraction_status IS NOT NULL
    GROUP BY sub_category, extraction_status
    ORDER BY sub_category, extraction_status
""")

print("\nExtraction Status by Category:")
display(summary)

if len(errors) > 0:
    print(f"\n⚠ {len(errors)} documents failed extraction:")
    for err in errors:
        print(f"  • {err['document_id']}: {err['error']}")

print("\n" + "="*80)
print("Extraction workflow complete!")
print("="*80)