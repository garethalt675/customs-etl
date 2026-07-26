# Databricks notebook source
# DBTITLE 1,Extract Provinces Statistics - Overview
# MAGIC %md
# MAGIC # Step 3: Extract Provincial Trade Statistics
# MAGIC
# MAGIC Turns the parsed monthly province report into structured rows in
# MAGIC `market_data.customs.provinces_trade_statistics`.
# MAGIC
# MAGIC ## Source shape
# MAGIC
# MAGIC The report is *"Trị giá xuất, nhập khẩu chia theo tỉnh/thành phố"*
# MAGIC (Biểu số 019.T/BCB-TC). Its grain is **province x trade flow x month**, valued
# MAGIC in **USD only** - there is no product breakdown and no quantity, unlike every
# MAGIC other workstream in this project. A previous version of this notebook was a
# MAGIC copy of the FDI product extractor and could never have produced a usable row.
# MAGIC
# MAGIC Each row of the table is one province with four value columns:
# MAGIC
# MAGIC | STT | TỈNH/THÀNH PHỐ | Tháng N (export) | N tháng (export) | Tháng N (import) | N tháng (import) |
# MAGIC |-----|----------------|------------------|------------------|------------------|------------------|
# MAGIC | 1   | An Giang       | 235,578,472      | 765,507,712      | 121,618,306      | 432,070,307      |
# MAGIC
# MAGIC "Tháng N" is the reporting month, "N tháng" is the year-to-date cumulative.
# MAGIC One PDF therefore yields **two rows per province** - one `export`, one `import`.
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC `provinces_trade_statistics`, one row per province x flow x month:
# MAGIC `province_name`, `trade_flow`, `report_month`, `period_value_usd`,
# MAGIC `cumulative_value_usd`.

# COMMAND ----------

# DBTITLE 1,Helper Functions
import json
import re
import unicodedata
from datetime import datetime
from calendar import monthrange
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html.parser import HTMLParser
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType,
    DecimalType, TimestampType,
)

print("=" * 80)
print("PROVINCES STATISTICS EXTRACTION WORKFLOW")
print("=" * 80)


class TableHTMLParser(HTMLParser):
    """Parse an HTML table into a list of rows, expanding colspan."""

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
        elif tag in ('td', 'th') and self.in_row:
            self.in_cell = True
            self.current_cell = ""
            self.current_colspan = 1
            for name, value in attrs:
                if name == 'colspan':
                    try:
                        self.current_colspan = int(value)
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
        elif tag in ('td', 'th') and self.in_cell:
            for _ in range(self.current_colspan - 1):
                self.current_row.append("")
            self.current_row.append(self.current_cell.strip())
            self.in_cell = False
            self.current_colspan = 1

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data


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
    """Convert a Vietnamese-formatted number to Decimal (dot = thousands)."""
    if value is None or value == '':
        return default
    try:
        clean = str(value).replace('.', '').replace(' ', '')
        clean = clean.replace(',', '.').replace('−', '-')
        if not re.search(r'\d', clean):
            return default
        return fit_decimal(Decimal(clean), default)
    except (InvalidOperation, ValueError):
        return default


def extract_report_metadata(parsed_text):
    """Pull the reporting month and its date range from the document header."""
    report_period = report_month = start_date = end_date = None
    try:
        # Header renders as "Tháng4năm2026" or "Tháng 4 năm 2026"
        m = re.search(r'Th[áa]ng\s*(\d{1,2})\s*n[ăa]m\s*(\d{4})', parsed_text, re.IGNORECASE)
        if m:
            month_num, year_num = int(m.group(1)), int(m.group(2))
            if 1 <= month_num <= 12:
                report_month = f"{year_num:04d}-{month_num:02d}"
                report_period = f"Tháng {month_num} năm {year_num}"
                start_date = datetime(year_num, month_num, 1).date()
                end_date = datetime(year_num, month_num, monthrange(year_num, month_num)[1]).date()
    except Exception as exc:
        print(f"  ⚠ Could not extract metadata: {exc}")
    return report_period, report_month, start_date, end_date


def strip_accents(text):
    nfd = unicodedata.normalize('NFD', str(text or ''))
    return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')


# Rows that are headers, totals or footers rather than provinces.
NON_PROVINCE_MARKERS = {
    'tinh/thanh pho', 'tinh/ thanh pho', 'tinhthanh pho', 'tong', 'tong cong',
    'cong', 'stt', 'trang', 'don vi tinh', 'xuat khau', 'nhap khau', 'thang',
}


def is_province_label(value):
    """Reject header, total and page-furniture rows."""
    if not value:
        return False
    key = re.sub(r'\s+', ' ', strip_accents(value).lower().replace('đ', 'd')).strip()
    key = key.strip('.:-')
    if not key or key in NON_PROVINCE_MARKERS:
        return False
    # Vietnamese statistical tables carry a column-index row ("A | 1 | 2 | 3 | 4").
    # The shortest real unit name normalizes to "hue", so require 3 characters.
    if len(key) < 3:
        return False
    if any(marker in key for marker in ('tong', 'cong ', 'trang')):
        return False
    # A province name is words, not digits.
    if not re.search(r'[a-z]', key):
        return False
    if re.fullmatch(r'[\d\s.,]+', key):
        return False
    return True


def clean_province_name(value):
    """Normalize whitespace in the raw province label."""
    return re.sub(r'\s+', ' ', str(value).strip())


print("✓ Helper functions loaded")

# COMMAND ----------

# DBTITLE 1,Load Parsed Documents
print("\nStep 1: Loading raw parsed data...")
print("-" * 80)

raw_df = spark.sql("""
    SELECT
        raw.document_id,
        raw.document_url,
        raw.sub_category,
        raw.parsed_json,
        url.report_month AS url_report_month
    FROM market_data.customs.provinces_parsed_documents_raw AS raw
    INNER JOIN market_data.customs.provinces_document_processing_log AS log
        ON raw.document_id = log.document_id
    LEFT JOIN market_data.customs.provinces_customs_documents_url AS url
        ON raw.document_url = url.url
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

# DBTITLE 1,Extract Province Rows
print("\nStep 2: Extracting province rows...")
print("-" * 80)


def extract_tables_and_text(parsed_json_str):
    """Split an ai_parse_document() v2.0 result into tables and header text."""
    try:
        data = json.loads(parsed_json_str) if isinstance(parsed_json_str, str) else parsed_json_str
        elements = data.get('document', {}).get('elements', [])

        tables = []
        for elem in elements:
            if elem.get('type') == 'table' and elem.get('content'):
                parser = TableHTMLParser()
                parser.feed(elem['content'])
                tables.extend(parser.tables)

        text_parts = [
            elem.get('content', '')
            for elem in elements
            if elem.get('type') in ('text', 'title', 'page_header', 'section_header')
            and elem.get('content')
        ]
        return tables, '\n'.join(text_parts)
    except Exception as exc:
        print(f"  ⚠ Error parsing JSON: {exc}")
        return [], ""


def numeric_cells(row, after_index=-1):
    """Return (index, Decimal) for numeric cells positioned after `after_index`.

    Anchoring after the province label keeps a leading STT (row number) column
    from being mistaken for the first value.
    """
    out = []
    for idx, cell in enumerate(row):
        if idx <= after_index:
            continue
        val = safe_decimal(cell)
        if val is not None:
            out.append((idx, val))
    return out


all_statistics = []
errors = []
skipped_rows = []

for row in raw_df.collect():
    doc_id = row['document_id']
    parsed_json = row['parsed_json']

    print(f"\nProcessing document: {doc_id}")

    try:
        tables, text = extract_tables_and_text(parsed_json)
        report_period, report_month, start_date, end_date = extract_report_metadata(text)

        # The header is sometimes lost to OCR; URL discovery already derived the
        # month from the API report title, which is authoritative.
        if not report_month and row['url_report_month']:
            report_month = row['url_report_month']
            year_num, month_num = int(report_month[:4]), int(report_month[5:7])
            report_period = f"Tháng {month_num} năm {year_num}"
            start_date = datetime(year_num, month_num, 1).date()
            end_date = datetime(year_num, month_num, monthrange(year_num, month_num)[1]).date()
            print(f"  Month from URL table: {report_month}")

        if not report_month:
            raise ValueError("no reporting month in document text or URL table")

        print(f"  Month: {report_month}, tables: {len(tables)}")

        row_number = 0
        doc_rows = 0

        for table in tables:
            for raw_row in table:
                if len(raw_row) < 3:
                    continue

                # The province label is the first non-numeric text cell.
                province_raw = None
                province_idx = -1
                for idx, cell in enumerate(raw_row):
                    if cell and is_province_label(cell):
                        province_raw = clean_province_name(cell)
                        province_idx = idx
                        break
                if not province_raw:
                    continue

                nums = numeric_cells(raw_row, after_index=province_idx)

                # Layout: export period, export cumulative, import period, import
                # cumulative. Anything else is a malformed row, not a province.
                if len(nums) < 4:
                    skipped_rows.append((province_raw, len(nums)))
                    continue

                values = [v for _, v in nums[:4]]
                export_period, export_cumulative, import_period, import_cumulative = values

                for flow, period_value, cumulative_value in (
                    ('export', export_period, export_cumulative),
                    ('import', import_period, import_cumulative),
                ):
                    row_number += 1
                    all_statistics.append({
                        'document_id': doc_id,
                        'report_period': report_period,
                        'report_month': report_month,
                        'report_start_date': start_date,
                        'report_end_date': end_date,
                        'row_number': row_number,
                        'province_name': province_raw,
                        'trade_flow': flow,
                        'period_value_usd': period_value,
                        'cumulative_value_usd': cumulative_value,
                        'parsed_timestamp': datetime.now(),
                    })
                    doc_rows += 1

        print(f"  ✓ Extracted {doc_rows} rows ({doc_rows // 2} provinces x 2 flows)")

        spark.sql(f"""
            UPDATE market_data.customs.provinces_document_processing_log
            SET extraction_status = 'success',
                extraction_timestamp = current_timestamp(),
                extraction_error_message = NULL,
                extraction_rows_inserted = {doc_rows},
                updated_at = current_timestamp()
            WHERE document_id = '{doc_id}'
        """)

    except Exception as exc:
        error_msg = str(exc).replace("'", "")[:400]
        print(f"  ✗ Error: {error_msg}")
        errors.append({'document_id': doc_id, 'error': error_msg})
        spark.sql(f"""
            UPDATE market_data.customs.provinces_document_processing_log
            SET extraction_status = 'failed',
                extraction_timestamp = current_timestamp(),
                extraction_error_message = '{error_msg}',
                updated_at = current_timestamp()
            WHERE document_id = '{doc_id}'
        """)

print(f"\nTotal rows extracted: {len(all_statistics)}")

# COMMAND ----------

# DBTITLE 1,Merge into provinces_trade_statistics
if all_statistics:
    print("\nStep 3: Merging into provinces_trade_statistics...")
    print("-" * 80)

    schema = StructType([
        StructField("document_id", StringType(), False),
        StructField("report_period", StringType(), True),
        StructField("report_month", StringType(), True),
        StructField("report_start_date", DateType(), True),
        StructField("report_end_date", DateType(), True),
        StructField("row_number", IntegerType(), False),
        StructField("province_name", StringType(), True),
        StructField("trade_flow", StringType(), True),
        StructField("period_value_usd", DecimalType(20, 3), True),
        StructField("cumulative_value_usd", DecimalType(20, 3), True),
        StructField("parsed_timestamp", TimestampType(), True),
    ])

    stats_df = spark.createDataFrame(all_statistics, schema=schema)
    stats_df.createOrReplaceTempView("new_province_statistics")

    # Keyed on the natural grain so a re-extraction overwrites in place.
    spark.sql("""
        MERGE INTO market_data.customs.provinces_trade_statistics AS target
        USING new_province_statistics AS source
        ON target.document_id = source.document_id
           AND target.province_name = source.province_name
           AND target.trade_flow = source.trade_flow
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Merged {len(all_statistics)} rows")
else:
    print("\n⚠ No rows extracted")

# COMMAND ----------

# DBTITLE 1,Extraction Summary
print("\n" + "=" * 80)
print("PROVINCES EXTRACTION SUMMARY")
print("=" * 80)

if skipped_rows:
    from collections import Counter
    print(f"\n⚠ Skipped {len(skipped_rows)} table row(s) with fewer than 4 numeric cells.")
    print("  Most common labels (check for a layout change):")
    for (label, count_found), n in Counter(skipped_rows).most_common(10):
        print(f"    {n:4}x  {label!r} ({count_found} numeric cells)")

if errors:
    print(f"\n⚠ {len(errors)} document(s) failed:")
    for err in errors:
        print(f"  • {err['document_id']}: {err['error'][:120]}")

display(spark.sql("""
    SELECT
        extraction_status,
        COUNT(*) AS documents,
        SUM(extraction_rows_inserted) AS rows_inserted
    FROM market_data.customs.provinces_document_processing_log
    WHERE extraction_status IS NOT NULL
    GROUP BY extraction_status
    ORDER BY extraction_status
"""))

print("\n" + "=" * 80)
print("Provinces extraction workflow complete!")
print("=" * 80)
