# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Transportation Quarterly Customs URL Table
# MAGIC
# MAGIC Descriptive URL discovery/update notebook for the Vietnam Customs workflow.
# MAGIC
# MAGIC Behavior:
# MAGIC - Fetch records from the Vietnam Customs backend API.
# MAGIC - Match only this workflow's report family.
# MAGIC - Use first valid URL found in SB â†’ DC â†’ CT order.
# MAGIC - Insert only missing `report_quarter | sub_category` combinations.
# MAGIC - Keep schema minimal: `url`, `sub_category`, `created_at`, `report_quarter`.
# MAGIC - Default mode is dry run. Set widget `dry_run=false` to write rows.

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import requests
import re
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

try:
    dbutils.widgets.text("dry_run", "true", "Dry run? true/false")
    DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() != "false"
except Exception:
    DRY_RUN = True

TARGET_TABLE = "market_data.customs.transportation_customs_documents_url"
WORKFLOW_NAME = "IMEX Goods by Transportation Quarterly"
PERIOD_TYPE = "QUARTER"
API_ENDPOINT = "https://www.customs.gov.vn/bridge?url=/customs/api/GetTKHQInfo"
FILE_DOMAIN = "files.customs.gov.vn"
REQUEST_TIMEOUT = 45
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# Field order intentionally preserves Giang's decision: first found wins.
URL_FIELDS = [
    ("FILE_SO_BO", "SB"),
    ("FILE_DIEU_CHINH", "DC"),
    ("FILE_CHINH_THUC", "CT"),
]

REPORT_CONFIGS = [
    {
        "name": "Transportation Import Quarterly",
        # 2019+ titles use "quy 4/2025"; 2017-2018 use "quy 4 nam 2018". Requiring
        # "quy" keeps the annual "... van tai nam 2016" reports out.
        "title_patterns": [r"^nhap khau hang hoa theo phuong thuc van tai quy\s*(i{1,3}|iv|[1-4])\s*(?:/|nam\s+)20\d{2}$"],
        "url_patterns": [],
        "sub_category": "Import by Transportation",
    },
    {
        "name": "Transportation Export Quarterly",
        "title_patterns": [r"^xuat khau hang hoa theo phuong thuc van tai quy\s*(i{1,3}|iv|[1-4])\s*(?:/|nam\s+)20\d{2}$"],
        "url_patterns": [],
        "sub_category": "Export by Transportation",
    },
]

print(f"Workflow: {WORKFLOW_NAME}")
print(f"Target table: {TARGET_TABLE}")
print(f"Period type: {PERIOD_TYPE}")
print(f"DRY_RUN: {DRY_RUN}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def remove_accents(text: str) -> str:
    if text is None:
        return ""
    nfd = unicodedata.normalize("NFD", text)
    no_accents = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    no_accents = no_accents.lower()
    no_accents = re.sub(r"\s+", " ", no_accents).strip()
    return no_accents


def validate_url(url: Optional[str]) -> bool:
    if not url:
        return False
    url = str(url).strip()
    if not url or url.lower() == "null":
        return False
    if not url.startswith(("http://", "https://")):
        return False
    if FILE_DOMAIN not in url:
        return False
    if not url.lower().split("?")[0].endswith(".pdf"):
        return False
    return True


def parse_month(title: str, url: str) -> Optional[str]:
    # Preferred: month/year in title.
    m = re.search(r"(\d{1,2})\s*/\s*(20\d{2})", title or "")
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    # Fallback: common filename styles: 2025-T04T, 2025-t4, 2025_T04, etc.
    m = re.search(r"(20\d{2})\s*[-_]?\s*[tT]\s*0?(\d{1,2})\s*[tT]?", url or "")
    if m:
        month = int(m.group(2))
        if 1 <= month <= 12:
            return f"{int(m.group(1)):04d}-{month:02d}"

    return None


def roman_to_quarter(value: str) -> Optional[int]:
    v = remove_accents(value).upper().replace(" ", "")
    mapping = {"I": 1, "II": 2, "III": 3, "IV": 4, "1": 1, "2": 2, "3": 3, "4": 4}
    return mapping.get(v)


def parse_quarter(title: str, url: str) -> Optional[str]:
    text = f"{title or ''} {url or ''}"
    norm = remove_accents(text)

    # Vietnamese title examples: quÃ½ IV/2025, quÃ½ 4/2024
    m = re.search(r"quy\s*(i{1,3}|iv|[1-4])\s*/\s*(20\d{2})", norm, re.I)
    if m:
        q = roman_to_quarter(m.group(1))
        if q:
            return f"{int(m.group(2)):04d}-Q{q}"

    # 2017-2018 titles spell it out: "Quy 3 nam 2018" instead of "quy 3/2018".
    m = re.search(r"quy\s*(i{1,3}|iv|[1-4])\s*nam\s*(20\d{2})", norm, re.I)
    if m:
        q = roman_to_quarter(m.group(1))
        if q:
            return f"{int(m.group(2)):04d}-Q{q}"

    # Filename examples: Q3 NK.pdf, PTVT-XKQ2-2022.pdf, 2020-Q4-6X(VN-CT).pdf
    m = re.search(r"(?:^|[^a-z0-9])q\s*([1-4])(?:[^0-9]|$).*?(20\d{2})", norm, re.I)
    if m:
        return f"{int(m.group(2)):04d}-Q{int(m.group(1))}"

    m = re.search(r"(20\d{2}).*?(?:^|[^a-z0-9])q\s*([1-4])", norm, re.I)
    if m:
        return f"{int(m.group(1)):04d}-Q{int(m.group(2))}"

    return None


def parse_period(title: str, url: str) -> Optional[str]:
    if PERIOD_TYPE == "QUARTER":
        return parse_quarter(title, url)
    return parse_month(title, url)


def title_matches(config: Dict, normalized_title: str) -> bool:
    for pattern in config.get("title_patterns", []):
        if re.search(pattern, normalized_title, re.I):
            return True
    return False


def url_matches(config: Dict, url: str) -> bool:
    patterns = config.get("url_patterns", [])
    if not patterns:
        return True
    return any(re.search(pattern, url or "", re.I) for pattern in patterns)

# COMMAND ----------

# DBTITLE 1,Fetch Customs API Data
def fetch_api_data() -> Dict:
    payload = {
        "skip": 0,
        "take": 5000,
        "ky": "",
        "textSearch": "",
        "the_loai": "0",
        "thoigianCongBo": "",
        "typeName": "GetListSoLieu",
        "language": "TIENG_VIET",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Fetching API data, attempt {attempt}/{MAX_RETRIES}...")
            response = requests.post(API_ENDPOINT, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            print(f"API attempt {attempt} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Failed to fetch Customs API after {MAX_RETRIES} attempts: {last_error}")

api_data = fetch_api_data()
api_rows = api_data.get("arr") or []
print(f"API rows fetched: {len(api_rows)}")

# COMMAND ----------

# DBTITLE 1,Discover Candidate URLs
def discover_candidates(records: List[Dict]) -> List[Dict]:
    candidates = []
    seen_in_batch = set()

    for record in records:
        title = (record.get("TIEU_DE") or "").strip()
        normalized_title = remove_accents(title)
        if not title:
            continue

        for config in REPORT_CONFIGS:
            if not title_matches(config, normalized_title):
                continue

            chosen_url = None
            chosen_field = None
            chosen_version = None

            for field_name, version in URL_FIELDS:
                url = (record.get(field_name) or "").strip()
                if validate_url(url) and url_matches(config, url):
                    chosen_url = url
                    chosen_field = field_name
                    chosen_version = version
                    break

            if not chosen_url:
                continue

            period = parse_period(title, chosen_url)
            if not period:
                print(f"WARNING: Could not parse period, skipping: Update Transportation Quarterly Customs URL Table -> {chosen_url}")
                continue

            # One report can intentionally feed multiple downstream categories (e.g. a single report feeding both trade flows).
            sub_categories = config.get("sub_categories") or [config["sub_category"]]
            for sub_category in sub_categories:
                key = f"{period}|{sub_category}"
                if key in seen_in_batch:
                    continue
                seen_in_batch.add(key)
                candidates.append({
                    "url": chosen_url,
                    "sub_category": sub_category,
                    "report_quarter": period,
                    "title": title,
                    "source_field": chosen_field,
                    "version": chosen_version,
                })

    return candidates

candidates = discover_candidates(api_rows)
print(f"Candidate rows after in-batch period/sub-category dedupe: {len(candidates)}")

if candidates:
    display(spark.createDataFrame(candidates))
else:
    print("No candidate rows discovered.")

# COMMAND ----------

# DBTITLE 1,Schema Migration: Ensure report_quarter Exists
def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def infer_period_from_existing_url(url: str) -> str:
    if PERIOD_TYPE == "QUARTER":
        parsed = parse_quarter("", url)
    else:
        parsed = parse_month("", url)
    return parsed or "Unknown"


def ensure_period_column(table_name: str):
    if not table_exists(table_name):
        print(f"Target table does not exist yet: {table_name}. It will be created on first write.")
        return

    df = spark.table(table_name)
    if "report_quarter" in df.columns:
        print("report_quarter column already exists.")
        return

    print(f"Adding report_quarter column to {table_name}...")
    try:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (report_quarter STRING)")
        print("Added report_quarter column via ALTER TABLE.")
    except Exception as exc:
        print(f"ALTER TABLE failed, using overwriteSchema fallback: {exc}")
        rows = df.collect()
        output_rows = []
        for row in rows:
            row_dict = row.asDict()
            output_rows.append((
                row_dict.get("url"),
                row_dict.get("sub_category"),
                row_dict.get("created_at"),
                infer_period_from_existing_url(row_dict.get("url") or "")
            ))
        schema = StructType([
            StructField("url", StringType(), False),
            StructField("sub_category", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("report_quarter", StringType(), True),
        ])
        spark.createDataFrame(output_rows, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        print("Fallback migration completed.")
        return

    # Backfill rows still null after ALTER.
    df2 = spark.table(table_name)
    rows = df2.collect()
    if not rows:
        print("Table is empty; no backfill needed.")
        return

    output_rows = []
    for row in rows:
        row_dict = row.asDict()
        report_quarter = row_dict.get("report_quarter") or infer_period_from_existing_url(row_dict.get("url") or "")
        output_rows.append((
            row_dict.get("url"),
            row_dict.get("sub_category"),
            row_dict.get("created_at"),
            report_quarter
        ))

    schema = StructType([
        StructField("url", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("created_at", TimestampType(), True),
        StructField("report_quarter", StringType(), True),
    ])
    spark.createDataFrame(output_rows, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print("Backfilled report_quarter for existing rows.")

ensure_period_column(TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Filter Existing Periods
def existing_keys(table_name: str) -> set:
    if not table_exists(table_name):
        return set()
    df = spark.table(table_name)
    if "report_quarter" not in df.columns:
        return set()
    keys = set()
    for row in df.select("report_quarter", "sub_category").where(F.col("report_quarter").isNotNull()).distinct().collect():
        keys.add(f"{row['report_quarter']}|{row['sub_category']}")
    return keys

existing = existing_keys(TARGET_TABLE)
print(f"Existing period/sub-category keys: {len(existing)}")

rows_to_insert = []
for item in candidates:
    key = f"{item['report_quarter']}|{item['sub_category']}"
    if item["report_quarter"] == "Unknown":
        continue
    if key not in existing:
        rows_to_insert.append(item)

print(f"New rows to insert: {len(rows_to_insert)}")
if rows_to_insert:
    display(spark.createDataFrame(rows_to_insert))

# COMMAND ----------

# DBTITLE 1,Insert New URL Rows
inserted_count = 0

if not rows_to_insert:
    print("No new rows to insert.")
elif DRY_RUN:
    print("DRY_RUN=true, not inserting. Set widget dry_run=false to write rows.")
else:
    now = datetime.utcnow()
    insert_rows = [(r["url"], r["sub_category"], now, r["report_quarter"]) for r in rows_to_insert]
    schema = StructType([
        StructField("url", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("created_at", TimestampType(), True),
        StructField("report_quarter", StringType(), False),
    ])
    insert_df = spark.createDataFrame(insert_rows, schema)
    insert_df.write.mode("append").saveAsTable(TARGET_TABLE)
    inserted_count = len(insert_rows)
    print(f"Inserted {inserted_count} rows into {TARGET_TABLE}.")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 80)
print(f"CUSTOMS URL UPDATE SUMMARY - {WORKFLOW_NAME}")
print("=" * 80)
print(f"Target table: {TARGET_TABLE}")
print(f"Dry run: {DRY_RUN}")
print(f"API rows fetched: {len(api_rows)}")
print(f"Candidate rows discovered: {len(candidates)}")
print(f"Existing keys skipped: {len(candidates) - len(rows_to_insert)}")
print(f"Rows to insert: {len(rows_to_insert)}")
print(f"Rows inserted: {inserted_count}")

if rows_to_insert:
    by_cat = {}
    for row in rows_to_insert:
        by_cat[row["sub_category"]] = by_cat.get(row["sub_category"], 0) + 1
    print("\nBreakdown by sub_category:")
    for cat, count in sorted(by_cat.items()):
        print(f"  - {cat}: {count}")
    print("\nSample rows:")
    for row in rows_to_insert[:5]:
        print(f"  - {row['report_quarter']} | {row['sub_category']} | {row['url']}")

print("=" * 80)


