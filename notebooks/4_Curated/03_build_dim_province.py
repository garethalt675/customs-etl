# Databricks notebook source
# DBTITLE 1,Build dim_province - Overview
# MAGIC %md
# MAGIC # Shared dimension: `dim_province`
# MAGIC
# MAGIC Maps every province/city spelling that appears in Vietnam Customs reports to
# MAGIC a **stable post-2025 geography**, so a dashboard can chart a consistent set of
# MAGIC units across the whole 2013-2026 history.
# MAGIC
# MAGIC ## Why this is needed
# MAGIC
# MAGIC National Assembly Resolution 202/2025/QH15 (operative 2025-07-01)
# MAGIC reorganised Vietnam from **63 provinces/cities into 34**. Reports before that
# MAGIC date name the old units, reports after name the new ones. Without a mapping,
# MAGIC any province time series breaks in mid-2025.
# MAGIC
# MAGIC Use `current_province_vi` / `current_province_en` for grouping. Keep
# MAGIC `province_name_vi` when you need what the source actually said.
# MAGIC
# MAGIC ## Joining
# MAGIC
# MAGIC Join on the accent-stripped key, which absorbs the spelling drift in the
# MAGIC source PDFs (`Đăk Lăk` vs `Đắk Lắk`, `TP Hồ Chí Minh` vs `Thành phố Hồ Chí Minh`):
# MAGIC
# MAGIC ```sql
# MAGIC LEFT JOIN dim_province d
# MAGIC   ON normalize_province(f.province_name) = d.province_name_normalized
# MAGIC ```
# MAGIC
# MAGIC The `normalize_province` SQL function is created below.
# MAGIC
# MAGIC **Re-runnable.** The MERGE refreshes mapping columns and inserts new
# MAGIC spellings; it never deletes.

# COMMAND ----------

# DBTITLE 1,Mapping Reference Data
import re
import unicodedata
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType

MERGER_EFFECTIVE = "2025-07-01"

# (current_vi, current_en, entity_type) - the 34 units after the 2025 reorganisation.
# Cross-checked against the Customs report for 2026-04, which lists exactly these.
CURRENT_UNITS = [
    ('Hà Nội', 'Hanoi', 'city'),
    ('Hải Phòng', 'Hai Phong', 'city'),
    ('Huế', 'Hue', 'city'),
    ('Đà Nẵng', 'Da Nang', 'city'),
    ('TP Hồ Chí Minh', 'Ho Chi Minh City', 'city'),
    ('Cần Thơ', 'Can Tho', 'city'),
    ('Lai Châu', 'Lai Chau', 'province'),
    ('Điện Biên', 'Dien Bien', 'province'),
    ('Sơn La', 'Son La', 'province'),
    ('Lạng Sơn', 'Lang Son', 'province'),
    ('Quảng Ninh', 'Quang Ninh', 'province'),
    ('Thanh Hóa', 'Thanh Hoa', 'province'),
    ('Nghệ An', 'Nghe An', 'province'),
    ('Hà Tĩnh', 'Ha Tinh', 'province'),
    ('Cao Bằng', 'Cao Bang', 'province'),
    ('Tuyên Quang', 'Tuyen Quang', 'province'),
    ('Lào Cai', 'Lao Cai', 'province'),
    ('Thái Nguyên', 'Thai Nguyen', 'province'),
    ('Phú Thọ', 'Phu Tho', 'province'),
    ('Bắc Ninh', 'Bac Ninh', 'province'),
    ('Hưng Yên', 'Hung Yen', 'province'),
    ('Ninh Bình', 'Ninh Binh', 'province'),
    ('Quảng Trị', 'Quang Tri', 'province'),
    ('Quảng Ngãi', 'Quang Ngai', 'province'),
    ('Gia Lai', 'Gia Lai', 'province'),
    ('Khánh Hòa', 'Khanh Hoa', 'province'),
    ('Lâm Đồng', 'Lam Dong', 'province'),
    ('Đắk Lắk', 'Dak Lak', 'province'),
    ('Đồng Nai', 'Dong Nai', 'province'),
    ('Tây Ninh', 'Tay Ninh', 'province'),
    ('Vĩnh Long', 'Vinh Long', 'province'),
    ('Đồng Tháp', 'Dong Thap', 'province'),
    ('An Giang', 'An Giang', 'province'),
    ('Cà Mau', 'Ca Mau', 'province'),
]

# Every pre-2025 unit -> the post-2025 unit that absorbed it.
# A unit mapping to itself survived the reorganisation unchanged.
MERGERS = {
    'Hà Nội': 'Hà Nội',
    'Cao Bằng': 'Cao Bằng',
    'Điện Biên': 'Điện Biên',
    'Hà Tĩnh': 'Hà Tĩnh',
    'Lai Châu': 'Lai Châu',
    'Lạng Sơn': 'Lạng Sơn',
    'Nghệ An': 'Nghệ An',
    'Quảng Ninh': 'Quảng Ninh',
    'Sơn La': 'Sơn La',
    'Thanh Hóa': 'Thanh Hóa',
    'Thừa Thiên Huế': 'Huế',
    'Hà Giang': 'Tuyên Quang',
    'Tuyên Quang': 'Tuyên Quang',
    'Lào Cai': 'Lào Cai',
    'Yên Bái': 'Lào Cai',
    'Bắc Kạn': 'Thái Nguyên',
    'Thái Nguyên': 'Thái Nguyên',
    'Vĩnh Phúc': 'Phú Thọ',
    'Phú Thọ': 'Phú Thọ',
    'Hòa Bình': 'Phú Thọ',
    'Bắc Giang': 'Bắc Ninh',
    'Bắc Ninh': 'Bắc Ninh',
    'Thái Bình': 'Hưng Yên',
    'Hưng Yên': 'Hưng Yên',
    'Hải Dương': 'Hải Phòng',
    'Hải Phòng': 'Hải Phòng',
    'Hà Nam': 'Ninh Bình',
    'Nam Định': 'Ninh Bình',
    'Ninh Bình': 'Ninh Bình',
    'Quảng Bình': 'Quảng Trị',
    'Quảng Trị': 'Quảng Trị',
    'Quảng Nam': 'Đà Nẵng',
    'Đà Nẵng': 'Đà Nẵng',
    'Kon Tum': 'Quảng Ngãi',
    'Quảng Ngãi': 'Quảng Ngãi',
    'Bình Định': 'Gia Lai',
    'Gia Lai': 'Gia Lai',
    'Ninh Thuận': 'Khánh Hòa',
    'Khánh Hòa': 'Khánh Hòa',
    'Đắk Nông': 'Lâm Đồng',
    'Bình Thuận': 'Lâm Đồng',
    'Lâm Đồng': 'Lâm Đồng',
    'Phú Yên': 'Đắk Lắk',
    'Đắk Lắk': 'Đắk Lắk',
    'Bình Dương': 'TP Hồ Chí Minh',
    'Bà Rịa - Vũng Tàu': 'TP Hồ Chí Minh',
    'TP Hồ Chí Minh': 'TP Hồ Chí Minh',
    'Bình Phước': 'Đồng Nai',
    'Đồng Nai': 'Đồng Nai',
    'Long An': 'Tây Ninh',
    'Tây Ninh': 'Tây Ninh',
    'Bến Tre': 'Vĩnh Long',
    'Trà Vinh': 'Vĩnh Long',
    'Vĩnh Long': 'Vĩnh Long',
    'Tiền Giang': 'Đồng Tháp',
    'Đồng Tháp': 'Đồng Tháp',
    'Bạc Liêu': 'Cà Mau',
    'Cà Mau': 'Cà Mau',
    'Kiên Giang': 'An Giang',
    'An Giang': 'An Giang',
    'Sóc Trăng': 'Cần Thơ',
    'Hậu Giang': 'Cần Thơ',
    'Cần Thơ': 'Cần Thơ',
}

# Spellings the source PDFs use beyond the canonical forms above.
ALIASES = {
    'Đăk Lăk': 'Đắk Lắk',
    'Đắc Lắc': 'Đắk Lắk',
    'Dak Lak': 'Đắk Lắk',
    'Đăk Nông': 'Đắk Nông',
    'Đắc Nông': 'Đắk Nông',
    'Bắc Cạn': 'Bắc Kạn',
    'Bắc Kan': 'Bắc Kạn',
    'Bà Rịa-Vũng Tàu': 'Bà Rịa - Vũng Tàu',
    'Bà Rịa Vũng Tàu': 'Bà Rịa - Vũng Tàu',
    'BR-VT': 'Bà Rịa - Vũng Tàu',
    'Thành phố Hồ Chí Minh': 'TP Hồ Chí Minh',
    'TP. Hồ Chí Minh': 'TP Hồ Chí Minh',
    'TP.Hồ Chí Minh': 'TP Hồ Chí Minh',
    'Hồ Chí Minh': 'TP Hồ Chí Minh',
    'Tp Hồ Chí Minh': 'TP Hồ Chí Minh',
    'Thừa Thiên - Huế': 'Thừa Thiên Huế',
    'Thừa Thiên-Huế': 'Thừa Thiên Huế',
    'TT Huế': 'Thừa Thiên Huế',
    'Khánh Hoà': 'Khánh Hòa',
    'Thanh Hoá': 'Thanh Hóa',
    'Hoà Bình': 'Hòa Bình',
    'Bình Ðịnh': 'Bình Định',
    'Quảng Nam - Đà Nẵng': 'Quảng Nam',
    'Hà Tây': 'Hà Nội',
    'Cần Thơ (TP)': 'Cần Thơ',
    'TP Cần Thơ': 'Cần Thơ',
    'TP Hải Phòng': 'Hải Phòng',
    'TP Đà Nẵng': 'Đà Nẵng',
    'TP Hà Nội': 'Hà Nội',
}

assert len(CURRENT_UNITS) == 34, "expected 34 post-merger units"
assert len(MERGERS) == 63, "expected 63 pre-merger units"
print(f"reference data: {len(CURRENT_UNITS)} current units, {len(MERGERS)} historical, {len(ALIASES)} aliases")

# COMMAND ----------

# DBTITLE 1,Normalization
def normalize_province(name):
    """Accent-stripped lowercase key, with any TP/Tỉnh prefix removed.

    NFD does not decompose Đ/đ (a distinct letter), so it is replaced explicitly.
    """
    if name is None:
        return None
    key = unicodedata.normalize("NFD", str(name))
    key = "".join(c for c in key if unicodedata.category(c) != "Mn")
    key = key.lower().replace("đ", "d")
    key = re.sub(r"^(tp\.?|thanh pho|tinh)\s+", "", key)
    key = re.sub(r"[^a-z0-9]+", " ", key)
    return re.sub(r"\s+", " ", key).strip()


spark.udf.register("normalize_province", normalize_province, StringType())
print("registered SQL function: normalize_province(name)")

# COMMAND ----------

# DBTITLE 1,Create dim_province
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS market_data.customs.dim_province (
# MAGIC   province_name_raw STRING COMMENT 'A spelling as it appears in source reports',
# MAGIC   province_name_normalized STRING COMMENT 'Accent-stripped lowercase join key',
# MAGIC   province_name_vi STRING COMMENT 'Canonical Vietnamese name of the unit as reported',
# MAGIC   current_province_vi STRING COMMENT 'Post-2025 unit this rolls up to',
# MAGIC   current_province_en STRING COMMENT 'Post-2025 unit, English',
# MAGIC   entity_type STRING COMMENT '"province" or "city" (centrally governed)',
# MAGIC   is_current_unit BOOLEAN COMMENT 'TRUE if this unit still exists after the 2025 reorganisation',
# MAGIC   merged_into STRING COMMENT 'Absorbing unit, NULL if it survived unchanged',
# MAGIC   merger_effective_date DATE COMMENT 'Date the absorption took effect',
# MAGIC   mapping_method STRING,
# MAGIC   needs_review BOOLEAN,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Vietnam province/city dimension spanning the 2025 reorganisation (63 -> 34 units)'

# COMMAND ----------

# DBTITLE 1,Build and Merge Rows
from datetime import date

current_en = {vi: en for vi, en, _ in CURRENT_UNITS}
current_type = {vi: t for vi, _, t in CURRENT_UNITS}
effective = date.fromisoformat(MERGER_EFFECTIVE)

rows, seen = [], set()


def add(raw, canonical_vi, method):
    key = normalize_province(raw)
    if key in seen:
        return
    seen.add(key)
    current_vi = MERGERS.get(canonical_vi, canonical_vi)
    rows.append((
        raw, key, canonical_vi, current_vi, current_en[current_vi],
        current_type[current_vi], canonical_vi in current_en,
        None if canonical_vi == current_vi else current_vi,
        None if canonical_vi == current_vi else effective,
        method,
    ))


for name in sorted(MERGERS):
    add(name, name, "manual_resolution_202_2025_qh15")
# A renamed unit (Thừa Thiên Huế -> Huế) appears under its new name, which is not
# a MERGERS key, so every current unit needs a self-referential row as well.
for name in sorted(current_en):
    add(name, name, "manual_resolution_202_2025_qh15")
for alias, canonical in sorted(ALIASES.items()):
    add(alias, canonical, "manual_alias")

schema = StructType([
    StructField("province_name_raw", StringType(), True),
    StructField("province_name_normalized", StringType(), True),
    StructField("province_name_vi", StringType(), True),
    StructField("current_province_vi", StringType(), True),
    StructField("current_province_en", StringType(), True),
    StructField("entity_type", StringType(), True),
    StructField("is_current_unit", BooleanType(), True),
    StructField("merged_into", StringType(), True),
    StructField("merger_effective_date", DateType(), True),
    StructField("mapping_method", StringType(), True),
])

spark.createDataFrame(rows, schema=schema).createOrReplaceTempView("dim_province_source")
print(f"prepared {len(rows)} dimension rows")

spark.sql("""
    MERGE INTO market_data.customs.dim_province AS target
    USING dim_province_source AS source
    ON target.province_name_normalized = source.province_name_normalized
    WHEN MATCHED THEN UPDATE SET
      target.province_name_raw = source.province_name_raw,
      target.province_name_vi = source.province_name_vi,
      target.current_province_vi = source.current_province_vi,
      target.current_province_en = source.current_province_en,
      target.entity_type = source.entity_type,
      target.is_current_unit = source.is_current_unit,
      target.merged_into = source.merged_into,
      target.merger_effective_date = source.merger_effective_date,
      target.mapping_method = source.mapping_method,
      target.needs_review = FALSE,
      target.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
      province_name_raw, province_name_normalized, province_name_vi,
      current_province_vi, current_province_en, entity_type, is_current_unit,
      merged_into, merger_effective_date, mapping_method, needs_review,
      created_at, updated_at
    ) VALUES (
      source.province_name_raw, source.province_name_normalized, source.province_name_vi,
      source.current_province_vi, source.current_province_en, source.entity_type,
      source.is_current_unit, source.merged_into, source.merger_effective_date,
      source.mapping_method, FALSE, current_timestamp(), current_timestamp()
    )
""")
print("✓ dim_province merged")

# COMMAND ----------

# DBTITLE 1,Validate Against the Fact Table
# Any province label in the facts that dim_province cannot resolve.
unmapped = spark.sql("""
    SELECT f.province_name, count(*) AS rows, min(f.report_month) AS first_seen,
           max(f.report_month) AS last_seen
    FROM market_data.customs.provinces_trade_statistics f
    LEFT JOIN market_data.customs.dim_province d
      ON normalize_province(f.province_name) = d.province_name_normalized
    WHERE d.province_name_normalized IS NULL
    GROUP BY f.province_name
    ORDER BY rows DESC
""")

count = unmapped.count()
if count:
    print(f"⚠ {count} province label(s) in the facts do not resolve - add them to ALIASES:")
    display(unmapped)
else:
    print("✓ every province label in provinces_trade_statistics resolves")

# COMMAND ----------

# DBTITLE 1,Merger Summary
display(spark.sql("""
    SELECT current_province_vi, current_province_en, entity_type,
           count(*) AS source_units,
           concat_ws(', ', sort_array(collect_set(
             CASE WHEN merged_into IS NOT NULL THEN province_name_vi END))) AS absorbed
    FROM market_data.customs.dim_province
    WHERE mapping_method = 'manual_resolution_202_2025_qh15'
    GROUP BY 1, 2, 3
    ORDER BY source_units DESC, current_province_vi
"""))
