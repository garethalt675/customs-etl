-- Databricks notebook source
-- MAGIC
-- MAGIC %md
-- MAGIC # Stage 4 Curated Trade Statistics
-- MAGIC
-- MAGIC Builds BI/dashboard-friendly curated tables for `market_data.customs`.
-- MAGIC
-- MAGIC Design choices:
-- MAGIC - Keep Stage 1-3 extraction tables unchanged.
-- MAGIC - Normalize `sub_category` into `trade_flow`.
-- MAGIC - Preserve raw labels and join to shared mapping dimensions for English BI labels.
-- MAGIC - Keep grains separate, then expose a long-format unified view.
-- MAGIC - Add `Other / Unallocated` reconciliation rows where dimensional slices do not sum to the canonical total.
-- MAGIC
-- MAGIC Source tables:
-- MAGIC - `trade_statistics`
-- MAGIC - `countries_trade_statistics`
-- MAGIC - `fdi_trade_statistics`
-- MAGIC - `provinces_trade_statistics`
-- MAGIC - `transportation_trade_statistics`
-- MAGIC
-- MAGIC Ignored table: `countries_trade_statistics_v2_comparison`.

-- COMMAND ----------

USE CATALOG market_data;
USE SCHEMA customs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Shared product/category mapping dimension
-- MAGIC
-- MAGIC This table is intentionally preserved across reruns. New raw values are inserted with review flags; manually curated English names should not be overwritten.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_product_category (
  product_category_raw STRING,
  product_category_normalized STRING,
  product_category_en STRING,
  parent_category_raw STRING,
  parent_category_normalized STRING,
  parent_category_en STRING,
  mapping_method STRING,
  confidence_score DOUBLE,
  needs_review BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;

MERGE INTO dim_product_category AS target
USING (
  WITH raw_values AS (
    SELECT product_category, parent_category FROM trade_statistics
    UNION
    SELECT product_category, CAST(NULL AS STRING) AS parent_category FROM countries_trade_statistics
    UNION
    SELECT product_category, parent_category FROM fdi_trade_statistics
    UNION
    SELECT product_category, parent_category FROM transportation_trade_statistics
  )
  SELECT DISTINCT
    product_category AS product_category_raw,
    lower(trim(product_category)) AS product_category_normalized,
    product_category AS product_category_en,
    parent_category AS parent_category_raw,
    lower(trim(parent_category)) AS parent_category_normalized,
    parent_category AS parent_category_en,
    'raw_fallback_pending_review' AS mapping_method,
    CAST(NULL AS DOUBLE) AS confidence_score,
    TRUE AS needs_review,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
  FROM raw_values
  WHERE product_category IS NOT NULL
) AS source
ON target.product_category_normalized <=> source.product_category_normalized
AND target.parent_category_normalized <=> source.parent_category_normalized
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Shared country mapping dimension

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_country (
  country_name_raw STRING,
  country_name_normalized STRING,
  country_name_en STRING,
  iso2 STRING,
  iso3 STRING,
  mapping_method STRING,
  confidence_score DOUBLE,
  needs_review BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;

MERGE INTO dim_country AS target
USING (
  SELECT DISTINCT
    country_name AS country_name_raw,
    lower(trim(country_name)) AS country_name_normalized,
    country_name AS country_name_en,
    CAST(NULL AS STRING) AS iso2,
    CAST(NULL AS STRING) AS iso3,
    'raw_fallback_pending_review' AS mapping_method,
    CAST(NULL AS DOUBLE) AS confidence_score,
    TRUE AS needs_review,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
  FROM countries_trade_statistics
  WHERE country_name IS NOT NULL
) AS source
ON target.country_name_normalized <=> source.country_name_normalized
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Canonical total fact

-- COMMAND ----------

CREATE OR REPLACE TABLE curated_trade_statistics_total
USING DELTA AS
SELECT
  'total' AS grain_type,
  'trade_statistics' AS source_table,
  document_id,
  report_period,
  report_month,
  CAST(NULL AS STRING) AS report_quarter,
  report_start_date,
  report_end_date,
  CASE
    WHEN sub_category IN ('Export Goods', 'Export by Destination', 'Export by Transportation') THEN 'export'
    WHEN sub_category IN ('Import Goods', 'Import by Origin', 'Import by Transportation') THEN 'import'
    ELSE lower(trim(sub_category))
  END AS trade_flow,
  sub_category AS sub_category_raw,
  product_category AS product_category_raw,
  p.product_category_en,
  parent_category AS parent_category_raw,
  p.parent_category_en,
  CAST(NULL AS STRING) AS country_name_raw,
  CAST(NULL AS STRING) AS country_name_en,
  CAST(NULL AS STRING) AS iso2,
  CAST(NULL AS STRING) AS iso3,
  CAST(NULL AS STRING) AS province_name_raw,
  CAST(NULL AS STRING) AS province_name_en,
  CAST(NULL AS STRING) AS vehicle_type,
  'all_enterprises' AS ownership_scope,
  unit,
  period_quantity,
  period_value_usd,
  cumulative_quantity,
  cumulative_value_usd,
  FALSE AS is_reconciliation_row,
  CAST(NULL AS STRING) AS reconciliation_basis,
  parsed_timestamp AS source_timestamp,
  current_timestamp() AS curated_at
FROM trade_statistics t
LEFT JOIN dim_product_category p
  ON lower(trim(t.product_category)) <=> p.product_category_normalized
 AND lower(trim(t.parent_category)) <=> p.parent_category_normalized;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Country fact + Other / Unallocated reconciliation

-- COMMAND ----------

-- DBTITLE 1,Cell 10
CREATE OR REPLACE TABLE curated_trade_statistics_country
USING DELTA AS
WITH country_known AS (
  SELECT
    'country' AS grain_type,
    'countries_trade_statistics' AS source_table,
    document_id,
    report_period,
    report_month,
    CAST(NULL AS STRING) AS report_quarter,
    report_start_date,
    report_end_date,
    CASE
      WHEN sub_category = 'Export by Destination' THEN 'export'
      WHEN sub_category = 'Import by Origin' THEN 'import'
      ELSE lower(trim(sub_category))
    END AS trade_flow,
    sub_category AS sub_category_raw,
    product_category AS product_category_raw,
    p.product_category_en,
    CAST(NULL AS STRING) AS parent_category_raw,
    CAST(NULL AS STRING) AS parent_category_en,
    country_name AS country_name_raw,
    c.country_name_en,
    c.iso2,
    c.iso3,
    CAST(NULL AS STRING) AS province_name_raw,
    CAST(NULL AS STRING) AS province_name_en,
    CAST(NULL AS STRING) AS vehicle_type,
    'all_enterprises' AS ownership_scope,
    unit,
    period_quantity,
    period_value_usd,
    cumulative_quantity,
    cumulative_value_usd,
    FALSE AS is_reconciliation_row,
    CAST(NULL AS STRING) AS reconciliation_basis,
    parsed_timestamp AS source_timestamp,
    current_timestamp() AS curated_at
  FROM countries_trade_statistics s
  LEFT JOIN dim_product_category p
    ON lower(trim(s.product_category)) <=> p.product_category_normalized
   AND p.parent_category_normalized IS NULL
  LEFT JOIN dim_country c
    ON lower(trim(s.country_name)) <=> c.country_name_normalized
  WHERE s.product_category IS NOT NULL
),
total_by_key AS (
  SELECT report_period, report_month, report_start_date, report_end_date, trade_flow,
         product_category_raw, product_category_en, parent_category_raw, parent_category_en, unit,
         SUM(period_quantity) AS total_period_quantity,
         SUM(period_value_usd) AS total_period_value_usd,
         SUM(cumulative_quantity) AS total_cumulative_quantity,
         SUM(cumulative_value_usd) AS total_cumulative_value_usd
  FROM curated_trade_statistics_total
  GROUP BY report_period, report_month, report_start_date, report_end_date, trade_flow,
           product_category_raw, product_category_en, parent_category_raw, parent_category_en, unit
),
known_by_key AS (
  SELECT report_period, report_month, report_start_date, report_end_date, trade_flow,
         product_category_raw, unit,
         SUM(period_quantity) AS known_period_quantity,
         SUM(period_value_usd) AS known_period_value_usd,
         SUM(cumulative_quantity) AS known_cumulative_quantity,
         SUM(cumulative_value_usd) AS known_cumulative_value_usd
  FROM country_known
  GROUP BY report_period, report_month, report_start_date, report_end_date, trade_flow, product_category_raw, unit
),
country_other AS (
  SELECT
    'country' AS grain_type,
    'countries_trade_statistics' AS source_table,
    CAST(NULL AS STRING) AS document_id,
    t.report_period,
    t.report_month,
    CAST(NULL AS STRING) AS report_quarter,
    t.report_start_date,
    t.report_end_date,
    t.trade_flow,
    CASE WHEN t.trade_flow = 'export' THEN 'Export by Destination' WHEN t.trade_flow = 'import' THEN 'Import by Origin' ELSE t.trade_flow END AS sub_category_raw,
    t.product_category_raw,
    t.product_category_en,
    t.parent_category_raw,
    t.parent_category_en,
    'Other / Unallocated' AS country_name_raw,
    'Other / Unallocated' AS country_name_en,
    CAST(NULL AS STRING) AS iso2,
    CAST(NULL AS STRING) AS iso3,
    CAST(NULL AS STRING) AS province_name_raw,
    CAST(NULL AS STRING) AS province_name_en,
    CAST(NULL AS STRING) AS vehicle_type,
    'all_enterprises' AS ownership_scope,
    t.unit,
    t.total_period_quantity - COALESCE(k.known_period_quantity, 0) AS period_quantity,
    t.total_period_value_usd - COALESCE(k.known_period_value_usd, 0) AS period_value_usd,
    t.total_cumulative_quantity - COALESCE(k.known_cumulative_quantity, 0) AS cumulative_quantity,
    t.total_cumulative_value_usd - COALESCE(k.known_cumulative_value_usd, 0) AS cumulative_value_usd,
    TRUE AS is_reconciliation_row,
    'total_minus_known_country_sum' AS reconciliation_basis,
    current_timestamp() AS source_timestamp,
    current_timestamp() AS curated_at
  FROM total_by_key t
  LEFT JOIN known_by_key k
    ON t.report_period <=> k.report_period
   AND t.report_month <=> k.report_month
   AND t.report_start_date <=> k.report_start_date
   AND t.report_end_date <=> k.report_end_date
   AND t.trade_flow <=> k.trade_flow
   AND t.product_category_raw <=> k.product_category_raw
   AND t.unit <=> k.unit
  WHERE abs(t.total_period_value_usd - COALESCE(k.known_period_value_usd, 0)) > 0.001
)
SELECT * FROM country_known
UNION ALL
SELECT * FROM country_other;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. FDI fact + Other / Unallocated reconciliation
-- MAGIC
-- MAGIC The source does not expose a separate FDI/non-FDI dimension column. Rows from `fdi_trade_statistics` are labeled `ownership_scope = 'fdi'`; reconciliation rows are labeled `Other / Non-FDI / Unallocated`.

-- COMMAND ----------

CREATE OR REPLACE TABLE curated_trade_statistics_fdi
USING DELTA AS
WITH fdi_known AS (
  SELECT
    'fdi' AS grain_type,
    'fdi_trade_statistics' AS source_table,
    document_id,
    report_period,
    report_month,
    CAST(NULL AS STRING) AS report_quarter,
    report_start_date,
    report_end_date,
    CASE
      WHEN sub_category = 'Export Goods' THEN 'export'
      WHEN sub_category = 'Import Goods' THEN 'import'
      ELSE lower(trim(sub_category))
    END AS trade_flow,
    sub_category AS sub_category_raw,
    product_category AS product_category_raw,
    p.product_category_en,
    parent_category AS parent_category_raw,
    p.parent_category_en,
    CAST(NULL AS STRING) AS country_name_raw,
    CAST(NULL AS STRING) AS country_name_en,
    CAST(NULL AS STRING) AS iso2,
    CAST(NULL AS STRING) AS iso3,
    CAST(NULL AS STRING) AS province_name_raw,
    CAST(NULL AS STRING) AS province_name_en,
    CAST(NULL AS STRING) AS vehicle_type,
    'fdi' AS ownership_scope,
    unit,
    period_quantity,
    period_value_usd,
    cumulative_quantity,
    cumulative_value_usd,
    FALSE AS is_reconciliation_row,
    CAST(NULL AS STRING) AS reconciliation_basis,
    parsed_timestamp AS source_timestamp,
    current_timestamp() AS curated_at
  FROM fdi_trade_statistics s
  LEFT JOIN dim_product_category p
    ON lower(trim(s.product_category)) <=> p.product_category_normalized
   AND lower(trim(s.parent_category)) <=> p.parent_category_normalized
),
known_by_key AS (
  SELECT report_period, report_month, report_start_date, report_end_date, trade_flow, product_category_raw, unit,
         SUM(period_quantity) AS known_period_quantity,
         SUM(period_value_usd) AS known_period_value_usd,
         SUM(cumulative_quantity) AS known_cumulative_quantity,
         SUM(cumulative_value_usd) AS known_cumulative_value_usd
  FROM fdi_known
  GROUP BY report_period, report_month, report_start_date, report_end_date, trade_flow, product_category_raw, unit
),
total_by_key AS (
  SELECT report_period, report_month, report_start_date, report_end_date, trade_flow,
         product_category_raw, product_category_en, parent_category_raw, parent_category_en, unit,
         SUM(period_quantity) AS total_period_quantity,
         SUM(period_value_usd) AS total_period_value_usd,
         SUM(cumulative_quantity) AS total_cumulative_quantity,
         SUM(cumulative_value_usd) AS total_cumulative_value_usd
  FROM curated_trade_statistics_total
  GROUP BY report_period, report_month, report_start_date, report_end_date, trade_flow,
           product_category_raw, product_category_en, parent_category_raw, parent_category_en, unit
),
fdi_other AS (
  SELECT
    'fdi' AS grain_type,
    'fdi_trade_statistics' AS source_table,
    CAST(NULL AS STRING) AS document_id,
    t.report_period,
    t.report_month,
    CAST(NULL AS STRING) AS report_quarter,
    t.report_start_date,
    t.report_end_date,
    t.trade_flow,
    CASE WHEN t.trade_flow = 'export' THEN 'Export Goods' WHEN t.trade_flow = 'import' THEN 'Import Goods' ELSE t.trade_flow END AS sub_category_raw,
    t.product_category_raw,
    t.product_category_en,
    t.parent_category_raw,
    t.parent_category_en,
    CAST(NULL AS STRING) AS country_name_raw,
    CAST(NULL AS STRING) AS country_name_en,
    CAST(NULL AS STRING) AS iso2,
    CAST(NULL AS STRING) AS iso3,
    CAST(NULL AS STRING) AS province_name_raw,
    CAST(NULL AS STRING) AS province_name_en,
    CAST(NULL AS STRING) AS vehicle_type,
    'Other / Non-FDI / Unallocated' AS ownership_scope,
    t.unit,
    t.total_period_quantity - COALESCE(k.known_period_quantity, 0) AS period_quantity,
    t.total_period_value_usd - COALESCE(k.known_period_value_usd, 0) AS period_value_usd,
    t.total_cumulative_quantity - COALESCE(k.known_cumulative_quantity, 0) AS cumulative_quantity,
    t.total_cumulative_value_usd - COALESCE(k.known_cumulative_value_usd, 0) AS cumulative_value_usd,
    TRUE AS is_reconciliation_row,
    'total_minus_known_fdi_sum' AS reconciliation_basis,
    current_timestamp() AS source_timestamp,
    current_timestamp() AS curated_at
  FROM total_by_key t
  LEFT JOIN known_by_key k
    ON t.report_period <=> k.report_period
   AND t.report_month <=> k.report_month
   AND t.report_start_date <=> k.report_start_date
   AND t.report_end_date <=> k.report_end_date
   AND t.trade_flow <=> k.trade_flow
   AND t.product_category_raw <=> k.product_category_raw
   AND t.unit <=> k.unit
  WHERE abs(t.total_period_value_usd - COALESCE(k.known_period_value_usd, 0)) > 0.001
)
SELECT * FROM fdi_known
UNION ALL
SELECT * FROM fdi_other;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Province fact
-- MAGIC
-- MAGIC Current metadata did not expose a `province_name` column on `provinces_trade_statistics`. This curated table keeps the source rows and sets province fields to null until extraction is fixed or confirmed.

-- COMMAND ----------

CREATE OR REPLACE TABLE curated_trade_statistics_province
USING DELTA AS
SELECT *
FROM curated_trade_statistics_total
WHERE 1 = 0;

-- NOTE: This token currently lacks SELECT on `market_data.customs.provinces_trade_statistics`.
-- When permission is granted, replace this placeholder using the companion notebook
-- `02_add_province_curated_after_permission`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Transportation fact

-- COMMAND ----------

CREATE OR REPLACE TABLE curated_trade_statistics_transportation
USING DELTA AS
SELECT
  'transportation' AS grain_type,
  'transportation_trade_statistics' AS source_table,
  document_id,
  report_period,
  CAST(NULL AS STRING) AS report_month,
  report_quarter,
  report_start_date,
  report_end_date,
  CASE
    WHEN sub_category = 'Export by Transportation' THEN 'export'
    WHEN sub_category = 'Import by Transportation' THEN 'import'
    ELSE lower(trim(sub_category))
  END AS trade_flow,
  sub_category AS sub_category_raw,
  product_category AS product_category_raw,
  p.product_category_en,
  parent_category AS parent_category_raw,
  p.parent_category_en,
  CAST(NULL AS STRING) AS country_name_raw,
  CAST(NULL AS STRING) AS country_name_en,
  CAST(NULL AS STRING) AS iso2,
  CAST(NULL AS STRING) AS iso3,
  province_name AS province_name_raw,
  province_name AS province_name_en,
  vehicle_type,
  'all_enterprises' AS ownership_scope,
  CAST(NULL AS STRING) AS unit,
  quantity AS period_quantity,
  value_usd AS period_value_usd,
  CAST(NULL AS DECIMAL(20,3)) AS cumulative_quantity,
  CAST(NULL AS DECIMAL(20,3)) AS cumulative_value_usd,
  FALSE AS is_reconciliation_row,
  CAST(NULL AS STRING) AS reconciliation_basis,
  s.created_at AS source_timestamp,
  current_timestamp() AS curated_at
FROM transportation_trade_statistics s
LEFT JOIN dim_product_category p
  ON lower(trim(s.product_category)) <=> p.product_category_normalized
 AND lower(trim(s.parent_category)) <=> p.parent_category_normalized;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Unified BI convenience view
-- MAGIC
-- MAGIC Warning: do not sum across `grain_type` unless intentionally comparing dimensions. Use `curated_trade_statistics_total` for official total import/export values.

-- COMMAND ----------

CREATE OR REPLACE VIEW curated_trade_statistics_unified AS
SELECT * FROM curated_trade_statistics_total
where left(report_month,4) >= '2018'
UNION ALL
SELECT * FROM curated_trade_statistics_country
where left(report_month,4) >= '2018'
UNION ALL
SELECT * FROM curated_trade_statistics_fdi
where left(report_month,4) >= '2018'
UNION ALL
SELECT * FROM curated_trade_statistics_province
where left(report_month,4) >= '2018'
UNION ALL
SELECT * FROM curated_trade_statistics_transportation
where left(report_month,4) >= '2018';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Basic validation queries

-- COMMAND ----------

CREATE OR REPLACE VIEW curated_trade_statistics_row_counts AS
SELECT 'curated_trade_statistics_total' AS object_name, COUNT(*) AS row_count FROM curated_trade_statistics_total
where left(report_month,4) >= '2018'
UNION ALL SELECT 'curated_trade_statistics_country', COUNT(*) FROM curated_trade_statistics_country
where left(report_month,4) >= '2018'
UNION ALL SELECT 'curated_trade_statistics_fdi', COUNT(*) FROM curated_trade_statistics_fdi
where left(report_month,4) >= '2018'
UNION ALL SELECT 'curated_trade_statistics_province', COUNT(*) FROM curated_trade_statistics_province
where left(report_month,4) >= '2018'
UNION ALL SELECT 'curated_trade_statistics_transportation', COUNT(*) FROM curated_trade_statistics_transportation
where left(report_month,4) >= '2018'
UNION ALL SELECT 'curated_trade_statistics_unified', COUNT(*) FROM curated_trade_statistics_unified
where left(report_month,4) >= '2018';

SELECT * FROM curated_trade_statistics_row_counts;