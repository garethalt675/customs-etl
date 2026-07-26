-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Add Province Curated Table After Permission Is Fixed
-- MAGIC
-- MAGIC Run this after the current user/service principal has SELECT on `market_data.customs.provinces_trade_statistics`.

-- COMMAND ----------

USE CATALOG market_data;
USE SCHEMA customs;

-- COMMAND ----------

CREATE OR REPLACE TABLE curated_trade_statistics_province
USING DELTA AS
SELECT
  'province' AS grain_type,
  'provinces_trade_statistics' AS source_table,
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
FROM provinces_trade_statistics s
LEFT JOIN dim_product_category p
  ON lower(trim(s.product_category)) <=> p.product_category_normalized
 AND lower(trim(s.parent_category)) <=> p.parent_category_normalized;

-- COMMAND ----------

CREATE OR REPLACE VIEW curated_trade_statistics_unified AS
SELECT * FROM curated_trade_statistics_total
UNION ALL
SELECT * FROM curated_trade_statistics_country
UNION ALL
SELECT * FROM curated_trade_statistics_fdi
UNION ALL
SELECT * FROM curated_trade_statistics_province
UNION ALL
SELECT * FROM curated_trade_statistics_transportation;

-- COMMAND ----------

SELECT COUNT(*) AS province_rows FROM curated_trade_statistics_province;
