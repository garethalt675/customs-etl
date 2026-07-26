-- Databricks notebook source
-- MAGIC %md
-- MAGIC # QA checks
-- MAGIC
-- MAGIC Runs after the pipelines and **fails the task** if anything material is wrong,
-- MAGIC so a scheduled run turns red instead of quietly publishing bad numbers.
-- MAGIC
-- MAGIC Each check returns a severity:
-- MAGIC
-- MAGIC - `FAIL` - wrong or missing data. The notebook raises at the end.
-- MAGIC - `WARN` - worth a look, does not fail the run.
-- MAGIC
-- MAGIC The thresholds encode what has actually gone wrong on this project before:
-- MAGIC misfiled periods, a wiped fact table, unresolved dimension labels, and
-- MAGIC duplicate rows from a non-idempotent write.

-- COMMAND ----------

USE CATALOG market_data;
USE SCHEMA customs;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW qa_results AS

-- 1. Empty fact tables. A table that goes to zero means something destructive ran.
SELECT 'fact_not_empty' AS check_name, 'trade_statistics' AS subject,
       CASE WHEN count(*) = 0 THEN 'FAIL' ELSE 'PASS' END AS severity,
       concat(count(*), ' rows') AS detail
FROM trade_statistics
UNION ALL
SELECT 'fact_not_empty', 'countries_trade_statistics',
       CASE WHEN count(*) = 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' rows')
FROM countries_trade_statistics
UNION ALL
SELECT 'fact_not_empty', 'fdi_trade_statistics',
       CASE WHEN count(*) = 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' rows')
FROM fdi_trade_statistics
UNION ALL
SELECT 'fact_not_empty', 'provinces_trade_statistics',
       CASE WHEN count(*) = 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' rows')
FROM provinces_trade_statistics
UNION ALL
SELECT 'fact_not_empty', 'transportation_trade_statistics',
       CASE WHEN count(*) = 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' rows')
FROM transportation_trade_statistics

-- 2. Coverage gaps outside the publication lag.
UNION ALL
SELECT 'coverage_gaps', workstream,
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
       concat(count(*), ' actionable gap(s): ',
              coalesce(concat_ws(', ', sort_array(collect_list(period))), 'none'))
FROM customs_coverage_gaps
GROUP BY workstream

-- 3. Duplicate rows at the natural grain - the signature of an append that
--    should have been a MERGE.
UNION ALL
SELECT 'no_duplicate_grain', 'trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' duplicated key(s)')
FROM (SELECT sub_category, document_id, row_number FROM trade_statistics
      GROUP BY 1,2,3 HAVING count(*) > 1)
UNION ALL
SELECT 'no_duplicate_grain', 'provinces_trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' duplicated key(s)')
FROM (SELECT document_id, province_name, trade_flow FROM provinces_trade_statistics
      GROUP BY 1,2,3 HAVING count(*) > 1)
UNION ALL
SELECT 'no_duplicate_grain', 'transportation_trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' duplicated key(s)')
FROM (SELECT sub_category, document_id, row_number FROM transportation_trade_statistics
      GROUP BY 1,2,3 HAVING count(*) > 1)

-- 4. Period integrity. A null period means the row cannot be placed in time.
UNION ALL
SELECT 'period_not_null', 'provinces_trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' null report_month')
FROM provinces_trade_statistics WHERE report_month IS NULL
UNION ALL
SELECT 'period_not_null', 'transportation_trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' null report_quarter')
FROM transportation_trade_statistics WHERE report_quarter IS NULL

-- 5. Periods must not run ahead of today - the signature of a misparsed year.
UNION ALL
SELECT 'no_future_periods', 'all monthly facts',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END, concat(count(*), ' future-dated row(s)')
FROM (
  SELECT report_month FROM trade_statistics
  UNION ALL SELECT report_month FROM countries_trade_statistics
  UNION ALL SELECT report_month FROM fdi_trade_statistics
  UNION ALL SELECT report_month FROM provinces_trade_statistics
) WHERE report_month > date_format(add_months(current_date(), 1), 'yyyy-MM')

-- 6. Transport modes must stay within the four canonical values.
UNION ALL
SELECT 'vehicle_type_canonical', 'transportation_trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
       concat(count(*), ' non-canonical value(s)')
FROM (SELECT DISTINCT vehicle_type FROM transportation_trade_statistics)
WHERE vehicle_type IS NULL
   OR vehicle_type NOT IN ('Đường bộ', 'Đường không', 'Đường thủy', 'Loại khác')

-- 7. Every province label must resolve through dim_province.
UNION ALL
SELECT 'province_labels_resolve', 'provinces_trade_statistics',
       CASE WHEN count(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
       concat(count(*), ' unresolved label(s)')
FROM (
  SELECT DISTINCT f.province_name
  FROM provinces_trade_statistics f
  LEFT JOIN dim_province d ON normalize_province(f.province_name) = d.province_name_normalized
  WHERE d.province_name_normalized IS NULL
)

-- 8. The unified view must include every grain. Transportation vanished from it
--    once already because the period filter ignored report_quarter.
UNION ALL
SELECT 'unified_has_all_grains', 'curated_trade_statistics_unified',
       CASE WHEN count(DISTINCT grain_type) < 5 THEN 'FAIL' ELSE 'PASS' END,
       concat(count(DISTINCT grain_type), ' of 5 grains: ',
              concat_ws(', ', sort_array(collect_set(grain_type))))
FROM curated_trade_statistics_unified

-- 9. Curated must not lag the sources it is built from.
UNION ALL
SELECT 'curated_matches_source', 'total',
       CASE WHEN c <> s THEN 'FAIL' ELSE 'PASS' END, concat('curated ', c, ' vs source ', s)
FROM (SELECT (SELECT count(*) FROM curated_trade_statistics_total) AS c,
             (SELECT count(*) FROM trade_statistics) AS s)
UNION ALL
SELECT 'curated_matches_source', 'province',
       CASE WHEN c <> s THEN 'FAIL' ELSE 'PASS' END, concat('curated ', c, ' vs source ', s)
FROM (SELECT (SELECT count(*) FROM curated_trade_statistics_province) AS c,
             (SELECT count(*) FROM provinces_trade_statistics) AS s)
UNION ALL
SELECT 'curated_matches_source', 'transportation',
       CASE WHEN c <> s THEN 'FAIL' ELSE 'PASS' END, concat('curated ', c, ' vs source ', s)
FROM (SELECT (SELECT count(*) FROM curated_trade_statistics_transportation) AS c,
             (SELECT count(*) FROM transportation_trade_statistics) AS s)

-- 10. Dimension translation progress. Advisory: a fresh category is expected to
--     arrive untranslated and be picked up on the next translation run.
UNION ALL
SELECT 'dimensions_translated', 'dim_country',
       CASE WHEN count(*) FILTER (WHERE country_name_en = country_name_raw) > 0
            THEN 'WARN' ELSE 'PASS' END,
       concat(count(*) FILTER (WHERE country_name_en = country_name_raw), ' of ',
              count(*), ' untranslated')
FROM dim_country
UNION ALL
SELECT 'dimensions_translated', 'dim_product_category',
       CASE WHEN count(*) FILTER (WHERE product_category_en = product_category_raw) > 0
            THEN 'WARN' ELSE 'PASS' END,
       concat(count(*) FILTER (WHERE product_category_en = product_category_raw), ' of ',
              count(*), ' untranslated')
FROM dim_product_category

-- 11. Extraction failures recorded in the processing logs.
UNION ALL
SELECT 'no_extraction_failures', 'all workstreams',
       CASE WHEN sum(n) > 0 THEN 'WARN' ELSE 'PASS' END, concat(sum(n), ' failed document(s)')
FROM (
  SELECT count(*) AS n FROM document_processing_log WHERE extraction_status = 'failed'
  UNION ALL SELECT count(*) FROM countries_document_processing_log WHERE extraction_status = 'failed'
  UNION ALL SELECT count(*) FROM fdi_document_processing_log WHERE extraction_status = 'failed'
  UNION ALL SELECT count(*) FROM provinces_document_processing_log WHERE extraction_status = 'failed'
  UNION ALL SELECT count(*) FROM transportation_document_processing_log WHERE extraction_status = 'failed'
);

-- COMMAND ----------

SELECT severity, check_name, subject, detail
FROM qa_results
ORDER BY CASE severity WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, check_name, subject;

-- COMMAND ----------

SELECT
  count(*) FILTER (WHERE severity = 'FAIL') AS failures,
  count(*) FILTER (WHERE severity = 'WARN') AS warnings,
  count(*) FILTER (WHERE severity = 'PASS') AS passed,
  count(*) AS total_checks
FROM qa_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fail the task on any FAIL
-- MAGIC
-- MAGIC `raise_error` aborts the statement, which fails the notebook and therefore the
-- MAGIC job task. Without this the checks above would be decoration.

-- COMMAND ----------

SELECT
  CASE
    WHEN count(*) FILTER (WHERE severity = 'FAIL') > 0
    THEN raise_error(concat(
           'QA FAILED: ',
           count(*) FILTER (WHERE severity = 'FAIL'), ' check(s) failed -> ',
           concat_ws(' | ', sort_array(collect_list(
             CASE WHEN severity = 'FAIL'
                  THEN concat(check_name, '[', subject, ']: ', detail) END)))))
    ELSE concat('QA passed: ',
                count(*) FILTER (WHERE severity = 'PASS'), ' passed, ',
                count(*) FILTER (WHERE severity = 'WARN'), ' warning(s)')
  END AS qa_outcome
FROM qa_results;
