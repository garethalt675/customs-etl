-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Coverage views
-- MAGIC
-- MAGIC Makes period gaps visible instead of something you notice by accident.
-- MAGIC
-- MAGIC - `customs_coverage` - one row per workstream x period, with the flows present
-- MAGIC - `customs_coverage_gaps` - only the periods that are missing or incomplete
-- MAGIC
-- MAGIC Scope starts at **2018-01**, the agreed backfill horizon and the same cut the
-- MAGIC curated unified view applies.
-- MAGIC
-- MAGIC Transport-mode reports are **quarterly**; everything else is monthly, so
-- MAGIC periods are compared as text keys (`YYYY-MM` / `YYYY-QN`) per workstream
-- MAGIC rather than forced onto one calendar.

-- COMMAND ----------

USE CATALOG market_data;
USE SCHEMA customs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Expected periods
-- MAGIC
-- MAGIC Every month from 2018-01 to the current month, and every quarter from
-- MAGIC 2018-Q1 to the quarter before the current one. Customs publishes in arrears,
-- MAGIC so the newest period or two being absent is normal, not a fault - the
-- MAGIC `is_recent` flag marks those so alerting can ignore them.

-- COMMAND ----------

CREATE OR REPLACE VIEW customs_expected_periods AS
WITH months AS (
  SELECT date_format(add_months(to_date('2018-01-01'), n), 'yyyy-MM') AS period
  FROM (SELECT explode(sequence(0, 240)) AS n)
  WHERE add_months(to_date('2018-01-01'), n) <= current_date()
),
quarters AS (
  SELECT concat(cast(year(d) AS string), '-Q', cast(quarter(d) AS string)) AS period
  FROM (
    SELECT add_months(to_date('2018-01-01'), n * 3) AS d
    FROM (SELECT explode(sequence(0, 80)) AS n)
  )
  WHERE d <= add_months(current_date(), -3)
),
streams AS (
  SELECT * FROM VALUES
    ('totals',         'month'),
    ('countries',      'month'),
    ('fdi',            'month'),
    ('provinces',      'month'),
    ('transportation', 'quarter')
  AS t(workstream, period_type)
)
SELECT s.workstream, s.period_type, p.period
FROM streams s
JOIN (
  SELECT 'month' AS period_type, period FROM months
  UNION ALL
  SELECT 'quarter', period FROM quarters
) p ON p.period_type = s.period_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Actual coverage
-- MAGIC
-- MAGIC `flows_present` should be 2 everywhere. The province stream stores the flow
-- MAGIC in `trade_flow` rather than `sub_category`, so it is read differently.

-- COMMAND ----------

CREATE OR REPLACE VIEW customs_actual_periods AS
SELECT 'totals' AS workstream, report_month AS period,
       count(DISTINCT sub_category) AS flows_present, count(*) AS rows
FROM trade_statistics WHERE report_month IS NOT NULL GROUP BY report_month
UNION ALL
SELECT 'countries', report_month, count(DISTINCT sub_category), count(*)
FROM countries_trade_statistics WHERE report_month IS NOT NULL GROUP BY report_month
UNION ALL
SELECT 'fdi', report_month, count(DISTINCT sub_category), count(*)
FROM fdi_trade_statistics WHERE report_month IS NOT NULL GROUP BY report_month
UNION ALL
SELECT 'provinces', report_month, count(DISTINCT trade_flow), count(*)
FROM provinces_trade_statistics WHERE report_month IS NOT NULL GROUP BY report_month
UNION ALL
SELECT 'transportation', report_quarter, count(DISTINCT sub_category), count(*)
FROM transportation_trade_statistics WHERE report_quarter IS NOT NULL GROUP BY report_quarter;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Known source gaps
-- MAGIC
-- MAGIC Periods Customs never made publicly retrievable. These are excluded from
-- MAGIC `customs_coverage_gaps` so QA is not permanently red for something no amount
-- MAGIC of re-running can fix - but they stay visible in `customs_coverage` with
-- MAGIC status `source_unavailable`, and the reason is recorded here rather than in
-- MAGIC someone's memory.
-- MAGIC
-- MAGIC Add a row only after confirming the report is genuinely unobtainable.

-- COMMAND ----------

CREATE OR REPLACE VIEW customs_known_source_gaps AS
SELECT * FROM VALUES
  ('countries', '2022-10', 'import',
   'Customs published this report only at 10.224.128.185:8080, an internal RFC1918 host unreachable from outside their network. Export side is present. Confirmed 2026-07-26.')
AS t(workstream, period, missing_flow, reason);

-- COMMAND ----------

CREATE OR REPLACE VIEW customs_coverage AS
SELECT
  e.workstream,
  e.period_type,
  e.period,
  coalesce(a.flows_present, 0) AS flows_present,
  coalesce(a.rows, 0)          AS rows,
  CASE
    WHEN g.period IS NOT NULL   THEN 'source_unavailable'
    WHEN a.period IS NULL       THEN 'missing'
    WHEN a.flows_present < 2    THEN 'incomplete'
    ELSE 'ok'
  END AS status,
  -- Customs publishes in arrears; the newest periods are legitimately absent.
  e.period >= CASE e.period_type
                WHEN 'month' THEN date_format(add_months(current_date(), -2), 'yyyy-MM')
                ELSE concat(cast(year(add_months(current_date(), -6)) AS string), '-Q',
                            cast(quarter(add_months(current_date(), -6)) AS string))
              END AS is_recent
FROM customs_expected_periods e
LEFT JOIN customs_actual_periods a
  ON a.workstream = e.workstream AND a.period = e.period
LEFT JOIN customs_known_source_gaps g
  ON g.workstream = e.workstream AND g.period = e.period;

-- COMMAND ----------

CREATE OR REPLACE VIEW customs_coverage_gaps AS
SELECT * FROM customs_coverage
WHERE status NOT IN ('ok', 'source_unavailable') AND NOT is_recent
ORDER BY workstream, period;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary

-- COMMAND ----------

SELECT
  workstream,
  count(*)                                              AS expected_periods,
  sum(CASE WHEN status = 'ok' THEN 1 ELSE 0 END)         AS ok,
  sum(CASE WHEN status = 'incomplete' THEN 1 ELSE 0 END) AS incomplete,
  sum(CASE WHEN status = 'missing' THEN 1 ELSE 0 END)    AS missing,
  sum(CASE WHEN status <> 'ok' AND NOT is_recent THEN 1 ELSE 0 END) AS actionable_gaps,
  min(period) AS first_period,
  max(period) AS last_period
FROM customs_coverage
GROUP BY workstream
ORDER BY workstream;

-- COMMAND ----------

SELECT * FROM customs_coverage_gaps;
