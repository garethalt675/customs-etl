"""SQL for every dataset in the Vietnam Import-Export dashboard.

Grain rules that these queries encode (see notebooks/4_Curated/METADATA.md):

* Headline national figures come from ``grain_type = 'total'`` only. The country
  grain sums 2-5% ABOVE the national total because its per-category
  reconciliation row is floored at zero, so it must never be used for headlines.
* ``trade_flow`` is stored lower case (``export`` / ``import``). Everything here
  title-cases it once, at the dataset boundary, so widget filters can use the
  readable label.
* The FDI grain's reconciliation rows are the *non*-FDI residual. Excluding them
  gives FDI; total minus FDI gives domestic.
* Transport-mode data is quarterly and has no province dimension.
* Province data is USD only - no products, no quantities.
* Values are divided by 1e9 at the dataset boundary so every axis reads in
  US$ billions without per-widget number formatting.
"""

VIEW = "market_data.customs.curated_trade_statistics_unified"

# Vietnamese transport modes as printed by Customs.
MODE_CASE = """CASE vehicle_type
        WHEN 'Đường thủy'  THEN 'Sea / waterway'
        WHEN 'Đường không' THEN 'Air'
        WHEN 'Đường bộ'    THEN 'Road'
        ELSE 'Other'
      END"""

# '2025-Q4' -> 2025-10-01, so quarters sort and plot on a real time axis.
QUARTER_DATE = """to_date(concat(
        substr(report_quarter, 1, 4), '-',
        lpad(cast((cast(substr(report_quarter, 7, 1) AS int) - 1) * 3 + 1 AS string), 2, '0'),
        '-01'))"""


DATASETS = {}


DATASETS["ds_kpi"] = f"""
-- Headline counters: rolling 12 months ending at the latest published month,
-- with the preceding 12 months as the year-on-year comparison.
WITH monthly AS (
  SELECT report_month, trade_flow, sum(period_value_usd) AS v
  FROM {VIEW}
  WHERE grain_type = 'total' AND report_month IS NOT NULL
  GROUP BY report_month, trade_flow
),
bounds AS (
  SELECT to_date(max(report_month) || '-01') AS mx FROM monthly
),
f AS (
  SELECT to_date(m.report_month || '-01') AS d, m.trade_flow, m.v, b.mx
  FROM monthly m CROSS JOIN bounds b
),
agg AS (
  SELECT
    mx,
    sum(CASE WHEN trade_flow = 'export' AND d >  add_months(mx, -12) THEN v END) AS exp_now,
    sum(CASE WHEN trade_flow = 'import' AND d >  add_months(mx, -12) THEN v END) AS imp_now,
    sum(CASE WHEN trade_flow = 'export' AND d >  add_months(mx, -24)
                                        AND d <= add_months(mx, -12) THEN v END) AS exp_prev,
    sum(CASE WHEN trade_flow = 'import' AND d >  add_months(mx, -24)
                                        AND d <= add_months(mx, -12) THEN v END) AS imp_prev
  FROM f GROUP BY mx
)
SELECT
  date_format(mx, 'MMM yyyy')                            AS latest_month,
  round(exp_now / 1e9, 1)                                AS exports_bn,
  round(imp_now / 1e9, 1)                                AS imports_bn,
  round((exp_now - imp_now) / 1e9, 1)                    AS balance_bn,
  round((exp_now + imp_now) / 1e9, 1)                    AS turnover_bn,
  round((exp_now / nullif(exp_prev, 0) - 1) * 100, 1)    AS exports_yoy_pct,
  round((imp_now / nullif(imp_prev, 0) - 1) * 100, 1)    AS imports_yoy_pct
FROM agg
"""


DATASETS["ds_national_monthly"] = f"""
-- One row per month per flow, national totals.
SELECT
  to_date(report_month || '-01')                    AS report_date,
  report_month,
  year(to_date(report_month || '-01'))              AS report_year,
  initcap(trade_flow)                               AS flow,
  round(sum(period_value_usd) / 1e9, 3)             AS value_bn
FROM {VIEW}
WHERE grain_type = 'total' AND report_month IS NOT NULL
GROUP BY 1, 2, 3, 4
"""


DATASETS["ds_national_balance"] = f"""
-- Exports, imports and the surplus/deficit side by side, one row per month.
SELECT
  to_date(report_month || '-01')       AS report_date,
  report_month,
  year(to_date(report_month || '-01')) AS report_year,
  round(sum(CASE WHEN trade_flow = 'export' THEN period_value_usd ELSE 0 END) / 1e9, 3) AS exports_bn,
  round(sum(CASE WHEN trade_flow = 'import' THEN period_value_usd ELSE 0 END) / 1e9, 3) AS imports_bn,
  round(sum(CASE WHEN trade_flow = 'export' THEN period_value_usd
                 ELSE -period_value_usd END) / 1e9, 3)                                  AS balance_bn
FROM {VIEW}
WHERE grain_type = 'total' AND report_month IS NOT NULL
GROUP BY 1, 2, 3
"""


DATASETS["ds_annual"] = f"""
-- Calendar-year totals. The newest year is partial - flagged so the chart can say so.
WITH y AS (
  SELECT
    year(to_date(report_month || '-01')) AS report_year,
    initcap(trade_flow)                  AS flow,
    count(DISTINCT report_month)         AS months_present,
    sum(period_value_usd)                AS v
  FROM {VIEW}
  WHERE grain_type = 'total' AND report_month IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  report_year,
  flow,
  round(v / 1e9, 1)                                     AS value_bn,
  months_present,
  CASE WHEN months_present < 12 THEN 'Partial year' ELSE 'Full year' END AS year_status
FROM y
"""


DATASETS["ds_products"] = f"""
-- Product x month x flow, national totals, with a stable overall rank per flow
-- so widgets can take a readable top-N without re-ranking on every filter.
WITH base AS (
  SELECT
    to_date(report_month || '-01') AS report_date,
    report_month,
    initcap(trade_flow)            AS flow,
    product_category_en            AS category,
    period_value_usd               AS v
  FROM {VIEW}
  WHERE grain_type = 'total'
    AND report_month IS NOT NULL
    AND product_category_en IS NOT NULL
),
ranked AS (
  SELECT flow, category,
         row_number() OVER (PARTITION BY flow ORDER BY sum(v) DESC) AS rank_overall
  FROM base GROUP BY flow, category
)
SELECT
  b.report_date,
  b.report_month,
  year(b.report_date)               AS report_year,
  b.flow,
  b.category,
  r.rank_overall,
  round(sum(b.v) / 1e9, 4)          AS value_bn
FROM base b
JOIN ranked r ON r.flow = b.flow AND r.category = b.category
GROUP BY 1, 2, 3, 4, 5, 6
"""


DATASETS["ds_product_summary"] = f"""
-- Product league table: latest 12 months vs the 12 before, per flow.
WITH base AS (
  SELECT
    to_date(report_month || '-01') AS d,
    initcap(trade_flow)            AS flow,
    product_category_en            AS category,
    period_value_usd               AS v
  FROM {VIEW}
  WHERE grain_type = 'total'
    AND report_month IS NOT NULL
    AND product_category_en IS NOT NULL
),
b AS (SELECT max(d) AS mx FROM base),
agg AS (
  SELECT
    base.flow,
    base.category,
    sum(CASE WHEN d >  add_months(mx, -12) THEN v END) AS now_v,
    sum(CASE WHEN d >  add_months(mx, -24)
              AND d <= add_months(mx, -12) THEN v END) AS prev_v
  FROM base CROSS JOIN b
  GROUP BY base.flow, base.category
)
SELECT
  flow,
  category,
  round(now_v / 1e9, 2)                                   AS last_12m_bn,
  round(prev_v / 1e9, 2)                                  AS prior_12m_bn,
  round((now_v / nullif(prev_v, 0) - 1) * 100, 1)         AS yoy_pct,
  round(now_v * 100.0 / nullif(sum(now_v) OVER (PARTITION BY flow), 0), 2) AS share_pct,
  row_number() OVER (PARTITION BY flow ORDER BY now_v DESC)               AS rank_overall
FROM agg
WHERE now_v IS NOT NULL
"""


DATASETS["ds_countries"] = f"""
-- Country x month x flow. 'Other / Unallocated' is the reconciliation row that
-- closes the gap to the national total; it is flagged rather than hidden.
WITH base AS (
  SELECT
    to_date(report_month || '-01') AS report_date,
    report_month,
    initcap(trade_flow)            AS flow,
    country_name_en                AS country,
    is_reconciliation_row          AS is_unallocated,
    period_value_usd               AS v
  FROM {VIEW}
  WHERE grain_type = 'country'
    AND report_month IS NOT NULL
    AND country_name_en IS NOT NULL
),
ranked AS (
  SELECT flow, country,
         row_number() OVER (PARTITION BY flow ORDER BY sum(v) DESC) AS rank_overall
  FROM base WHERE NOT is_unallocated GROUP BY flow, country
)
SELECT
  b.report_date,
  b.report_month,
  year(b.report_date)                AS report_year,
  b.flow,
  b.country,
  b.is_unallocated,
  coalesce(r.rank_overall, 9999)     AS rank_overall,
  round(sum(b.v) / 1e9, 4)           AS value_bn
FROM base b
LEFT JOIN ranked r ON r.flow = b.flow AND r.country = b.country
GROUP BY 1, 2, 3, 4, 5, 6, 7
"""


DATASETS["ds_country_balance"] = f"""
-- Bilateral position per partner over the latest 12 months: what Vietnam sells
-- them, what it buys, and the surplus or deficit that leaves.
WITH base AS (
  SELECT to_date(report_month || '-01') AS d, trade_flow, country_name_en AS country,
         period_value_usd AS v
  FROM {VIEW}
  WHERE grain_type = 'country'
    AND report_month IS NOT NULL
    AND country_name_en IS NOT NULL
    AND NOT is_reconciliation_row
),
b AS (SELECT max(d) AS mx FROM base),
agg AS (
  SELECT
    base.country,
    sum(CASE WHEN trade_flow = 'export' AND d > add_months(mx, -12) THEN v ELSE 0 END) AS e,
    sum(CASE WHEN trade_flow = 'import' AND d > add_months(mx, -12) THEN v ELSE 0 END) AS i
  FROM base CROSS JOIN b
  GROUP BY base.country
)
SELECT
  country,
  round(e / 1e9, 2)                                          AS exports_bn,
  round(i / 1e9, 2)                                          AS imports_bn,
  round((e - i) / 1e9, 2)                                    AS balance_bn,
  round((e + i) / 1e9, 2)                                    AS turnover_bn,
  CASE WHEN e >= i THEN 'Surplus' ELSE 'Deficit' END          AS position,
  row_number() OVER (ORDER BY (e + i) DESC)                   AS rank_overall
FROM agg
WHERE e + i > 0
"""


DATASETS["ds_provinces"] = f"""
-- Province x month x flow. USD only - province reports carry no products and no
-- quantities. Names are already rolled up to the 34 post-2025 units, so the
-- series stays continuous across the July 2025 reorganisation.
WITH base AS (
  SELECT
    to_date(report_month || '-01') AS report_date,
    report_month,
    initcap(trade_flow)            AS flow,
    province_name_en               AS province,
    period_value_usd               AS v
  FROM {VIEW}
  WHERE grain_type = 'province'
    AND report_month IS NOT NULL
    AND province_name_en IS NOT NULL
),
ranked AS (
  SELECT flow, province,
         row_number() OVER (PARTITION BY flow ORDER BY sum(v) DESC) AS rank_overall
  FROM base GROUP BY flow, province
)
SELECT
  b.report_date,
  b.report_month,
  year(b.report_date)      AS report_year,
  b.flow,
  b.province,
  r.rank_overall,
  round(sum(b.v) / 1e9, 4) AS value_bn
FROM base b
JOIN ranked r ON r.flow = b.flow AND r.province = b.province
GROUP BY 1, 2, 3, 4, 5, 6
"""


DATASETS["ds_province_balance"] = f"""
-- Province league table over the latest 12 months.
WITH base AS (
  SELECT to_date(report_month || '-01') AS d, trade_flow, province_name_en AS province,
         period_value_usd AS v
  FROM {VIEW}
  WHERE grain_type = 'province'
    AND report_month IS NOT NULL
    AND province_name_en IS NOT NULL
),
b AS (SELECT max(d) AS mx FROM base),
agg AS (
  SELECT
    base.province,
    sum(CASE WHEN trade_flow = 'export' AND d > add_months(mx, -12) THEN v ELSE 0 END) AS e,
    sum(CASE WHEN trade_flow = 'import' AND d > add_months(mx, -12) THEN v ELSE 0 END) AS i
  FROM base CROSS JOIN b
  GROUP BY base.province
)
SELECT
  province,
  round(e / 1e9, 2)                                 AS exports_bn,
  round(i / 1e9, 2)                                 AS imports_bn,
  round((e - i) / 1e9, 2)                           AS balance_bn,
  round((e + i) / 1e9, 2)                           AS turnover_bn,
  round(e * 100.0 / nullif(sum(e) OVER (), 0), 2)   AS export_share_pct,
  row_number() OVER (ORDER BY (e + i) DESC)         AS rank_overall
FROM agg
WHERE e + i > 0
"""


DATASETS["ds_transport"] = f"""
-- Transport mode x product x quarter. Quarterly by nature; Customs has
-- published nothing for 2026, so the series ending at 2025-Q4 is expected.
WITH base AS (
  SELECT
    {QUARTER_DATE}      AS quarter_date,
    report_quarter,
    initcap(trade_flow) AS flow,
    {MODE_CASE}         AS mode,
    product_category_en AS category,
    period_value_usd    AS v
  FROM {VIEW}
  WHERE grain_type = 'transportation'
    AND report_quarter IS NOT NULL
    AND vehicle_type IS NOT NULL
),
ranked AS (
  SELECT flow, category,
         row_number() OVER (PARTITION BY flow ORDER BY sum(v) DESC) AS rank_overall
  FROM base GROUP BY flow, category
)
SELECT
  b.quarter_date,
  b.report_quarter,
  year(b.quarter_date)      AS report_year,
  b.flow,
  b.mode,
  b.category,
  r.rank_overall,
  round(sum(b.v) / 1e9, 4)  AS value_bn
FROM base b
JOIN ranked r ON r.flow = b.flow AND r.category = b.category
GROUP BY 1, 2, 3, 4, 5, 6, 7
"""


DATASETS["ds_transport_share"] = f"""
-- Share of each quarter's trade carried by each mode.
WITH base AS (
  SELECT
    {QUARTER_DATE}      AS quarter_date,
    report_quarter,
    initcap(trade_flow) AS flow,
    {MODE_CASE}         AS mode,
    sum(period_value_usd) AS v
  FROM {VIEW}
  WHERE grain_type = 'transportation'
    AND report_quarter IS NOT NULL
    AND vehicle_type IS NOT NULL
  GROUP BY 1, 2, 3, 4
)
SELECT
  quarter_date,
  report_quarter,
  year(quarter_date)                                                       AS report_year,
  flow,
  mode,
  round(v / 1e9, 3)                                                        AS value_bn,
  round(v * 100.0 / sum(v) OVER (PARTITION BY report_quarter, flow), 1)    AS share_pct
FROM base
"""


DATASETS["ds_fdi"] = f"""
-- Foreign-invested vs domestic enterprises. The FDI grain's reconciliation rows
-- are the non-FDI residual, so FDI is the non-reconciliation part and domestic
-- is the national total minus it.
WITH tot AS (
  SELECT report_month, trade_flow, sum(period_value_usd) AS v
  FROM {VIEW} WHERE grain_type = 'total' AND report_month IS NOT NULL
  GROUP BY 1, 2
),
fdi AS (
  SELECT report_month, trade_flow, sum(period_value_usd) AS v
  FROM {VIEW} WHERE grain_type = 'fdi' AND report_month IS NOT NULL
    AND NOT is_reconciliation_row
  GROUP BY 1, 2
)
SELECT
  to_date(t.report_month || '-01')                            AS report_date,
  t.report_month,
  year(to_date(t.report_month || '-01'))                      AS report_year,
  initcap(t.trade_flow)                                       AS flow,
  round(f.v / 1e9, 3)                                         AS fdi_bn,
  round(greatest(t.v - f.v, 0) / 1e9, 3)                      AS domestic_bn,
  round(least(f.v / nullif(t.v, 0), 1) * 100, 1)              AS fdi_share_pct
FROM tot t
JOIN fdi f ON f.report_month = t.report_month AND f.trade_flow = t.trade_flow
"""


DATASETS["ds_fdi_products"] = f"""
-- Where foreign-invested enterprises concentrate, latest 12 months.
WITH base AS (
  SELECT to_date(report_month || '-01') AS d, initcap(trade_flow) AS flow,
         product_category_en AS category, period_value_usd AS v
  FROM {VIEW}
  WHERE grain_type = 'fdi' AND report_month IS NOT NULL
    AND NOT is_reconciliation_row
    AND product_category_en IS NOT NULL
),
b AS (SELECT max(d) AS mx FROM base),
agg AS (
  SELECT base.flow, base.category, sum(CASE WHEN d > add_months(mx, -12) THEN v END) AS v
  FROM base CROSS JOIN b GROUP BY base.flow, base.category
)
SELECT
  flow,
  category,
  round(v / 1e9, 3)                                        AS value_bn,
  row_number() OVER (PARTITION BY flow ORDER BY v DESC)    AS rank_overall
FROM agg WHERE v IS NOT NULL
"""


DATASETS["ds_coverage"] = """
-- How complete each source pipeline is, straight from the coverage views.
SELECT
  workstream,
  count(*)                                                          AS expected_periods,
  sum(CASE WHEN status = 'ok' THEN 1 ELSE 0 END)                    AS complete,
  sum(CASE WHEN status = 'incomplete' THEN 1 ELSE 0 END)            AS incomplete,
  sum(CASE WHEN status = 'missing' THEN 1 ELSE 0 END)               AS missing,
  sum(CASE WHEN status = 'source_unavailable' THEN 1 ELSE 0 END)    AS never_published,
  sum(CASE WHEN status <> 'ok' AND NOT is_recent THEN 1 ELSE 0 END) AS actionable_gaps,
  min(period)                                                       AS first_period,
  max(CASE WHEN status = 'ok' THEN period END)                      AS last_complete_period
FROM market_data.customs.customs_coverage
GROUP BY workstream
"""


DATASETS["ds_grain_reconciliation"] = f"""
-- Sanity panel: how each breakdown compares with the official national total.
-- Provinces reconcile to the dollar; the country grain runs a few per cent high
-- because its per-category residual is floored at zero. Shown, not hidden.
WITH t AS (
  SELECT report_month, trade_flow, sum(period_value_usd) AS tot
  FROM {VIEW} WHERE grain_type = 'total' AND report_month IS NOT NULL GROUP BY 1, 2
),
g AS (
  SELECT report_month, trade_flow, grain_type, sum(period_value_usd) AS v
  FROM {VIEW}
  WHERE grain_type IN ('country', 'province') AND report_month IS NOT NULL
  GROUP BY 1, 2, 3
  UNION ALL
  SELECT report_month, trade_flow, 'fdi + domestic', sum(period_value_usd)
  FROM {VIEW} WHERE grain_type = 'fdi' AND report_month IS NOT NULL GROUP BY 1, 2
)
SELECT
  to_date(t.report_month || '-01')                              AS report_date,
  t.report_month,
  initcap(t.trade_flow)                                         AS flow,
  g.grain_type                                                  AS breakdown,
  round((g.v / nullif(t.tot, 0) - 1) * 100, 2)                  AS deviation_pct
FROM t JOIN g ON g.report_month = t.report_month AND g.trade_flow = t.trade_flow
"""
