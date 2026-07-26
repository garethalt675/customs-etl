-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Translate the shared dimensions
-- MAGIC
-- MAGIC `dim_product_category` and `dim_country` are populated by the curated build
-- MAGIC with `*_en` set to the raw Vietnamese value and `needs_review = TRUE`. This
-- MAGIC notebook fills in real English labels with `ai_query`, and assigns ISO codes
-- MAGIC to countries.
-- MAGIC
-- MAGIC ## How it behaves
-- MAGIC
-- MAGIC - **Only untranslated rows are sent.** A row already curated (either by hand or
-- MAGIC   by an earlier run) is left alone, so re-running is cheap and non-destructive.
-- MAGIC - **Low confidence stays flagged.** `needs_review` is cleared only above the
-- MAGIC   threshold; anything the model is unsure of remains visible for a human.
-- MAGIC - **Nothing is deleted.** Orphan cleanup below is opt-in and reversible.
-- MAGIC
-- MAGIC Re-runnable and idempotent.

-- COMMAND ----------

USE CATALOG market_data;
USE SCHEMA customs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Country names and ISO codes
-- MAGIC
-- MAGIC Country labels are a small, well-known vocabulary, so the model is asked for a
-- MAGIC strict JSON object and the result is parsed rather than trusted as prose.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW country_translations AS
SELECT
  country_name_raw,
  country_name_normalized,
  ai_query(
    'databricks-claude-sonnet-5',
    concat(
      'You are given the Vietnamese name of a country or territory as used in ',
      'Vietnam Customs trade statistics. Return ONLY a JSON object with keys ',
      '"en" (the standard English name), "iso2" (ISO 3166-1 alpha-2), "iso3" ',
      '(ISO 3166-1 alpha-3), and "confidence" (0.0-1.0). If it is a grouping ',
      'rather than a country (for example a continent or trade bloc), set iso2 ',
      'and iso3 to null and give the English name of the grouping. ',
      'No explanation, no markdown. Vietnamese name: ', country_name_raw
    )
  ) AS response
FROM dim_country
WHERE needs_review = TRUE
  AND (country_name_en IS NULL OR country_name_en = country_name_raw);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW country_parsed AS
SELECT
  country_name_normalized,
  nullif(trim(get_json_object(response, '$.en')), '')   AS country_name_en,
  upper(nullif(trim(get_json_object(response, '$.iso2')), '')) AS iso2,
  upper(nullif(trim(get_json_object(response, '$.iso3')), '')) AS iso3,
  try_cast(get_json_object(response, '$.confidence') AS DOUBLE) AS confidence_score
FROM country_translations;

-- COMMAND ----------

MERGE INTO dim_country AS target
USING country_parsed AS source
ON target.country_name_normalized = source.country_name_normalized
WHEN MATCHED AND source.country_name_en IS NOT NULL THEN UPDATE SET
  target.country_name_en   = source.country_name_en,
  -- Only accept codes of the right shape; the model occasionally invents one.
  target.iso2              = CASE WHEN source.iso2 RLIKE '^[A-Z]{2}$' THEN source.iso2 END,
  target.iso3              = CASE WHEN source.iso3 RLIKE '^[A-Z]{3}$' THEN source.iso3 END,
  target.mapping_method    = 'llm_ai_query',
  target.confidence_score  = source.confidence_score,
  target.needs_review      = coalesce(source.confidence_score, 0) < 0.8,
  target.updated_at        = current_timestamp();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Product categories
-- MAGIC
-- MAGIC These are commodity group labels, so the prompt asks for trade terminology
-- MAGIC rather than a literal translation.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW product_translations AS
SELECT
  product_category_normalized,
  parent_category_normalized,
  ai_query(
    'databricks-claude-sonnet-5',
    concat(
      'Translate this Vietnamese commodity group label from Vietnam Customs ',
      'trade statistics into concise English trade terminology. Return ONLY a ',
      'JSON object with keys "en" (the English label) and "confidence" ',
      '(0.0-1.0). Keep it short - it is a chart axis label, not a sentence. ',
      'No explanation, no markdown. Vietnamese label: ', product_category_raw
    )
  ) AS response
FROM dim_product_category
WHERE needs_review = TRUE
  AND (product_category_en IS NULL OR product_category_en = product_category_raw);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW product_parsed AS
SELECT
  product_category_normalized,
  parent_category_normalized,
  nullif(trim(get_json_object(response, '$.en')), '') AS product_category_en,
  try_cast(get_json_object(response, '$.confidence') AS DOUBLE) AS confidence_score
FROM product_translations;

-- COMMAND ----------

MERGE INTO dim_product_category AS target
USING product_parsed AS source
ON target.product_category_normalized <=> source.product_category_normalized
AND target.parent_category_normalized <=> source.parent_category_normalized
WHEN MATCHED AND source.product_category_en IS NOT NULL THEN UPDATE SET
  target.product_category_en = source.product_category_en,
  target.mapping_method      = 'llm_ai_query',
  target.confidence_score    = source.confidence_score,
  target.needs_review        = coalesce(source.confidence_score, 0) < 0.8,
  target.updated_at          = current_timestamp();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Propagate parent labels
-- MAGIC
-- MAGIC A parent category is itself a product category elsewhere in the table, so its
-- MAGIC English label is reused rather than translated twice.

-- COMMAND ----------

MERGE INTO dim_product_category AS target
USING (
  SELECT DISTINCT p.parent_category_normalized, c.product_category_en AS parent_en
  FROM dim_product_category p
  JOIN dim_product_category c
    ON c.product_category_normalized = p.parent_category_normalized
   AND c.parent_category_normalized IS NULL
  WHERE p.parent_category_normalized IS NOT NULL
    AND c.product_category_en IS NOT NULL
    AND c.product_category_en <> c.product_category_raw
) AS source
ON target.parent_category_normalized = source.parent_category_normalized
WHEN MATCHED THEN UPDATE SET
  target.parent_category_en = source.parent_en,
  target.updated_at         = current_timestamp();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Orphan review
-- MAGIC
-- MAGIC `dim_country` accumulated entries from an earlier version of the country
-- MAGIC extraction that no longer appear in the facts. They are **reported, not
-- MAGIC deleted** - an orphan may simply be a country absent from recent months.

-- COMMAND ----------

CREATE OR REPLACE VIEW dim_country_orphans AS
SELECT d.country_name_raw, d.country_name_en, d.iso2, d.mapping_method
FROM dim_country d
WHERE NOT EXISTS (
  SELECT 1 FROM countries_trade_statistics f
  WHERE lower(trim(f.country_name)) = d.country_name_normalized
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Results

-- COMMAND ----------

SELECT
  'dim_country' AS dimension,
  count(*) AS rows,
  count(*) FILTER (WHERE country_name_en <> country_name_raw) AS translated,
  count(*) FILTER (WHERE iso2 IS NOT NULL) AS with_iso2,
  count(*) FILTER (WHERE needs_review) AS needs_review,
  round(avg(confidence_score), 3) AS avg_confidence
FROM dim_country
UNION ALL
SELECT
  'dim_product_category',
  count(*),
  count(*) FILTER (WHERE product_category_en <> product_category_raw),
  NULL,
  count(*) FILTER (WHERE needs_review),
  round(avg(confidence_score), 3)
FROM dim_product_category;

-- COMMAND ----------

SELECT count(*) AS orphan_countries FROM dim_country_orphans;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sample of what was produced - worth eyeballing before trusting a dashboard.

-- COMMAND ----------

SELECT country_name_raw, country_name_en, iso2, iso3, confidence_score, needs_review
FROM dim_country
WHERE mapping_method = 'llm_ai_query'
ORDER BY confidence_score NULLS FIRST
LIMIT 25;

-- COMMAND ----------

SELECT product_category_raw, product_category_en, confidence_score, needs_review
FROM dim_product_category
WHERE mapping_method = 'llm_ai_query'
ORDER BY confidence_score NULLS FIRST
LIMIT 25;
