# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 4 Curated Trade Statistics Spec
# MAGIC 
# MAGIC ## Context
# MAGIC 
# MAGIC Build a Stage 4 curated layer for Vietnam Customs trade statistics in Databricks.
# MAGIC 
# MAGIC Catalog/schema:
# MAGIC 
# MAGIC - `market_data.customs`
# MAGIC 
# MAGIC Source tables to include:
# MAGIC 
# MAGIC - `trade_statistics` — main total table
# MAGIC - `countries_trade_statistics` — country dimension
# MAGIC - `fdi_trade_statistics` — FDI dimension
# MAGIC - `provinces_trade_statistics` — province dimension
# MAGIC - `transportation_trade_statistics` — transportation/vehicle dimension
# MAGIC 
# MAGIC Explicitly ignore:
# MAGIC 
# MAGIC - `countries_trade_statistics_v2_comparison`
# MAGIC 
# MAGIC ## Design Goal
# MAGIC 
# MAGIC Create BI/dashboard-friendly curated tables that preserve every useful dimension from the source tables, while making the data easier to query consistently.
# MAGIC 
# MAGIC The source tables do **not** all represent the same grain:
# MAGIC 
# MAGIC - `trade_statistics` has the main total amount.
# MAGIC - Child/dimensional tables represent slices by country, FDI, province, or transportation.
# MAGIC - Child tables may miss components and will not always sum to the total.
# MAGIC 
# MAGIC Therefore, do **not** force all child tables into a single additive fact table where every row is assumed to sum cleanly to the official total.
# MAGIC 
# MAGIC ## Chosen Approach
# MAGIC 
# MAGIC Use **Option B**:
# MAGIC 
# MAGIC Create a curated Stage 4 layer with separate but standardized fact tables/views by analytical grain, plus shared dimensions/mapping tables.
# MAGIC 
# MAGIC Recommended curated outputs:
# MAGIC 
# MAGIC 1. `curated_trade_statistics_total`
# MAGIC    - Grain: official product/category total by period and trade flow.
# MAGIC    - Source: `trade_statistics`.
# MAGIC    - This is the canonical source for total import/export values.
# MAGIC 
# MAGIC 2. `curated_trade_statistics_country`
# MAGIC    - Grain: product/category x country x period x trade flow.
# MAGIC    - Source: `countries_trade_statistics`.
# MAGIC 
# MAGIC 3. `curated_trade_statistics_fdi`
# MAGIC    - Grain: product/category x FDI/non-FDI style slice x period x trade flow.
# MAGIC    - Source: `fdi_trade_statistics`.
# MAGIC 
# MAGIC 4. `curated_trade_statistics_province`
# MAGIC    - Grain: product/category x province x period x trade flow.
# MAGIC    - Source: `provinces_trade_statistics`.
# MAGIC 
# MAGIC 5. `curated_trade_statistics_transportation`
# MAGIC    - Grain: product/category x province x vehicle type x quarter x trade flow.
# MAGIC    - Source: `transportation_trade_statistics`.
# MAGIC 
# MAGIC 6. Optional BI convenience view: `curated_trade_statistics_unified`
# MAGIC    - A unioned, long-format view for dashboards.
# MAGIC    - Must include a column like `source_dimension` / `grain_type` so BI users know whether a row is total, country, FDI, province, or transportation.
# MAGIC    - Values in this view should not be blindly summed across `grain_type`.
# MAGIC 
# MAGIC ## Dimensional Reconciliation Rule
# MAGIC 
# MAGIC If a child dimension does not sum to the total, add a synthetic `Other / Unallocated` row for the gap.
# MAGIC 
# MAGIC Example:
# MAGIC 
# MAGIC - Total from `trade_statistics` = 100
# MAGIC - Sum of known countries = 92
# MAGIC - Add country row: `country_name = 'Other / Unallocated'`, value = 8
# MAGIC 
# MAGIC Apply this reconciliation separately per appropriate grain:
# MAGIC 
# MAGIC - period
# MAGIC - trade flow
# MAGIC - product/category level
# MAGIC - unit where applicable
# MAGIC - metric type: period quantity/value and cumulative quantity/value
# MAGIC 
# MAGIC Important:
# MAGIC 
# MAGIC - Reconciliation rows are analytical helpers, not raw facts.
# MAGIC - Add flags:
# MAGIC   - `is_reconciliation_row` boolean
# MAGIC   - `reconciliation_basis` string, e.g. `total_minus_known_country_sum`
# MAGIC   - `source_table` string
# MAGIC 
# MAGIC ## Trade Flow Cleaning
# MAGIC 
# MAGIC Create a clean `trade_flow` column across all curated outputs.
# MAGIC 
# MAGIC Map current `sub_category` values into normalized trade flow:
# MAGIC 
# MAGIC - `Export Goods` -> `export`
# MAGIC - `Import Goods` -> `import`
# MAGIC - `Export by Destination` -> `export`
# MAGIC - `Import by Origin` -> `import`
# MAGIC - `Export by Transportation` -> `export`
# MAGIC - `Import by Transportation` -> `import`
# MAGIC 
# MAGIC Keep original values too:
# MAGIC 
# MAGIC - `sub_category_raw`
# MAGIC 
# MAGIC Do not rely on `sub_category` as the final dashboard field.
# MAGIC 
# MAGIC ## Category Meaning
# MAGIC 
# MAGIC Current observed meaning:
# MAGIC 
# MAGIC - `sub_category` is the trade-flow/report-section label, not a true product dimension.
# MAGIC - `parent_category` is mostly null in the current tables observed, but should be retained because it may become useful if future extraction identifies product hierarchy.
# MAGIC - `product_category` is the current main product/category dimension.
# MAGIC 
# MAGIC Curated fields should include:
# MAGIC 
# MAGIC - `trade_flow`
# MAGIC - `sub_category_raw`
# MAGIC - `product_category_raw`
# MAGIC - `product_category_en`
# MAGIC - `parent_category_raw`
# MAGIC - `parent_category_en`
# MAGIC 
# MAGIC ## Product Name Translation / Cleaning
# MAGIC 
# MAGIC Need a shared product mapping process across all tables.
# MAGIC 
# MAGIC Chosen approach:
# MAGIC 
# MAGIC - Stage 4 curated cleaning, not bronze/extraction.
# MAGIC - Build and maintain a reusable product dimension/mapping table.
# MAGIC - Apply the same mapping across all curated outputs.
# MAGIC 
# MAGIC Recommended table:
# MAGIC 
# MAGIC `market_data.customs.dim_product_category`
# MAGIC 
# MAGIC Suggested columns:
# MAGIC 
# MAGIC - `product_category_raw`
# MAGIC - `product_category_normalized`
# MAGIC - `product_category_en`
# MAGIC - `parent_category_raw`
# MAGIC - `parent_category_en`
# MAGIC - `mapping_method` — e.g. `manual`, `dictionary`, `fuzzy`, `llm_reviewed`
# MAGIC - `confidence_score`
# MAGIC - `needs_review`
# MAGIC - `created_at`
# MAGIC - `updated_at`
# MAGIC 
# MAGIC Recommended method:
# MAGIC 
# MAGIC 1. Collect distinct product/category names from all source tables.
# MAGIC 2. Normalize text for matching:
# MAGIC    - trim
# MAGIC    - lowercase
# MAGIC    - normalize Unicode
# MAGIC    - remove/standardize diacritics only for matching key, not as final raw value
# MAGIC    - clean obvious mojibake/encoding artifacts where possible
# MAGIC 3. Use dictionary/manual mappings for known Vietnamese customs categories first.
# MAGIC 4. Use fuzzy matching only to cluster likely duplicates.
# MAGIC 5. Do not auto-merge low-confidence fuzzy matches without review.
# MAGIC 6. Use LLM translation as an assistant for English labels, but store confidence/review status.
# MAGIC 
# MAGIC Important:
# MAGIC 
# MAGIC - Preserve raw Vietnamese/corrupted names for lineage.
# MAGIC - Use English names for BI friendliness.
# MAGIC - Do not overwrite bronze/raw extraction output.
# MAGIC 
# MAGIC ## Country Name Translation / Cleaning
# MAGIC 
# MAGIC Do the same for country names.
# MAGIC 
# MAGIC Recommended table:
# MAGIC 
# MAGIC `market_data.customs.dim_country`
# MAGIC 
# MAGIC Suggested columns:
# MAGIC 
# MAGIC - `country_name_raw`
# MAGIC - `country_name_normalized`
# MAGIC - `country_name_en`
# MAGIC - `iso2`
# MAGIC - `iso3`
# MAGIC - `mapping_method`
# MAGIC - `confidence_score`
# MAGIC - `needs_review`
# MAGIC - `created_at`
# MAGIC - `updated_at`
# MAGIC 
# MAGIC Recommended method:
# MAGIC 
# MAGIC 1. Collect distinct `country_name` from `countries_trade_statistics`.
# MAGIC 2. Normalize text for matching.
# MAGIC 3. Map to standard English country names and ISO codes.
# MAGIC 4. Use deterministic country reference data where possible.
# MAGIC 5. Use fuzzy matching for variants/misspellings/encoding issues, but require review for ambiguous matches.
# MAGIC 6. Preserve `country_name_raw` in curated country-level output.
# MAGIC 
# MAGIC ## Why Cleaning Belongs in Stage 4, Not Bronze
# MAGIC 
# MAGIC Extraction notebooks currently sit in the Stage 3 extraction layer. Their job is to extract structured rows from parsed documents into source tables such as:
# MAGIC 
# MAGIC - `trade_statistics`
# MAGIC - `countries_trade_statistics`
# MAGIC - `fdi_trade_statistics`
# MAGIC - `provinces_trade_statistics`
# MAGIC - `transportation_trade_statistics`
# MAGIC 
# MAGIC Do not put English translation, fuzzy matching, or BI semantic reconciliation into the extraction notebooks.
# MAGIC 
# MAGIC Reason:
# MAGIC 
# MAGIC - Bronze/raw/extraction should preserve source truth and lineage.
# MAGIC - Translation and fuzzy matching are opinionated transformations.
# MAGIC - Reconciliation rows are derived analytical constructs.
# MAGIC - BI-friendly names and dimensional conformance belong in curated/silver-gold style processing.
# MAGIC 
# MAGIC So the selected design is:
# MAGIC 
# MAGIC - Stage 1: crawl/download documents
# MAGIC - Stage 2: parse documents
# MAGIC - Stage 3: extract tables/statistics into raw structured Delta tables
# MAGIC - Stage 4: curated/conformed BI layer with cleaning, translation, dimensions, and reconciliation
# MAGIC 
# MAGIC ## Source Table Columns Observed
# MAGIC 
# MAGIC ### `market_data.customs.trade_statistics`
# MAGIC 
# MAGIC - `sub_category` string
# MAGIC - `document_id` string
# MAGIC - `report_period` string
# MAGIC - `report_month` string
# MAGIC - `report_start_date` date
# MAGIC - `report_end_date` date
# MAGIC - `row_number` int
# MAGIC - `product_category` string
# MAGIC - `parent_category` string
# MAGIC - `unit` string
# MAGIC - `period_quantity` decimal(20,3)
# MAGIC - `period_value_usd` decimal(20,3)
# MAGIC - `cumulative_quantity` decimal(20,3)
# MAGIC - `cumulative_value_usd` decimal(20,3)
# MAGIC - `parsed_timestamp` timestamp
# MAGIC 
# MAGIC ### `market_data.customs.countries_trade_statistics`
# MAGIC 
# MAGIC - `sub_category` string
# MAGIC - `document_id` string
# MAGIC - `report_period` string
# MAGIC - `report_month` string
# MAGIC - `report_start_date` date
# MAGIC - `report_end_date` date
# MAGIC - `row_number` int
# MAGIC - `country_name` string
# MAGIC - `unit` string
# MAGIC - `period_quantity` decimal(20,3)
# MAGIC - `period_value_usd` decimal(20,3)
# MAGIC - `cumulative_quantity` decimal(20,3)
# MAGIC - `cumulative_value_usd` decimal(20,3)
# MAGIC - `parsed_timestamp` timestamp
# MAGIC - `product_category` string
# MAGIC 
# MAGIC ### `market_data.customs.fdi_trade_statistics`
# MAGIC 
# MAGIC - `sub_category` string
# MAGIC - `document_id` string
# MAGIC - `report_period` string
# MAGIC - `report_month` string
# MAGIC - `report_start_date` date
# MAGIC - `report_end_date` date
# MAGIC - `row_number` int
# MAGIC - `product_category` string
# MAGIC - `parent_category` string
# MAGIC - `unit` string
# MAGIC - `period_quantity` decimal(20,3)
# MAGIC - `period_value_usd` decimal(20,3)
# MAGIC - `cumulative_quantity` decimal(20,3)
# MAGIC - `cumulative_value_usd` decimal(20,3)
# MAGIC - `parsed_timestamp` timestamp
# MAGIC 
# MAGIC ### `market_data.customs.provinces_trade_statistics`
# MAGIC 
# MAGIC - `sub_category` string
# MAGIC - `document_id` string
# MAGIC - `report_period` string
# MAGIC - `report_month` string
# MAGIC - `report_start_date` date
# MAGIC - `report_end_date` date
# MAGIC - `row_number` int
# MAGIC - `product_category` string
# MAGIC - `parent_category` string
# MAGIC - `unit` string
# MAGIC - `period_quantity` decimal(20,3)
# MAGIC - `period_value_usd` decimal(20,3)
# MAGIC - `cumulative_quantity` decimal(20,3)
# MAGIC - `cumulative_value_usd` decimal(20,3)
# MAGIC - `parsed_timestamp` timestamp
# MAGIC 
# MAGIC Note: despite the table name, no `province_name` column was visible in metadata at the time of review. Re-check before implementation.
# MAGIC 
# MAGIC ### `market_data.customs.transportation_trade_statistics`
# MAGIC 
# MAGIC - `sub_category` string
# MAGIC - `document_id` string
# MAGIC - `report_period` string
# MAGIC - `report_quarter` string
# MAGIC - `report_start_date` date
# MAGIC - `report_end_date` date
# MAGIC - `row_number` int
# MAGIC - `product_category` string
# MAGIC - `parent_category` string
# MAGIC - `province_name` string
# MAGIC - `quantity` decimal(20,3)
# MAGIC - `value_usd` decimal(20,3)
# MAGIC - `created_at` timestamp
# MAGIC - `vehicle_type` string
# MAGIC 
# MAGIC ## Implementation Notes
# MAGIC 
# MAGIC - The implementation should avoid mutating Stage 1-3 notebooks unless explicitly requested.
# MAGIC - Create a new Stage 4 notebook/job for curated outputs.
# MAGIC - Use Delta tables in `market_data.customs`.
# MAGIC - Keep raw columns for lineage and add cleaned/conformed columns for BI.
# MAGIC - Add quality flags and reconciliation flags.
# MAGIC - For dashboard users, clearly document that only the total table/view is canonical for official totals.
# MAGIC - Dimension-specific views are analytical slices and may include `Other / Unallocated` rows to tie back to totals.
# MAGIC 
# MAGIC ## Open Questions for Implementer
# MAGIC 
# MAGIC 1. Confirm whether `provinces_trade_statistics` should have a province dimension; metadata did not show `province_name`.
# MAGIC 2. Decide final names for curated tables/views.
# MAGIC 3. Decide whether reconciliation should be generated for quantity, value, cumulative quantity, and cumulative value, or value-only at first.
# MAGIC 4. Decide threshold for fuzzy-match auto-acceptance vs manual review.
# MAGIC 5. Decide whether to build manual review tables for product and country mappings before publishing BI views.
