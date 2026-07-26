-- Databricks notebook source
-- Databricks notebook source
%md
# Stage 4 Curated Trade Statistics Spec

## Context

Build a Stage 4 curated layer for Vietnam Customs trade statistics in Databricks.

Catalog/schema:

- `market_data.customs`

Source tables to include:

- `trade_statistics` — main total table
- `countries_trade_statistics` — country dimension
- `fdi_trade_statistics` — FDI dimension
- `provinces_trade_statistics` — province dimension
- `transportation_trade_statistics` — transportation/vehicle dimension

Explicitly ignore:

- `countries_trade_statistics_v2_comparison`

## Design Goal

Create BI/dashboard-friendly curated tables that preserve every useful dimension from the source tables, while making the data easier to query consistently.

The source tables do **not** all represent the same grain:

- `trade_statistics` has the main total amount.
- Child/dimensional tables represent slices by country, FDI, province, or transportation.
- Child tables may miss components and will not always sum to the total.

Therefore, do **not** force all child tables into a single additive fact table where every row is assumed to sum cleanly to the official total.

## Chosen Approach

Use **Option B**:

Create a curated Stage 4 layer with separate but standardized fact tables/views by analytical grain, plus shared dimensions/mapping tables.

Recommended curated outputs:

1. `curated_trade_statistics_total`
   - Grain: official product/category total by period and trade flow.
   - Source: `trade_statistics`.
   - This is the canonical source for total import/export values.

2. `curated_trade_statistics_country`
   - Grain: product/category x country x period x trade flow.
   - Source: `countries_trade_statistics`.

3. `curated_trade_statistics_fdi`
   - Grain: product/category x FDI/non-FDI style slice x period x trade flow.
   - Source: `fdi_trade_statistics`.

4. `curated_trade_statistics_province`
   - Grain: product/category x province x period x trade flow.
   - Source: `provinces_trade_statistics`.

5. `curated_trade_statistics_transportation`
   - Grain: product/category x province x vehicle type x quarter x trade flow.
   - Source: `transportation_trade_statistics`.

6. Optional BI convenience view: `curated_trade_statistics_unified`
   - A unioned, long-format view for dashboards.
   - Must include a column like `source_dimension` / `grain_type` so BI users know whether a row is total, country, FDI, province, or transportation.
   - Values in this view should not be blindly summed across `grain_type`.

## Dimensional Reconciliation Rule

If a child dimension does not sum to the total, add a synthetic `Other / Unallocated` row for the gap.

Example:

- Total from `trade_statistics` = 100
- Sum of known countries = 92
- Add country row: `country_name = 'Other / Unallocated'`, value = 8

Apply this reconciliation separately per appropriate grain:

- period
- trade flow
- product/category level
- unit where applicable
- metric type: period quantity/value and cumulative quantity/value

Important:

- Reconciliation rows are analytical helpers, not raw facts.
- Add flags:
  - `is_reconciliation_row` boolean
  - `reconciliation_basis` string, e.g. `total_minus_known_country_sum`
  - `source_table` string

## Trade Flow Cleaning

Create a clean `trade_flow` column across all curated outputs.

Map current `sub_category` values into normalized trade flow:

- `Export Goods` -> `export`
- `Import Goods` -> `import`
- `Export by Destination` -> `export`
- `Import by Origin` -> `import`
- `Export by Transportation` -> `export`
- `Import by Transportation` -> `import`

Keep original values too:

- `sub_category_raw`

Do not rely on `sub_category` as the final dashboard field.

## Category Meaning

Current observed meaning:

- `sub_category` is the trade-flow/report-section label, not a true product dimension.
- `parent_category` is mostly null in the current tables observed, but should be retained because it may become useful if future extraction identifies product hierarchy.
- `product_category` is the current main product/category dimension.

Curated fields should include:

- `trade_flow`
- `sub_category_raw`
- `product_category_raw`
- `product_category_en`
- `parent_category_raw`
- `parent_category_en`

## Product Name Translation / Cleaning

Need a shared product mapping process across all tables.

Chosen approach:

- Stage 4 curated cleaning, not bronze/extraction.
- Build and maintain a reusable product dimension/mapping table.
- Apply the same mapping across all curated outputs.

Recommended table:

`market_data.customs.dim_product_category`

Suggested columns:

- `product_category_raw`
- `product_category_normalized`
- `product_category_en`
- `parent_category_raw`
- `parent_category_en`
- `mapping_method` — e.g. `manual`, `dictionary`, `fuzzy`, `llm_reviewed`
- `confidence_score`
- `needs_review`
- `created_at`
- `updated_at`

Recommended method:

1. Collect distinct product/category names from all source tables.
2. Normalize text for matching:
   - trim
   - lowercase
   - normalize Unicode
   - remove/standardize diacritics only for matching key, not as final raw value
   - clean obvious mojibake/encoding artifacts where possible
3. Use dictionary/manual mappings for known Vietnamese customs categories first.
4. Use fuzzy matching only to cluster likely duplicates.
5. Do not auto-merge low-confidence fuzzy matches without review.
6. Use LLM translation as an assistant for English labels, but store confidence/review status.

Important:

- Preserve raw Vietnamese/corrupted names for lineage.
- Use English names for BI friendliness.
- Do not overwrite bronze/raw extraction output.

## Country Name Translation / Cleaning

Do the same for country names.

Recommended table:

`market_data.customs.dim_country`

Suggested columns:

- `country_name_raw`
- `country_name_normalized`
- `country_name_en`
- `iso2`
- `iso3`
- `mapping_method`
- `confidence_score`
- `needs_review`
- `created_at`
- `updated_at`

Recommended method:

1. Collect distinct `country_name` from `countries_trade_statistics`.
2. Normalize text for matching.
3. Map to standard English country names and ISO codes.
4. Use deterministic country reference data where possible.
5. Use fuzzy matching for variants/misspellings/encoding issues, but require review for ambiguous matches.
6. Preserve `country_name_raw` in curated country-level output.

## Why Cleaning Belongs in Stage 4, Not Bronze

Extraction notebooks currently sit in the Stage 3 extraction layer. Their job is to extract structured rows from parsed documents into source tables such as:

- `trade_statistics`
- `countries_trade_statistics`
- `fdi_trade_statistics`
- `provinces_trade_statistics`
- `transportation_trade_statistics`

Do not put English translation, fuzzy matching, or BI semantic reconciliation into the extraction notebooks.

Reason:

- Bronze/raw/extraction should preserve source truth and lineage.
- Translation and fuzzy matching are opinionated transformations.
- Reconciliation rows are derived analytical constructs.
- BI-friendly names and dimensional conformance belong in curated/silver-gold style processing.

So the selected design is:

- Stage 1: crawl/download documents
- Stage 2: parse documents
- Stage 3: extract tables/statistics into raw structured Delta tables
- Stage 4: curated/conformed BI layer with cleaning, translation, dimensions, and reconciliation

## Source Table Columns Observed

### `market_data.customs.trade_statistics`

- `sub_category` string
- `document_id` string
- `report_period` string
- `report_month` string
- `report_start_date` date
- `report_end_date` date
- `row_number` int
- `product_category` string
- `parent_category` string
- `unit` string
- `period_quantity` decimal(20,3)
- `period_value_usd` decimal(20,3)
- `cumulative_quantity` decimal(20,3)
- `cumulative_value_usd` decimal(20,3)
- `parsed_timestamp` timestamp

### `market_data.customs.countries_trade_statistics`

- `sub_category` string
- `document_id` string
- `report_period` string
- `report_month` string
- `report_start_date` date
- `report_end_date` date
- `row_number` int
- `country_name` string
- `unit` string
- `period_quantity` decimal(20,3)
- `period_value_usd` decimal(20,3)
- `cumulative_quantity` decimal(20,3)
- `cumulative_value_usd` decimal(20,3)
- `parsed_timestamp` timestamp
- `product_category` string

### `market_data.customs.fdi_trade_statistics`

- `sub_category` string
- `document_id` string
- `report_period` string
- `report_month` string
- `report_start_date` date
- `report_end_date` date
- `row_number` int
- `product_category` string
- `parent_category` string
- `unit` string
- `period_quantity` decimal(20,3)
- `period_value_usd` decimal(20,3)
- `cumulative_quantity` decimal(20,3)
- `cumulative_value_usd` decimal(20,3)
- `parsed_timestamp` timestamp

### `market_data.customs.provinces_trade_statistics`

- `sub_category` string
- `document_id` string
- `report_period` string
- `report_month` string
- `report_start_date` date
- `report_end_date` date
- `row_number` int
- `product_category` string
- `parent_category` string
- `unit` string
- `period_quantity` decimal(20,3)
- `period_value_usd` decimal(20,3)
- `cumulative_quantity` decimal(20,3)
- `cumulative_value_usd` decimal(20,3)
- `parsed_timestamp` timestamp

Note: despite the table name, no `province_name` column was visible in metadata at the time of review. Re-check before implementation.

### `market_data.customs.transportation_trade_statistics`

- `sub_category` string
- `document_id` string
- `report_period` string
- `report_quarter` string
- `report_start_date` date
- `report_end_date` date
- `row_number` int
- `product_category` string
- `parent_category` string
- `province_name` string
- `quantity` decimal(20,3)
- `value_usd` decimal(20,3)
- `created_at` timestamp
- `vehicle_type` string

## Implementation Notes

- The implementation should avoid mutating Stage 1-3 notebooks unless explicitly requested.
- Create a new Stage 4 notebook/job for curated outputs.
- Use Delta tables in `market_data.customs`.
- Keep raw columns for lineage and add cleaned/conformed columns for BI.
- Add quality flags and reconciliation flags.
- For dashboard users, clearly document that only the total table/view is canonical for official totals.
- Dimension-specific views are analytical slices and may include `Other / Unallocated` rows to tie back to totals.

## Open Questions for Implementer

1. Confirm whether `provinces_trade_statistics` should have a province dimension; metadata did not show `province_name`.
2. Decide final names for curated tables/views.
3. Decide whether reconciliation should be generated for quantity, value, cumulative quantity, and cumulative value, or value-only at first.
4. Decide threshold for fuzzy-match auto-acceptance vs manual review.
5. Decide whether to build manual review tables for product and country mappings before publishing BI views.