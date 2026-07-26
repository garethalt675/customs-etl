# Stage 4: Curated Trade Statistics - Metadata

## Overview

**Purpose**: Transform Vietnam Customs raw trade statistics from Stage 3 extraction into BI/dashboard-ready curated tables.

**Catalog**: `market_data`  
**Schema**: `customs`  
**Architecture**: Medallion-style with separate grain-specific fact tables plus shared dimension tables

---

## Pipeline Design Philosophy

### Core Principles

1. **Preserve Source Truth**: Stage 1-3 extraction tables remain unchanged; all transformations happen in Stage 4
2. **Multiple Grains**: Different analytical perspectives are kept separate (total, country, FDI, province, transportation)
3. **Dimensional Conformance**: Shared dimension tables (`dim_product_category`, `dim_country`) provide standardized mappings
4. **Reconciliation**: Where dimensional slices don't sum to totals, synthetic "Other / Unallocated" rows bridge the gap
5. **BI-Friendly**: Normalized trade flow, English labels, and unified views for dashboard consumption

### Why Not a Single Fact Table?

Source tables represent different analytical grains and **do not sum cleanly**:
- `trade_statistics` contains official totals
- Child tables (`countries_trade_statistics`, `fdi_trade_statistics`, etc.) are dimensional slices that may miss components
- Forcing all into one additive fact would create false precision

---

## Source Tables (Stage 3)

All from `market_data.customs`:

| Table | Grain | Key Dimensions | Metrics |
|-------|-------|----------------|---------|
| `trade_statistics` | Official total by product/period | product_category, parent_category, report_period | period_quantity, period_value_usd, cumulative_quantity, cumulative_value_usd |
| `countries_trade_statistics` | Country breakdown | country_name, product_category, report_period | Same as above |
| `fdi_trade_statistics` | FDI enterprise breakdown | product_category, parent_category, report_period | Same as above |
| `provinces_trade_statistics` | Province breakdown | product_category, parent_category, report_period | Same as above (⚠️ no province column visible) |
| `transportation_trade_statistics` | Transportation mode | vehicle_type, province_name, product_category, report_quarter | quantity, value_usd (no cumulative) |

**Ignored**: `countries_trade_statistics_v2_comparison` (not production-ready)

---

## Output Tables & Views

### Dimension Tables

#### `dim_product_category`
**Purpose**: Standardized product/category mappings with English translations

| Column | Type | Description |
|--------|------|-------------|
| `product_category_raw` | STRING | Original Vietnamese/raw product name from source |
| `product_category_normalized` | STRING | Lowercased, trimmed for matching |
| `product_category_en` | STRING | English translation for BI |
| `parent_category_raw` | STRING | Parent category (often null) |
| `parent_category_normalized` | STRING | Normalized parent |
| `parent_category_en` | STRING | English parent category |
| `mapping_method` | STRING | `manual`, `dictionary`, `fuzzy`, `llm_reviewed` |
| `confidence_score` | DOUBLE | 0.0-1.0 confidence in mapping |
| `needs_review` | BOOLEAN | Flag for manual review |
| `created_at` | TIMESTAMP | First inserted |
| `updated_at` | TIMESTAMP | Last modified |

**Maintenance**: MERGE logic inserts new raw values with `needs_review=TRUE`; manual curation updates English names without overwriting

#### `dim_country`
**Purpose**: Country name standardization with ISO codes

| Column | Type | Description |
|--------|------|-------------|
| `country_name_raw` | STRING | Original country name from source |
| `country_name_normalized` | STRING | Normalized for matching |
| `country_name_en` | STRING | Standard English country name |
| `iso2` | STRING | ISO 3166-1 alpha-2 code |
| `iso3` | STRING | ISO 3166-1 alpha-3 code |
| `mapping_method` | STRING | Mapping approach |
| `confidence_score` | DOUBLE | Match confidence |
| `needs_review` | BOOLEAN | Manual review flag |
| `created_at` | TIMESTAMP | First inserted |
| `updated_at` | TIMESTAMP | Last modified |

---

### Fact Tables

All fact tables share a **common schema** with grain-specific populated fields:

| Column | Type | Description |
|--------|------|-------------|
| `grain_type` | STRING | `total`, `country`, `fdi`, `province`, `transportation` |
| `source_table` | STRING | Source table name from Stage 3 |
| `document_id` | STRING | Source document identifier |
| `report_period` | STRING | Reporting period label |
| `report_month` | STRING | Month (monthly reports) |
| `report_quarter` | STRING | Quarter (transportation only) |
| `report_start_date` | DATE | Period start |
| `report_end_date` | DATE | Period end |
| `trade_flow` | STRING | **Normalized**: `export` or `import` |
| `sub_category_raw` | STRING | Original sub_category value |
| `product_category_raw` | STRING | Raw product/category name |
| `product_category_en` | STRING | English product name (from dim) |
| `parent_category_raw` | STRING | Raw parent category |
| `parent_category_en` | STRING | English parent (from dim) |
| `country_name_raw` | STRING | Raw country name (country grain only) |
| `country_name_en` | STRING | English country name (from dim) |
| `iso2` | STRING | ISO 2-letter code |
| `iso3` | STRING | ISO 3-letter code |
| `province_name_raw` | STRING | Raw province name |
| `province_name_en` | STRING | English province name |
| `vehicle_type` | STRING | Transportation mode (transportation only) |
| `ownership_scope` | STRING | `all_enterprises` or `fdi` |
| `unit` | STRING | Quantity unit |
| `period_quantity` | DECIMAL(20,3) | Quantity for the period |
| `period_value_usd` | DECIMAL(20,3) | Value in USD for the period |
| `cumulative_quantity` | DECIMAL(20,3) | Year-to-date quantity |
| `cumulative_value_usd` | DECIMAL(20,3) | Year-to-date value USD |
| `is_reconciliation_row` | BOOLEAN | TRUE for synthetic "Other/Unallocated" rows |
| `reconciliation_basis` | STRING | Description of reconciliation logic |
| `source_timestamp` | TIMESTAMP | Original parsed_timestamp from source |
| `curated_at` | TIMESTAMP | When curated row was created |

#### `curated_trade_statistics_total`
**Grain**: Official total by product/period  
**Source**: `trade_statistics`  
**Purpose**: Canonical source for total import/export values  
**Key Fields**: `product_category_raw`, `product_category_en`, `trade_flow`, metrics

#### `curated_trade_statistics_country`
**Grain**: Product × Country × Period  
**Source**: `countries_trade_statistics`  
**Reconciliation**: Adds `country_name_raw = 'Other / Unallocated'` rows when country sums don't match totals  
**Key Fields**: `country_name_raw`, `country_name_en`, `iso2`, `iso3`, plus all product/period fields

#### `curated_trade_statistics_fdi`
**Grain**: Product × FDI/Non-FDI × Period  
**Source**: `fdi_trade_statistics`  
**Reconciliation**: Adds `ownership_scope = 'Other / Non-FDI / Unallocated'` rows for gaps  
**Key Fields**: `ownership_scope`, plus product/period fields

#### `curated_trade_statistics_province`
**Grain**: Product × Province × Period  
**Source**: `provinces_trade_statistics`  
**Status**: ⚠️ **EMPTY** - awaiting SELECT permission fix  
**Note**: Source table lacks visible `province_name` column; placeholder created

#### `curated_trade_statistics_transportation`
**Grain**: Product × Province × Vehicle Type × Quarter  
**Source**: `transportation_trade_statistics`  
**Key Fields**: `vehicle_type`, `province_name_raw`, quarterly reporting (no cumulative metrics)

---

### Unified View

#### `curated_trade_statistics_unified`
**Type**: VIEW (UNION ALL of all fact tables)  
**Purpose**: Long-format convenience view for BI dashboards  
**Warning**: ⚠️ **Do NOT sum across `grain_type`** - use `curated_trade_statistics_total` for official totals  
**Row Count**: 390,714 rows (as of last run)

---

## Key Transformations

### 1. Trade Flow Normalization

**Problem**: `sub_category` contains mixed trade flow and report section labels

**Solution**: Map to clean `trade_flow` column:

```sql
CASE
  WHEN sub_category IN ('Export Goods', 'Export by Destination', 'Export by Transportation') THEN 'export'
  WHEN sub_category IN ('Import Goods', 'Import by Origin', 'Import by Transportation') THEN 'import'
  ELSE lower(trim(sub_category))
END AS trade_flow
```

**Preserved**: `sub_category_raw` retains original value

### 2. Reconciliation Logic

**When**: Dimensional slices don't sum to official totals  
**Action**: Add synthetic rows to bridge the gap

**Example** (Country):
```
Total from trade_statistics = $100M
Sum of known countries = $92M
→ Add row: country_name_raw = 'Other / Unallocated', value = $8M
```

**Flags**:
- `is_reconciliation_row = TRUE`
- `reconciliation_basis = 'total_minus_known_country_sum'`

**Applied Per**:
- Report period
- Trade flow
- Product/category
- Unit
- Each metric type (period quantity/value, cumulative quantity/value)

### 3. Dimension Mapping (JOIN to shared dims)

All fact tables LEFT JOIN to dimension tables on normalized keys:

```sql
LEFT JOIN dim_product_category p
  ON lower(trim(s.product_category)) <=> p.product_category_normalized
 AND lower(trim(s.parent_category)) <=> p.parent_category_normalized
```

**Benefit**: English labels populated from single source of truth

---

## Implementation Notebooks

### 1. `00_specs_stage4_curated_trade_statistics`
**Type**: Documentation  
**Purpose**: Design specification and architectural decisions  
**Run**: No execution - reference only

### 2. `01_build_curated_trade_statistics`
**Type**: ETL Implementation  
**Language**: SQL  
**Purpose**: Main curated layer builder

**Execution Order**:
1. USE `market_data.customs`
2. Create/update `dim_product_category` (MERGE)
3. Create/update `dim_country` (MERGE)
4. CREATE OR REPLACE `curated_trade_statistics_total`
5. CREATE OR REPLACE `curated_trade_statistics_country` (with reconciliation)
6. CREATE OR REPLACE `curated_trade_statistics_fdi` (with reconciliation)
7. CREATE OR REPLACE `curated_trade_statistics_province` (placeholder - empty)
8. CREATE OR REPLACE `curated_trade_statistics_transportation`
9. CREATE OR REPLACE VIEW `curated_trade_statistics_unified`
10. CREATE OR REPLACE VIEW `curated_trade_statistics_row_counts` (validation)

**Current Row Counts** (last run):
- `curated_trade_statistics_total`: 15,434
- `curated_trade_statistics_country`: 340,661
- `curated_trade_statistics_fdi`: 24,884
- `curated_trade_statistics_province`: 0 ⚠️
- `curated_trade_statistics_transportation`: 9,735
- `curated_trade_statistics_unified`: 390,714

### 3. `02_add_province_curated_after_permission`
**Type**: Deferred Implementation  
**Purpose**: Populate province fact table after SELECT permission granted  
**Status**: Ready to run once `market_data.customs.provinces_trade_statistics` access is fixed

**Includes**:
- Province fact table population
- Rebuild unified view with province data
- Row count validation

---

## Data Quality & Validation

### Implemented Checks

1. **Row Count Validation**: `curated_trade_statistics_row_counts` view tracks all table sizes
2. **Reconciliation Flags**: `is_reconciliation_row` identifies synthetic rows
3. **Mapping Confidence**: `needs_review` flag in dimension tables marks uncertain mappings
4. **Schema Consistency**: All fact tables share unified schema for easier querying

### Known Limitations

1. **Province Data Missing**: 
   - Source table lacks `province_name` column in metadata
   - Permission issues prevent current user/service principal from SELECT
   - Placeholder table created but empty

2. **Manual Translation Required**:
   - `dim_product_category` and `dim_country` require manual review/curation for English names
   - Initial MERGE populates with `needs_review=TRUE` and null English fields

3. **Reconciliation Not Yet Implemented for All Metrics**:
   - Current reconciliation focuses on value metrics
   - Quantity reconciliation logic present but needs validation

---

## Business Rules

### Canonical Total Source
**Always use `curated_trade_statistics_total` for official import/export totals.**

Other fact tables are dimensional slices:
- May not include all components
- May contain reconciliation rows
- Should not be summed without understanding grain

### Trade Flow Interpretation
- `trade_flow = 'export'` → goods leaving Vietnam
- `trade_flow = 'import'` → goods entering Vietnam
- Original `sub_category_raw` preserved for audit

### Reconciliation Rows
- Are **analytical helpers**, not raw facts
- Should be filterable: `WHERE is_reconciliation_row = FALSE`
- Help analysts understand coverage gaps

---

## Usage Guidelines for Analysts

### Querying Total Trade Values
```sql
-- ✅ Correct: Use total fact table
SELECT 
  trade_flow,
  product_category_en,
  SUM(period_value_usd) AS total_value
FROM market_data.customs.curated_trade_statistics_total
WHERE report_period = '2024-Q4'
GROUP BY trade_flow, product_category_en;
```

### Analyzing Country Breakdown
```sql
-- ✅ Correct: Use country fact table, optionally filter reconciliation
SELECT 
  country_name_en,
  product_category_en,
  SUM(period_value_usd) AS country_value
FROM market_data.customs.curated_trade_statistics_country
WHERE report_period = '2024-Q4'
  AND trade_flow = 'export'
  AND is_reconciliation_row = FALSE  -- exclude synthetic rows
GROUP BY country_name_en, product_category_en;
```

### ❌ Incorrect: Summing Across Grains
```sql
-- ❌ WRONG: Will double-count totals
SELECT 
  SUM(period_value_usd) 
FROM market_data.customs.curated_trade_statistics_unified
WHERE report_period = '2024-Q4';
-- This sums total + country + fdi + province + transportation = inflated result
```

---

## Maintenance Tasks

### Weekly
- Monitor dimension table growth (`dim_product_category`, `dim_country`)
- Review `needs_review = TRUE` rows in dimension tables

### Monthly
- Manually curate English translations for high-volume product categories
- Validate reconciliation row counts (should be < 10% of total)

### Ad-hoc
- Rerun `01_build_curated_trade_statistics` after Stage 3 data refreshes
- Update `02_add_province_curated_after_permission` once permissions fixed

---

## Future Enhancements

1. **Province Data**: Complete once source table and permissions resolved
2. **LLM-Assisted Translation**: Automate English label generation with confidence scoring
3. **Fuzzy Matching**: Implement cluster-based deduplication for product/country names
4. **Data Quality Metrics**: Add completeness, accuracy, and freshness tracking
5. **Incremental Processing**: Move from full table replace to MERGE for large fact tables
6. **Granular Reconciliation**: Implement quantity + cumulative metric reconciliation

---

## Contact & Ownership

**Data Engineer**: [Your Name]  
**Last Updated**: 2024-12-17  
**Version**: 1.0  
**Catalog**: `market_data.customs`
