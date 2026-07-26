# Stage 4: Curated Trade Statistics — Metadata

**Catalog / schema:** `market_data.customs`
**Last verified:** 2026-07-26

BI-ready tables built from the five Stage 1–3 extraction pipelines, plus shared
dimensions. Stage 1–3 tables are never modified here.

---

## Grains — read this before writing a query

The five fact tables are **different grains and do not sum together**. Two of them
have no product dimension at all, which is the most common mistake:

| Table | Grain | Metrics | Period |
|---|---|---|---|
| `curated_trade_statistics_total` | product × month | qty + USD, period + cumulative | `report_month` |
| `curated_trade_statistics_country` | product × country × month | qty + USD, period + cumulative | `report_month` |
| `curated_trade_statistics_fdi` | product × month (FDI enterprises) | qty + USD, period + cumulative | `report_month` |
| `curated_trade_statistics_province` | **province × flow × month** | **USD only**, period + cumulative | `report_month` |
| `curated_trade_statistics_transportation` | product × **transport mode** × quarter | qty + USD, period + cumulative | `report_quarter` |

- **Province** reports carry no product breakdown and no quantities. `product_*`,
  `unit` and `*_quantity` are null by nature, not by omission.
- **Transportation** reports carry no province dimension, despite older naming in
  this project suggesting otherwise. It is **quarterly**.

Use `curated_trade_statistics_total` for official headline import/export values.

## Dimensions

| Table | Rows | Notes |
|---|---|---|
| `dim_province` | 74 | Maps 63 pre-2025 units + spelling aliases → **34 post-2025 units**. Join via `normalize_province()`. |
| `dim_country` | 112 | English names + ISO2/ISO3 via `ai_query`. |
| `dim_product_category` | 439 | English commodity labels via `ai_query`. |

### `dim_province` and the 2025 reorganisation

Resolution 202/2025/QH15 (effective 2025-07-01) merged Vietnam's 63
provinces/cities into 34. Reports before that date name the old units, after it the
new ones. **Group on `current_province_en` / `current_province_vi`** for a series
that stays continuous across the break; `province_name_vi` preserves what the
source printed.

Verified: every month rolls up to exactly 34 units, and Ho Chi Minh City's export
series is continuous across the boundary (it absorbed Bình Dương and
Bà Rịa–Vũng Tàu).

### Translation caveat

Translations come from `ai_query` and are **not fully trustworthy**. The model
returns `confidence = 1.0` on garbled input — it mapped `BÁI LOAN` to Ireland
(Taiwan) and `BỒ ĐIÊN NGA` to Russia (Portugal). Model confidence does not detect
OCR damage.

`dim_country_suspect_variants` catches these with a string-similarity rule instead:
near-identical labels resolving to different countries. Anything it finds is
flagged `needs_review = TRUE` and left for a human — a wrong auto-fix would be
worse than a flag. Currently 5 pairs, all genuine OCR damage of Taiwan, Portugal
and Sweden.

## Unified view

`curated_trade_statistics_unified` — UNION ALL of all five facts, 2018 onward.

**Do not sum across `grain_type`.** It double counts.

The 2018 cut uses `coalesce(report_month, report_quarter)`. Filtering on
`report_month` alone silently excluded the entire quarterly transportation grain —
that bug was live and invisible for some time.

## Coverage

`customs_coverage` — one row per workstream × expected period, status
`ok` / `incomplete` / `missing` / `source_unavailable`.
`customs_coverage_gaps` — only actionable gaps, excluding periods Customs has not
published yet and known-unobtainable ones.

`customs_known_source_gaps` documents periods that will never arrive, with the
reason. Currently one: **countries 2022-10 import**, published by Customs only at
`10.224.128.185:8080`, an internal RFC1918 host unreachable from outside their
network.

Current state: **zero actionable gaps** across all five workstreams.

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_specs_stage4_curated_trade_statistics` | Original design notes (historical) |
| `01_build_curated_trade_statistics` | Builds dimensions + all five fact tables + unified view |
| `03_build_dim_province` | Province dimension incl. the 63 → 34 merger mapping |
| `04_build_coverage_views` | Coverage and gap views |
| `05_translate_dimensions` | Orphan cleanup → `ai_query` translation → variant flagging |
| `06_qa_checks` | Asserts data quality; **raises** on failure |

Run order is enforced by the `etl data customs (curated)` job:
`03 → 01 → 05 → 01 → 04 → 06`. `01` runs twice on purpose — the first pass seeds
new raw values into the dimensions, `05` translates them, the second pass writes
the English labels into the facts.

`02_add_province_curated_after_permission` was **deleted**. Its premise — that the
province table was blocked by a SELECT permission — was wrong; the table was simply
empty and its schema was wrong. `01` now builds provinces directly.

## QA checks

`06_qa_checks` fails the job on: empty fact tables, actionable coverage gaps,
duplicate rows at the natural grain, null or future periods, non-canonical
transport modes, unresolvable province labels, a unified view missing any grain, or
curated row counts not matching source.

That last check exists because a duplicate dimension key once fanned out the facts
— curated `total` read 20,486 against a source of 20,296, and every run was green.
Nothing looked wrong; only the count comparison caught it.

## Gotchas that cost real time

- **Preliminary vs official.** Customs publishes `(VN-SB)` preliminary then
  `(VN-CT)` official for the same month, at different URLs — so a different
  `document_id`. Extractors must clear the month before inserting or both versions
  coexist and everything double counts. The preliminary files are also far worse
  OCR: 695 distinct product labels vs 254, 198 countries vs 89. **Prefer CT.**
- **Dimension MERGEs only insert.** Superseded garbage labels linger and then get
  translated into confident nonsense. `05` drops orphans first.
- **Case-variant labels.** `Ổ tộ` and `ổ tộ` collapse to one normalized key. Pick
  one row per key or the dimension gains duplicates and fans out every fact.
