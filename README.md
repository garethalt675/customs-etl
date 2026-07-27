# Vietnam Customs Trade Statistics ETL

Pipelines that turn Vietnam Customs' published PDF reports into queryable trade
statistics in Databricks (`market_data.customs`).

Everything runs in Databricks; this repo is the source of truth for the notebooks
and job definitions so they can be edited from any machine and synced back.

---

## Workstreams

Five independent pipelines, each `URL discovery -> download -> AI parse -> extract`,
feeding a shared curated layer.

| Workstream | Grain | Cadence | Fact table |
|---|---|---|---|
| **Monthly totals** | product x month | monthly | `trade_statistics` |
| **By country** | product x country x month | monthly | `countries_trade_statistics` |
| **By FDI** | product x FDI/non-FDI x month | monthly | `fdi_trade_statistics` |
| **By province** | province x flow x month (USD only) | monthly | `provinces_trade_statistics` |
| **By transport mode** | product x transport mode x quarter | quarterly | `transportation_trade_statistics` |

Two of these do **not** have a product dimension or quantities, which is easy to
get wrong:

- **Provinces** is province x flow x month, valued in USD only. One PDF holds both
  trade flows side by side, so each document is registered, downloaded and parsed
  **once**, and extraction emits two rows per province.
- **Transport mode** has no province dimension at all, despite historical naming
  in this project suggesting otherwise.

### Pipeline stages

| Notebook | Does |
|---|---|
| `0_Setup_*_Workflow_Prerequisites` | Create volumes and tables. Idempotent, run once. |
| `0_Update_*_URL_Table` | Discover report URLs from the Customs API by title pattern. Defaults to `dry_run=true`; jobs pass `dry_run=false`. |
| `1_Download_*_Documents` | Fetch PDFs into a Unity Catalog volume. |
| `2_Parse_*_Documents` | `ai_parse_document()` into raw JSON. |
| `3_Extract_*_Statistics` | Parse tables into the typed fact table. |

Progress is tracked per document in `*_document_processing_log`
(`download_status` / `parse_status` / `extraction_status`), so every stage is
resumable and skips completed work.

### Curated layer (`4_Curated/`)

Builds BI-facing tables plus shared dimensions. `03_build_dim_province` maps every
province spelling to a **stable post-2025 geography** — Resolution 202/2025/QH15
reorganised Vietnam from 63 provinces into 34 on 2025-07-01, so without it any
provincial time series breaks in mid-2025.

---

## Working on this repo

### Requirements

```bash
pip install -r scripts/requirements.txt
```

Authentication uses the `DEFAULT` profile in `~/.databrickscfg`, or the
`DATABRICKS_HOST` / `DATABRICKS_TOKEN` environment variables. **No credentials are
stored in this repo** — see `.gitignore`.

### Scheduled runs come from this repo

Every job task uses `source: GIT` against `main` of this repository. Databricks
checks the repo out fresh on each run, so **what is on `main` is what runs** —
editing a notebook in the Databricks UI no longer changes a scheduled run.

To ship a change: commit and push to `main`. That is the whole deployment.

```bash
python scripts/jobs_use_git.py --dry-run   # show which tasks would be repointed
python scripts/jobs_use_git.py             # apply (idempotent)
```

Checkout of this private repo needs a GitHub credential in the workspace under
**Settings → Linked accounts**. Without it every task fails at checkout.

### Editing in the Databricks UI

There is a **Git folder** at

```
/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/customs-etl
```

It is a real clone of this repo on `main`, with Pull and Commit & Push buttons in
the Databricks UI. Edit a notebook there, commit, push — then the next scheduled
run picks it up, because runs read `main`.

Pull before you start. The Git folder does not update itself, and it is the one
place where a stale checkout can quietly cost you an afternoon.

The old plain folder `1. Data ETL/1. Customs` was **deleted on 2026-07-27**. It
was a copy that no longer ran anything, which made it easy to edit the wrong
file. `scripts/databricks_sync.py` existed to keep it in step and is now retired
— do not point it at the Git folder, it writes through the workspace API and
would fight the checkout.

### Job definitions

```bash
python scripts/export_jobs.py     # refresh jobs/*.json from the workspace
```

`jobs/*.json` records each workflow DAG. They are exported for reference and
disaster recovery; `databricks_sync.py` does not apply them.

### Dashboard

```bash
python dashboards/build_dashboard.py            # regenerate the JSON
python dashboards/build_dashboard.py --deploy   # regenerate and publish
```

`dashboards/queries.py` is the source of truth for every figure on the
"Vietnam Import-Export Statistics" dashboard. Editing the dashboard in the
Databricks UI works, but the next `--deploy` overwrites those edits.

---

## Conventions worth keeping

These were each learned from a production failure. Ignoring them reintroduces a
real bug.

- **Never `DELETE ... WHERE TRUE` in an extractor.** A leftover "reset" cell in the
  transportation extractor wiped its fact table on every job run while reporting
  success. Extractors `MERGE` on the natural grain, so re-extraction overwrites in
  place and a wipe is never needed.
- **No UTF-8 BOM in notebook sources.** A leading BOM fails a notebook as a job
  task with `SyntaxError: invalid non-printable character U+FEFF`, even though it
  runs fine interactively. The sync script strips BOMs in both directions.
- **Guard `read_files()` against missing and empty paths.** It errors on a missing
  directory and cannot infer a schema from an empty one — both normal when nothing
  new was downloaded. Download stages `mkdirs` their target first.
- **Clamp decimals before `createDataFrame`.** A mis-parsed cell exceeding
  `DECIMAL(20,3)` raises `decimal.InvalidOperation` and kills the whole run. Every
  extractor routes values through `fit_decimal()`.
- **List MERGE columns explicitly when the source is partial.** `UPDATE SET *`
  needs the source to carry every target column; a download-stage source does not,
  and using it would also clear parse/extraction status.
- **Audit title patterns per year.** Customs changes report-title wording between
  years (`quý 3/2018` vs `quý 3 năm 2018`), which silently hides whole years from
  URL discovery. Dump API titles and count matched vs unmatched by year.
- **Normalize Vietnamese text for joins.** Source PDFs are OCR'd and carry accent
  and case damage (`Đường`/`Dường`/`Dương`, `Đăk Lăk`/`Đắk Lắk`). Note that NFD does
  **not** decompose `Đ`/`đ` — replace it explicitly.
- **`trade_flow` is stored lower case** (`export` / `import`). Filtering for
  `'Export'` matches nothing and returns a silently empty result rather than an
  error — this emptied every KPI on the first version of the dashboard.
- **Take headline totals from the `total` grain only.** The country and FDI
  breakdowns sum 3–4% above the national total, because Customs publishes a
  partial breakdown plus a residual and that residual is floored at zero instead
  of going negative. Rankings are sound; the sums are not.

## Status

All five workstreams are live and current, the curated layer is built and QA
gated, and the orchestrator runs unattended on the **15th of each month at 06:00
Asia/Ho_Chi_Minh**, executing this repo's `main` branch.

Transport-mode reports are **quarterly** and Customs has published nothing for
2026 — a transport series ending at 2025-Q4 is expected, not a failure.
