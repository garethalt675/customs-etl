"""Build and deploy the 'Vietnam Import-Export Statistics' Lakeview dashboard.

    python dashboards/build_dashboard.py            # write JSON only
    python dashboards/build_dashboard.py --deploy   # write JSON and push to Databricks

The dashboard is generated rather than hand-edited so the SQL in queries.py stays
the single source of truth. Editing the dashboard in the Databricks UI works, but
the next run of this script overwrites those edits - change queries.py instead.
"""

import argparse
import json
import os

from queries import DATASETS

DASHBOARD_ID = "01f1894a22d21cbeb1df374f79459b95"
DISPLAY_NAME = "Vietnam Import-Export Statistics"
WAREHOUSE_ID = "7eb5fd2336243915"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vietnam_trade.lvdash.json")

EXPORT_COLOR = "#00A972"
IMPORT_COLOR = "#FF8C00"
FLOW_SCALE = {
    "type": "categorical",
    "mappings": [
        {"value": "Export", "color": EXPORT_COLOR},
        {"value": "Import", "color": IMPORT_COLOR},
    ],
}

USD_BN = "US$ billion"


# --------------------------------------------------------------------------
# widget helpers
# --------------------------------------------------------------------------

def _pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(name, lines, x, y, w, h):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": _pos(x, y, w, h),
    }


def _query(dataset, fields, filters=None):
    q = {"datasetName": dataset, "fields": fields, "disaggregated": False}
    if filters:
        q["filters"] = [{"expression": f} for f in filters]
    return {"name": "main_query", "query": q}


def counter(name, dataset, field, title, description=None, x=0, y=0, w=3, h=3,
            filters=None, positive_is_good=True):
    """A single big number. `field` is a plain column of a one-row dataset."""
    frame = {"showTitle": True, "title": title}
    if description:
        frame["showDescription"] = True
        frame["description"] = description
    value = {"fieldName": field}
    if positive_is_good is not None:
        value["style"] = {
            "rules": [
                {"condition": {"operator": ">=", "value": 0}, "color": EXPORT_COLOR},
                {"condition": {"operator": "<", "value": 0}, "color": IMPORT_COLOR},
            ]
        }
    return {
        "widget": {
            "name": name,
            "queries": [_query(dataset, [{"name": field, "expression": f"`{field}`"}], filters)],
            "spec": {
                "version": 2,
                "frame": frame,
                "widgetType": "counter",
                "encodings": {"value": value},
                "data": {"queryName": "main_query"},
            },
        },
        "position": _pos(x, y, w, h),
    }


def label_counter(name, dataset, field, title, x, y, w, h):
    """Counter showing a text value (e.g. the latest published month)."""
    return {
        "widget": {
            "name": name,
            "queries": [_query(dataset, [{"name": field, "expression": f"`{field}`"}])],
            "spec": {
                "version": 2,
                "frame": {"showTitle": True, "title": title},
                "widgetType": "counter",
                "encodings": {"value": {"fieldName": field}},
                "data": {"queryName": "main_query"},
            },
        },
        "position": _pos(x, y, w, h),
    }


def chart(name, dataset, widget_type, fields, encodings, title, x, y, w, h,
          filters=None, description=None):
    frame = {"showTitle": True, "title": title}
    if description:
        frame["showDescription"] = True
        frame["description"] = description
    return {
        "widget": {
            "name": name,
            "queries": [_query(dataset, fields, filters)],
            "spec": {
                "version": 3,
                "widgetType": widget_type,
                "frame": frame,
                "encodings": encodings,
                "data": {"queryName": "main_query"},
            },
        },
        "position": _pos(x, y, w, h),
    }


def f_dim(alias, expr=None):
    return {"name": alias, "expression": expr or f"`{alias}`"}


def f_sum(col):
    return {"name": f"sum({col})", "expression": f"SUM(`{col}`)"}


def quant(field, title):
    return {"fieldName": field, "scale": {"type": "quantitative"}, "axis": {"title": title}}


def cat(field, title="", sort=None):
    scale = {"type": "categorical"}
    if sort:
        scale["sort"] = sort
    return {"fieldName": field, "scale": scale, "axis": {"title": title}}


def temporal(field, title=""):
    return {"fieldName": field, "scale": {"type": "temporal"}, "axis": {"title": title}}


def measures(pairs, title):
    """Plot two or more measure columns side by side (no categorical field exists)."""
    return {
        "fields": [{"fieldName": f, "displayName": label} for f, label in pairs],
        "scale": {"type": "quantitative"},
        "axis": {"title": title},
    }


def flow_color():
    return {"fieldName": "flow", "scale": FLOW_SCALE, "legend": {"title": "Flow"}}


_COL_BOILERPLATE = {
    "booleanValues": ["false", "true"],
    "imageUrlTemplate": "{{ @ }}",
    "imageTitleTemplate": "{{ @ }}",
    "imageWidth": "",
    "imageHeight": "",
    "linkUrlTemplate": "{{ @ }}",
    "linkTextTemplate": "{{ @ }}",
    "linkTitleTemplate": "{{ @ }}",
    "linkOpenInNewTab": True,
    "allowSearch": False,
    "allowHTML": False,
    "highlightLinks": False,
    "useMonospaceFont": False,
    "preserveWhitespace": False,
}


def table(name, dataset, columns, title, x, y, w, h, filters=None, order=None,
          description=None):
    """`columns` is a list of (field, title, kind) where kind is 'text' or 'num'."""
    fields, encoded = [], []
    for i, (field, col_title, kind) in enumerate(columns):
        fields.append(f_dim(field))
        col = dict(_COL_BOILERPLATE)
        col.update({
            "fieldName": field,
            "title": col_title,
            "visible": True,
            "order": i,
            "type": "float" if kind == "num" else "string",
            "displayAs": "number" if kind == "num" else "string",
            "alignContent": "right" if kind == "num" else "left",
        })
        if kind == "num":
            col["numberFormat"] = "0,0.00"
        encoded.append(col)

    q = {"datasetName": dataset, "fields": fields, "disaggregated": True}
    if filters:
        q["filters"] = [{"expression": f} for f in filters]
    if order:
        q["orders"] = order

    frame = {"showTitle": True, "title": title}
    if description:
        frame["showDescription"] = True
        frame["description"] = description

    return {
        "widget": {
            "name": name,
            "queries": [{"name": "main_query", "query": q}],
            "spec": {
                "version": 1,
                "widgetType": "table",
                "frame": frame,
                "encodings": {"columns": encoded},
            },
        },
        "position": _pos(x, y, w, h),
    }


def desc(field):
    return {"direction": "DESC", "expression": f"`{field}`", "name": field}


def filter_widget(name, widget_type, bindings, title, x, y, w, h):
    """`bindings` is a list of (dataset, field); one filter can drive many datasets."""
    queries, fields = [], []
    for i, (dataset, field) in enumerate(bindings):
        qname = f"{name}_q{i}"
        queries.append({
            "name": qname,
            "query": {
                "datasetName": dataset,
                "fields": [
                    {"name": field, "expression": f"`{field}`"},
                    {"name": f"{field}_associativity",
                     "expression": "COUNT_IF(`associative_filter_predicate_group`)"},
                ],
                "disaggregated": False,
            },
        })
        fields.append({"fieldName": field, "queryName": qname})
    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "widgetType": widget_type,
                "frame": {"showTitle": True, "title": title},
                "encodings": {"fields": fields},
            },
        },
        "position": _pos(x, y, w, h),
    }


# --------------------------------------------------------------------------
# pages
# --------------------------------------------------------------------------

def page_overview():
    L = []
    L.append(text("ov_title", [
        "# Vietnam Import & Export Statistics\n",
        "Official General Department of Vietnam Customs figures, 2018 to date. ",
        "Headline numbers on this page are national totals; the tabs above break the ",
        "same trade down by product, partner country, province, transport mode and ",
        "enterprise ownership.\n",
    ], 0, 0, 12, 2))

    L.append(label_counter("ov_latest", "ds_kpi", "latest_month",
                           "Latest published month", 0, 2, 2, 3))
    L.append(counter("ov_exports", "ds_kpi", "exports_bn",
                     "Exports · last 12 months (US$ bn)",
                     "Rolling 12 months ending at the latest published month",
                     2, 2, 3, 3, positive_is_good=None))
    L.append(counter("ov_imports", "ds_kpi", "imports_bn",
                     "Imports · last 12 months (US$ bn)",
                     "Rolling 12 months ending at the latest published month",
                     5, 2, 3, 3, positive_is_good=None))
    L.append(counter("ov_balance", "ds_kpi", "balance_bn",
                     "Trade balance · last 12 months (US$ bn)",
                     "Exports minus imports. Green = surplus, orange = deficit.",
                     8, 2, 2, 3))
    L.append(counter("ov_turnover", "ds_kpi", "turnover_bn",
                     "Total turnover (US$ bn)",
                     "Exports plus imports over the last 12 months",
                     10, 2, 2, 3, positive_is_good=None))

    L.append(chart("ov_trend", "ds_national_monthly", "line",
                   [f_dim("report_date"), f_dim("flow"), f_sum("value_bn")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Monthly exports and imports", 0, 5, 12, 7,
                   description="National totals. Every other view on this dashboard is a "
                               "breakdown of these two lines."))

    L.append(chart("ov_balance_chart", "ds_national_balance", "bar",
                   [f_dim("report_date"), f_sum("balance_bn")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(balance_bn)", USD_BN)},
                   "Monthly trade balance", 0, 12, 6, 6,
                   description="Above zero is a surplus month, below zero a deficit."))

    L.append(chart("ov_annual", "ds_annual", "bar",
                   [f_dim("report_year"), f_dim("flow"), f_sum("value_bn")],
                   {"x": cat("report_year", "Year"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Annual totals", 6, 12, 6, 6,
                   description="The most recent year is partial - it only covers the "
                               "months published so far."))

    L.append(text("ov_h_products", ["## What Vietnam trades"], 0, 18, 12, 1))
    L.append(chart("ov_top_exp_prod", "ds_products", "bar",
                   [f_dim("category"), f_sum("value_bn")],
                   {"y": cat("category", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN)},
                   "Top 12 export categories · all years", 0, 19, 6, 8,
                   filters=["`flow` = 'Export'", "`rank_overall` <= 12"]))
    L.append(chart("ov_top_imp_prod", "ds_products", "bar",
                   [f_dim("category"), f_sum("value_bn")],
                   {"y": cat("category", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN)},
                   "Top 12 import categories · all years", 6, 19, 6, 8,
                   filters=["`flow` = 'Import'", "`rank_overall` <= 12"]))

    L.append(text("ov_h_partners", ["## Who Vietnam trades with"], 0, 27, 12, 1))
    L.append(chart("ov_top_partners", "ds_country_balance", "bar",
                   [f_dim("country"), f_sum("exports_bn"), f_sum("imports_bn")],
                   {"y": cat("country", "", {"by": "x-reversed"}),
                    "x": measures([("sum(exports_bn)", "Vietnam exports to"),
                                   ("sum(imports_bn)", "Vietnam imports from")], USD_BN)},
                   "Top 12 partners by total trade · last 12 months", 0, 28, 6, 8,
                   filters=["`rank_overall` <= 12"]))
    L.append(chart("ov_partner_balance", "ds_country_balance", "bar",
                   [f_dim("country"), f_sum("balance_bn")],
                   {"y": cat("country", "", {"by": "x-reversed"}),
                    "x": quant("sum(balance_bn)", USD_BN)},
                   "Surplus and deficit by partner · last 12 months", 6, 28, 6, 8,
                   filters=["`rank_overall` <= 12"],
                   description="Positive means Vietnam sells more than it buys."))

    L.append(text("ov_footer", [
        "---\n",
        "**Reading this dashboard.** The five breakdowns slice the same trade in "
        "different ways and must not be added together. Use this Overview page for "
        "headline figures - it is the only page built on the official national "
        "totals.\n",
        "\n",
        "Values are US dollars. Product and partner-country figures cover goods only. "
        "Transport-mode data is quarterly; everything else is monthly.\n",
    ], 0, 36, 12, 3))
    return {"name": "overview", "displayName": "Overview", "layout": L}


def page_products():
    L = []
    L.append(text("pr_title", [
        "## Products\n",
        "What Vietnam actually ships and buys, from the national totals. "
        "Use the filters to narrow the period or the direction of trade.\n",
    ], 0, 0, 12, 2))

    L.append(filter_widget("pr_f_date", "filter-date-range-picker",
                           [("ds_products", "report_date")], "Period", 0, 2, 4, 2))
    L.append(filter_widget("pr_f_flow", "filter-single-select",
                           [("ds_products", "flow")], "Trade flow", 4, 2, 4, 2))
    L.append(filter_widget("pr_f_cat", "filter-multi-select",
                           [("ds_products", "category")], "Category", 8, 2, 4, 2))

    L.append(chart("pr_top", "ds_products", "bar",
                   [f_dim("category"), f_dim("flow"), f_sum("value_bn")],
                   {"y": cat("category", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Top 15 categories in the selected period", 0, 4, 12, 9,
                   filters=["`rank_overall` <= 15"]))

    L.append(chart("pr_trend", "ds_products", "line",
                   [f_dim("report_date"), f_dim("category"), f_sum("value_bn")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": cat("category")},
                   "How the top 8 categories have moved", 0, 13, 12, 8,
                   filters=["`rank_overall` <= 8"]))

    L.append(chart("pr_share", "ds_products", "pie",
                   [f_dim("category"), f_sum("value_bn")],
                   {"angle": quant("sum(value_bn)", USD_BN),
                    "color": cat("category")},
                   "Trade mix · top 10 categories", 0, 21, 5, 8,
                   filters=["`rank_overall` <= 10"],
                   description="Combines both directions unless you pick one with the "
                               "Trade flow filter."))

    L.append(table("pr_table", "ds_product_summary",
                   [("category", "Category", "text"),
                    ("flow", "Flow", "text"),
                    ("last_12m_bn", "Last 12m (US$ bn)", "num"),
                    ("prior_12m_bn", "Prior 12m (US$ bn)", "num"),
                    ("yoy_pct", "Change %", "num"),
                    ("share_pct", "Share of flow %", "num")],
                   "Category league table · last 12 months", 5, 21, 7, 8,
                   filters=["`rank_overall` <= 25"], order=[desc("last_12m_bn")],
                   description="Fixed 12-month window - the period filter above does "
                               "not apply to this table."))
    return {"name": "products", "displayName": "Products", "layout": L}


def page_partners():
    L = []
    L.append(text("pa_title", [
        "## Trading partners\n",
        "Where the goods go and where they come from.\n",
    ], 0, 0, 12, 2))

    L.append(filter_widget("pa_f_date", "filter-date-range-picker",
                           [("ds_countries", "report_date")], "Period", 0, 2, 4, 2))
    L.append(filter_widget("pa_f_flow", "filter-single-select",
                           [("ds_countries", "flow")], "Trade flow", 4, 2, 4, 2))
    L.append(filter_widget("pa_f_country", "filter-multi-select",
                           [("ds_countries", "country")], "Country", 8, 2, 4, 2))

    L.append(chart("pa_top", "ds_countries", "bar",
                   [f_dim("country"), f_dim("flow"), f_sum("value_bn")],
                   {"y": cat("country", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Top 15 partners in the selected period", 0, 4, 12, 9,
                   filters=["`rank_overall` <= 15"]))

    L.append(chart("pa_trend", "ds_countries", "line",
                   [f_dim("report_date"), f_dim("country"), f_sum("value_bn")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": cat("country")},
                   "How the top 8 partners have moved", 0, 13, 12, 8,
                   filters=["`rank_overall` <= 8"]))

    L.append(chart("pa_balance", "ds_country_balance", "bar",
                   [f_dim("country"), f_sum("balance_bn")],
                   {"y": cat("country", "", {"by": "x-reversed"}),
                    "x": quant("sum(balance_bn)", USD_BN)},
                   "Surplus / deficit by partner · last 12 months", 0, 21, 5, 9,
                   filters=["`rank_overall` <= 15"]))

    L.append(table("pa_table", "ds_country_balance",
                   [("country", "Partner", "text"),
                    ("exports_bn", "Vietnam exports (US$ bn)", "num"),
                    ("imports_bn", "Vietnam imports (US$ bn)", "num"),
                    ("balance_bn", "Balance (US$ bn)", "num"),
                    ("turnover_bn", "Turnover (US$ bn)", "num"),
                    ("position", "Position", "text")],
                   "Partner league table · last 12 months", 5, 21, 7, 9,
                   filters=["`rank_overall` <= 30"], order=[desc("turnover_bn")]))

    L.append(text("pa_note", [
        "> **Why partner totals don't add up to the headline.** Customs publishes a "
        "country breakdown that covers most but not all trade. The gap is carried as "
        "an *Other / Unallocated* row, which the rankings above exclude. Summed across "
        "all countries the breakdown runs about 3-4% above the national total, so use "
        "the Overview page for headline figures and this page for relative standings.\n",
    ], 0, 30, 12, 3))
    return {"name": "partners", "displayName": "Trading partners", "layout": L}


def page_provinces():
    L = []
    L.append(text("pv_title", [
        "## Provinces\n",
        "Trade by province of the exporting or importing business. Values only - "
        "province reports carry no product or quantity detail.\n",
    ], 0, 0, 12, 2))

    L.append(filter_widget("pv_f_date", "filter-date-range-picker",
                           [("ds_provinces", "report_date")], "Period", 0, 2, 4, 2))
    L.append(filter_widget("pv_f_flow", "filter-single-select",
                           [("ds_provinces", "flow")], "Trade flow", 4, 2, 4, 2))
    L.append(filter_widget("pv_f_prov", "filter-multi-select",
                           [("ds_provinces", "province")], "Province", 8, 2, 4, 2))

    L.append(chart("pv_top", "ds_provinces", "bar",
                   [f_dim("province"), f_dim("flow"), f_sum("value_bn")],
                   {"y": cat("province", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Provinces ranked · selected period", 0, 4, 12, 10))

    L.append(chart("pv_trend", "ds_provinces", "line",
                   [f_dim("report_date"), f_dim("province"), f_sum("value_bn")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": cat("province")},
                   "How the top 8 provinces have moved", 0, 14, 12, 8,
                   filters=["`rank_overall` <= 8"]))

    L.append(table("pv_table", "ds_province_balance",
                   [("province", "Province", "text"),
                    ("exports_bn", "Exports (US$ bn)", "num"),
                    ("imports_bn", "Imports (US$ bn)", "num"),
                    ("balance_bn", "Balance (US$ bn)", "num"),
                    ("export_share_pct", "Share of national exports %", "num")],
                   "Province league table · last 12 months", 0, 22, 12, 9,
                   order=[desc("exports_bn")]))

    L.append(text("pv_note", [
        "> **The 2025 reorganisation.** Resolution 202/2025/QH15 merged Vietnam's 63 "
        "provinces and cities into 34 with effect from 1 July 2025. Every figure here "
        "is expressed in the 34 current units, including months before the merger, so "
        "the series stays comparable across the break. Ho Chi Minh City, for example, "
        "now includes the former Binh Duong and Ba Ria-Vung Tau.\n",
    ], 0, 31, 12, 3))
    return {"name": "provinces", "displayName": "Provinces", "layout": L}


def page_transport():
    L = []
    L.append(text("tr_title", [
        "## Transport modes\n",
        "How goods physically move. This source is **quarterly**, not monthly, and "
        "Customs has published nothing for 2026 yet.\n",
    ], 0, 0, 12, 2))

    L.append(filter_widget("tr_f_flow", "filter-single-select",
                           [("ds_transport_share", "flow"), ("ds_transport", "flow")],
                           "Trade flow", 0, 2, 4, 2))
    L.append(filter_widget("tr_f_mode", "filter-multi-select",
                           [("ds_transport_share", "mode"), ("ds_transport", "mode")],
                           "Mode", 4, 2, 4, 2))
    L.append(filter_widget("tr_f_year", "filter-multi-select",
                           [("ds_transport_share", "report_year"),
                            ("ds_transport", "report_year")], "Year", 8, 2, 4, 2))

    L.append(chart("tr_share", "ds_transport_share", "area",
                   [f_dim("quarter_date"), f_dim("mode"), f_sum("value_bn")],
                   {"x": temporal("quarter_date", "Quarter"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": cat("mode")},
                   "Trade value by transport mode, by quarter", 0, 4, 12, 8))

    L.append(chart("tr_mix", "ds_transport_share", "bar",
                   [f_dim("mode"), f_dim("flow"), f_sum("value_bn")],
                   {"x": cat("mode", "Mode"),
                    "y": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Total carried by each mode", 0, 12, 5, 8))

    L.append(chart("tr_cat", "ds_transport", "bar",
                   [f_dim("category"), f_dim("mode"), f_sum("value_bn")],
                   {"y": cat("category", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN),
                    "color": cat("mode")},
                   "Which mode carries which goods · top 12 categories", 5, 12, 7, 8,
                   filters=["`rank_overall` <= 12"]))
    return {"name": "transport", "displayName": "Transport modes", "layout": L}


def page_fdi():
    L = []
    L.append(text("fd_title", [
        "## Foreign-invested vs domestic enterprises\n",
        "Customs reports the share of trade handled by foreign-invested (FDI) "
        "enterprises. The remainder is domestic.\n",
    ], 0, 0, 12, 2))

    L.append(filter_widget("fd_f_date", "filter-date-range-picker",
                           [("ds_fdi", "report_date")], "Period", 0, 2, 6, 2))
    L.append(filter_widget("fd_f_flow", "filter-single-select",
                           [("ds_fdi", "flow")], "Trade flow", 6, 2, 6, 2))

    L.append(chart("fd_share", "ds_fdi", "line",
                   [f_dim("report_date"), f_dim("flow"), f_sum("fdi_share_pct")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(fdi_share_pct)", "% of national total"),
                    "color": flow_color()},
                   "FDI share of trade", 0, 4, 12, 7,
                   description="Share of the national total handled by foreign-invested "
                               "enterprises."))

    L.append(chart("fd_split", "ds_fdi", "area",
                   [f_dim("report_date"), f_sum("fdi_bn"), f_sum("domestic_bn")],
                   {"x": temporal("report_date", "Month"),
                    "y": measures([("sum(fdi_bn)", "Foreign-invested"),
                                   ("sum(domestic_bn)", "Domestic")], USD_BN)},
                   "Foreign-invested vs domestic trade value", 0, 11, 6, 7,
                   description="Pick a single trade flow above, or the two flows are "
                               "stacked together."))

    L.append(chart("fd_cat", "ds_fdi_products", "bar",
                   [f_dim("category"), f_dim("flow"), f_sum("value_bn")],
                   {"y": cat("category", "", {"by": "x-reversed"}),
                    "x": quant("sum(value_bn)", USD_BN),
                    "color": flow_color()},
                   "Where FDI concentrates · top 12, last 12 months", 6, 11, 6, 7,
                   filters=["`rank_overall` <= 12"]))

    L.append(text("fd_note", [
        "> Customs does not break every commodity out by ownership. Categories it "
        "leaves out are counted as domestic here, so the FDI share is a floor rather "
        "than an exact figure.\n",
    ], 0, 18, 12, 2))
    return {"name": "fdi", "displayName": "FDI vs domestic", "layout": L}


def page_data():
    L = []
    L.append(text("dq_title", [
        "## Data quality\n",
        "Everything on this dashboard is extracted from PDF reports published by the "
        "General Department of Vietnam Customs. This page shows how complete that "
        "extraction is and where it is known to be imperfect.\n",
    ], 0, 0, 12, 3))

    L.append(table("dq_coverage", "ds_coverage",
                   [("workstream", "Source", "text"),
                    ("expected_periods", "Periods expected", "num"),
                    ("complete", "Complete", "num"),
                    ("incomplete", "Partial", "num"),
                    ("missing", "Missing", "num"),
                    ("never_published", "Never published", "num"),
                    ("actionable_gaps", "Actionable gaps", "num"),
                    ("first_period", "From", "text"),
                    ("last_complete_period", "Latest complete", "text")],
                   "Coverage by source", 0, 3, 12, 6,
                   description="'Actionable gaps' excludes periods Customs has not "
                               "released yet and periods it never made retrievable."))

    L.append(chart("dq_recon", "ds_grain_reconciliation", "line",
                   [f_dim("report_date"), f_dim("breakdown"), f_sum("deviation_pct")],
                   {"x": temporal("report_date", "Month"),
                    "y": quant("sum(deviation_pct)", "% difference from national total"),
                    "color": cat("breakdown")},
                   "How closely each breakdown reconciles to the national total",
                   0, 9, 12, 7,
                   description="Zero means the breakdown sums exactly to the official "
                               "total. Provinces sit on zero; country and FDI run a few "
                               "per cent high."))

    L.append(text("dq_notes", [
        "### Known limitations\n",
        "\n",
        "**Country and FDI breakdowns run 3-4% above the national total.** Customs "
        "publishes a partial breakdown plus a residual, and where the named entries "
        "already exceed the category total the residual is floored at zero rather than "
        "going negative. Rankings and trends are sound; the sums are not. Take headline "
        "figures from the Overview page.\n",
        "\n",
        "**A handful of country names come from damaged scans.** Older PDFs were "
        "OCR'd poorly and a few country labels are mangled - mostly Taiwan, Portugal "
        "and Sweden. Rows the pipeline could not resolve confidently are flagged in "
        "`market_data.customs.dim_country_suspect_variants`.\n",
        "\n",
        "**October 2022 imports by country are missing permanently.** Customs "
        "published that one report only on an internal host that is unreachable from "
        "outside their network. The export side of that month is present.\n",
        "\n",
        "**Transport-mode data is quarterly and stops at 2025-Q4.** Customs has not "
        "released 2026 quarters. This is a publication schedule, not a pipeline "
        "failure.\n",
        "\n",
        "**Province figures use the 34 post-2025 units throughout,** including for "
        "months before the July 2025 merger, so that series stay comparable.\n",
        "\n",
        "The pipeline refreshes on the 15th of each month and fails loudly rather than "
        "publishing bad numbers.\n",
    ], 0, 16, 12, 12))
    return {"name": "data_quality", "displayName": "Data quality", "layout": L}


# --------------------------------------------------------------------------

def build():
    datasets = [
        {"name": name,
         "displayName": name.replace("ds_", "").replace("_", " ").title(),
         "queryLines": [line + "\n" for line in sql.strip().splitlines()]}
        for name, sql in DATASETS.items()
    ]
    return {
        "datasets": datasets,
        "pages": [
            page_overview(),
            page_products(),
            page_partners(),
            page_provinces(),
            page_transport(),
            page_fdi(),
            page_data(),
        ],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deploy", action="store_true", help="push to Databricks")
    args = ap.parse_args()

    dashboard = build()
    payload = json.dumps(dashboard, indent=2, ensure_ascii=False)
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(payload)

    widgets = sum(len(p["layout"]) for p in dashboard["pages"])
    print(f"wrote {OUT}")
    print(f"  {len(dashboard['datasets'])} datasets, "
          f"{len(dashboard['pages'])} pages, {widgets} widgets")

    if not args.deploy:
        return

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import Dashboard

    w = WorkspaceClient(profile="DEFAULT")
    w.lakeview.update(
        dashboard_id=DASHBOARD_ID,
        dashboard=Dashboard(
            display_name=DISPLAY_NAME,
            warehouse_id=WAREHOUSE_ID,
            serialized_dashboard=payload,
        ),
    )
    w.lakeview.publish(dashboard_id=DASHBOARD_ID, warehouse_id=WAREHOUSE_ID,
                       embed_credentials=True)
    print(f"deployed and published: {DASHBOARD_ID}")


if __name__ == "__main__":
    main()
