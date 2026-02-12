import pandas as pd
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def generate_report(comparison_data: dict, config: dict) -> str:
    report_cfg = config.get("report", {})
    output_dir = report_cfg.get("output_dir", "output")
    pattern = report_cfg.get("filename_pattern", "holdings_sectors_{date}.html")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    dates = comparison_data["dates"]
    funds = comparison_data["funds"]
    df = comparison_data["details"]

    if df.empty:
        logger.warning("No data to generate report")
        return ""

    fund_data = _prepare_fund_data(funds, dates, df)

    # Template rendering
    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)
    template = env.get_template("report.html")

    num_dates = len(dates)
    inner_width = (num_dates * 330) + 50
    outer_width = inner_width + 150

    html = template.render(
        funds=fund_data,
        dates=dates,
        inner_width=inner_width,
        outer_width=outer_width,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    filename = pattern.format(date=datetime.now().strftime("%Y-%m-%d"))
    output_path = Path(output_dir) / filename
    output_path.write_text(html, encoding="utf-8")
    logger.info("Report written to %s", output_path)
    return str(output_path)


def _prepare_fund_data(funds: list, dates: list, df: pd.DataFrame) -> list:
    result = []
    num_dates = len(dates)

    for fund_name in funds:
        fund_df = df[df["fund_name"] == fund_name]
        fund_entry = {
            "fund_name": fund_name,
            "holdings": {},
            "sectors": {},
        }

        for date_idx, log_date in enumerate(dates):
            is_first = date_idx == 0
            is_last = date_idx == num_dates - 1
            date_label = _format_date_label(log_date)

            for category in ("Holdings", "Sectors"):
                cat_df = fund_df[
                    (fund_df["log_date"] == log_date) &
                    (fund_df["category"] == category)
                ]

                # Sort: non-removed entries first by percentage desc, removed at end
                if not cat_df.empty:
                    regular = cat_df[cat_df.get("is_removed", False) != True].copy()
                    removed = cat_df[cat_df.get("is_removed", False) == True].copy()
                    if not regular.empty:
                        regular = regular.sort_values("percentage", ascending=False)
                    cat_df = pd.concat([regular, removed], ignore_index=True)

                entries = []
                data_date = ""
                for _, row in cat_df.iterrows():
                    if not data_date and pd.notna(row.get("date_of_data")):
                        data_date = _format_short_date(row["date_of_data"])

                    diff = row.get("diff")
                    is_new = bool(row.get("is_new", False))
                    is_removed = bool(row.get("is_removed", False))
                    is_returning = bool(row.get("is_returning", False))

                    icon = _get_diff_icon(diff, is_new, is_removed, is_returning, is_first, is_last)

                    pct = row["percentage"]
                    diff_val = float(diff) if pd.notna(diff) else None

                    entries.append({
                        "name": row["company_sector"],
                        "percentage": pct,
                        "diff": diff_val,
                        "diff_icon": icon,
                    })

                date_data = {
                    "date_label": date_label,
                    "data_date": data_date,
                    "entries": entries,
                }

                if category == "Holdings":
                    fund_entry["holdings"][log_date] = date_data
                else:
                    fund_entry["sectors"][log_date] = date_data

        result.append(fund_entry)
    return result


def _get_diff_icon(diff, is_new: bool, is_removed: bool, is_returning: bool,
                   is_first_date: bool, is_last_date: bool) -> str:
    if is_first_date:
        return "none"

    if is_returning:
        return "returning"

    if is_new:
        return "new"

    if is_removed:
        return "removed"

    return "none"


def _format_date_label(iso_date: str) -> str:
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d")
        return dt.strftime("%B %d, %Y")
    except ValueError:
        return iso_date


def _format_short_date(iso_date: str) -> str:
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return iso_date
