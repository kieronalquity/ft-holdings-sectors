"""
Migrates historical holdings/sectors data from the C# app's HTML report
into the new SQLite database.

Usage: python migrate_html.py AllHoldings_Sectors_December.html
"""
import sys
import re
import logging
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
import yaml

from db import init_db, insert_scrape_results
from scraper import ScrapedEntry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_date(text: str) -> str:
    """Parse a date string like 'July 29, 2025' or '29/07/2025' to ISO format."""
    text = text.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def parse_percentage(text: str) -> float:
    """Parse '6.43%' -> 6.43"""
    text = text.strip().replace("%", "").replace(",", "").strip()
    if not text or text in ("N/A", "--", "-"):
        return None
    try:
        return round(float(text), 4)
    except ValueError:
        return None


def extract_from_html(html_path: str) -> dict:
    """
    Parse the historical HTML report and extract all holdings/sectors data.

    Returns dict keyed by log_date -> list of ScrapedEntry.
    """
    logger.info("Reading %s", html_path)
    html = Path(html_path).read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "lxml")

    items = soup.select("li.list-group-item")
    logger.info("Found %d funds in HTML", len(items))

    # Group entries by log_date for batch insertion
    entries_by_date = {}
    total_entries = 0

    for item in items:
        # Extract fund name
        name_div = item.select_one('div[style*="#535050"] b')
        if not name_div:
            continue
        fund_name = name_div.get_text(strip=True)

        # Get all inner 330px tables
        inner_tables = item.select('table[style*="width:330px"]')
        if not inner_tables:
            continue

        # Detect which are sectors (purple background) vs holdings
        holdings_tables = []
        sectors_tables = []
        for tbl in inner_tables:
            style = tbl.get("style", "")
            if "f5eef8" in style:
                sectors_tables.append(tbl)
            else:
                holdings_tables.append(tbl)

        # The C# report shows Holdings first, then Sectors, then Regions.
        # Regions tables are also non-purple so they end up in holdings_tables.
        # We know there are N dates, so the first N holdings_tables are real holdings,
        # and any beyond that are regions (skip them).
        num_dates = len(sectors_tables)
        if num_dates == 0:
            # Fallback: count from holdings tables
            num_dates = len(holdings_tables) // 2 if len(holdings_tables) > 4 else len(holdings_tables)

        # Take only the first num_dates holdings tables (skip regions)
        real_holdings = holdings_tables[:num_dates]

        # Process holdings
        for tbl in real_holdings:
            header_date, entries = _extract_table_entries(tbl, fund_name, "Holdings")
            if header_date and entries:
                entries_by_date.setdefault(header_date, []).extend(entries)
                total_entries += len(entries)

        # Process sectors
        for tbl in sectors_tables:
            header_date, entries = _extract_table_entries(tbl, fund_name, "Sectors")
            if header_date and entries:
                entries_by_date.setdefault(header_date, []).extend(entries)
                total_entries += len(entries)

    logger.info("Extracted %d total entries across %d dates", total_entries, len(entries_by_date))
    return entries_by_date


def _extract_table_entries(table, fund_name: str, category: str) -> tuple:
    """Extract entries from a single 330px inner table.

    Returns (header_date, list_of_entries) where header_date is the column
    header date (the original LogDate from the C# database) used for grouping.
    """
    entries = []

    # Get the date from the header row (bold text in first row)
    # This is the LogDate — the date the scrape ran — used for grouping
    header_b = table.select_one("tr td b")
    if not header_b:
        return None, entries
    header_date = parse_date(header_b.get_text(strip=True))

    # Get the "Updated on" date if available (the FT "as of" date)
    data_date = header_date
    updated_td = table.select_one('td[style*="color:blue"]')
    if updated_td:
        updated_text = updated_td.get_text(strip=True)
        match = re.search(r"Updated on\s*:\s*(.+)", updated_text)
        if match:
            data_date = parse_date(match.group(1).strip())

    # Extract data rows (rows with 3+ tds that contain company/sector + percentage)
    rows = table.select("tr")
    for row in rows:
        tds = row.select("td")
        if len(tds) < 3:
            continue

        # Skip header rows and "Updated on" rows
        first_text = tds[0].get_text(strip=True)
        if "Updated on" in first_text or not any(
            "%" in td.get_text() for td in tds
        ):
            continue

        # The structure is: [icon_td, name_td, percentage_td, diff_td]
        # or sometimes [name_td, percentage_td, diff_td] or with colspan
        # Find the td with a percentage
        name = None
        pct = None

        for i, td in enumerate(tds):
            text = td.get_text(strip=True)
            if "%" in text and pct is None:
                pct = parse_percentage(text)
                # Name is the previous td
                if i > 0:
                    name = tds[i - 1].get_text(strip=True)
                break

        if name and pct is not None and name not in ("", " "):
            entries.append(ScrapedEntry(
                category=category,
                fund_name=fund_name,
                company_sector=name,
                percentage=pct,
                date_of_data=data_date,
                url="",  # Not available from HTML
            ))

    return header_date, entries


def main():
    if len(sys.argv) < 2:
        print("Usage: python migrate_html.py <path_to_html_file>")
        sys.exit(1)

    html_path = sys.argv[1]
    if not Path(html_path).exists():
        print(f"File not found: {html_path}")
        sys.exit(1)

    # Load config for DB path
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    db_path = config.get("database", {}).get("path", "data/holdings_sectors.db")

    init_db(db_path)

    # Extract data from HTML
    entries_by_date = extract_from_html(html_path)

    if not entries_by_date:
        print("No data extracted from HTML file.")
        return

    # Insert each date's entries
    total_inserted = 0
    for log_date in sorted(entries_by_date.keys()):
        entries = entries_by_date[log_date]
        count = insert_scrape_results(db_path, entries, log_date)
        total_inserted += count
        print(f"  {log_date}: {count} entries")

    print(f"\nMigration complete: {total_inserted} total entries across {len(entries_by_date)} dates.")


if __name__ == "__main__":
    main()
