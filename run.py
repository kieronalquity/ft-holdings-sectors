import argparse
import logging
import re
import sys
from pathlib import Path

import yaml

from scraper import scrape_all_funds
from db import init_db, insert_scrape_results, get_comparison_data
from report import generate_report
from bloomberg_loader import (
    create_ft_snapshot, attach_ft_data, ingest_bloomberg_excel, import_historical_html,
)


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_latest_bloomberg_file(data_dir: str) -> Path | None:
    """Newest *_peerholdings.xlsx in data_dir, by the YYYY-MM-DD date in the filename."""
    d = Path(data_dir)
    if not d.is_dir():
        return None
    candidates = sorted(d.glob("*_peerholdings.xlsx"), key=lambda p: p.name)
    return candidates[-1] if candidates else None


def main():
    parser = argparse.ArgumentParser(description="FT Holdings & Sectors Automation")
    parser.add_argument("--update", nargs="?", const="", default=None, metavar="XLSX",
                        help="Full peer update: ingest the Bloomberg Excel (newest in bloomberg_data/ "
                             "if no path given) AND scrape FT into ONE combined snapshot labelled "
                             "'Run DD/MM - BBG and FT updated'")
    parser.add_argument("--replace", action="store_true", help="With --update: overwrite an existing snapshot for the same date")
    parser.add_argument("--scrape", action="store_true", help="Scrape fund data from FT")
    parser.add_argument("--report", action="store_true", help="Generate comparison HTML report")
    parser.add_argument("--ft-only", action="store_true", help="FT scrape only — update Old View in dashboard (no Bloomberg re-ingest)")
    parser.add_argument("--label", type=str, default=None, help="Override the snapshot label")
    parser.add_argument("--import-history", type=str, default=None, help="Import historical FT data from HTML file")
    parser.add_argument("--config", default="config.yaml", help="Path to config file (default: config.yaml)")
    args = parser.parse_args()

    # If no flags are set, do scrape + report (legacy behaviour)
    if (not args.scrape and not args.report and not args.ft_only
            and not args.import_history and args.update is None):
        args.scrape = True
        args.report = True

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config(args.config)
    db_path = config.get("database", {}).get("path", "data/holdings_sectors.db")

    init_db(db_path)

    if args.update is not None:
        from datetime import datetime as _dt
        data_dir = config.get("bloomberg", {}).get("data_dir", "bloomberg_data")
        xlsx = Path(args.update) if args.update else find_latest_bloomberg_file(data_dir)
        if not xlsx or not xlsx.exists():
            print(f"No Bloomberg Excel found (looked in {data_dir}/ for *_peerholdings.xlsx).")
            return

        # Label from the date in the filename: 2026-09-18_... -> 'Run 18/09 - BBG and FT updated'
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", xlsx.name)
        run_date = _dt(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else _dt.today()
        label = args.label or f"Run {run_date.strftime('%d/%m')} - BBG and FT updated"

        print(f"\n[1/2] Ingesting Bloomberg file: {xlsx.name}  ->  '{label}'")
        result = ingest_bloomberg_excel(str(xlsx), db_path, replace=args.replace, label=label)
        if "error" in result:
            print(f"Error: {result['error']}")
            print("Re-run with --replace to overwrite that snapshot.")
            return
        sid = result["snapshot_id"]
        print(f"  snapshot id={sid}: {result['num_funds']} funds, "
              f"{result['num_holdings']} holdings, {result['num_master_rows']} master rows"
              + (f", errors: {result['errors']}" if result["errors"] else ""))

        print(f"\n[2/2] Scraping FT for {len(config.get('funds', []))} funds...")
        entries = scrape_all_funds(config)
        if not entries:
            print("No data scraped from FT — snapshot has Bloomberg data only.")
        else:
            ft = attach_ft_data(db_path, sid, entries)
            print(f"  attached {ft['num_entries']} FT entries to snapshot id={sid}")

        print(f"\nDone: '{label}' (snapshot id={sid}). Push to GitHub to update the live dashboard.")
        return

    if args.scrape:
        print(f"\nScraping {len(config.get('funds', []))} funds from FT...")
        entries = scrape_all_funds(config)
        if entries:
            count = insert_scrape_results(db_path, entries)
            print(f"Stored {count} entries in database.")
        else:
            print("No data scraped.")

    if args.report:
        num_snapshots = config.get("report", {}).get("snapshots_to_compare", 4)
        print(f"\nGenerating comparison report (last {num_snapshots} snapshots)...")
        comparison = get_comparison_data(db_path, num_snapshots)

        if not comparison["dates"]:
            print("No data in database. Run with --scrape first.")
            return

        output_path = generate_report(comparison, config)
        if output_path:
            print(f"Report saved to: {output_path}")
        else:
            print("Report generation failed.")

    if args.import_history:
        print(f"\nImporting historical FT data from: {args.import_history}")
        result = import_historical_html(db_path, args.import_history)
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Dates found: {result['dates_found']}")
            for s in result["snapshots"]:
                if s.get("skipped"):
                    print(f"  {s['label']}: already exists (id={s['snapshot_id']})")
                else:
                    print(f"  {s['label']}: {s['num_entries']} entries (id={s['snapshot_id']})")

    if args.ft_only:
        from datetime import date
        print(f"\nFT-only scrape: updating Old View data...")
        entries = scrape_all_funds(config)
        if not entries:
            print("No data scraped from FT.")
            return
        label = args.label or f"Run {date.today().strftime('%d/%m')} - only Old view update"
        result = create_ft_snapshot(db_path, entries, label=label)
        print(f"FT snapshot created: {result['label']} ({result['num_entries']} entries)")
        print("Push to GitHub to update the live dashboard.")


if __name__ == "__main__":
    main()
