import argparse
import logging
import sys
from pathlib import Path

import yaml

from scraper import scrape_all_funds
from db import init_db, insert_scrape_results, get_comparison_data
from report import generate_report


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="FT Holdings & Sectors Automation")
    parser.add_argument("--scrape", action="store_true", help="Scrape fund data from FT")
    parser.add_argument("--report", action="store_true", help="Generate comparison HTML report")
    parser.add_argument("--config", default="config.yaml", help="Path to config file (default: config.yaml)")
    args = parser.parse_args()

    # If neither flag is set, do both
    if not args.scrape and not args.report:
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


if __name__ == "__main__":
    main()
