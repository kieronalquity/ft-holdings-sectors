import sqlite3
from datetime import date
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS holdings_sectors_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    log_date       TEXT NOT NULL,
    category       TEXT NOT NULL,
    fund_name      TEXT NOT NULL,
    company_sector TEXT NOT NULL,
    percentage     REAL NOT NULL,
    date_of_data   TEXT,
    url            TEXT NOT NULL
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_log_date ON holdings_sectors_log(log_date);",
    "CREATE INDEX IF NOT EXISTS idx_fund_category ON holdings_sectors_log(fund_name, category);",
    "CREATE INDEX IF NOT EXISTS idx_lookup ON holdings_sectors_log(log_date, fund_name, category);",
]


def _normalize_fund_name(name: str) -> str:
    """Normalize unicode dashes/special chars for consistent matching."""
    for ch in ("\u2013", "\u2014", "\u2012", "\u2015"):
        name = name.replace(ch, "-")
    name = name.replace("\u00a0", " ")
    name = name.replace("\ufffd", "-")
    # Fix corrupted dashes stored as literal '?' (e.g. en-dash -> ?)
    name = name.replace(" ? ", " - ")
    return name


def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(CREATE_TABLE_SQL)
        for idx_sql in CREATE_INDEXES_SQL:
            conn.execute(idx_sql)
        conn.commit()
        logger.info("Database initialized at %s", db_path)
    finally:
        conn.close()


def insert_scrape_results(db_path: str, entries: list, log_date: str = None) -> int:
    if not entries:
        return 0
    if log_date is None:
        log_date = date.today().isoformat()

    rows = [
        (log_date, e.category, e.fund_name, e.company_sector,
         e.percentage, e.date_of_data, e.url)
        for e in entries
    ]

    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            "INSERT INTO holdings_sectors_log "
            "(log_date, category, fund_name, company_sector, percentage, date_of_data, url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
        count = len(rows)
        logger.info("Inserted %d rows for log_date=%s", count, log_date)
        return count
    finally:
        conn.close()


def get_distinct_funds(db_path: str) -> list:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT DISTINCT fund_name FROM holdings_sectors_log "
            "WHERE category='Holdings' ORDER BY fund_name"
        )
        names = [_normalize_fund_name(row[0]) for row in cursor.fetchall()]
        # Deduplicate after normalization while preserving order
        seen = set()
        result = []
        for name in names:
            if name not in seen:
                seen.add(name)
                result.append(name)
        return result
    finally:
        conn.close()


def get_last_n_dates(db_path: str, n: int = 4) -> list:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT DISTINCT log_date FROM holdings_sectors_log "
            "ORDER BY log_date DESC LIMIT ?",
            (n,)
        )
        dates = [row[0] for row in cursor.fetchall()]
        dates.reverse()
        return dates
    finally:
        conn.close()


def get_comparison_data(db_path: str, num_snapshots: int = 4) -> dict:
    dates = get_last_n_dates(db_path, num_snapshots)
    if not dates:
        return {"funds": [], "dates": [], "details": pd.DataFrame()}

    funds = get_distinct_funds(db_path)
    if not funds:
        return {"funds": [], "dates": dates, "details": pd.DataFrame()}

    placeholders = ",".join("?" for _ in dates)
    query = (
        f"SELECT log_date, category, fund_name, company_sector, percentage, date_of_data, url "
        f"FROM holdings_sectors_log WHERE log_date IN ({placeholders}) "
        f"ORDER BY category, log_date, fund_name, company_sector"
    )

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(query, conn, params=dates)
    finally:
        conn.close()

    if df.empty:
        return {"funds": funds, "dates": dates, "details": df}

    # Normalize fund names to handle encoding inconsistencies (e.g. en-dash vs ?)
    df["fund_name"] = df["fund_name"].apply(_normalize_fund_name)

    # Deduplicate: keep first occurrence per (log_date, fund_name, category, company_sector)
    df = df.drop_duplicates(
        subset=["log_date", "fund_name", "category", "company_sector"],
        keep="first"
    )

    # Compute previous percentage and diff for each entry
    df = _compute_diffs(df, dates)

    return {"funds": funds, "dates": dates, "details": df}


def _compute_diffs(df: pd.DataFrame, dates: list) -> pd.DataFrame:
    results = []

    for i, current_date in enumerate(dates):
        current = df[df["log_date"] == current_date].copy()

        if i == 0:
            current["prev_percentage"] = None
            current["diff"] = None
            current["is_new"] = False
            current["is_removed"] = False
            current["is_returning"] = False
            results.append(current)
            continue

        prev_date = dates[i - 1]
        prev = df[df["log_date"] == prev_date]

        # Merge current with previous on fund+category+company
        merged = current.merge(
            prev[["fund_name", "category", "company_sector", "percentage"]],
            on=["fund_name", "category", "company_sector"],
            how="left",
            suffixes=("", "_prev")
        )
        merged.rename(columns={"percentage_prev": "prev_percentage"}, inplace=True)
        merged["diff"] = merged.apply(
            lambda r: round(r["percentage"] - r["prev_percentage"], 2)
            if pd.notna(r["prev_percentage"]) else None,
            axis=1
        )
        merged["is_new"] = merged["prev_percentage"].isna()
        merged["is_removed"] = False

        # Check for returning entries (existed 2 dates ago, not in previous, back now)
        if i >= 2:
            two_ago_date = dates[i - 2]
            two_ago = df[df["log_date"] == two_ago_date]
            two_ago_keys = set(
                zip(two_ago["fund_name"], two_ago["category"], two_ago["company_sector"])
            )
            merged["is_returning"] = merged.apply(
                lambda r: r["is_new"] and (r["fund_name"], r["category"], r["company_sector"]) in two_ago_keys,
                axis=1
            )
        else:
            merged["is_returning"] = False

        results.append(merged)

        # Find removed entries (in previous but not in current)
        prev_keys = set(zip(prev["fund_name"], prev["category"], prev["company_sector"]))
        current_keys = set(zip(current["fund_name"], current["category"], current["company_sector"]))
        removed_keys = prev_keys - current_keys

        if removed_keys:
            removed_rows = []
            for fund, cat, comp in removed_keys:
                prev_row = prev[
                    (prev["fund_name"] == fund) &
                    (prev["category"] == cat) &
                    (prev["company_sector"] == comp)
                ].iloc[0]
                removed_rows.append({
                    "log_date": current_date,
                    "category": cat,
                    "fund_name": fund,
                    "company_sector": comp,
                    "percentage": None,
                    "date_of_data": prev_row["date_of_data"],
                    "url": prev_row["url"],
                    "prev_percentage": prev_row["percentage"],
                    "diff": None,
                    "is_new": False,
                    "is_removed": True,
                    "is_returning": False,
                })
            results.append(pd.DataFrame(removed_rows))

    return pd.concat(results, ignore_index=True)
