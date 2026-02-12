"""Bloomberg Excel ingestion and SQLite storage for peer holdings analysis."""

import sqlite3
import re
import logging
from pathlib import Path
from datetime import datetime

import openpyxl
import pandas as pd

from exchange_country_map import get_country_from_exchange, get_country_from_bbg_code

logger = logging.getLogger(__name__)

PEER_SHEETS = ["India", "Asia", "FW"]

# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS bbg_snapshots (
        snapshot_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date  TEXT NOT NULL,
        file_name      TEXT NOT NULL,
        ingested_at    TEXT NOT NULL,
        UNIQUE(snapshot_date, file_name)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS bbg_peer_groups (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id    INTEGER NOT NULL REFERENCES bbg_snapshots(snapshot_id),
        fund_name      TEXT NOT NULL,
        fund_isin      TEXT,
        peer_set       TEXT NOT NULL,
        is_alquity     INTEGER NOT NULL DEFAULT 0,
        holdings_date  TEXT,
        has_holdings   INTEGER NOT NULL DEFAULT 1,
        sheet_name     TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS bbg_holdings (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id    INTEGER NOT NULL REFERENCES bbg_snapshots(snapshot_id),
        fund_name      TEXT NOT NULL,
        ticker         TEXT NOT NULL,
        weight         REAL NOT NULL,
        exchange_code  TEXT,
        country_derived TEXT,
        is_cash        INTEGER NOT NULL DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS bbg_master_data (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id    INTEGER NOT NULL REFERENCES bbg_snapshots(snapshot_id),
        ticker         TEXT NOT NULL,
        short_name     TEXT,
        gics_industry  TEXT,
        gics_sector    TEXT,
        country_bbg    TEXT,
        market_cap_raw TEXT,
        market_cap_usd REAL,
        isin           TEXT,
        bb_unique_id   TEXT
    );
    """,
]

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_bbg_holdings_snapshot ON bbg_holdings(snapshot_id);",
    "CREATE INDEX IF NOT EXISTS idx_bbg_holdings_fund ON bbg_holdings(snapshot_id, fund_name);",
    "CREATE INDEX IF NOT EXISTS idx_bbg_holdings_ticker ON bbg_holdings(ticker);",
    "CREATE INDEX IF NOT EXISTS idx_bbg_peers_snapshot ON bbg_peer_groups(snapshot_id);",
    "CREATE INDEX IF NOT EXISTS idx_bbg_master_ticker ON bbg_master_data(snapshot_id, ticker);",
    "CREATE INDEX IF NOT EXISTS idx_bbg_master_isin ON bbg_master_data(snapshot_id, isin);",
]


def init_bbg_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        for sql in CREATE_TABLES_SQL:
            conn.execute(sql)
        for sql in CREATE_INDEXES_SQL:
            conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_bloomberg_excel(file_path: str, db_path: str, replace: bool = False) -> dict:
    """Ingest a Bloomberg peer holdings Excel file into the database.

    Returns summary dict with counts and any errors.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    init_bbg_db(db_path)

    snapshot_date = _extract_snapshot_date(path.name)
    file_name = path.name

    conn = sqlite3.connect(db_path)
    try:
        # Check for existing snapshot
        existing = conn.execute(
            "SELECT snapshot_id FROM bbg_snapshots WHERE snapshot_date=? AND file_name=?",
            (snapshot_date, file_name),
        ).fetchone()

        if existing:
            if replace:
                sid = existing[0]
                conn.execute("DELETE FROM bbg_holdings WHERE snapshot_id=?", (sid,))
                conn.execute("DELETE FROM bbg_peer_groups WHERE snapshot_id=?", (sid,))
                conn.execute("DELETE FROM bbg_master_data WHERE snapshot_id=?", (sid,))
                conn.execute("DELETE FROM bbg_snapshots WHERE snapshot_id=?", (sid,))
                conn.commit()
            else:
                return {"error": f"Snapshot {snapshot_date} / {file_name} already exists. Use replace=True to overwrite."}

        wb = openpyxl.load_workbook(str(path), data_only=True)

        # Insert snapshot
        cur = conn.execute(
            "INSERT INTO bbg_snapshots (snapshot_date, file_name, ingested_at) VALUES (?, ?, ?)",
            (snapshot_date, file_name, datetime.now().isoformat()),
        )
        snapshot_id = cur.lastrowid

        # Parse peer groups
        no_bbg_isins = _parse_no_bbg_holdings(wb)
        peer_rows, errors = _parse_peer_sheets(wb, snapshot_id, no_bbg_isins)

        # Match fund names to sheet names and parse holdings
        sheet_names = wb.sheetnames
        holdings_rows = []
        for pr in peer_rows:
            if not pr["has_holdings"]:
                continue
            sheet = _match_fund_to_sheet(pr["fund_name"], sheet_names)
            if sheet:
                pr["sheet_name"] = sheet
                fund_holdings = _parse_fund_sheet(wb[sheet], pr["fund_name"])
                holdings_rows.extend(
                    (snapshot_id, pr["fund_name"], h["ticker"], h["weight"],
                     h["exchange_code"], h["country_derived"], h["is_cash"])
                    for h in fund_holdings
                )
            else:
                pr["has_holdings"] = 0
                errors.append(f"No matching sheet for: {pr['fund_name']}")

        # Insert peer groups
        conn.executemany(
            "INSERT INTO bbg_peer_groups "
            "(snapshot_id, fund_name, fund_isin, peer_set, is_alquity, holdings_date, has_holdings, sheet_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (snapshot_id, pr["fund_name"], pr["fund_isin"], pr["peer_set"],
                 pr["is_alquity"], pr["holdings_date"], pr["has_holdings"], pr.get("sheet_name"))
                for pr in peer_rows
            ],
        )

        # Insert holdings
        conn.executemany(
            "INSERT INTO bbg_holdings "
            "(snapshot_id, fund_name, ticker, weight, exchange_code, country_derived, is_cash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            holdings_rows,
        )

        # Parse and insert master data
        master_rows = _parse_master_data(wb["master_data"], snapshot_id)
        conn.executemany(
            "INSERT INTO bbg_master_data "
            "(snapshot_id, ticker, short_name, gics_industry, gics_sector, "
            "country_bbg, market_cap_raw, market_cap_usd, isin, bb_unique_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            master_rows,
        )

        conn.commit()

        summary = {
            "snapshot_id": snapshot_id,
            "snapshot_date": snapshot_date,
            "num_funds": len(peer_rows),
            "num_funds_with_holdings": sum(1 for p in peer_rows if p["has_holdings"]),
            "num_holdings": len(holdings_rows),
            "num_master_rows": len(master_rows),
            "errors": errors,
        }
        logger.info("Ingested %s: %s", file_name, summary)
        return summary

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _extract_snapshot_date(filename: str) -> str:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if m:
        return m.group(1)
    return datetime.now().strftime("%Y-%m-%d")


def _parse_no_bbg_holdings(wb) -> set:
    """Return set of ISINs from 'No bbg holdings' sheet."""
    isins = set()
    if "No bbg holdings" not in wb.sheetnames:
        return isins
    ws = wb["No bbg holdings"]
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row and row[1]:
            isins.add(str(row[1]).strip())
    return isins


def _parse_peer_sheets(wb, snapshot_id: int, no_bbg_isins: set) -> tuple:
    """Parse India/Asia/FW sheets. Returns (list of peer dicts, list of error strings)."""
    peers = []
    errors = []
    for sheet_name in PEER_SHEETS:
        if sheet_name not in wb.sheetnames:
            errors.append(f"Peer sheet '{sheet_name}' not found")
            continue
        ws = wb[sheet_name]
        for idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
            if not row or not row[0]:
                continue
            name = str(row[0]).strip()
            isin = str(row[1]).strip() if row[1] else None
            holdings_date = None
            if row[3] and not str(row[3]).startswith("#"):
                raw = row[3]
                if isinstance(raw, datetime):
                    holdings_date = raw.strftime("%Y-%m-%d")
                else:
                    holdings_date = str(raw).strip()

            has_holdings = 1
            if isin and isin in no_bbg_isins:
                has_holdings = 0

            peers.append({
                "fund_name": name,
                "fund_isin": isin,
                "peer_set": sheet_name,
                "is_alquity": 1 if idx == 0 else 0,
                "holdings_date": holdings_date,
                "has_holdings": has_holdings,
                "sheet_name": None,
            })
    return peers, errors


def _match_fund_to_sheet(fund_name: str, sheet_names: list) -> str | None:
    """Match a fund name to a potentially truncated Excel sheet name (max 31 chars)."""
    for sn in sheet_names:
        stripped = sn.rstrip()
        if fund_name.startswith(stripped) or stripped.startswith(fund_name[:30]):
            # Verify it's a holdings sheet (not a peer/meta sheet)
            if stripped in ("master_data", "Process", "India", "Asia", "FW", "No bbg holdings"):
                continue
            return sn
    return None


def _parse_fund_sheet(ws, fund_name: str) -> list:
    """Parse a single fund's holdings sheet into list of dicts."""
    holdings = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue
        ticker = str(row[0]).strip()
        if ticker.startswith("#N/A") or not ticker:
            continue

        weight = 0.0
        if row[1] is not None:
            try:
                weight = float(row[1])
            except (ValueError, TypeError):
                continue

        is_cash = 1 if "Curncy" in ticker else 0
        exchange_code = _extract_exchange_code(ticker)
        country_derived = get_country_from_exchange(exchange_code)

        holdings.append({
            "ticker": ticker,
            "weight": weight,
            "exchange_code": exchange_code,
            "country_derived": country_derived,
            "is_cash": is_cash,
        })
    return holdings


def _extract_exchange_code(ticker: str) -> str | None:
    """Extract exchange code from Bloomberg ticker like 'HDFCB IN Equity' -> 'IN'."""
    parts = ticker.split()
    if len(parts) >= 3 and parts[-1] == "Equity":
        return parts[-2]
    return None


def _parse_master_data(ws, snapshot_id: int) -> list:
    """Parse master_data sheet into list of tuples for insertion."""
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue
        ticker = str(row[0]).strip()
        short_name = str(row[1]).strip() if row[1] else None
        gics_industry = str(row[2]).strip() if row[2] else None
        gics_sector = str(row[3]).strip() if row[3] else None
        country_bbg = str(row[4]).strip() if row[4] else None
        market_cap_raw = str(row[5]).strip() if row[5] else None
        market_cap_usd = _parse_market_cap(market_cap_raw)
        isin = str(row[6]).strip() if row[6] else None
        bb_unique_id = str(row[7]).strip() if row[7] else None

        rows.append((
            snapshot_id, ticker, short_name, gics_industry, gics_sector,
            country_bbg, market_cap_raw, market_cap_usd, isin, bb_unique_id,
        ))
    return rows


def _parse_market_cap(raw: str | None) -> float | None:
    """Parse Bloomberg market cap strings to millions. '21.22B' -> 21220.0, '916.81M' -> 916.81."""
    if not raw or raw.startswith("#"):
        return None
    raw = raw.strip()
    try:
        if raw.upper().endswith("T"):
            return float(raw[:-1]) * 1_000_000
        elif raw.upper().endswith("B"):
            return float(raw[:-1]) * 1_000
        elif raw.upper().endswith("M"):
            return float(raw[:-1])
        elif raw.upper().endswith("K"):
            return float(raw[:-1]) / 1_000
        else:
            return float(raw)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Query helpers (used by dashboard)
# ---------------------------------------------------------------------------

def get_available_snapshots(db_path: str) -> list:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT snapshot_id, snapshot_date, file_name, ingested_at "
            "FROM bbg_snapshots ORDER BY snapshot_date DESC"
        )
        return [
            {"snapshot_id": r[0], "snapshot_date": r[1], "file_name": r[2], "ingested_at": r[3]}
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def get_peer_funds(db_path: str, snapshot_id: int, peer_set: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            "SELECT * FROM bbg_peer_groups WHERE snapshot_id=? AND peer_set=? ORDER BY is_alquity DESC, fund_name",
            conn, params=(snapshot_id, peer_set),
        )
    finally:
        conn.close()


def load_holdings(db_path: str, snapshot_id: int, peer_set: str,
                  exclude_cash: bool = True, min_weight: float = 0.0) -> pd.DataFrame:
    """Load all holdings for funds in a given peer set, joined with peer group info."""
    conn = sqlite3.connect(db_path)
    try:
        query = """
            SELECT h.fund_name, h.ticker, h.weight, h.exchange_code, h.country_derived, h.is_cash,
                   p.is_alquity, p.peer_set
            FROM bbg_holdings h
            JOIN bbg_peer_groups p ON h.snapshot_id = p.snapshot_id AND h.fund_name = p.fund_name
            WHERE h.snapshot_id = ? AND p.peer_set = ? AND p.has_holdings = 1
        """
        params = [snapshot_id, peer_set]
        if exclude_cash:
            query += " AND h.is_cash = 0"
        if min_weight > 0:
            query += " AND h.weight > ?"
            params.append(min_weight)
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def load_master_data(db_path: str, snapshot_id: int) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            "SELECT ticker, short_name, gics_industry, gics_sector, country_bbg, "
            "market_cap_usd, isin FROM bbg_master_data WHERE snapshot_id=?",
            conn, params=(snapshot_id,),
        )
    finally:
        conn.close()
