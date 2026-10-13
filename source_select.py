"""Choose, per fund and per block, which source the Top 10 tab displays.

Blocks: 'Top 10', 'Countries', 'Sectors'. Sources: manager factsheet (factsheet_data), FT
(ft_snapshot_data), Bloomberg (derived from bbg_holdings + master data). Every source yields a
Candidate; nothing is discarded, so the dashboard can show what lost and why.

Selection rule (per fund, per block):
  1. drop candidates that fail validation (parser read garbage)
  2. prefer complete over incomplete
  3. latest portfolio as-of date
  4. tie on date -> factsheet, then Bloomberg, then FT
  5. nothing complete -> latest incomplete, labelled
Alquity's own funds use their Bloomberg holdings regardless (internal data).

Staleness is measured against the snapshot date: > STALE_WARN_MONTHS shows a warning,
> STALE_SUPPRESS_MONTHS is suppressed with a note. FT's Top 10 carries no portfolio date, so it
borrows the FT sectors date and is marked inferred.
"""
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from bloomberg_loader import load_holdings, load_master_data, get_peer_funds, load_ft_snapshot_data, FT_TO_BBG_NAME
from peer_analytics import enrich_holdings

# Bloomberg fund name -> FT fund name (FT names are the identity used on the Top 10 tab)
BBG_TO_FT_NAME = {v: k for k, v in FT_TO_BBG_NAME.items()}

BLOCKS = ["Top 10", "Countries", "Sectors"]
HIERARCHY = {"internal": 0, "factsheet": 1, "bloomberg": 2, "ft": 3, "hl": 4}
STALE_WARN_MONTHS = 3
STALE_SUPPRESS_MONTHS = 12
SUM_LOW, SUM_HIGH = 90.0, 101.0      # completeness band for Countries / Sectors
MIN_ROWS = 5                          # minimum rows for a complete Countries / Sectors table
CONFLICT_TOP10_MIN_OVERLAP = 7        # names in common out of 10 before two same-date sources 'agree'
CONFLICT_PCT_TOL = 5.0                # max abs diff on a mapped label before two same-date sources 'agree'

# House mapping to Morningstar sector names (what FT uses) for cross-source comparison only.
SECTOR_TO_MS = {
    # GICS
    "Information Technology": "Technology", "Financials": "Financial Services",
    "Consumer Discretionary": "Consumer Cyclical", "Health Care": "Healthcare", "Materials": "Basic Materials",
    "Consumer Staples": "Consumer Defensive", "Industrials": "Industrials",
    "Communication Services": "Communication Services", "Real Estate": "Real Estate", "Energy": "Energy",
    "Utilities": "Utilities",
    # Morningstar (identity)
    "Technology": "Technology", "Financial Services": "Financial Services", "Consumer Cyclical": "Consumer Cyclical",
    "Healthcare": "Healthcare", "Basic Materials": "Basic Materials", "Consumer Defensive": "Consumer Defensive",
    "Other": "Other",
    # Gemway
    "Banks": "Financial Services", "Non Bank Financials": "Financial Services", "Consumer Durables": "Consumer Cyclical",
    "Consumer Non-durables": "Consumer Defensive", "Internet & Cloud": "Communication Services",
    "Telecom": "Communication Services", "Healthcare Services": "Healthcare",
    # cash-like
    "Cash": "Cash", "Cash and equivalents": "Cash", "Cash and Cash Equivalents": "Cash", "Less Liabilities": "Cash",
    "Unclassified": "Unclassified",
}
COUNTRY_ALIAS = {"Korea": "South Korea", "Korea (South)": "South Korea", "Korea, Republic of": "South Korea",
                 "USA": "United States", "US": "United States", "Viet Nam": "Vietnam",
                 "Cash and equivalents": "Cash", "Cash and Cash Equivalents": "Cash", "Others": "Other"}
COMBINED_COUNTRY_LABELS = {"China/Hong Kong", "Korea & Taiwan", "China & Hong Kong", "ASEAN", "Pakistan & Sri Lanka"}


@dataclass
class Candidate:
    fund_name: str
    block: str
    source: str
    rows: list                      # [(name, pct)] in published order
    as_of: str | None               # ISO
    as_of_inferred: bool = False
    vehicle: str = "same_fund"
    taxonomy: str = ""
    carried_forward_from: int | None = None   # snapshot_id the rows came from, if not this one
    is_alquity: bool = False
    complete: bool = False
    complete_reason: str = ""
    valid: bool = True
    valid_reason: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def label(self):
        src = {"factsheet": "Factsheet", "bloomberg": "Bloomberg", "ft": "FT", "internal": "Internal", "hl": "HL"}[self.source]
        d = self.as_of or "date unknown"
        bits = [src, "as at " + d + (" (inferred)" if self.as_of_inferred else "")]
        if self.vehicle == "sister":
            bits.append("sister vehicle")
        if self.carried_forward_from:
            bits.append("carried forward")
        if not self.complete:
            bits.append("incomplete")
        return " · ".join(bits)


@dataclass
class Selection:
    fund_name: str
    block: str
    chosen: Candidate | None
    reason: str
    flags: list
    candidates: list


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
_SUFFIX_RE = re.compile(r"\b(?:limited|ltd|incorporated|inc|corporation|corp|company|co|plc|holdings|hldgs|holding|group|"
                        r"grp|llc|llp|ag|se|nv|sa|mfg|manufacturing|class a|the)\b\.?", re.I)
_ALIAS = {"tsmc": "taiwan semiconductor", "hkex": "hong kong exchanges", "catl": "contemporary amperex",
          "zijin mng": "zijin mining", "samsung elec": "samsung electronics", "hyosung h": "hyosung heavy",
          "ase tech": "ase technology", "baba": "alibaba", "baba w": "alibaba", "ccb": "china construction",
          "ccb h": "china construction", "hdfc": "hdfc bank", "icici": "icici bank"}


def names_match(a: str, b: str) -> bool:
    """Bloomberg short names are truncated ('SAMSUNG ELECTRON', 'DELTA ELEC'), so a key that is a
    prefix of the other (5+ chars) counts as the same company."""
    ka, kb = name_key(a), name_key(b)
    if ka == kb:
        return True
    short, long_ = (ka, kb) if len(ka) <= len(kb) else (kb, ka)
    return len(short) >= 5 and long_.startswith(short)


def name_key(name: str) -> str:
    s = (name or "").replace("&", " and ")
    s = _SUFFIX_RE.sub(" ", s)
    s = re.sub(r"[^A-Za-z0-9 ]", " ", s).lower()
    toks = s.split()
    k = " ".join(toks[:2])
    return _ALIAS.get(toks[0] if toks else "", _ALIAS.get(k, k))


def norm_country(c: str) -> str:
    return COUNTRY_ALIAS.get(c, c)


def _months_between(a: str, b: str) -> float:
    da, db_ = date.fromisoformat(a), date.fromisoformat(b)
    return (db_.year - da.year) * 12 + (db_.month - da.month) + (db_.day - da.day) / 30.0


def _bbg_date_iso(s):
    if not s or not isinstance(s, str):
        return None
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", s)
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


# ---------------------------------------------------------------------------
# assessment
# ---------------------------------------------------------------------------
def assess(c: Candidate, prev_rows: list | None = None) -> None:
    """Set complete/valid on the candidate. prev_rows: last run's rows from the same source, if any."""
    rows = [(n, p) for n, p in c.rows if n]
    # validation: parser sanity
    bad = [n for n, p in rows if p is None or p < 0 or p > 100]
    # fragment test is for holding names only; 2-letter country codes are legitimate
    frag = [n for n, p in rows if len(n.strip()) < 3 or re.match(r"^\d", n.strip())] if c.block == "Top 10" else []
    if not rows:
        c.valid, c.valid_reason = False, "no rows"
    elif bad:
        c.valid, c.valid_reason = False, f"weights out of range: {bad[:3]}"
    elif frag:
        c.valid, c.valid_reason = False, f"name fragments: {frag[:3]}"
    elif c.block == "Top 10" and prev_rows and len(prev_rows) >= 5:
        overlap = sum(1 for n, _ in rows if any(names_match(n, m) for m, _ in prev_rows))
        if overlap < 6 and len(rows) >= 10:
            c.valid, c.valid_reason = False, f"only {overlap} of {len(rows)} names match last run's parse of this source"
    # completeness
    if c.block == "Top 10":
        n = len(rows)
        expected = min(10, c.extra.get("num_holdings", 10) or 10)
        c.complete = n >= expected
        c.complete_reason = f"{n} names" + ("" if c.complete else f" (expected {expected})")
    else:
        total = sum(p for _, p in rows)
        combined = [n for n, _ in rows if n in COMBINED_COUNTRY_LABELS] if c.block == "Countries" else []
        ok_sum = SUM_LOW <= total <= SUM_HIGH
        ok_rows = len(rows) >= MIN_ROWS
        c.complete = ok_sum and ok_rows and not combined
        why = []
        if not ok_sum: why.append(f"sums to {total:.1f}%")
        if not ok_rows: why.append(f"only {len(rows)} rows")
        if combined: why.append("grouped labels " + ", ".join(combined))
        c.complete_reason = "; ".join(why) if why else f"{len(rows)} rows, {total:.1f}%"
    if c.as_of is None and c.valid:
        # no date is not invalid, but it can never win against a dated candidate
        c.complete_reason += "; no as-of date"


# ---------------------------------------------------------------------------
# candidate builders
# ---------------------------------------------------------------------------
def _ft_names_by_isin(db_path: str) -> dict:
    """ISIN -> most recently seen FT fund name (table filled by the FT scrape)."""
    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute("SELECT name FROM sqlite_master WHERE name='ft_fund_isin'").fetchone():
            return {}
        rows = conn.execute("SELECT isin, fund_name FROM ft_fund_isin ORDER BY last_seen").fetchall()
        return {r[0].upper(): r[1] for r in rows}
    finally:
        conn.close()


def _snapshot_date(conn, snapshot_id):
    r = conn.execute("SELECT snapshot_date FROM bbg_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone()
    return r[0] if r else None


def factsheet_candidates(db_path: str, snapshot_id: int, peer_set: str, config_funds: list | None = None) -> list:
    """Factsheet rows for this snapshot; configured funds with no rows this snapshot carry the
    latest earlier snapshot's rows forward (same as-of date)."""
    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute("SELECT name FROM sqlite_master WHERE name='factsheet_data'").fetchone():
            return []
        df = pd.read_sql("SELECT * FROM factsheet_data WHERE snapshot_id=? AND peer_set=?", conn, params=(snapshot_id, peer_set))
        present = set(df.fund_name)
        for fund in (config_funds or []):
            if fund in present:
                continue
            prev = pd.read_sql(
                "SELECT * FROM factsheet_data WHERE fund_name=? AND peer_set=? AND snapshot_id < ? "
                "AND snapshot_id=(SELECT MAX(snapshot_id) FROM factsheet_data WHERE fund_name=? AND snapshot_id < ?)",
                conn, params=(fund, peer_set, snapshot_id, fund, snapshot_id))
            if not prev.empty:
                prev["carried_from"] = prev["snapshot_id"]
                df = pd.concat([df, prev], ignore_index=True)
    finally:
        conn.close()
    out = []
    for (fund, block), g in df.groupby(["fund_name", "block"]):
        g = g.sort_values("rank")
        out.append(Candidate(fund_name=fund, block=block, source="factsheet",
                             rows=list(zip(g["name"], g["pct"])), as_of=g["as_of"].iloc[0],
                             vehicle=g["vehicle"].iloc[0] or "same_fund", taxonomy=g["taxonomy"].iloc[0] or "",
                             carried_forward_from=int(g["carried_from"].iloc[0]) if "carried_from" in g and pd.notna(g["carried_from"].iloc[0]) else None,
                             is_alquity=bool(g["is_alquity"].iloc[0]),
                             extra={"url": g["source_url"].iloc[0]}))
    return out


def ft_candidates(db_path: str, snapshot_id: int, peer_set: str) -> list:
    df = load_ft_snapshot_data(db_path, snapshot_id, peer_set=peer_set)
    out = []
    if df.empty:
        return out
    for fund, g in df.groupby("fund_name"):
        sec = g[g.category == "Sectors"].sort_values("percentage", ascending=False)
        hold = g[g.category == "Holdings"].sort_values("percentage", ascending=False).head(10)
        sec_date = sec.date_of_data.iloc[0] if len(sec) else None
        alq = bool(g.is_alquity.iloc[0])
        if len(hold):
            out.append(Candidate(fund, "Top 10", "ft", list(zip(hold["name"], hold["percentage"])),
                                 as_of=sec_date, as_of_inferred=True, taxonomy="", is_alquity=alq))
        if len(sec):
            out.append(Candidate(fund, "Sectors", "ft", list(zip(sec["name"], sec["percentage"])),
                                 as_of=sec_date, taxonomy="Morningstar", is_alquity=alq))
    return out


def bloomberg_candidates(db_path: str, snapshot_id: int, peer_set: str, unmapped: list | None = None) -> list:
    """Top 10 / Countries / Sectors derived from Bloomberg holdings, keyed by the FT fund name.
    Bloomberg funds with no FT name in FT_TO_BBG_NAME are skipped and reported via `unmapped`."""
    peers = get_peer_funds(db_path, snapshot_id, peer_set)
    if peers.empty:
        return []
    holdings = load_holdings(db_path, snapshot_id, peer_set, exclude_cash=False, min_weight=0.0)
    if holdings.empty:
        return []
    df = enrich_holdings(holdings, load_master_data(db_path, snapshot_id))
    dates = dict(zip(peers.fund_name, peers.holdings_date))
    isins = dict(zip(peers.fund_name, peers.fund_isin))
    isin_to_ft = _ft_names_by_isin(db_path)
    out = []
    for bbg_fund, g in df.groupby("fund_name"):
        # bridge Bloomberg -> FT name: by ISIN (recorded at scrape time) first, then the manual map
        fund = isin_to_ft.get((isins.get(bbg_fund) or "").upper()) or BBG_TO_FT_NAME.get(bbg_fund)
        if not fund:
            if unmapped is not None:
                unmapped.append(bbg_fund)
            continue
        as_of = _bbg_date_iso(dates.get(bbg_fund))
        alq = bool(g.is_alquity.iloc[0])
        src = "internal" if alq else "bloomberg"
        g = g[g.weight > 0]
        eq = g[g.is_cash == 0]
        cash = float(g[g.is_cash == 1].weight.sum())
        top = eq.sort_values("weight", ascending=False).head(10)
        names = top["short_name"].fillna(top["ticker"])
        out.append(Candidate(fund, "Top 10", src, list(zip(names, top["weight"].round(2))), as_of=as_of,
                             is_alquity=alq, extra={"num_holdings": int(len(eq)), "bbg_name": bbg_fund}))
        cty = eq.assign(country=eq["country"].fillna("Unclassified")).groupby("country")["weight"].sum().sort_values(ascending=False)
        rows = [(k, round(v, 2)) for k, v in cty.items()] + ([("Cash", round(cash, 2))] if cash > 0 else [])
        out.append(Candidate(fund, "Countries", src, rows, as_of=as_of, taxonomy="country", is_alquity=alq,
                             extra={"bbg_name": bbg_fund}))
        sec = eq.assign(gics=eq["gics_sector"].fillna("Unclassified")).groupby("gics")["weight"].sum().sort_values(ascending=False)
        rows = [(k, round(v, 2)) for k, v in sec.items()] + ([("Cash", round(cash, 2))] if cash > 0 else [])
        out.append(Candidate(fund, "Sectors", src, rows, as_of=as_of, taxonomy="GICS", is_alquity=alq,
                             extra={"coverage": round(100 - sec.get("Unclassified", 0.0), 1), "bbg_name": bbg_fund}))
    return out


def build_candidates(db_path: str, snapshot_id: int, peer_set: str, config_funds: list | None = None,
                     prev_snapshot_id: int | None = None, unmapped: list | None = None) -> list:
    cands = (factsheet_candidates(db_path, snapshot_id, peer_set, config_funds)
             + ft_candidates(db_path, snapshot_id, peer_set)
             + bloomberg_candidates(db_path, snapshot_id, peer_set, unmapped))
    prev = {}
    if prev_snapshot_id:
        for c in (factsheet_candidates(db_path, prev_snapshot_id, peer_set) + ft_candidates(db_path, prev_snapshot_id, peer_set)):
            prev[(c.fund_name, c.block, c.source)] = c.rows
    for c in cands:
        assess(c, prev.get((c.fund_name, c.block, c.source)))
    return cands


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------
def _conflicts(a: Candidate, b: Candidate) -> str | None:
    """Return a description if two same-date complete candidates disagree materially."""
    if a.block == "Top 10":
        ov = sum(1 for n, _ in a.rows if any(names_match(n, m) for m, _ in b.rows))
        return f"only {ov} of 10 names in common" if ov < CONFLICT_TOP10_MIN_OVERLAP else None
    if a.block == "Sectors":
        ma = {}; mb = {}
        for n, p in a.rows: ma[SECTOR_TO_MS.get(n, n)] = ma.get(SECTOR_TO_MS.get(n, n), 0) + p
        for n, p in b.rows: mb[SECTOR_TO_MS.get(n, n)] = mb.get(SECTOR_TO_MS.get(n, n), 0) + p
    else:
        ma = {norm_country(n): p for n, p in a.rows}; mb = {norm_country(n): p for n, p in b.rows}
    worst = max(((k, abs(ma.get(k, 0) - mb.get(k, 0))) for k in set(ma) | set(mb) if k not in ("Cash", "Other", "Unclassified")),
                key=lambda x: x[1], default=(None, 0))
    return f"{worst[0]} differs by {worst[1]:.1f} pts" if worst[1] > CONFLICT_PCT_TOL else None


def select(fund: str, block: str, cands: list, snapshot_date: str, accept_stale: bool = False) -> Selection:
    cands = [c for c in cands if c.fund_name == fund and c.block == block]
    flags = []
    if not cands:
        return Selection(fund, block, None, "no candidates from any source", ["no_candidates"], cands)
    is_alq = any(c.is_alquity for c in cands)
    pool = [c for c in cands if c.valid]
    if is_alq and any(c.source == "internal" for c in pool):
        pool = [c for c in pool if c.source == "internal"]
    for c in cands:
        if not c.valid:
            flags.append(f"invalid:{c.source}:{c.valid_reason}")
    if not pool:
        return Selection(fund, block, None, "every candidate failed validation", flags + ["all_invalid"], cands)
    complete = [c for c in pool if c.complete]
    use = complete or pool
    if not complete:
        flags.append("incomplete_only")

    def rank(c):
        return (c.as_of or "0000-00-00", -HIERARCHY[c.source])
    use.sort(key=rank, reverse=True)
    chosen = use[0]
    reason = ("latest complete" if complete else "latest incomplete (nothing complete)") + f" · {chosen.source}"
    same_date = [c for c in use[1:] if c.as_of == chosen.as_of and c.complete == chosen.complete]
    if same_date:
        reason += f" wins tie on date vs {', '.join(c.source for c in same_date)}"
        for c in same_date:
            conf = _conflicts(chosen, c)
            if conf:
                flags.append(f"conflict:{chosen.source} vs {c.source}: {conf}")
    if chosen.as_of and snapshot_date:
        age = _months_between(chosen.as_of, snapshot_date)
        if age > STALE_SUPPRESS_MONTHS and not accept_stale:
            flags.append(f"stale_suppressed:{age:.0f} months")
            return Selection(fund, block, None, f"best available ({chosen.label}) is older than {STALE_SUPPRESS_MONTHS} months; suppressed", flags, cands)
        if age > STALE_WARN_MONTHS:
            flags.append(f"stale_warn:{age:.0f} months")
    elif not chosen.as_of:
        flags.append("undated")
    if chosen.vehicle == "sister":
        flags.append("sister_vehicle")
    if chosen.carried_forward_from:
        flags.append(f"carried_forward:{chosen.carried_forward_from}")
    return Selection(fund, block, chosen, reason, flags, cands)


def select_all(db_path: str, snapshot_id: int, peer_set: str, config_funds: list | None = None,
               prev_snapshot_id: int | None = None, unmapped: list | None = None) -> dict:
    """{(fund, block): Selection} for every fund that has any candidate in this peer set.
    Funds are keyed by FT name; Alquity's own fund sorts first."""
    conn = sqlite3.connect(db_path)
    try:
        snap_date = _snapshot_date(conn, snapshot_id)
    finally:
        conn.close()
    cands = build_candidates(db_path, snapshot_id, peer_set, config_funds, prev_snapshot_id, unmapped)
    funds = sorted({c.fund_name for c in cands}, key=lambda f: (not any(c.is_alquity for c in cands if c.fund_name == f), f))
    return {(f, b): select(f, b, cands, snap_date) for f in funds for b in BLOCKS}


# ---------------------------------------------------------------------------
# config rules: decisions already taken, so the same question is not asked every month
#   source_rules:
#     - fund: "<FT fund name>"      (required)
#       block: Countries            (optional; omitted = all blocks)
#       accept_incomplete: true     (show the incomplete table without asking)
#       allow_sister: true          (sister-vehicle factsheet approved for this fund)
#       accept_stale: true          (show even beyond the suppress threshold)
# ---------------------------------------------------------------------------
def _rule(rules: list, fund: str, block: str) -> dict:
    out = {}
    for r in rules or []:
        if r.get("fund") == fund and (not r.get("block") or r.get("block") == block):
            out.update({k: v for k, v in r.items() if k not in ("fund", "block")})
    return out


# ---------------------------------------------------------------------------
# run summary (notices + questions)
# ---------------------------------------------------------------------------
def unmapped_labels(selections: dict) -> dict:
    out = {"Sectors": set(), "Countries": set()}
    for (fund, block), sel in selections.items():
        for c in sel.candidates:
            if block == "Sectors":
                out["Sectors"].update(n for n, _ in c.rows if n not in SECTOR_TO_MS)
            elif block == "Countries":
                out["Countries"].update(n for n, _ in c.rows if n in COMBINED_COUNTRY_LABELS)
    return out


def run_summary(db_path: str, snapshot_id: int, peer_sets: list, config: dict, prev_snapshot_id: int | None = None) -> str:
    """Human-readable summary of what the Top 10 tab will show and what needs a decision."""
    cfg_funds = {}
    for fs in config.get("factsheets", []) or []:
        cfg_funds.setdefault(fs.get("peer_set", ""), []).append(fs["fund"])
    rules = config.get("source_rules", []) or []
    questions, notices, chosen_lines = [], [], []
    for ps in peer_sets:
        unmapped = []
        sels = select_all(db_path, snapshot_id, ps, cfg_funds.get(ps), prev_snapshot_id, unmapped)
        prev = select_all(db_path, prev_snapshot_id, ps, cfg_funds.get(ps)) if prev_snapshot_id else {}
        for (fund, block), sel in sels.items():
            short = fund[:55]
            rule = _rule(rules, fund, block)
            if sel.chosen:
                chosen_lines.append(f"  {ps:5s} {short:55s} {block:9s} -> {sel.chosen.label}")
            else:
                chosen_lines.append(f"  {ps:5s} {short:55s} {block:9s} -> (nothing shown) {sel.reason}")
            for fl in sel.flags:
                kind = fl.split(":")[0]
                line = f"{ps} / {short} / {block}: {fl}"
                ask = kind in ("invalid", "conflict", "incomplete_only", "stale_suppressed", "all_invalid", "sister_vehicle")
                if kind == "incomplete_only" and rule.get("accept_incomplete"):
                    ask = False
                if kind == "sister_vehicle" and rule.get("allow_sister"):
                    ask = False
                if kind == "stale_suppressed" and rule.get("accept_stale"):
                    ask = False
                (questions if ask else notices).append(line + ("" if ask else "  [accepted by source_rules]" if kind in ("incomplete_only", "sister_vehicle", "stale_suppressed") else ""))
        for b in sorted(set(unmapped)):
            questions.append(f"{ps}: Bloomberg fund '{b}' has no FT name in FT_TO_BBG_NAME (bloomberg_loader.py) - its holdings cannot be offered to the Top 10 tab")
            p = prev.get((fund, block))
            if p and p.chosen and sel.chosen and p.chosen.source != sel.chosen.source:
                notices.append(f"{ps} / {short} / {block}: source changed {p.chosen.source} -> {sel.chosen.source} (arrows paused this month)")
        um = unmapped_labels(sels)
        for n in sorted(um["Sectors"]):
            questions.append(f"{ps}: sector label '{n}' has no Morningstar mapping - add it to SECTOR_TO_MS in source_select.py")
        for n in sorted(um["Countries"]):
            notices.append(f"{ps}: grouped country label '{n}' seen - treated as incomplete, cannot map to a single country")
        for fund in cfg_funds.get(ps, []):
            if not any(f == fund for (f, _) in sels):
                questions.append(f"{ps}: configured factsheet fund '{fund}' has no data from any source this run")
    out = ["Top 10 tab sources (selection rule: valid > complete > latest as-of > factsheet/Bloomberg/FT)", *chosen_lines]
    out += ["", f"QUESTIONS ({len(questions)}) - need a decision or a parser fix:"] + ([f"  - {q}" for q in questions] or ["  none"])
    out += ["", f"NOTICES ({len(notices)}):"] + ([f"  - {n}" for n in notices] or ["  none"])
    return "\n".join(out)
