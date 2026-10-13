"""Manager factsheet fetch + parse into the peer database.

Each configured fund (config.yaml -> factsheets:) names a URL and a parser. A parser turns the
PDF into three blocks - 'Top 10', 'Countries', 'Sectors' - each a list of (name, pct) as
published, plus the portfolio as-of date read from the sheet. Rows are stored in
factsheet_data against the run's snapshot so source_select.py can weigh them against FT and
Bloomberg. Nothing here decides what is displayed; that is source_select.py's job.
"""
import io
import re
import logging
import sqlite3
from datetime import datetime, date
from calendar import monthrange

import requests
import pdfplumber

logger = logging.getLogger(__name__)

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS factsheet_data (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id  INTEGER NOT NULL REFERENCES bbg_snapshots(snapshot_id),
    fund_name    TEXT NOT NULL,
    peer_set     TEXT NOT NULL DEFAULT '',
    is_alquity   INTEGER NOT NULL DEFAULT 0,
    block        TEXT NOT NULL,          -- 'Top 10' | 'Countries' | 'Sectors'
    rank         INTEGER NOT NULL,
    name         TEXT NOT NULL,
    pct          REAL NOT NULL,
    as_of        TEXT,                   -- ISO portfolio date read from the sheet
    source_url   TEXT,
    parser       TEXT,
    vehicle      TEXT,                   -- same_class | same_fund | sister
    taxonomy     TEXT,                   -- e.g. GICS, Morningstar, manager
    fetched_at   TEXT NOT NULL
);
"""
CREATE_IDX = "CREATE INDEX IF NOT EXISTS idx_factsheet_snap ON factsheet_data(snapshot_id, fund_name, block);"


def init_factsheet_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(CREATE_SQL)
        conn.execute(CREATE_IDX)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1)}
MONTHS.update({m: i for i, m in enumerate(
    ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"], 1)})
MONTHS.update({m: i for i, m in enumerate(
    ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"], 1)})


def _iso(y, m, d):
    return date(int(y), int(m), int(d)).isoformat()


def _date_from_words(day, month_word, year):
    m = MONTHS.get(month_word.lower())
    return _iso(year, m, day) if m else None


def _month_end(month_word, year):
    m = MONTHS.get(month_word.lower())
    if not m:
        return None
    return _iso(year, m, monthrange(int(year), m)[1])


# ---------------------------------------------------------------------------
# Parsers. Each takes a pdfplumber document and returns a dict:
#   {as_of, top10:[(name,pct)], countries:[(name,pct)], sectors:[(name,pct)], taxonomy:{countries, sectors}}
# ---------------------------------------------------------------------------
def _pages(doc, dedupe=False):
    return [(p.dedupe_chars() if dedupe else p).extract_text() or "" for p in doc.pages]


def _f(x):
    return float(x.replace(",", "."))


def parse_matthews(doc):
    text = "\n".join(_pages(doc))
    countries = "China/Hong Kong|Taiwan|South Korea|India|Singapore|Malaysia|Thailand|Vietnam|Indonesia|Philippines|Macau|Japan|Australia|United States"
    sectors = ("Information Technology|Financials|Consumer Discretionary|Industrials|Communication Services|Health Care|"
               "Materials|Real Estate|Energy|Consumer Staples|Utilities")
    top, sec, cty = [], [], []
    for line in text.split("\n"):
        m = re.match(r"^(.+?) (" + countries + r") (\d+\.\d)%", line)
        if m and len(top) < 10 and "ALLOCATION" not in line:
            top.append((m.group(1).strip(), _f(m.group(3))))
        for m in re.finditer(r"(" + sectors + r") (-?\d+\.\d) (-?\d+\.\d) (-?\d+\.\d)", line):
            sec.append((m.group(1), _f(m.group(2))))
        for m in re.finditer(r"(" + countries + r") (-?\d+\.\d) (-?\d+\.\d) (-?\d+\.\d)", line):
            cty.append((m.group(1), _f(m.group(2))))
    m = re.search(r"Class I Shares (\d{1,2}) (\w+) (\d{4})", text)
    as_of = _date_from_words(*m.groups()) if m else None
    return dict(as_of=as_of, top10=top, countries=cty, sectors=sec, taxonomy=dict(countries="country", sectors="GICS"))


def parse_stewart_fsi(doc):
    """First Sentier / Stewart Investors auto-generated factsheet (Irish VCC or UK OEIC)."""
    p = doc.pages[0]
    words = p.extract_words()
    top = []
    ten = [w for w in words if w["text"] == "Ten"]
    if ten:
        y0 = ten[0]["top"]
        below = [w["top"] for w in words if w["text"] == "Sector" and w["top"] > y0]
        y1 = min(below) if below else y0 + 150
        crop = p.crop((ten[0]["x0"] - 5, y0, p.width, y1)).extract_text() or ""
        top = [(m.group(1).strip(), _f(m.group(2)))
               for m in re.finditer(r"([A-Z][A-Za-z.,()&'\- ]+?) (\d+\.\d)(?=\s|$)", crop)][:10]
    text = "\n".join(_pages(doc))
    cty = [(m.group(1), _f(m.group(2))) for m in re.finditer(
        r"^(Taiwan|China|South Korea|India|Hong Kong|Singapore|Japan|Indonesia|USA|United States|Australia|"
        r"Philippines|Thailand|Malaysia|Vietnam|Other|Cash and Cash Equivalents) (\d+\.\d)% \(", text, re.M)]
    m = re.search(r"Fund Factsheet\s+(\d{1,2}) (\w+) (\d{4})", text)
    as_of = _date_from_words(*m.groups()) if m else None
    # sector breakdown is a chart only on these sheets
    return dict(as_of=as_of, top10=top, countries=cty, sectors=[], taxonomy=dict(countries="country", sectors=""))


def parse_fidelity(doc):
    pages = _pages(doc)
    text = "\n".join(pages)
    cl = (r"China|India|Australia|Taiwan|Hong Kong|Korea \(South\)|Singapore|Canada|New Zealand|Thailand|Others|Japan|"
          r"Indonesia|Malaysia|Philippines|Vietnam|United States|United Kingdom")
    sl = ("Information Technology|Materials|Industrials|Communication Services|Financials|Consumer Discretionary|"
          "Consumer Staples|Health Care|Energy|Real Estate|Utilities")
    cty, sec, top = [], [], []
    i, j = text.find("Geographic Exposure"), text.find("Sector/Industry Exposure")
    for m in re.finditer(r"(" + cl + r") (\d+\.\d) (\d+\.\d) (-?\d+\.\d)", text[i:j] if i >= 0 else ""):
        cty.append((m.group(1).replace("Korea (South)", "South Korea"), _f(m.group(2))))
    k = text.find("Top Positions")
    for m in re.finditer(r"^(" + sl + r") (\d+\.\d) (\d+\.\d) (-?\d+\.\d)$", text[j:k] if j >= 0 else "", re.M):
        sec.append((m.group(1), _f(m.group(2))))
    # pdfplumber emits a wrapped name's tail ("CO LTD") on the line AFTER its row, so a short
    # all-caps line with no digits is appended to the previous holding.
    for line in (text[k:].split("\n")[2:] if k >= 0 else []):
        if line.startswith("FIDELITY FUNDS") or line.startswith("ESG") or line.startswith("mf."):
            break
        m = re.match(r"^(.*?)\s*(" + sl + r") (" + cl + r") (\d+\.\d) (\d+\.\d) (-?\d+\.\d)$", line)
        if m and len(top) < 10:
            top.append((m.group(1).strip(), _f(m.group(4))))
        elif top and re.fullmatch(r"[A-Z][A-Z&.' -]{1,30}", line.strip()) and not re.search(r"\d", line):
            top[-1] = (top[-1][0] + " " + line.strip(), top[-1][1])
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{2}) Monthly Factsheet", text)
    as_of = _iso(2000 + int(m.group(3)), m.group(2), m.group(1)) if m else None
    return dict(as_of=as_of, top10=top, countries=cty, sectors=sec, taxonomy=dict(countries="country", sectors="GICS"))


JPM_COUNTRY = {"Taiwán": "Taiwan", "Corea": "South Korea", "China": "China", "India": "India", "Hong Kong": "Hong Kong",
               "Singapur": "Singapore", "Malasia": "Malaysia", "Tailandia": "Thailand", "Indonesia": "Indonesia",
               "Filipinas": "Philippines", "Liquidez": "Cash", "Vietnam": "Vietnam", "Otros": "Other"}
JPM_SECTOR = {"Tecnología": "Information Technology", "Finanzas": "Financials",
              "Bienes de consumo discrecional": "Consumer Discretionary", "Industria": "Industrials",
              "Servicios de comunicación": "Communication Services", "Materiales básicos": "Materials",
              "Sanidad": "Health Care", "Bienes de consumo básico": "Consumer Staples", "Energía": "Energy",
              "Servicios": "Utilities", "Inmobiliarias": "Real Estate", "Liquidez": "Cash"}


def parse_jpm_es(doc):
    """JPM Spanish-language sheet (Finect). Text comes out with doubled glyphs; dedupe first.
    Country and sector labels are translated to English/GICS so they compare with other sources."""
    lines = "\n".join(_pages(doc, dedupe=True)).split("\n")
    cl = "|".join(re.escape(k) for k in JPM_COUNTRY)
    sl = "|".join(re.escape(k) for k in sorted(JPM_SECTOR, key=len, reverse=True))
    cty, sec, top = [], [], []
    in_top = False
    for line in lines:
        if line.startswith("Princ. 10"):
            in_top = True
        m = re.search(r"(" + cl + r") (\d+,\d) ([+-]?\d+,\d)$", line)
        if m and not sec:
            cty.append((JPM_COUNTRY[m.group(1)], _f(m.group(2))))
            continue
        m = re.search(r"(" + sl + r") (\d+,\d) ([+-]?\d+,\d)$", line)
        if m and "REGIONES" not in line:
            sec.append((JPM_SECTOR[m.group(1)], _f(m.group(2))))
        if in_top and len(top) < 10:
            m2 = re.match(r"^([A-Z][A-Za-z .&\-]+?) (?:(" + sl + r"|Bienes de consumo) )?(\d+,\d)(?: |$)", line)
            if m2 and m2.group(1) not in JPM_SECTOR and not m2.group(1).startswith("n "):
                top.append((m2.group(1).strip(), _f(m2.group(3))))
    text = "\n".join(lines)
    as_of = None
    for m in re.finditer(r"(\d{1,2}) ([A-Za-zéû]+) (\d{4})", text):
        if m.group(2).lower() in MONTHS:
            as_of = _date_from_words(*m.groups())
            break
    return dict(as_of=as_of, top10=top, countries=cty, sectors=sec, taxonomy=dict(countries="country", sectors="GICS"))


def parse_veritas(doc):
    text = "\n".join(_pages(doc))
    sl = ("Information Technology|Industrials|Communication Services|Health Care|Financials|Consumer Discretionary|"
          "Materials|Real Estate|Consumer Staples|Energy|Utilities|Cash and equivalents")
    cl = ("Taiwan|China|South Korea|India|Hong Kong|Singapore|Thailand|Japan|United States|Australia|Indonesia|"
          "Malaysia|Philippines|Vietnam|Cash and equivalents")
    top = [(m.group(1).strip(), _f(m.group(3))) for m in re.finditer(r"^(.+?) (" + sl + r") (\d+\.\d)$", text, re.M)][:10]
    i, j = text.find("Sector % Fund Country % Fund"), text.find("Please note")
    block = text[i:j] if i >= 0 else ""
    sec, cty, seen = [], [], set()
    for m in re.finditer(r"(?:^|\s)(" + sl + r") (\d+\.\d)(?=\s|$)", block):
        if ("s", m.group(1)) not in seen:
            seen.add(("s", m.group(1))); sec.append((m.group(1), _f(m.group(2))))
    for m in re.finditer(r"(?:^|\s)(" + cl + r") (\d+\.\d)(?=\s|$)", block):
        if ("c", m.group(1)) not in seen:
            seen.add(("c", m.group(1))); cty.append((m.group(1), _f(m.group(2))))
    m = re.search(r"Fund factsheet .*?Class (\d{1,2}) (\w+) (\d{4})", text)
    as_of = _date_from_words(*m.groups()) if m else None
    return dict(as_of=as_of, top10=top, countries=cty, sectors=sec, taxonomy=dict(countries="country", sectors="GICS"))


GEM_SECTORS = ["Energy", "Basic Materials", "Industrials", "Banks", "Non Bank Financials", "Real Estate", "Technology",
               "Internet & Cloud", "Telecom", "Consumer Durables", "Consumer Non-durables", "Healthcare Services",
               "Utilities", "Cash"]


def parse_gemway(doc):
    pages = _pages(doc)
    p1, p2 = pages[0], pages[1] if len(pages) > 1 else ""
    cl = "Korea|Taiwan|China|Hong Kong|Singapore|Malaysia|Thailand|Indonesia|Philippines|Vietnam|India|Pakistan & Sri Lanka|Others|Cash"
    sl = "|".join(re.escape(k) for k in sorted(GEM_SECTORS, key=len, reverse=True))
    cty, sec, top = [], [], []
    i_c, i_s = p1.find("COUNTRY"), p1.find("SECTOR")
    for line in p1[i_c:i_s].split("\n"):
        m = re.search(r"(" + cl + r") (\d+\.\d)%(?: (\d+\.\d)%)?\s*$", line)
        if m:
            cty.append((m.group(1).replace("Korea", "South Korea"), _f(m.group(2))))
    for line in p1[i_s:].split("\n"):
        m = re.search(r"(" + sl + r") (\d+\.\d)%(?: (\d+\.\d)%)?\s*$", line)
        if m:
            sec.append((m.group(1), _f(m.group(2))))
    for line in p2.split("\n"):
        m = re.search(r"([A-Za-z0-9 .&\-]+?) (Taiwan|Korea|China|Singapore|India|Hong Kong|Indonesia|Thailand|Malaysia|"
                      r"Philippines|Vietnam) (\d+\.\d)% (\d+\.\d)%$", line)
        if m and len(top) < 10:
            name = re.sub(r"^.*?\d+\s+(?=[A-Z])", "", m.group(1).strip())
            top.append((name, _f(m.group(3))))
    m = re.search(r"MONTHLY REPORTING - (\w+) (\d{4})", p1)
    as_of = _month_end(m.group(1), m.group(2)) if m else None
    return dict(as_of=as_of, top10=top, countries=cty, sectors=sec, taxonomy=dict(countries="country", sectors="manager"))


PARSERS = {
    "matthews": parse_matthews,
    "stewart_fsi": parse_stewart_fsi,
    "fidelity": parse_fidelity,
    "jpm_es": parse_jpm_es,
    "veritas": parse_veritas,
    "gemway": parse_gemway,
}


# ---------------------------------------------------------------------------
# Fetch + store
# ---------------------------------------------------------------------------
def fetch_pdf(url: str, session: requests.Session, timeout: int = 60) -> bytes:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    if r.content[:4] != b"%PDF":
        raise ValueError(f"not a PDF ({r.headers.get('content-type', '?')}, {len(r.content)} bytes)")
    return r.content


def parse_pdf(parser: str, pdf_bytes: bytes) -> dict:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as doc:
        return PARSERS[parser](doc)


def fetch_all_factsheets(config: dict, session: requests.Session | None = None) -> list:
    """Fetch and parse every configured factsheet. Returns one result dict per fund with
    either 'blocks' (parsed) or 'error'. Nothing is written here."""
    cfg = config.get("factsheets", []) or []
    if not cfg:
        return []
    session = session or requests.Session()
    session.headers.setdefault("User-Agent", config.get("scraping", {}).get("user_agent", USER_AGENT))
    results = []
    for i, fs in enumerate(cfg, 1):
        res = dict(fund_name=fs["fund"], peer_set=fs.get("peer_set", ""), is_alquity=bool(fs.get("is_alquity", False)),
                   url=fs["url"], parser=fs["parser"], vehicle=fs.get("vehicle", "same_fund"))
        logger.info("[%d/%d] Factsheet %s (%s)", i, len(cfg), fs["fund"], fs["parser"])
        try:
            parsed = parse_pdf(fs["parser"], fetch_pdf(fs["url"], session, config.get("scraping", {}).get("timeout", 60)))
            res["as_of"] = parsed["as_of"]
            res["taxonomy"] = parsed["taxonomy"]
            res["blocks"] = {"Top 10": parsed["top10"], "Countries": parsed["countries"], "Sectors": parsed["sectors"]}
            logger.info("  -> as of %s: top10=%d countries=%d sectors=%d", parsed["as_of"], len(parsed["top10"]),
                        len(parsed["countries"]), len(parsed["sectors"]))
        except Exception as e:  # noqa: BLE001 - one bad sheet must not stop the run
            res["error"] = f"{type(e).__name__}: {e}"
            logger.error("  factsheet failed for %s: %s", fs["fund"], res["error"])
        results.append(res)
    return results


def attach_factsheet_data(db_path: str, snapshot_id: int, results: list) -> dict:
    """Replace this snapshot's factsheet rows with the given results. Funds that errored
    store nothing (source_select carries their last good sheet forward)."""
    init_factsheet_db(db_path)
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for res in results:
        if "blocks" not in res:
            continue
        for block, items in res["blocks"].items():
            tax = res["taxonomy"]["sectors"] if block == "Sectors" else res["taxonomy"]["countries"] if block == "Countries" else ""
            for rank, (name, pct) in enumerate(items, 1):
                rows.append((snapshot_id, res["fund_name"], res["peer_set"], 1 if res["is_alquity"] else 0, block, rank,
                             name, float(pct), res["as_of"], res["url"], res["parser"], res["vehicle"], tax, now))
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM factsheet_data WHERE snapshot_id=?", (snapshot_id,))
        conn.executemany(
            "INSERT INTO factsheet_data (snapshot_id, fund_name, peer_set, is_alquity, block, rank, name, pct, as_of, "
            "source_url, parser, vehicle, taxonomy, fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()
    finally:
        conn.close()
    return {"snapshot_id": snapshot_id, "num_rows": len(rows),
            "funds_ok": [r["fund_name"] for r in results if "blocks" in r],
            "funds_failed": {r["fund_name"]: r["error"] for r in results if "error" in r}}
