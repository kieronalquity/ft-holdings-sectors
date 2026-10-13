import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime, date
import time
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class ScrapedEntry:
    category: str        # 'Holdings', 'Sectors' or 'Regions'
    fund_name: str
    company_sector: str
    percentage: float
    date_of_data: str    # ISO YYYY-MM-DD
    url: str
    peer_set: str = ""   # 'India', 'Asia', 'FW'
    is_alquity: bool = False


def scrape_all_funds(config: dict) -> list:
    entries = []
    scraping_cfg = config.get("scraping", {})
    delay = scraping_cfg.get("delay_between_requests", 2.0)
    timeout = scraping_cfg.get("timeout", 30)
    user_agent = scraping_cfg.get("user_agent", "Mozilla/5.0")
    max_retries = scraping_cfg.get("max_retries", 3)
    # Peer sets for which the FT 'Regions' weighting table is also scraped
    # (FT publishes Morningstar regions, not countries). Start with Asia; add 'FW' to expand.
    regions_peer_sets = set(scraping_cfg.get("regions_peer_sets", []) or [])

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    funds = config.get("funds", [])
    total = len(funds)

    for i, fund_cfg in enumerate(funds, 1):
        url = fund_cfg["url"]
        peer_set = fund_cfg.get("peer_set", "")
        is_alquity = fund_cfg.get("is_alquity", False)
        want_regions = peer_set in regions_peer_sets
        logger.info("[%d/%d] Scraping %s", i, total, url)
        for attempt in range(1, max_retries + 1):
            try:
                fund_entries = scrape_single_fund(url, session, timeout, with_regions=want_regions,
                                                  strict=attempt < max_retries)
                for e in fund_entries:
                    e.peer_set = peer_set
                    e.is_alquity = is_alquity
                entries.extend(fund_entries)
                logger.info("  -> %d entries scraped", len(fund_entries))
                break
            except Exception as e:
                logger.warning("  Attempt %d failed: %s", attempt, e)
                if attempt == max_retries:
                    logger.error("  Skipping %s after %d attempts", url, max_retries)
                else:
                    time.sleep(delay)

        if i < total:
            time.sleep(delay)

    logger.info("Total entries scraped: %d", len(entries))
    return entries


class DegeneratePageError(RuntimeError):
    """FT served a variant of the tearsheet with the holdings table missing / regions all zero."""


def _regions_ok(regions: list) -> bool:
    return bool(regions) and sum(e.percentage for e in regions) >= 50


def scrape_single_fund(url: str, session: requests.Session, timeout: int = 30,
                       with_regions: bool = False, strict: bool = False) -> list:
    """Fetch one FT holdings tearsheet and return Holdings / Sectors (/ Regions) entries.

    FT intermittently serves a variant of the page with the Top 10 module missing, or with the
    Regions table all zeros, and the bare '?s=ISIN' URL (same share class) often works when the
    '?s=ISIN:CCY' one does not. So each URL variant is tried (twice over) until a page has both
    holdings and, if requested, a sane regions table.

    strict=True raises DegeneratePageError when no variant yields a Top 10 table, so the caller's
    retry loop refetches instead of storing a sectors-only snapshot. strict=False keeps the best
    partial page.
    """
    best = None  # (score, fund_name, holdings, sectors, regions)
    variants = list(_url_variants(url)) * 2
    for i, variant in enumerate(variants):
        if i:
            time.sleep(1)
        response = session.get(variant, timeout=timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        fund_name = _extract_fund_name(soup)
        if not fund_name:
            logger.warning("Could not extract fund name from %s", variant)
            continue
        holdings = _extract_holdings(soup, fund_name, url)
        sectors = _extract_sectors(soup, fund_name, url)
        regions = _extract_regions(soup, fund_name, url) if with_regions else []
        score = (bool(holdings), (not with_regions) or _regions_ok(regions))
        if best is None or score > best[0]:
            best = (score, fund_name, holdings, sectors, regions)
        if all(score):
            if variant != url:
                logger.info("  Full page served only by %s", variant)
            break
        logger.info("  Page variant %d incomplete (holdings=%s, regions_ok=%s) for %s",
                    i + 1, score[0], score[1], fund_name)

    if best is None:
        return []
    _, fund_name, holdings, sectors, regions = best
    if not holdings:
        if strict:
            raise DegeneratePageError(f"no Top 10 holdings table served for {fund_name}")
        logger.warning("  No Top 10 holdings for %s after all variants; keeping partial data", fund_name)
    if with_regions and not _regions_ok(regions):
        logger.warning("  Regions table empty for %s after all variants; skipping regions", fund_name)
        regions = []
    return holdings + sectors + regions


def _url_variants(url: str):
    """The configured URL first, then the same symbol without its ':CCY' suffix."""
    yield url
    bare = re.sub(r"(s=[A-Za-z0-9]+):[A-Z]{3}\b", r"\1", url)
    if bare != url:
        yield bare


def _normalize_name(text: str) -> str:
    """Normalize unicode dashes/special chars to ASCII equivalents."""
    # Replace en-dash, em-dash, and common variants with regular hyphen
    for ch in ("\u2013", "\u2014", "\u2012", "\u2015"):
        text = text.replace(ch, "-")
    # Replace non-breaking spaces
    text = text.replace("\u00a0", " ")
    # Replace replacement character
    text = text.replace("\ufffd", "-")
    # Fix corrupted dashes stored as literal '?'
    text = text.replace(" ? ", " - ")
    return text


def _extract_fund_name(soup: BeautifulSoup) -> str:
    el = soup.select_one("div.mod-tearsheet-overview__header__container h1")
    if el:
        return _normalize_name(el.get_text(strip=True))
    return ""


def _extract_holdings(soup: BeautifulSoup, fund_name: str, url: str) -> list:
    entries = []
    holdings_date = _extract_holdings_date(soup)

    tables = soup.select("div.mod-top-ten div.mod-module__content table.mod-ui-table")
    if not tables:
        logger.warning("No holdings tables found for %s", fund_name)
        return entries

    # C# quirk: uses index [1] when multiple tables exist (line 520 of Program.cs)
    target_table = tables[1] if len(tables) > 1 else tables[0]

    rows = target_table.select("tr")
    count = 0
    seen_names = set()
    for row in rows:
        tds = row.select("td")
        if not tds:
            continue

        # Company name: try linked text first, fallback to first td
        link = row.select_one("td a.mod-ui-link")
        if link:
            name = link.get_text(strip=True)
        else:
            name = tds[0].get_text(strip=True)

        if not name:
            continue

        # Skip duplicate company names (e.g. different share classes of same company)
        if name in seen_names:
            continue
        seen_names.add(name)

        # Percentage from third td (index 2)
        if len(tds) < 3:
            continue
        pct = _parse_percentage(tds[2].get_text(strip=True))
        if pct is None:
            continue

        entries.append(ScrapedEntry(
            category="Holdings",
            fund_name=fund_name,
            company_sector=name,
            percentage=pct,
            date_of_data=holdings_date,
            url=url,
        ))
        count += 1
        if count >= 10:
            break

    return entries


def _extract_sectors(soup: BeautifulSoup, fund_name: str, url: str) -> list:
    entries = []
    sectors_date = _extract_sectors_date(soup)

    containers = soup.select("div.mod-weightings__sectors__table")
    if not containers:
        logger.warning("No sectors table found for %s", fund_name)
        return entries

    seen_names = set()
    for container in containers:
        rows = container.select("tbody tr")
        for row in rows:
            tds = row.select("td")
            if len(tds) < 2:
                continue

            name = tds[0].get_text(strip=True)
            pct = _parse_percentage(tds[1].get_text(strip=True))
            if not name or pct is None:
                continue

            # Skip duplicate sector names across containers
            if name in seen_names:
                continue
            seen_names.add(name)

            entries.append(ScrapedEntry(
                category="Sectors",
                fund_name=fund_name,
                company_sector=name,
                percentage=pct,
                date_of_data=sectors_date,
                url=url,
            ))

    return entries


def _extract_regions(soup: BeautifulSoup, fund_name: str, url: str) -> list:
    """FT 'Regions' weighting table (Morningstar regions; FT has no country split).

    The table nests sub-regions under parent rows (Americas / Greater Asia / Greater Europe),
    parents carrying class 'mod-weightings__regions__table__row--colored'. Only the sub-region
    rows are stored (e.g. Developed Asia, Emerging Asia, United Kingdom), since they sum to
    ~100% on their own and the dashboard sorts rows by weight.
    """
    entries = []
    regions_date = _extract_regions_date(soup)

    containers = soup.select("div.mod-weightings__regions__table")
    if not containers:
        logger.warning("No regions table found for %s", fund_name)
        return entries

    seen_names = set()
    for container in containers:
        for row in container.select("tbody tr"):
            if "mod-weightings__regions__table__row--colored" in (row.get("class") or []):
                continue  # parent/aggregate row
            tds = row.select("td")
            if len(tds) < 2:
                continue
            name = tds[0].get_text(strip=True)
            pct = _parse_percentage(tds[1].get_text(strip=True))
            if not name or pct is None or name in seen_names:
                continue
            seen_names.add(name)
            entries.append(ScrapedEntry(
                category="Regions",
                fund_name=fund_name,
                company_sector=name,
                percentage=pct,
                date_of_data=regions_date,
                url=url,
            ))
    return entries


def _extract_holdings_date(soup: BeautifulSoup) -> str:
    try:
        disclaimers = soup.select(".mod-module__footer div.mod-disclaimer")
        if disclaimers:
            text = disclaimers[0].get_text(strip=True)
            return _parse_date_text(text)
    except (IndexError, Exception) as e:
        logger.debug("Could not extract holdings date: %s", e)
    return date.today().isoformat()


def _extract_sectors_date(soup: BeautifulSoup) -> str:
    try:
        disclaimers = soup.select("div.mod-disclaimer")
        if len(disclaimers) > 1:
            text = disclaimers[1].get_text(strip=True)
            return _parse_date_text(text)
    except (IndexError, Exception) as e:
        logger.debug("Could not extract sectors date: %s", e)
    return date.today().isoformat()


def _extract_regions_date(soup: BeautifulSoup) -> str:
    try:
        for d in soup.select("div.mod-weightings div.mod-disclaimer"):
            text = d.get_text(strip=True)
            if "Regions weighting" in text:
                return _parse_date_text(text.split(".")[0])
    except Exception as e:
        logger.debug("Could not extract regions date: %s", e)
    return _extract_sectors_date(soup)


def _parse_date_text(text: str) -> str:
    # Strip known prefixes from FT disclaimer text
    text = re.sub(r"Data delayed at least \d+ minutes,?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"as of\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*(BST|GMT|EST|CET)\s*", "", text, flags=re.IGNORECASE)
    text = text.strip(" .")

    # Try common date formats
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y",
                "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%b. %d, %Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date().isoformat()
        except ValueError:
            continue

    logger.debug("Could not parse date from: '%s'", text)
    return date.today().isoformat()


def _parse_percentage(text: str) -> float:
    text = text.strip().replace("%", "").replace(",", "").strip()
    if not text or text in ("N/A", "--", "-"):
        return None
    try:
        return round(float(text), 4)
    except ValueError:
        logger.debug("Could not parse percentage: '%s'", text)
        return None
