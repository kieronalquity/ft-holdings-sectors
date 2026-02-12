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
    category: str        # 'Holdings' or 'Sectors'
    fund_name: str
    company_sector: str
    percentage: float
    date_of_data: str    # ISO YYYY-MM-DD
    url: str


def scrape_all_funds(config: dict) -> list:
    entries = []
    scraping_cfg = config.get("scraping", {})
    delay = scraping_cfg.get("delay_between_requests", 2.0)
    timeout = scraping_cfg.get("timeout", 30)
    user_agent = scraping_cfg.get("user_agent", "Mozilla/5.0")
    max_retries = scraping_cfg.get("max_retries", 3)

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    fund_urls = [f["url"] for f in config.get("funds", [])]
    total = len(fund_urls)

    for i, url in enumerate(fund_urls, 1):
        logger.info("[%d/%d] Scraping %s", i, total, url)
        for attempt in range(1, max_retries + 1):
            try:
                fund_entries = scrape_single_fund(url, session, timeout)
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


def scrape_single_fund(url: str, session: requests.Session, timeout: int = 30) -> list:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    fund_name = _extract_fund_name(soup)
    if not fund_name:
        logger.warning("Could not extract fund name from %s", url)
        return []

    entries = []
    entries.extend(_extract_holdings(soup, fund_name, url))
    entries.extend(_extract_sectors(soup, fund_name, url))
    return entries


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


def _parse_date_text(text: str) -> str:
    # Strip known prefixes from FT disclaimer text
    text = re.sub(r"Data delayed at least \d+ minutes,?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"as of\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*(BST|GMT|EST|CET)\s*", "", text, flags=re.IGNORECASE)
    text = text.strip(" .")

    # Try common date formats
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%b. %d, %Y"):
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
