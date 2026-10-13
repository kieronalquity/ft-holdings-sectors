# Top 10 tab: source selection

The Top 10 tab shows, per fund, three blocks: **Top 10**, **Countries**, **Sectors**. For peer
sets listed under `top10_sources.peer_sets` in `config.yaml` (currently Asia), each block is
filled from whichever source is most recent *and* complete. Other peer sets keep the plain FT view.

The Bloomberg-driven analytics tabs (Overview, Conviction, Country, Sector, ...) are unaffected:
they always use Bloomberg holdings.

## Sources

| Source | Where it comes from | Blocks | Notes |
|---|---|---|---|
| Manager factsheet | `factsheets:` entries in `config.yaml`, fetched by `factsheets.py` into `factsheet_data` | all three | the manager's own numbers; portfolio date read from the sheet |
| Bloomberg | `bbg_holdings` + master data, derived at display time | all three | holdings-derived; Top 10 = largest positions, Countries/Sectors = sums by country / GICS |
| FT (Morningstar) | `ft_snapshot_data` from the FT scrape | Top 10, Sectors | FT has no countries, only regions, which are shown separately as "Regions (FT)" |
| Internal | Bloomberg holdings of Alquity's own funds | all three | always wins for Alquity funds |

Bloomberg funds are matched to FT names by ISIN (`ft_fund_isin`, filled by the FT scrape) and,
failing that, by `FT_TO_BBG_NAME` in `bloomberg_loader.py`.

## The rule (per fund, per block)

1. Drop candidates that fail **validation** (parser read garbage: weights outside 0-100, name
   fragments, or fewer than 6 of 10 Top 10 names matching last run's parse of the same source).
2. Prefer **complete** candidates. Top 10 is complete with 10 names (or the fund's full holding
   count if fewer). Countries and Sectors are complete with at least 5 rows summing to 90-101%
   including cash and no grouped labels such as "China/Hong Kong".
3. Take the **latest portfolio as-of date**.
4. On a tie, prefer factsheet, then Bloomberg, then FT.
5. If nothing is complete, show the latest incomplete candidate labelled "incomplete".

Then: older than 3 months shows a warning; older than 12 months is suppressed with a note.
FT's Top 10 carries no portfolio date on FT.com (the "data delayed ... as of" line is the price
timestamp), so it borrows the FT sectors date and is labelled "(inferred)".

Month-on-month arrows compare against the previous run **only when the same source won both
months**. When the source flips, the caption says "source changed from X, arrows paused".

Every candidate is kept. The expander under each fund on the tab lists all of them with their
validation and completeness results, so you can always see what lost and why.

## Running it

- `python run.py --update` does Bloomberg ingest, FT scrape, factsheet fetch, then prints
  **QUESTIONS** and **NOTICES**. The full per-fund source table is written to
  `output/run_summary_snapshot_<id>.txt`.
- `python run.py --factsheets` refetches factsheets into the latest snapshot and reprints the summary.
- `python run.py --summary` reprints the summary without fetching anything.

A factsheet that fails to fetch keeps its last good rows (carried forward with their original
date) so it simply ages out; the run summary reports the failure.

## Questions and how to answer them

Each question names peer set / fund / block and the reason. Answer by editing `config.yaml`;
the question then becomes a notice.

| Question | Meaning | Answer |
|---|---|---|
| `invalid:<source>:...` | parser output failed validation | check the PDF layout; fix the parser in `factsheets.py` |
| `conflict:<a> vs <b>: ...` | two complete sources with the same date disagree (fewer than 7 of 10 names, or a mapped label more than 5 pts apart) | hierarchy decides by default; investigate if it recurs |
| `incomplete_only` | nothing complete exists, an incomplete table is shown | add `accept_incomplete: true` under `source_rules` for that fund/block to stop asking |
| `sister_vehicle` | the winning factsheet is a sister fund (same strategy, different vehicle) | add `allow_sister: true` once you are happy with it |
| `stale_suppressed:<n> months` | best available is older than 12 months, so nothing is shown | add `accept_stale: true` to show it anyway, or find a source |
| `sector label '<x>' has no Morningstar mapping` | a factsheet introduced a new sector name | add it to `SECTOR_TO_MS` in `source_select.py` |
| `Bloomberg fund '<x>' has no FT name` | holdings exist but cannot be offered to the tab | add it to `FT_TO_BBG_NAME`, or it will resolve by ISIN after the next FT scrape |
| `configured factsheet fund '<x>' has no data from any source` | FT name in config no longer matches | correct `fund:` in the `factsheets:` entry |

`source_rules` example:

```yaml
source_rules:
  - fund: "Stewart Investors Asia Pacific Leaders Fund Class B (Accumulation) GBP"
    allow_sister: true
  - fund: "GemAsia I USD"
    block: Countries
    accept_incomplete: true
```

## Adding a fund or a peer set

1. Find the factsheet PDF URL and confirm it has a country table (see `asia-factsheet-sources`
   notes for the sources used so far).
2. Add a `factsheets:` entry: `fund` (exact FT name), `peer_set`, `parser`, `vehicle`, `url`.
   If no existing parser fits the layout, add one to `PARSERS` in `factsheets.py`.
3. To switch a whole peer set over, add it to `top10_sources.peer_sets`.
4. Run `python run.py --factsheets` and clear the questions.

Factsheets are published two to three weeks after month end, so run the update mid-month or
later to pick up the previous month's sheets.
