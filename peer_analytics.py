"""Pure analytics functions for peer holdings comparison.

All functions take pandas DataFrames and return DataFrames/dicts.
No database access in this module.
"""

import pandas as pd
import numpy as np

from exchange_country_map import get_country_from_bbg_code


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def enrich_holdings(holdings_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join holdings with master_data on ticker. Adds short_name, gics_sector,
    gics_industry, country (resolved: master preferred, exchange-derived fallback).
    Filters out negative-weight positions (FX adjustments) and deduplicates master_data."""
    if holdings_df.empty:
        return holdings_df

    # Filter out negative/zero weight positions (e.g. VAT CN Equity FX adjustments)
    df = holdings_df[holdings_df["weight"] > 0].copy()

    # Deduplicate master_data by ticker (keep first row per ticker)
    master_dedup = master_df.drop_duplicates("ticker", keep="first")

    df = df.merge(
        master_dedup[["ticker", "short_name", "gics_industry", "gics_sector", "country_bbg", "market_cap_usd"]],
        on="ticker", how="left",
    )
    # Resolve country: use master's country_bbg mapped to display name, fallback to exchange-derived
    df["country_master"] = df["country_bbg"].apply(get_country_from_bbg_code)
    df["country"] = df["country_master"].fillna(df["country_derived"])
    df.drop(columns=["country_master"], inplace=True)
    return df


def _split_alquity(df: pd.DataFrame) -> tuple:
    """Split into (alquity_df, peers_df)."""
    alq = df[df["is_alquity"] == 1].copy()
    peers = df[df["is_alquity"] == 0].copy()
    return alq, peers


def _peer_fund_count(peers_df: pd.DataFrame) -> int:
    return peers_df["fund_name"].nunique()


# ---------------------------------------------------------------------------
# 1. Holdings Overlap
# ---------------------------------------------------------------------------

def holdings_overlap(df: pd.DataFrame) -> pd.DataFrame:
    """Compute overlap between Alquity and each peer fund.

    Returns DataFrame with one row per peer: fund_name, overlap_count,
    alquity_total, peer_total, overlap_weight_alquity, overlap_weight_peer,
    jaccard_index.
    """
    alq, peers = _split_alquity(df)
    if alq.empty:
        return pd.DataFrame()

    alq_tickers = set(alq["ticker"])
    alq_weights = alq.groupby("ticker")["weight"].sum()
    rows = []

    for fund, group in peers.groupby("fund_name"):
        peer_tickers = set(group["ticker"])
        shared = alq_tickers & peer_tickers
        union = alq_tickers | peer_tickers

        peer_weights = group.groupby("ticker")["weight"].sum()

        rows.append({
            "fund_name": fund,
            "overlap_count": len(shared),
            "alquity_total": len(alq_tickers),
            "peer_total": len(peer_tickers),
            "overlap_weight_alquity": alq_weights.reindex(shared).sum() if shared else 0,
            "overlap_weight_peer": peer_weights.reindex(shared).sum() if shared else 0,
            "jaccard_index": len(shared) / len(union) if union else 0,
        })

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("overlap_count", ascending=False).reset_index(drop=True)
    return result


# ---------------------------------------------------------------------------
# 2. Conviction / Active Positions
# ---------------------------------------------------------------------------

def conviction_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Compare Alquity weight vs peer-group average for each ticker.

    Peer average = sum of weights across ALL peers / number of peers
    (gives 0 weight to peers that don't hold the ticker).

    Returns DataFrame: ticker, short_name, alquity_weight, peer_avg_weight,
    peer_holder_count, active_weight, country, gics_sector.
    """
    alq, peers = _split_alquity(df)
    if alq.empty:
        return pd.DataFrame()

    num_peers = _peer_fund_count(peers)
    if num_peers == 0:
        return pd.DataFrame()

    # Aggregate duplicates, keep first metadata
    alq_agg = alq.groupby("ticker").agg(
        alquity_weight=("weight", "sum"),
        short_name=("short_name", "first"),
        country=("country", "first"),
        gics_sector=("gics_sector", "first"),
    )
    alq_w = alq_agg

    # Peer stats per ticker
    peer_stats = peers.groupby("ticker").agg(
        peer_total_weight=("weight", "sum"),
        peer_holder_count=("fund_name", "nunique"),
    )
    peer_stats["peer_avg_weight"] = peer_stats["peer_total_weight"] / num_peers

    # Union of all tickers
    all_tickers = set(alq["ticker"]) | set(peers["ticker"])
    result = pd.DataFrame(index=list(all_tickers))
    result.index.name = "ticker"

    result = result.join(alq_w).join(peer_stats)
    result["alquity_weight"] = result["alquity_weight"].fillna(0)
    result["peer_avg_weight"] = result["peer_avg_weight"].fillna(0)
    result["peer_holder_count"] = result["peer_holder_count"].fillna(0).astype(int)
    result["active_weight"] = result["alquity_weight"] - result["peer_avg_weight"]

    # Fill short_name/country/sector from peers if not in Alquity
    if "short_name" in peers.columns:
        peer_names = peers.drop_duplicates("ticker").set_index("ticker")[["short_name", "country", "gics_sector"]]
        for col in ["short_name", "country", "gics_sector"]:
            result[col] = result[col].fillna(peer_names[col] if col in peer_names.columns else None)

    result = result.reset_index()
    result = result.sort_values("active_weight", ascending=False, key=abs).reset_index(drop=True)
    result.drop(columns=["peer_total_weight"], inplace=True, errors="ignore")
    return result


# ---------------------------------------------------------------------------
# 3. Unique Positions
# ---------------------------------------------------------------------------

def unique_positions(df: pd.DataFrame) -> dict:
    """Returns dict with:
    - alquity_unique: DataFrame of tickers held only by Alquity (no peers)
    - alquity_rare: DataFrame of tickers held by Alquity and <=2 peers
    - peer_consensus_missing: tickers held by >50% of peers but NOT by Alquity
    """
    alq, peers = _split_alquity(df)
    num_peers = _peer_fund_count(peers)

    alq_tickers = set(alq["ticker"])
    peer_counts = peers.groupby("ticker")["fund_name"].nunique()

    # Alquity-only: zero peers hold it
    alq_only_tickers = alq_tickers - set(peer_counts.index)
    alq_unique = alq[alq["ticker"].isin(alq_only_tickers)].copy()
    alq_unique = alq_unique.sort_values("weight", ascending=False)

    # Alquity rare: held by <=2 peers
    rare_tickers = set(peer_counts[peer_counts <= 2].index) & alq_tickers
    alq_rare = alq[alq["ticker"].isin(rare_tickers | alq_only_tickers)].copy()
    alq_rare["peer_count"] = alq_rare["ticker"].map(peer_counts).fillna(0).astype(int)
    alq_rare = alq_rare.sort_values("weight", ascending=False)

    # Peer consensus missing from Alquity: held by >50% peers, not by Alquity
    threshold = max(1, num_peers * 0.5)
    consensus_tickers = set(peer_counts[peer_counts >= threshold].index) - alq_tickers
    peer_consensus = peers[peers["ticker"].isin(consensus_tickers)].copy()
    if not peer_consensus.empty:
        agg = peer_consensus.groupby("ticker").agg(
            holder_count=("fund_name", "nunique"),
            avg_weight=("weight", "mean"),
            short_name=("short_name", "first"),
            country=("country", "first"),
            gics_sector=("gics_sector", "first"),
        ).reset_index()
        agg = agg.sort_values("holder_count", ascending=False)
    else:
        agg = pd.DataFrame()

    return {
        "alquity_unique": alq_unique,
        "alquity_rare": alq_rare,
        "peer_consensus_missing": agg,
    }


# ---------------------------------------------------------------------------
# 4. Consensus Holdings
# ---------------------------------------------------------------------------

def consensus_holdings(df: pd.DataFrame) -> pd.DataFrame:
    """Most widely held stocks across the entire peer group (including Alquity).

    Returns: ticker, short_name, num_holders, pct_of_funds, avg_weight,
    max_weight, max_weight_fund, held_by_alquity, alquity_weight, country, gics_sector.
    """
    total_funds = df["fund_name"].nunique()
    alq_tickers = set(df[df["is_alquity"] == 1]["ticker"])
    alq_weights = df[df["is_alquity"] == 1].groupby("ticker")["weight"].sum()

    agg = df.groupby("ticker").agg(
        num_holders=("fund_name", "nunique"),
        avg_weight=("weight", "mean"),
        max_weight=("weight", "max"),
        short_name=("short_name", "first"),
        country=("country", "first"),
        gics_sector=("gics_sector", "first"),
    )

    # Find which fund has max weight per ticker
    idx_max = df.groupby("ticker")["weight"].idxmax()
    agg["max_weight_fund"] = df.loc[idx_max.values, "fund_name"].values

    agg["pct_of_funds"] = (agg["num_holders"] / total_funds * 100).round(1)
    agg["held_by_alquity"] = agg.index.isin(alq_tickers)
    agg["alquity_weight"] = agg.index.map(alq_weights).fillna(0)

    agg = agg.sort_values(["num_holders", "avg_weight"], ascending=[False, False]).reset_index()
    return agg


# ---------------------------------------------------------------------------
# 5. Country Allocation
# ---------------------------------------------------------------------------

def country_allocation(df: pd.DataFrame) -> dict:
    """Compare country allocation between Alquity and peer average.

    Returns dict with:
    - comparison: DataFrame with country, alquity_weight, peer_avg_weight, active_weight
    - per_fund: dict[fund_name -> DataFrame with country, weight]
    """
    alq, peers = _split_alquity(df)
    num_peers = _peer_fund_count(peers)

    # Alquity country weights
    alq_countries = alq.groupby("country")["weight"].sum().rename("alquity_weight")

    # Peer average: sum all peer weights by country, divide by num_peers
    if num_peers > 0:
        peer_country_totals = peers.groupby("country")["weight"].sum()
        peer_avg = (peer_country_totals / num_peers).rename("peer_avg_weight")
    else:
        peer_avg = pd.Series(dtype=float, name="peer_avg_weight")

    comparison = pd.DataFrame({"alquity_weight": alq_countries, "peer_avg_weight": peer_avg}).fillna(0)
    comparison["active_weight"] = comparison["alquity_weight"] - comparison["peer_avg_weight"]
    comparison = comparison.sort_values("alquity_weight", ascending=False).reset_index()
    comparison.rename(columns={"index": "country"}, inplace=True)

    # Per-fund breakdown
    per_fund = {}
    for fund, group in peers.groupby("fund_name"):
        fc = group.groupby("country")["weight"].sum().sort_values(ascending=False).reset_index()
        fc.columns = ["country", "weight"]
        per_fund[fund] = fc

    return {"comparison": comparison, "per_fund": per_fund}


# ---------------------------------------------------------------------------
# 6. Sector Allocation
# ---------------------------------------------------------------------------

def sector_allocation(df: pd.DataFrame) -> dict:
    """Compare GICS sector allocation between Alquity and peer average.

    Returns dict with:
    - comparison: DataFrame with gics_sector, alquity_weight, peer_avg_weight, active_weight
    - coverage: dict[fund_name -> pct of weight with GICS data]
    """
    # Only include rows with GICS sector data
    has_sector = df[df["gics_sector"].notna() & (df["gics_sector"] != "")].copy()

    # Compute coverage per fund
    coverage = {}
    for fund, group in df.groupby("fund_name"):
        total_w = group["weight"].sum()
        sector_w = group[group["gics_sector"].notna() & (group["gics_sector"] != "")]["weight"].sum()
        coverage[fund] = round(sector_w / total_w * 100, 1) if total_w > 0 else 0

    alq, peers = _split_alquity(has_sector)
    num_peers = _peer_fund_count(peers)

    alq_sectors = alq.groupby("gics_sector")["weight"].sum().rename("alquity_weight")

    if num_peers > 0:
        peer_totals = peers.groupby("gics_sector")["weight"].sum()
        peer_avg = (peer_totals / num_peers).rename("peer_avg_weight")
    else:
        peer_avg = pd.Series(dtype=float, name="peer_avg_weight")

    comparison = pd.DataFrame({"alquity_weight": alq_sectors, "peer_avg_weight": peer_avg}).fillna(0)
    comparison["active_weight"] = comparison["alquity_weight"] - comparison["peer_avg_weight"]
    comparison = comparison.sort_values("alquity_weight", ascending=False).reset_index()
    comparison.rename(columns={"index": "gics_sector"}, inplace=True)

    return {"comparison": comparison, "coverage": coverage}


# ---------------------------------------------------------------------------
# 7. Concentration Metrics
# ---------------------------------------------------------------------------

def concentration_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute concentration metrics for each fund.

    Returns: fund_name, is_alquity, num_positions, top_5_weight, top_10_weight,
    top_20_weight, hhi, effective_positions, max_position_weight, max_position_ticker.
    """
    rows = []
    for (fund, is_alq), group in df.groupby(["fund_name", "is_alquity"]):
        weights = group["weight"].sort_values(ascending=False)
        n = len(weights)
        total = weights.sum()

        # Normalize weights to sum to 100 for HHI calculation
        if total > 0:
            norm = weights / total * 100
            hhi = (norm ** 2).sum()
        else:
            hhi = 0

        max_idx = weights.idxmax() if n > 0 else None
        max_ticker = group.loc[max_idx, "ticker"] if max_idx is not None else ""
        max_name = group.loc[max_idx, "short_name"] if max_idx is not None and pd.notna(group.loc[max_idx, "short_name"]) else max_ticker

        rows.append({
            "fund_name": fund,
            "is_alquity": is_alq,
            "num_positions": n,
            "top_5_weight": weights.head(5).sum(),
            "top_10_weight": weights.head(10).sum(),
            "top_20_weight": weights.head(20).sum(),
            "hhi": round(hhi, 1),
            "effective_positions": round(10000 / hhi, 1) if hhi > 0 else n,
            "max_position_weight": weights.iloc[0] if n > 0 else 0,
            "max_position_name": max_name,
        })

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("is_alquity", ascending=False).reset_index(drop=True)
    return result


# ---------------------------------------------------------------------------
# 8. Active Share
# ---------------------------------------------------------------------------

def active_share(df: pd.DataFrame) -> dict:
    """Compute active share of Alquity vs peer consensus and vs each individual peer.

    Active share = 0.5 * sum(|w_alquity_i - w_benchmark_i|) over all tickers.

    Returns dict with:
    - vs_consensus: float (0-100)
    - vs_each_peer: DataFrame with fund_name, active_share
    - top_contributors: DataFrame of tickers contributing most to active share vs consensus
    """
    alq, peers = _split_alquity(df)
    if alq.empty:
        return {"vs_consensus": 0, "vs_each_peer": pd.DataFrame(), "top_contributors": pd.DataFrame()}

    num_peers = _peer_fund_count(peers)

    # Alquity weights normalized to 100%
    alq_w = alq.groupby("ticker")["weight"].sum()
    alq_total = alq_w.sum()
    if alq_total > 0:
        alq_norm = alq_w / alq_total * 100
    else:
        alq_norm = alq_w

    # Consensus portfolio: average across all peers, normalized to 100%
    if num_peers > 0:
        peer_totals = peers.groupby("ticker")["weight"].sum() / num_peers
        consensus_total = peer_totals.sum()
        if consensus_total > 0:
            consensus_norm = peer_totals / consensus_total * 100
        else:
            consensus_norm = peer_totals
    else:
        consensus_norm = pd.Series(dtype=float)

    # Active share vs consensus
    all_tickers = set(alq_norm.index) | set(consensus_norm.index)
    alq_full = alq_norm.reindex(all_tickers, fill_value=0)
    cons_full = consensus_norm.reindex(all_tickers, fill_value=0)
    diffs = (alq_full - cons_full).abs()
    vs_consensus = diffs.sum() / 2

    # Top contributors to active share
    contrib = pd.DataFrame({
        "alquity_weight": alq_full,
        "consensus_weight": cons_full,
        "contribution": diffs,
    })
    # Add short_name from the data
    name_map = df.drop_duplicates("ticker").set_index("ticker")["short_name"]
    contrib["short_name"] = contrib.index.map(name_map)
    contrib = contrib.sort_values("contribution", ascending=False).head(20).reset_index()
    contrib.rename(columns={"index": "ticker"}, inplace=True)

    # Active share vs each peer
    peer_rows = []
    for fund, group in peers.groupby("fund_name"):
        pw = group.groupby("ticker")["weight"].sum()
        p_total = pw.sum()
        if p_total > 0:
            p_norm = pw / p_total * 100
        else:
            p_norm = pw
        all_t = set(alq_norm.index) | set(p_norm.index)
        a = alq_norm.reindex(all_t, fill_value=0)
        p = p_norm.reindex(all_t, fill_value=0)
        peer_as = (a - p).abs().sum() / 2
        peer_rows.append({"fund_name": fund, "active_share": round(peer_as, 1)})

    vs_peers = pd.DataFrame(peer_rows).sort_values("active_share").reset_index(drop=True)

    return {
        "vs_consensus": round(vs_consensus, 1),
        "vs_each_peer": vs_peers,
        "top_contributors": contrib,
    }


# ---------------------------------------------------------------------------
# 9. Market Cap Analysis
# ---------------------------------------------------------------------------

_CAP_THRESHOLDS = [
    ("Large Cap", 10_000),   # >= $10bn (market_cap_usd is in millions)
    ("Mid Cap", 2_500),      # >= $2.5bn
    ("Small Cap", 0),        # < $2.5bn
]


def _cap_bucket(mc):
    """Assign a market cap bucket label. mc is in USD millions."""
    if pd.isna(mc):
        return "Unknown"
    if mc >= 10_000:
        return "Large Cap"
    if mc >= 2_500:
        return "Mid Cap"
    return "Small Cap"


def market_cap_analysis(df: pd.DataFrame) -> dict:
    """Analyse portfolio allocations by market cap bucket.

    Returns dict with:
      - comparison: DataFrame (fund_name, is_alquity, Large Cap, Mid Cap, Small Cap, Unknown)
      - alquity_detail: DataFrame of Alquity holdings with cap_bucket column
      - peer_avg: DataFrame (cap_bucket, alquity_weight, peer_avg_weight, active_weight)
    """
    df = df.copy()
    df["cap_bucket"] = df["market_cap_usd"].apply(_cap_bucket)

    buckets = ["Large Cap", "Mid Cap", "Small Cap", "Unknown"]

    # Per-fund breakdown
    rows = []
    for fund, group in df.groupby("fund_name"):
        total = group["weight"].sum()
        is_alq = int(group["is_alquity"].iloc[0])
        row = {"fund_name": fund, "is_alquity": is_alq}
        for b in buckets:
            bw = group[group["cap_bucket"] == b]["weight"].sum()
            row[b] = round(bw / total * 100, 2) if total > 0 else 0
        rows.append(row)

    comparison = pd.DataFrame(rows)
    # Sort: Alquity first, then alphabetical
    comparison = comparison.sort_values(["is_alquity", "fund_name"], ascending=[False, True]).reset_index(drop=True)

    # Alquity vs peer average
    alq_row = comparison[comparison["is_alquity"] == 1]
    peer_rows = comparison[comparison["is_alquity"] == 0]

    avg_rows = []
    for b in buckets:
        alq_val = alq_row[b].iloc[0] if not alq_row.empty else 0
        peer_val = round(peer_rows[b].mean(), 2) if not peer_rows.empty else 0
        avg_rows.append({
            "cap_bucket": b,
            "alquity_weight": alq_val,
            "peer_avg_weight": peer_val,
            "active_weight": round(alq_val - peer_val, 2),
        })
    peer_avg = pd.DataFrame(avg_rows)

    # Alquity detail with bucket
    alq_detail = df[df["is_alquity"] == 1][["ticker", "short_name", "weight", "market_cap_usd",
                                              "cap_bucket", "country", "gics_sector"]].copy()
    alq_detail = alq_detail.sort_values("weight", ascending=False).reset_index(drop=True)

    return {
        "comparison": comparison,
        "peer_avg": peer_avg,
        "alquity_detail": alq_detail,
    }
