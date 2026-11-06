"""Peer groups: named subsets of a peer set (config: peer_groups), e.g. FW 'True ESG'.

Each group lists its funds as  label: ISIN  or  label: [ISINs / exact fund names]  (a list when
Bloomberg and FT track different share classes). Funds are matched to Bloomberg names through the
peer-group ISIN and to FT names through the ft_fund_isin table. No database access in this module.
"""

import pandas as pd

from peer_analytics import holdings_overlap, active_share, country_allocation, sector_allocation


def load_groups(cfg: dict, peer_set: str) -> dict:
    """{group: {label: {keys}}} for a peer set, in config order. {} when none are configured."""
    out = {}
    for group, funds in ((cfg.get("peer_groups") or {}).get(peer_set) or {}).items():
        out[group] = {
            label: {str(k).strip() for k in (ids if isinstance(ids, list) else [ids])}
            for label, ids in (funds or {}).items()
        }
    return out


def resolve(groups: dict, names_by_key: dict) -> tuple:
    """Match group members to the funds actually available.

    names_by_key: {ISIN or fund name: fund name}.
    Returns ({fund_name: group}, {group: [labels with no matching fund]})."""
    fund_group, missing = {}, {}
    for group, funds in groups.items():
        for label, keys in funds.items():
            names = {names_by_key[k] for k in keys if k in names_by_key}
            if not names:
                missing.setdefault(group, []).append(label)
            for n in names:
                fund_group[n] = group
    return fund_group, missing


def _top(comparison: pd.DataFrame, col: str, weight: str) -> str:
    c = comparison[comparison[weight] > 0]
    if c.empty:
        return ""
    r = c.sort_values(weight, ascending=False).iloc[0]
    return f"{r[col]} ({r[weight]:.1f}%)"


def group_comparison(df: pd.DataFrame, groups: dict, fund_group: dict) -> pd.DataFrame:
    """One row per peer group (plus Alquity and all peers): Alquity measured against that group only.

    df: enriched holdings for the whole peer set, Alquity included."""
    alq = df[df["is_alquity"] == 1]
    peers = df[df["is_alquity"] == 0]
    if alq.empty:
        return pd.DataFrame()

    own_c = country_allocation(alq)["comparison"]
    own_s = sector_allocation(alq)["comparison"]
    rows = [{
        "Group": "Alquity", "Funds": "", "Avg Shared Holdings": None, "Avg Overlap (Alquity Wt %)": None,
        "Active Share vs Group %": None,
        "Top Country": _top(own_c, "country", "alquity_weight"),
        "Top Sector": _top(own_s, "gics_sector", "alquity_weight"),
    }]

    held = set(peers["fund_name"])
    sets = [(g, {f for f, fg in fund_group.items() if fg == g} & held, len(groups[g])) for g in groups]
    sets.append(("All peers", held, len(held)))
    for name, funds, size in sets:
        row = {"Group": name, "Funds": f"{len(funds)} of {size}" if len(funds) != size else str(size)}
        if funds:
            sub = pd.concat([alq, peers[peers["fund_name"].isin(funds)]])
            ov = holdings_overlap(sub)
            row.update({
                "Avg Shared Holdings": round(ov["overlap_count"].mean(), 1),
                "Avg Overlap (Alquity Wt %)": round(ov["overlap_weight_alquity"].mean(), 1),
                "Active Share vs Group %": active_share(sub)["vs_consensus"],
                "Top Country": _top(country_allocation(sub)["comparison"], "country", "peer_avg_weight"),
                "Top Sector": _top(sector_allocation(sub)["comparison"], "gics_sector", "peer_avg_weight"),
            })
        rows.append(row)
    return pd.DataFrame(rows)
