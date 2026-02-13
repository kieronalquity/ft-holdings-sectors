"""Alquity Peer Analysis Dashboard — Streamlit app."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from bloomberg_loader import (
    init_bbg_db, get_available_snapshots,
    get_peer_funds, load_holdings, load_master_data,
)
from peer_analytics import (
    enrich_holdings, holdings_overlap, conviction_positions,
    unique_positions, consensus_holdings, country_allocation,
    sector_allocation, concentration_metrics, active_share,
    market_cap_analysis,
)

DB_PATH = "data/holdings_sectors.db"

# ---------------------------------------------------------------------------
# pyarrow-free dataframe renderer (pyarrow has no wheel for Py3.14 ARM64 Win)
# ---------------------------------------------------------------------------

_TABLE_CSS = """
<style>
.styled-table {
    border-collapse: collapse; width: 100%; font-size: 13px; font-family: sans-serif;
    margin: 0.5rem 0 1rem 0;
}
.styled-table thead tr { background-color: #f0f2f6; text-align: left; }
.styled-table th, .styled-table td {
    padding: 6px 10px; border-bottom: 1px solid #e0e0e0; white-space: nowrap; text-align: left;
}
.styled-table tbody tr:hover { background-color: #f8f9fb; }
.styled-table tbody tr:nth-child(even) { background-color: #fafafa; }
</style>
"""


def show_df(df: pd.DataFrame, max_rows: int = 200) -> None:
    """Render a DataFrame as a styled HTML table (no pyarrow needed)."""
    if df.empty:
        st.info("No data to display.")
        return
    display = df.head(max_rows)
    html = _TABLE_CSS + display.to_html(
        classes="styled-table", index=False, na_rep="", float_format=lambda x: f"{x:.2f}",
    )
    if len(df) > max_rows:
        html += f"<p style='color:#888;font-size:12px;'>Showing {max_rows} of {len(df)} rows</p>"
    st.markdown(html, unsafe_allow_html=True)
PEER_SET_LABELS = {
    "India": "Indian Subcontinent",
    "Asia": "Asia",
    "FW": "Future World / Emerging Markets",
}


def main():
    st.set_page_config(page_title="Alquity Peer Analysis", layout="wide", initial_sidebar_state="expanded")
    init_bbg_db(DB_PATH)

    # ── Sidebar ──────────────────────────────────────────────────
    with st.sidebar:
        st.title("Alquity Peer Analysis")

        snapshots = get_available_snapshots(DB_PATH)
        if not snapshots:
            st.warning("No data loaded.")
            return

        # Peer set selector (first)
        peer_set = st.selectbox("Peer Set", list(PEER_SET_LABELS.keys()), format_func=lambda k: PEER_SET_LABELS[k])

        # Reserve a visual slot for Filter Peers (rendered above Snapshot)
        peers_container = st.container()

        # Snapshot selector (visually below Filter Peers)
        snap_options = {s["snapshot_id"]: f"{s['snapshot_date']} ({s['file_name']})" for s in snapshots}
        selected_snap = st.selectbox("Snapshot", list(snap_options.keys()), format_func=lambda x: snap_options[x])

        # Load peer list and render the filter in the reserved slot above
        peers_info = get_peer_funds(DB_PATH, selected_snap, peer_set)
        all_peer_names = peers_info[peers_info["is_alquity"] == 0]["fund_name"].tolist()
        with peers_container:
            selected_peers = st.multiselect("Filter Peers", all_peer_names, default=all_peer_names)

        st.divider()
        st.caption(f"Snapshot: {snapshots[0]['snapshot_date']}")

    # ── Load & enrich ────────────────────────────────────────────
    holdings = load_holdings(DB_PATH, selected_snap, peer_set, exclude_cash=True, min_weight=0.0)
    master = load_master_data(DB_PATH, selected_snap)
    df = enrich_holdings(holdings, master)

    # Apply peer filter
    alq_name = peers_info[peers_info["is_alquity"] == 1]["fund_name"].iloc[0]
    df = df[(df["fund_name"] == alq_name) | (df["fund_name"].isin(selected_peers))]

    if df.empty:
        st.warning("No holdings data for this selection.")
        return

    # ── Tabs ─────────────────────────────────────────────────────
    tabs = st.tabs([
        "Overview", "Conviction", "Country", "Sector", "Market Cap",
        "Unique Holdings", "Holdings Overlap", "Concentration & Active Share",
        "Consensus", "Old View",
    ])

    with tabs[0]:
        render_overview(df, peers_info, selected_peers, alq_name)
    with tabs[1]:
        render_conviction(df)
    with tabs[2]:
        render_country(df, alq_name)
    with tabs[3]:
        render_sector(df, alq_name)
    with tabs[4]:
        render_market_cap(df, alq_name)
    with tabs[5]:
        render_unique(df)
    with tabs[6]:
        render_overlap(df, alq_name)
    with tabs[7]:
        render_concentration_active(df, alq_name)
    with tabs[8]:
        render_consensus(df)
    with tabs[9]:
        render_old_view(df, alq_name)


# =====================================================================
# Tab renderers
# =====================================================================

def render_overview(df, peers_info, selected_peers, alq_name):
    """Tab 1: Overview with key metrics and peer summary."""
    ashr = active_share(df)
    conc = concentration_metrics(df)
    uniq = unique_positions(df)

    alq_conc = conc[conc["is_alquity"] == 1].iloc[0] if not conc[conc["is_alquity"] == 1].empty else {}
    sa_data = sector_allocation(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Alquity Holdings", int(alq_conc.get("num_positions", 0)))
    c2.metric("Active Share vs Consensus", f"{ashr['vs_consensus']}%")
    c3.metric("Top 10 Concentration", f"{alq_conc.get('top_10_weight', 0):.1f}%")
    c4.metric("Unique Positions", len(uniq["alquity_unique"]))

    st.subheader("Peer Fund Summary")
    # Build a holdings_date lookup from peers_info
    date_lookup = dict(zip(peers_info["fund_name"], peers_info["holdings_date"]))

    summary_rows = []
    for _, row in conc.iterrows():
        tag = " (Alquity)" if row["is_alquity"] else ""
        fund = row["fund_name"]
        summary_rows.append({
            "Fund": fund + tag,
            "Portfolio Date": date_lookup.get(fund, ""),
            "Positions": int(row["num_positions"]),
            "Top 10 %": round(row["top_10_weight"], 1),
            "Top 20 %": round(row["top_20_weight"], 1),
            "Largest Holding": row["max_position_name"],
            "Max %": round(row["max_position_weight"], 2),
        })
    show_df(pd.DataFrame(summary_rows))

    # Coverage
    cov = sa_data["coverage"]
    alq_cov = [v for k, v in cov.items() if "Alquity" in k]
    if alq_cov:
        st.info(f"Master data sector coverage for Alquity: **{alq_cov[0]}%** of portfolio weight")


def render_overlap(df, alq_name):
    """Tab 2: Holdings overlap analysis."""
    ov = holdings_overlap(df)
    if ov.empty:
        st.info("No overlap data.")
        return

    st.subheader("Overlap with Each Peer")

    # Bar chart
    fig = px.bar(
        ov, x="overlap_count", y="fund_name", orientation="h",
        color="jaccard_index", color_continuous_scale="Blues",
        labels={"overlap_count": "Shared Holdings", "fund_name": "", "jaccard_index": "Jaccard Index"},
        text="overlap_count",
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=max(300, len(ov) * 35))
    st.plotly_chart(fig, width="stretch")

    # Heatmap: peers x Alquity top-20 tickers
    st.subheader("Heatmap: Peer Weights in Alquity's Top 20 Holdings")
    alq = df[df["is_alquity"] == 1].nlargest(20, "weight")
    top_tickers = alq["ticker"].tolist()
    top_labels = alq.apply(lambda r: r["short_name"] if pd.notna(r["short_name"]) else r["ticker"], axis=1).tolist()

    peers_df = df[(df["is_alquity"] == 0) & (df["ticker"].isin(top_tickers))]
    pivot = peers_df.pivot_table(index="fund_name", columns="ticker", values="weight", fill_value=0)
    pivot = pivot.reindex(columns=top_tickers, fill_value=0)

    fig2 = go.Figure(data=go.Heatmap(
        z=pivot.values, x=top_labels, y=pivot.index.tolist(),
        colorscale="Blues", text=pivot.values.round(1), texttemplate="%{text}",
        hovertemplate="Fund: %{y}<br>Stock: %{x}<br>Weight: %{z:.2f}%<extra></extra>",
    ))
    fig2.update_layout(height=max(300, len(pivot) * 30), xaxis_tickangle=-45)
    st.plotly_chart(fig2, width="stretch")

    # Drill-down
    st.subheader("Peer Drill-Down")
    peer_choice = st.selectbox("Select peer fund", ov["fund_name"].tolist(), key="overlap_drill")
    if peer_choice:
        # Build metadata lookup from all holdings (covers both Alquity and peer tickers)
        meta_cols = ["ticker", "short_name", "country", "gics_sector"]
        meta = df.drop_duplicates("ticker")[meta_cols].set_index("ticker")

        # Aggregate duplicate tickers per fund
        alq_w = df[df["is_alquity"] == 1].groupby("ticker")["weight"].sum().rename("Alquity %")
        peer_w = df[df["fund_name"] == peer_choice].groupby("ticker")["weight"].sum().rename("Peer %")

        merged = pd.DataFrame({"Alquity %": alq_w, "Peer %": peer_w}).fillna(0)
        merged = merged.join(meta)
        merged["Diff"] = merged["Alquity %"] - merged["Peer %"]
        merged = merged.reset_index().rename(columns={"index": "ticker"})
        merged = merged[["ticker", "short_name", "Alquity %", "country", "gics_sector", "Peer %", "Diff"]]
        merged = merged.sort_values("Diff", ascending=False, key=abs)
        show_df(merged.round(2))


def render_conviction(df):
    """Tab 3: Conviction / active positions."""
    conv = conviction_positions(df)
    if conv.empty:
        st.info("No conviction data.")
        return

    # Only Alquity holdings
    alq_conv = conv[conv["alquity_weight"] > 0].copy()

    st.subheader("Alquity Active Positions vs Peer Average")

    # Diverging bar chart - top 15 each direction
    top_ow = alq_conv.head(15)
    top_uw = alq_conv.tail(15)
    chart_data = pd.concat([top_ow, top_uw]).drop_duplicates("ticker")
    chart_data["label"] = chart_data.apply(
        lambda r: r["short_name"] if pd.notna(r["short_name"]) else r["ticker"], axis=1)
    chart_data = chart_data.sort_values("active_weight")

    fig = px.bar(
        chart_data, x="active_weight", y="label", orientation="h",
        color="active_weight", color_continuous_scale=["#d32f2f", "#f5f5f5", "#388e3c"],
        color_continuous_midpoint=0,
        labels={"active_weight": "Active Weight (%)", "label": ""},
        hover_data={"alquity_weight": ":.2f", "peer_avg_weight": ":.2f"},
    )
    fig.update_layout(height=max(400, len(chart_data) * 25), showlegend=False)
    st.plotly_chart(fig, width="stretch")

    # Scatter plot
    st.subheader("Alquity Weight vs Peer Average")
    scatter_data = alq_conv[alq_conv["peer_avg_weight"] > 0].copy()
    scatter_data["label"] = scatter_data.apply(
        lambda r: r["short_name"] if pd.notna(r["short_name"]) else r["ticker"], axis=1)
    fig2 = px.scatter(
        scatter_data, x="peer_avg_weight", y="alquity_weight",
        size=scatter_data["active_weight"].abs(), hover_name="label",
        color="active_weight", color_continuous_scale=["#d32f2f", "#f5f5f5", "#388e3c"],
        color_continuous_midpoint=0,
        labels={"peer_avg_weight": "Peer Avg Weight (%)", "alquity_weight": "Alquity Weight (%)"},
    )
    max_val = max(scatter_data["peer_avg_weight"].max(), scatter_data["alquity_weight"].max()) * 1.1
    fig2.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(dash="dash", color="gray"))
    st.plotly_chart(fig2, width="stretch")

    # Full table
    with st.expander("Full Active Positions Table"):
        display = alq_conv[["ticker", "short_name", "alquity_weight", "peer_avg_weight",
                            "active_weight", "peer_holder_count", "country", "gics_sector"]].copy()
        display.columns = ["Ticker", "Name", "Alquity %", "Peer Avg %", "Active Wt",
                           "Peer Holders", "Country", "Sector"]
        show_df(display.round(2))


def render_unique(df):
    """Tab 4: Unique and rare positions."""
    uniq = unique_positions(df)

    st.subheader("Alquity-Only Positions (No Peer Holds)")
    if uniq["alquity_unique"].empty:
        st.info("All Alquity holdings are shared with at least one peer.")
    else:
        u = uniq["alquity_unique"][["ticker", "short_name", "weight", "country", "gics_sector"]].copy()
        u.columns = ["Ticker", "Name", "Weight %", "Country", "Sector"]
        show_df(u.round(2))

    st.subheader("Rare Positions (Held by 0-2 Peers)")
    if uniq["alquity_rare"].empty:
        st.info("No rare positions.")
    else:
        r = uniq["alquity_rare"][["ticker", "short_name", "weight", "peer_count", "country", "gics_sector"]].copy()
        r.columns = ["Ticker", "Name", "Weight %", "Peer Count", "Country", "Sector"]
        show_df(r.round(2))

    st.subheader("Peer Consensus Missing from Alquity")
    st.caption("Stocks held by >50% of peers that Alquity does not own")
    if uniq["peer_consensus_missing"].empty:
        st.success("Alquity holds all major consensus positions.")
    else:
        m = uniq["peer_consensus_missing"].copy()
        m.columns = ["Ticker", "# Peers Holding", "Avg Weight %", "Name", "Country", "Sector"]
        m = m[["Ticker", "Name", "# Peers Holding", "Avg Weight %", "Country", "Sector"]]
        show_df(m.round(2))


def render_consensus(df):
    """Tab 5: Consensus holdings across the peer group."""
    cons = consensus_holdings(df)
    if cons.empty:
        st.info("No consensus data.")
        return

    st.subheader("Most Widely Held Stocks")

    # Bar chart: top 20 by holder count
    top20 = cons.head(20).copy()
    top20["label"] = top20.apply(
        lambda r: r["short_name"] if pd.notna(r["short_name"]) else r["ticker"], axis=1)
    top20["color"] = top20["held_by_alquity"].map({True: "Held by Alquity", False: "Not held"})

    fig = px.bar(
        top20, x="label", y="num_holders", color="color",
        color_discrete_map={"Held by Alquity": "#1976d2", "Not held": "#e0e0e0"},
        labels={"num_holders": "# Funds Holding", "label": ""},
        hover_data={"avg_weight": ":.2f", "alquity_weight": ":.2f"},
    )
    fig.update_layout(xaxis_tickangle=-45, height=400)
    st.plotly_chart(fig, width="stretch")

    # Full table
    display = cons.head(30)[["ticker", "short_name", "num_holders", "pct_of_funds",
                              "avg_weight", "alquity_weight", "max_weight", "max_weight_fund",
                              "country", "gics_sector"]].copy()
    display.columns = ["Ticker", "Name", "# Holders", "% Funds", "Avg Wt %",
                        "Alquity Wt %", "Max Wt %", "Max Fund", "Country", "Sector"]
    show_df(display.round(2))


def render_country(df, alq_name):
    """Tab 6: Country allocation comparison."""
    ca = country_allocation(df)
    comp = ca["comparison"]

    if comp.empty:
        st.info("No country data.")
        return

    st.subheader("Country Allocation: Alquity vs Peer Average")

    # Grouped bar chart
    chart = comp.head(15).melt(id_vars="country", value_vars=["alquity_weight", "peer_avg_weight"],
                                var_name="source", value_name="weight")
    chart["source"] = chart["source"].map({"alquity_weight": "Alquity", "peer_avg_weight": "Peer Average"})
    fig = px.bar(chart, x="country", y="weight", color="source", barmode="group",
                 color_discrete_map={"Alquity": "#1976d2", "Peer Average": "#bdbdbd"},
                 labels={"weight": "Weight (%)", "country": ""})
    fig.update_layout(height=400)
    st.plotly_chart(fig, width="stretch")

    # Active weight table
    st.subheader("Active Country Bets")
    display = comp.copy()
    display.columns = ["Country", "Alquity %", "Peer Avg %", "Active Wt"]
    display = display.sort_values("Active Wt", ascending=False, key=abs)
    show_df(display.round(2))

    # Drill-down
    with st.expander("Country Drill-Down"):
        country_choice = st.selectbox("Select country", comp["country"].tolist(), key="country_drill")
        if country_choice:
            country_df = df[df["country"] == country_choice][["fund_name", "ticker", "short_name", "weight", "gics_sector"]].copy()
            country_df = country_df.sort_values(["fund_name", "weight"], ascending=[True, False])
            country_df.columns = ["Fund", "Ticker", "Name", "Weight %", "Sector"]
            show_df(country_df.round(2))


def render_sector(df, alq_name):
    """Tab 7: Sector allocation comparison."""
    sa = sector_allocation(df)
    comp = sa["comparison"]
    cov = sa["coverage"]

    if comp.empty:
        st.info("No sector data.")
        return

    # Coverage warning
    alq_cov = [v for k, v in cov.items() if "Alquity" in k]
    if alq_cov and alq_cov[0] < 90:
        st.warning(f"Sector data covers {alq_cov[0]}% of Alquity's portfolio weight. "
                   "Results may be incomplete — improve master_data coverage for accuracy.")

    st.subheader("Sector Allocation: Alquity vs Peer Average")

    chart = comp.melt(id_vars="gics_sector", value_vars=["alquity_weight", "peer_avg_weight"],
                      var_name="source", value_name="weight")
    chart["source"] = chart["source"].map({"alquity_weight": "Alquity", "peer_avg_weight": "Peer Average"})
    fig = px.bar(chart, x="gics_sector", y="weight", color="source", barmode="group",
                 color_discrete_map={"Alquity": "#1976d2", "Peer Average": "#bdbdbd"},
                 labels={"weight": "Weight (%)", "gics_sector": ""})
    fig.update_layout(xaxis_tickangle=-45, height=400)
    st.plotly_chart(fig, width="stretch")

    st.subheader("Active Sector Bets")
    display = comp.copy()
    display.columns = ["Sector", "Alquity %", "Peer Avg %", "Active Wt"]
    display = display.sort_values("Active Wt", ascending=False, key=abs)
    show_df(display.round(2))


def render_concentration_active(df, alq_name):
    """Tab 8: Concentration metrics and active share."""
    conc = concentration_metrics(df)
    ashr = active_share(df)

    # Active share headline
    c1, c2 = st.columns(2)
    c1.metric("Active Share vs Consensus", f"{ashr['vs_consensus']}%")
    avg_peer_as = ashr["vs_each_peer"]["active_share"].mean() if not ashr["vs_each_peer"].empty else 0
    c2.metric("Avg Active Share vs Peers", f"{avg_peer_as:.1f}%")

    # Concentration table
    st.subheader("Concentration Comparison")
    display = conc[["fund_name", "is_alquity", "num_positions", "top_5_weight",
                     "top_10_weight", "top_20_weight", "hhi", "effective_positions",
                     "max_position_name", "max_position_weight"]].copy()
    display.columns = ["Fund", "Alquity", "Positions", "Top 5 %", "Top 10 %", "Top 20 %",
                        "HHI", "Eff. Positions", "Max Holding", "Max %"]
    display["Alquity"] = display["Alquity"].map({1: "Yes", 0: ""})
    show_df(display.round(1))

    # Active share vs each peer - bar chart
    st.subheader("Active Share vs Each Peer")
    if not ashr["vs_each_peer"].empty:
        fig = px.bar(
            ashr["vs_each_peer"], x="active_share", y="fund_name", orientation="h",
            labels={"active_share": "Active Share (%)", "fund_name": ""},
            color="active_share", color_continuous_scale="Reds",
        )
        fig.update_layout(yaxis=dict(autorange="reversed"), height=max(300, len(ashr["vs_each_peer"]) * 30))
        st.plotly_chart(fig, width="stretch")

    # Top contributors to active share
    st.subheader("Top Active Share Contributors (vs Consensus)")
    if not ashr["top_contributors"].empty:
        tc = ashr["top_contributors"][["ticker", "short_name", "alquity_weight",
                                        "consensus_weight", "contribution"]].copy()
        tc.columns = ["Ticker", "Name", "Alquity Wt (norm) %", "Consensus Wt (norm) %", "Contribution"]
        show_df(tc.round(2))


def render_market_cap(df, alq_name):
    """Tab 9: Market cap bucket analysis."""
    mca = market_cap_analysis(df)
    comp = mca["comparison"]
    pavg = mca["peer_avg"]
    detail = mca["alquity_detail"]

    # Coverage note
    alq_df = df[df["is_alquity"] == 1]
    has_mc = alq_df["market_cap_usd"].notna().sum()
    total = len(alq_df)
    if has_mc < total:
        st.info(f"Market cap data available for {has_mc}/{total} Alquity holdings.")

    # Headline metrics from Alquity row
    alq_comp = comp[comp["is_alquity"] == 1]
    if not alq_comp.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Large Cap (>=$10bn)", f"{alq_comp['Large Cap'].iloc[0]:.1f}%")
        c2.metric("Mid Cap ($2.5-10bn)", f"{alq_comp['Mid Cap'].iloc[0]:.1f}%")
        c3.metric("Small Cap (<$2.5bn)", f"{alq_comp['Small Cap'].iloc[0]:.1f}%")

    # Grouped bar: Alquity vs Peer Average
    st.subheader("Market Cap Allocation: Alquity vs Peer Average")
    chart = pavg[pavg["cap_bucket"] != "Unknown"].melt(
        id_vars="cap_bucket", value_vars=["alquity_weight", "peer_avg_weight"],
        var_name="source", value_name="weight",
    )
    chart["source"] = chart["source"].map({"alquity_weight": "Alquity", "peer_avg_weight": "Peer Average"})
    fig = px.bar(
        chart, x="cap_bucket", y="weight", color="source", barmode="group",
        color_discrete_map={"Alquity": "#1976d2", "Peer Average": "#bdbdbd"},
        labels={"weight": "Weight (%)", "cap_bucket": ""},
        category_orders={"cap_bucket": ["Large Cap", "Mid Cap", "Small Cap"]},
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, width="stretch")

    # Active weight table
    st.subheader("Active Market Cap Bets")
    active_display = pavg.copy()
    active_display.columns = ["Bucket", "Alquity %", "Peer Avg %", "Active Wt"]
    show_df(active_display.round(2))

    # All funds comparison table
    st.subheader("All Funds: Market Cap Breakdown")
    fund_display = comp.copy()
    fund_display["Fund"] = fund_display.apply(
        lambda r: r["fund_name"] + " (Alquity)" if r["is_alquity"] else r["fund_name"], axis=1)
    fund_display = fund_display[["Fund", "Large Cap", "Mid Cap", "Small Cap", "Unknown"]]
    show_df(fund_display.round(1))

    # Alquity holdings detail by bucket
    with st.expander("Alquity Holdings by Market Cap Bucket"):
        for bucket in ["Large Cap", "Mid Cap", "Small Cap", "Unknown"]:
            bucket_df = detail[detail["cap_bucket"] == bucket]
            if bucket_df.empty:
                continue
            st.markdown(f"**{bucket}** ({len(bucket_df)} positions, "
                        f"{bucket_df['weight'].sum():.1f}% total weight)")
            d = bucket_df[["ticker", "short_name", "weight", "market_cap_usd",
                           "country", "gics_sector"]].copy()
            d.columns = ["Ticker", "Name", "Weight %", "Mkt Cap ($M)", "Country", "Sector"]
            show_df(d.round(2))


def _old_view_table_html(rows: list[tuple[str, str]], header_label: str, style: str = "holdings") -> str:
    """Build an HTML table string for old-view cards.
    style: 'holdings' (gray), 'sector' (purple), 'country' (blue-tinted).
    """
    styles = {
        "holdings": ("", "background-color:LightGray;", "border:1px solid gray;"),
        "sector": ("background-color:#f5eef8;", "background-color:#d7bde2;", "border:1px solid #f5eef8;"),
        "country": ("background-color:#eaf4fc;", "background-color:#a8d0e6;", "border:1px solid #eaf4fc;"),
    }
    bg, hdr_bg, border = styles.get(style, styles["holdings"])

    lines = [f'<table style="{border}{bg}font-family:arial,sans-serif;font-size:9pt;'
             f'border-collapse:collapse;width:100%;" cellpadding="1" cellspacing="2">']
    lines.append(f'<tr style="{hdr_bg}border-bottom:1px solid #ddd;text-align:left;">'
                 f'<td colspan="2" style="padding:4px 6px;{hdr_bg}"><b>{header_label}</b></td></tr>')
    for name, pct in rows:
        lines.append(f'<tr style="border-bottom:1px solid #ddd;text-align:left;">'
                     f'<td style="padding:4px 6px;">{name}</td>'
                     f'<td style="padding:4px 6px;white-space:nowrap;text-align:right;">{pct}</td></tr>')
    lines.append('</table>')
    return "\n".join(lines)


def render_old_view(df, alq_name):
    """Tab 9: Old-style card view showing Top 10, Sectors, Country for each fund."""

    # Order: Alquity first, then peers alphabetically
    funds = [alq_name] + sorted(
        [f for f in df["fund_name"].unique() if f != alq_name]
    )

    for fund_name in funds:
        fund_df = df[df["fund_name"] == fund_name].copy()
        if fund_df.empty:
            continue

        is_alq = fund_name == alq_name

        # Aggregate duplicate tickers
        agg = fund_df.groupby("ticker").agg(
            weight=("weight", "sum"),
            short_name=("short_name", "first"),
            gics_sector=("gics_sector", "first"),
            country=("country", "first"),
        ).reset_index()
        agg = agg.sort_values("weight", ascending=False)

        # Top 10 holdings
        top10 = agg.head(10)
        h_rows = []
        for _, row in top10.iterrows():
            name = row["short_name"] if pd.notna(row["short_name"]) and row["short_name"] else row["ticker"]
            h_rows.append((name, f"{row['weight']:.2f}%"))

        # Sector breakdown
        sector_df = agg[agg["gics_sector"].notna() & (agg["gics_sector"] != "")]
        sectors = sector_df.groupby("gics_sector")["weight"].sum().sort_values(ascending=False).reset_index()
        s_rows = [(r["gics_sector"], f"{r['weight']:.2f}%") for _, r in sectors.iterrows()]

        # Country breakdown
        country_df = agg[agg["country"].notna() & (agg["country"] != "")]
        countries = country_df.groupby("country")["weight"].sum().sort_values(ascending=False).reset_index()
        c_rows = [(r["country"], f"{r['weight']:.2f}%") for _, r in countries.iterrows()]

        # Header - dark for Alquity, slightly lighter for peers
        hdr_bg = "#535050" if is_alq else "#6b6b6b"
        tag = " (Alquity)" if is_alq else ""
        st.markdown(
            f'<div style="background-color:{hdr_bg};padding:10px;color:white;'
            f'font-family:arial,sans-serif;font-size:10pt;font-weight:bold;'
            f'margin-top:16px;border-radius:4px 4px 0 0;">'
            f'{fund_name}{tag}</div>',
            unsafe_allow_html=True,
        )

        # 3 columns: Top 10 | Sectors | Country
        col_h, col_s, col_c = st.columns(3)
        with col_h:
            st.markdown("**Top 10 Holdings**")
            st.markdown(_old_view_table_html(h_rows, "Top 10", style="holdings"), unsafe_allow_html=True)
        with col_s:
            st.markdown("**Sectors**")
            st.markdown(_old_view_table_html(s_rows, "GICS Sector", style="sector"), unsafe_allow_html=True)
        with col_c:
            st.markdown("**Country / Region**")
            st.markdown(_old_view_table_html(c_rows, "Country", style="country"), unsafe_allow_html=True)


if __name__ == "__main__":
    main()
