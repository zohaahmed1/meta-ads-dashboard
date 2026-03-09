"""
Meta Ads Dashboard — Interactive Streamlit app for analyzing Meta ad performance.

Features:
  - Period-over-period comparison with % change arrows
  - Custom date range picker
  - Campaign status filter (Active/Paused/Archived)
  - Budget pacing (budget vs actual spend)
  - Funnel visualization (impressions → clicks → conversions)
  - Frequency alerts (flags high ad frequency)
  - Attribution window toggle (1d click, 7d click, etc.)
  - Auto-refresh on configurable interval
  - Send report summary to Slack via webhook
  - Ad creative thumbnails alongside performance data

Launch: streamlit run meta_ads/dashboard.py
   or: python3 meta_ads_tool.py
"""

import streamlit as st
import streamlit.components.v1
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
import time
import requests as req_lib
from datetime import datetime, timedelta, date

# Ensure meta_ads package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meta_ads.meta_api import (
    fetch_insights, fetch_campaigns, fetch_ads, VALID_DATE_PRESETS,
    VALID_LEVELS, VALID_BREAKDOWNS, _check_credentials,
    get_accounts, set_active_account, get_active_account,
    get_comparison_dates, ATTRIBUTION_WINDOWS,
    get_creative_thumbnails, fetch_ad_previews,
    get_last_api_error,
)
from meta_ads.metrics import (
    insights_to_dataframe, summary_metrics, daily_trend,
    campaign_comparison, creative_performance, audience_breakdown,
    format_currency, format_number, format_pct, format_roas,
    period_comparison, funnel_metrics, budget_pacing,
    generate_recommendations, efficiency_quadrant,
)

# Try optional auto-refresh package
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False


# ── Page config ──
st.set_page_config(
    page_title="Meta Ads Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Cached data fetchers ──
# NOTE: account_id is included in all cache keys so switching accounts works correctly.
@st.cache_data(ttl=300, show_spinner=False)
def load_insights(account_id, date_preset, level, time_increment=None,
                  since=None, until=None, attribution_windows=None):
    raw = fetch_insights(
        date_preset=date_preset, level=level, time_increment=time_increment,
        since=since, until=until, action_attribution_windows=attribution_windows,
        account_id=account_id,
    )
    return insights_to_dataframe(raw)


@st.cache_data(ttl=300, show_spinner=False)
def load_insights_with_breakdown(account_id, date_preset, level, breakdown,
                                 since=None, until=None, attribution_windows=None):
    raw = fetch_insights(
        date_preset=date_preset, level=level, breakdowns=[breakdown],
        since=since, until=until, action_attribution_windows=attribution_windows,
        account_id=account_id,
    )
    return insights_to_dataframe(raw)


@st.cache_data(ttl=600, show_spinner=False)
def load_campaigns(account_id):
    return fetch_campaigns(account_id=account_id)


@st.cache_data(ttl=600, show_spinner=False)
def load_ads(account_id):
    return fetch_ads(account_id=account_id)


@st.cache_data(ttl=600, show_spinner=False)
def load_creative_thumbnails(account_id):
    return get_creative_thumbnails(account_id=account_id)


# ── Sidebar ──
def render_sidebar():
    st.sidebar.title("Meta Ads Dashboard")
    st.sidebar.markdown("---")

    # Account selector
    accounts = get_accounts()
    active_account_id = ""
    if len(accounts) > 1:
        account_ids = list(accounts.keys())
        account_labels = [f"{accounts[a]}" for a in account_ids]
        selected_label = st.sidebar.selectbox("Ad Account", account_labels)
        selected_idx = account_labels.index(selected_label)
        active_account_id = account_ids[selected_idx]
        set_active_account(active_account_id)
    elif accounts:
        only_id = list(accounts.keys())[0]
        st.sidebar.text(f"Account: {accounts[only_id]}")
        active_account_id = only_id
        set_active_account(only_id)

    st.sidebar.markdown("---")

    # ── Date range: preset or custom ──
    date_mode = st.sidebar.radio(
        "Date Range Mode", ["Preset", "Custom"],
        horizontal=True, label_visibility="collapsed",
    )

    date_preset = None
    custom_since = None
    custom_until = None

    if date_mode == "Preset":
        date_preset = st.sidebar.selectbox(
            "Date Range",
            VALID_DATE_PRESETS,
            index=VALID_DATE_PRESETS.index("last_30d"),
            format_func=lambda x: x.replace("_", " ").title(),
        )
    else:
        today = date.today()
        col1, col2 = st.sidebar.columns(2)
        with col1:
            custom_start = st.date_input("From", today - timedelta(days=7))
        with col2:
            custom_end = st.date_input("To", today - timedelta(days=1))
        custom_since = custom_start.isoformat()
        custom_until = custom_end.isoformat()

    # ── Campaign status filter (default: all = no filter) ──
    status_options = ["ACTIVE", "PAUSED", "ARCHIVED"]
    selected_statuses = st.sidebar.multiselect(
        "Campaign Status", status_options, default=[],
        placeholder="All statuses (no filter)",
    )

    # ── Reporting level ──
    level = st.sidebar.selectbox(
        "Reporting Level", VALID_LEVELS,
        format_func=lambda x: x.title(),
    )

    # ── Attribution window ──
    attribution = st.sidebar.selectbox(
        "Attribution Window",
        ["Default (7d click + 1d view)", "1d_click", "7d_click", "28d_click", "1d_view"],
    )
    attr_windows = None
    if attribution != "Default (7d click + 1d view)":
        attr_windows = [attribution]

    st.sidebar.markdown("---")

    # ── Auto-refresh ──
    auto_refresh = st.sidebar.checkbox("Auto-refresh")
    refresh_mins = 5
    if auto_refresh:
        refresh_mins = st.sidebar.slider("Interval (min)", 1, 30, 5)
        if HAS_AUTOREFRESH:
            st_autorefresh(interval=refresh_mins * 60 * 1000, key="auto_refresh")
        else:
            st.sidebar.caption("Install `streamlit-autorefresh` for true auto-refresh")

    # ── Slack webhook (collapsible) ──
    with st.sidebar.expander("Slack Integration"):
        slack_webhook = st.text_input(
            "Webhook URL", type="password",
            placeholder="https://hooks.slack.com/services/...",
            key="slack_webhook",
        )

    if st.sidebar.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    return {
        "account_id": active_account_id,
        "date_preset": date_preset,
        "custom_since": custom_since,
        "custom_until": custom_until,
        "level": level,
        "statuses": selected_statuses,
        "attr_windows": attr_windows,
        "auto_refresh": auto_refresh,
        "refresh_mins": refresh_mins,
        "slack_webhook": slack_webhook,
    }


# ── KPI row with period-over-period deltas ──
def render_kpis(summary, comparison=None):
    cols = st.columns(6)
    kpis = [
        ("Spend", "total_spend", format_currency),
        ("Impressions", "total_impressions", format_number),
        ("Clicks", "total_clicks", format_number),
        ("CTR", "avg_ctr", format_pct),
        ("Conversions", "total_conversions", format_number),
        ("ROAS", "roas", format_roas),
    ]
    for col, (label, key, fmt) in zip(cols, kpis):
        value = fmt(summary[key])
        if comparison and key in comparison:
            delta_pct = comparison[key]["change_pct"]
            delta_str = f"{delta_pct:+.1f}%"
            # For spend, lower could be good or bad depending on context
            # For CTR/ROAS/conversions higher is better
            col.metric(label, value, delta=delta_str)
        else:
            col.metric(label, value)


# ── Funnel tab ──
def render_funnel(df):
    funnel = funnel_metrics(df)
    if not funnel:
        st.info("No funnel data available.")
        return

    st.subheader("Conversion Funnel")

    names = [f[0] for f in funnel]
    values = [f[1] for f in funnel]

    fig = go.Figure(go.Funnel(
        y=names,
        x=values,
        textinfo="value+percent initial+percent previous",
        marker=dict(color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"][:len(funnel)]),
        connector=dict(line=dict(color="gray", width=1)),
    ))
    fig.update_layout(height=400, margin=dict(t=20, b=20, l=20, r=20))
    st.plotly_chart(fig, use_container_width=True)

    # Conversion rates between stages
    st.subheader("Stage-to-Stage Conversion Rates")
    rate_cols = st.columns(len(funnel) - 1)
    for i, col in enumerate(rate_cols):
        if values[i] > 0:
            rate = values[i + 1] / values[i] * 100
            col.metric(
                f"{names[i]} → {names[i+1]}",
                f"{rate:.1f}%",
            )


# ── Overview tab ──
def render_overview(df, config):
    if df.empty:
        st.info("No data for the selected date range.")
        return

    daily_df = load_insights(
        config["account_id"], config["date_preset"], "campaign", time_increment=1,
        since=config["custom_since"], until=config["custom_until"],
        attribution_windows=config["attr_windows"],
    )
    trend = daily_trend(daily_df)

    if not trend.empty:
        st.subheader("Daily Trends")
        col1, col2 = st.columns(2)

        with col1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend["date_start"], y=trend["spend"],
                name="Spend ($)", line=dict(color="#1f77b4"),
            ))
            fig.add_trace(go.Scatter(
                x=trend["date_start"], y=trend["clicks"],
                name="Clicks", yaxis="y2", line=dict(color="#ff7f0e"),
            ))
            fig.update_layout(
                title="Spend & Clicks",
                yaxis=dict(title="Spend ($)"),
                yaxis2=dict(title="Clicks", overlaying="y", side="right"),
                legend=dict(x=0, y=1.15, orientation="h"),
                height=350, margin=dict(t=60, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend["date_start"], y=trend["total_conversions"],
                name="Conversions", line=dict(color="#2ca02c"),
            ))
            fig.add_trace(go.Scatter(
                x=trend["date_start"], y=trend["roas"],
                name="ROAS", yaxis="y2", line=dict(color="#d62728"),
            ))
            fig.update_layout(
                title="Conversions & ROAS",
                yaxis=dict(title="Conversions"),
                yaxis2=dict(title="ROAS", overlaying="y", side="right"),
                legend=dict(x=0, y=1.15, orientation="h"),
                height=350, margin=dict(t=60, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

    camp_df = campaign_comparison(df)
    if not camp_df.empty:
        st.subheader("Spend by Campaign")
        fig = px.bar(
            camp_df, x="campaign_name", y="spend",
            color="spend", color_continuous_scale="Blues",
            labels={"campaign_name": "Campaign", "spend": "Spend ($)"},
        )
        fig.update_layout(showlegend=False, height=350, margin=dict(t=20, b=40))
        st.plotly_chart(fig, use_container_width=True)


# ── Campaigns tab ──
def render_campaigns(df, campaigns_data):
    if df.empty:
        st.info("No campaign data available.")
        return

    camp_df = campaign_comparison(df)
    if camp_df.empty:
        st.info("No campaigns to compare.")
        return

    # ── Frequency alerts ──
    if "frequency" in df.columns:
        high_freq = df[df["frequency"] > 3.0]
        if not high_freq.empty:
            freq_campaigns = high_freq["campaign_name"].unique()
            st.warning(
                f"⚠️ **High Frequency Alert**: {len(freq_campaigns)} campaign(s) "
                f"have frequency > 3.0 — audiences may be experiencing ad fatigue.\n\n"
                + ", ".join(f"**{c}**" for c in freq_campaigns[:5])
            )

    st.subheader("Campaign Comparison")

    display_df = camp_df[[
        "campaign_name", "spend", "impressions", "clicks",
        "ctr", "total_conversions", "cost_per_conversion", "roas",
    ]].copy()
    display_df.columns = [
        "Campaign", "Spend", "Impressions", "Clicks",
        "CTR (%)", "Conversions", "Cost/Conv", "ROAS",
    ]
    display_df["Spend"] = display_df["Spend"].map(lambda x: f"${x:,.2f}")
    display_df["Impressions"] = display_df["Impressions"].map(lambda x: f"{x:,}")
    display_df["Clicks"] = display_df["Clicks"].map(lambda x: f"{x:,}")
    display_df["CTR (%)"] = display_df["CTR (%)"].map(lambda x: f"{x:.2f}%")
    display_df["Conversions"] = display_df["Conversions"].map(lambda x: f"{x:,.0f}")
    display_df["Cost/Conv"] = display_df["Cost/Conv"].map(lambda x: f"${x:,.2f}")
    display_df["ROAS"] = display_df["ROAS"].map(lambda x: f"{x:.2f}x")

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            camp_df, x="campaign_name", y="total_conversions",
            title="Conversions by Campaign",
            labels={"campaign_name": "Campaign", "total_conversions": "Conversions"},
            color="total_conversions", color_continuous_scale="Greens",
        )
        fig.update_layout(showlegend=False, height=350, margin=dict(t=40, b=40))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            camp_df, x="campaign_name", y="roas",
            title="ROAS by Campaign",
            labels={"campaign_name": "Campaign", "roas": "ROAS"},
            color="roas", color_continuous_scale="Reds",
        )
        fig.update_layout(showlegend=False, height=350, margin=dict(t=40, b=40))
        st.plotly_chart(fig, use_container_width=True)

    # ── Budget pacing ──
    if campaigns_data:
        pacing_df = budget_pacing(campaigns_data, df)
        pacing_df = pacing_df[pacing_df["budget"] > 0]
        if not pacing_df.empty:
            st.markdown("---")
            st.subheader("Budget Pacing")

            fig = go.Figure()
            colors = []
            for _, row in pacing_df.iterrows():
                pct = row["pacing_pct"]
                if pct < 80:
                    colors.append("#2ca02c")  # Under budget — green
                elif pct < 100:
                    colors.append("#ff7f0e")  # Approaching — orange
                else:
                    colors.append("#d62728")  # Over budget — red

            fig.add_trace(go.Bar(
                y=pacing_df["campaign_name"],
                x=pacing_df["actual_spend"],
                orientation="h",
                name="Actual Spend",
                marker_color=colors,
                text=pacing_df["pacing_pct"].map(lambda x: f"{x:.0f}%"),
                textposition="auto",
            ))
            fig.add_trace(go.Bar(
                y=pacing_df["campaign_name"],
                x=pacing_df["budget"] - pacing_df["actual_spend"].clip(upper=pacing_df["budget"]),
                orientation="h",
                name="Remaining Budget",
                marker_color="rgba(200,200,200,0.4)",
            ))
            fig.update_layout(
                barmode="stack",
                height=max(200, len(pacing_df) * 50),
                margin=dict(t=20, b=40, l=20, r=20),
                legend=dict(x=0, y=1.1, orientation="h"),
                xaxis_title="Dollars ($)",
            )
            st.plotly_chart(fig, use_container_width=True)


# ── Creatives tab ──
def render_creatives(df, config):
    ad_df = load_insights(
        config["account_id"], config["date_preset"], "ad",
        since=config["custom_since"], until=config["custom_until"],
        attribution_windows=config["attr_windows"],
    )
    if ad_df.empty:
        st.info("No ad-level data available.")
        return

    creative_df = creative_performance(ad_df)
    if creative_df.empty:
        st.info("No creative data to analyze.")
        return

    # Fetch ad creative thumbnails via the /adcreatives endpoint (reliable)
    # 1. Map ad_id -> creative_id from ads data
    ads_data = load_ads(config["account_id"])
    ad_to_creative = {}
    for ad in ads_data:
        ad_id = ad.get("id", "")
        creative = ad.get("creative", {})
        if ad_id and creative.get("id"):
            ad_to_creative[ad_id] = creative["id"]

    # 2. Get creative_id -> thumbnail_url from creatives endpoint
    creative_thumbs = load_creative_thumbnails(config["account_id"])

    # 3. Build ad_id -> thumbnail_url
    creative_map = {}
    for ad_id, creative_id in ad_to_creative.items():
        creative_map[ad_id] = creative_thumbs.get(creative_id, "")

    # 4. For ads without thumbnails, try ad preview API
    missing_ids = [
        row.get("ad_id", "") for _, row in creative_df.iterrows()
        if row.get("ad_id", "") and not creative_map.get(row.get("ad_id", ""))
    ]
    preview_map = {}
    if missing_ids:
        with st.spinner("Loading ad previews..."):
            preview_map = fetch_ad_previews(missing_ids[:10])  # Cap at 10 to avoid rate limits

    st.subheader("Ad Creative Performance")

    # Show creatives with thumbnails
    for _, row in creative_df.iterrows():
        ad_id = row.get("ad_id", "")
        thumb = creative_map.get(ad_id, "")
        preview_html = preview_map.get(ad_id, "")

        col_img, col_data = st.columns([1, 4])

        with col_img:
            if thumb:
                st.image(thumb, width=120)
            elif preview_html:
                st.components.v1.html(preview_html, height=250, scrolling=True)
            else:
                st.markdown("🖼️ *No preview*")

        with col_data:
            st.markdown(f"**{row['ad_name']}**")
            mcols = st.columns(6)
            mcols[0].metric("Spend", f"${row['spend']:,.2f}")
            mcols[1].metric("Impressions", f"{row['impressions']:,}")
            mcols[2].metric("Clicks", f"{row['clicks']:,}")
            mcols[3].metric("CTR", f"{row['ctr']:.2f}%")
            mcols[4].metric("Conversions", f"{row['total_conversions']:,.0f}")
            mcols[5].metric("ROAS", f"{row['roas']:.2f}x")

        st.markdown("---")

    # Scatter: spend vs conversions
    if len(creative_df) > 1:
        st.subheader("Spend vs Conversions")
        fig = px.scatter(
            creative_df,
            x="spend", y="total_conversions",
            size="impressions", color="roas",
            hover_name="ad_name",
            labels={
                "spend": "Spend ($)",
                "total_conversions": "Conversions",
                "roas": "ROAS",
            },
            color_continuous_scale="RdYlGn",
        )
        fig.update_layout(height=400, margin=dict(t=20, b=40))
        st.plotly_chart(fig, use_container_width=True)


# ── Audience tab ──
def render_audience(config):
    st.subheader("Audience Breakdowns")

    breakdown_choice = st.selectbox(
        "Select breakdown",
        ["age", "gender", "country", "publisher_platform"],
        format_func=lambda x: x.replace("_", " ").title(),
    )

    with st.spinner(f"Loading {breakdown_choice} breakdown..."):
        bd_df = load_insights_with_breakdown(
            config["account_id"], config["date_preset"], "campaign", breakdown_choice,
            since=config["custom_since"], until=config["custom_until"],
            attribution_windows=config["attr_windows"],
        )

    if bd_df.empty:
        st.info(f"No {breakdown_choice} data available.")
        return

    bd_agg = audience_breakdown(bd_df, breakdown_choice)
    if bd_agg.empty:
        st.info(f"No {breakdown_choice} breakdown data.")
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        if breakdown_choice == "gender":
            fig = px.pie(
                bd_agg, values="spend", names=breakdown_choice,
                title=f"Spend by {breakdown_choice.title()}",
            )
        else:
            fig = px.bar(
                bd_agg, x=breakdown_choice, y="spend",
                title=f"Spend by {breakdown_choice.replace('_', ' ').title()}",
                color="spend", color_continuous_scale="Blues",
            )
        fig.update_layout(height=400, margin=dict(t=40, b=40))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        display_bd = bd_agg.copy()
        display_bd["spend"] = display_bd["spend"].map(lambda x: f"${x:,.2f}")
        display_bd["impressions"] = display_bd["impressions"].map(lambda x: f"{x:,}")
        display_bd["clicks"] = display_bd["clicks"].map(lambda x: f"{x:,}")
        display_bd["ctr"] = display_bd["ctr"].map(lambda x: f"{x:.2f}%")
        display_bd["total_conversions"] = display_bd["total_conversions"].map(lambda x: f"{x:,.0f}")
        display_bd["roas"] = display_bd["roas"].map(lambda x: f"{x:.2f}x")
        st.dataframe(display_bd, use_container_width=True, hide_index=True)


# ── Analysis & Optimizations tab ──
def render_analysis(df, summary, comparison, config):
    if df.empty:
        st.info("No data to analyze.")
        return

    # ── Optimization Recommendations ──
    st.subheader("Optimization Recommendations")
    camp_df = campaign_comparison(df)
    recs = generate_recommendations(camp_df, summary)

    if recs:
        for icon, msg in recs:
            st.markdown(f"{icon} {msg}")
    else:
        st.success("All campaigns look healthy — no immediate actions needed.")

    st.markdown("---")

    # ── Efficiency Quadrant ──
    if not camp_df.empty and len(camp_df) > 1:
        st.subheader("Campaign Efficiency Quadrant")
        quad_df = efficiency_quadrant(camp_df)

        color_map = {
            "Stars": "#2ca02c",
            "Question Marks": "#ff7f0e",
            "Cash Cows": "#1f77b4",
            "Dogs": "#d62728",
        }

        fig = px.scatter(
            quad_df, x="spend", y="roas",
            size="total_conversions",
            color="quadrant",
            hover_name="campaign_name",
            color_discrete_map=color_map,
            labels={"spend": "Spend ($)", "roas": "ROAS", "quadrant": "Quadrant"},
        )
        # Add quadrant lines
        fig.add_hline(y=quad_df["roas"].median(), line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=quad_df["spend"].median(), line_dash="dash", line_color="gray", opacity=0.5)
        fig.update_layout(height=450, margin=dict(t=20, b=40))
        st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3, col4 = st.columns(4)
        for col, q, desc in [
            (col1, "Stars", "High ROAS, high spend — scale these"),
            (col2, "Question Marks", "High ROAS, low spend — test & scale"),
            (col3, "Cash Cows", "Low ROAS, high spend — optimize or cut"),
            (col4, "Dogs", "Low ROAS, low spend — consider pausing"),
        ]:
            q_camps = quad_df[quad_df["quadrant"] == q]["campaign_name"].tolist()
            with col:
                st.markdown(f"**{q}**")
                st.caption(desc)
                if q_camps:
                    for c in q_camps:
                        st.markdown(f"- {c}")
                else:
                    st.markdown("*None*")

        st.markdown("---")

    # ── Period-over-period deep dive ──
    if comparison:
        st.subheader("Period-over-Period Analysis")

        metrics_to_show = [
            ("Spend", "total_spend", format_currency),
            ("Impressions", "total_impressions", format_number),
            ("Clicks", "total_clicks", format_number),
            ("CTR", "avg_ctr", format_pct),
            ("CPC", "avg_cpc", format_currency),
            ("Conversions", "total_conversions", format_number),
            ("Cost/Conv", "cost_per_conversion", format_currency),
            ("ROAS", "roas", format_roas),
        ]

        rows = []
        for label, key, fmt in metrics_to_show:
            c = comparison.get(key, {})
            rows.append({
                "Metric": label,
                "Current": fmt(c.get("current", 0)),
                "Previous": fmt(c.get("previous", 0)),
                "Change": f"{c.get('change_pct', 0):+.1f}%",
                "Direction": "Improved" if c.get("improved") else "Declined",
            })

        comp_df = pd.DataFrame(rows)
        st.dataframe(comp_df, use_container_width=True, hide_index=True)

    # ── Top & Bottom performers ──
    if not camp_df.empty:
        st.subheader("Performance Rankings")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Top Performers (by ROAS)**")
            top = camp_df.nlargest(5, "roas")[["campaign_name", "roas", "spend", "total_conversions"]].copy()
            top.columns = ["Campaign", "ROAS", "Spend", "Conversions"]
            top["ROAS"] = top["ROAS"].map(lambda x: f"{x:.2f}x")
            top["Spend"] = top["Spend"].map(lambda x: f"${x:,.2f}")
            top["Conversions"] = top["Conversions"].map(lambda x: f"{x:,.0f}")
            st.dataframe(top, use_container_width=True, hide_index=True)

        with col2:
            st.markdown("**Lowest Performers (by ROAS)**")
            bottom = camp_df.nsmallest(5, "roas")[["campaign_name", "roas", "spend", "total_conversions"]].copy()
            bottom.columns = ["Campaign", "ROAS", "Spend", "Conversions"]
            bottom["ROAS"] = bottom["ROAS"].map(lambda x: f"{x:.2f}x")
            bottom["Spend"] = bottom["Spend"].map(lambda x: f"${x:,.2f}")
            bottom["Conversions"] = bottom["Conversions"].map(lambda x: f"{x:,.0f}")
            st.dataframe(bottom, use_container_width=True, hide_index=True)

    # ── Cost efficiency breakdown ──
    st.subheader("Cost Efficiency")
    eff_cols = st.columns(4)
    eff_cols[0].metric("Avg CPC", format_currency(summary["avg_cpc"]))
    eff_cols[1].metric("Avg CPM", format_currency(summary["avg_cpm"]))
    eff_cols[2].metric("Cost/Conversion", format_currency(summary["cost_per_conversion"]))
    eff_cols[3].metric("Avg Frequency", f"{summary['avg_frequency']:.1f}")


# ── Raw data tab ──
def render_raw_data(df):
    if df.empty:
        st.info("No data to display.")
        return

    st.subheader("Raw Data")
    st.dataframe(df, use_container_width=True, height=500, hide_index=True)

    csv = df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="meta_ads_export.csv",
        mime="text/csv",
    )


# ── Slack report ──
def send_slack_report(webhook_url, summary, account_name, date_label):
    """Post a formatted summary to Slack via incoming webhook."""
    text = (
        f"📊 *Meta Ads Report — {account_name}*\n"
        f"Period: {date_label}\n\n"
        f"💰 Spend: {format_currency(summary['total_spend'])}\n"
        f"👁️ Impressions: {format_number(summary['total_impressions'])}\n"
        f"🖱️ Clicks: {format_number(summary['total_clicks'])}\n"
        f"📈 CTR: {format_pct(summary['avg_ctr'])}\n"
        f"🎯 Conversions: {format_number(summary['total_conversions'])}\n"
        f"💎 ROAS: {format_roas(summary['roas'])}\n"
        f"💵 CPC: {format_currency(summary['avg_cpc'])}\n"
        f"📉 Cost/Conv: {format_currency(summary['cost_per_conversion'])}"
    )
    try:
        resp = req_lib.post(webhook_url, json={"text": text}, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


# ── Main app ──
def main():
    if not _check_credentials():
        st.error(
            "Meta Ads credentials not configured. "
            "Set META_ACCESS_TOKEN and META_AD_ACCOUNT_ID in your .env file."
        )
        st.code(
            "# Add to .env:\n"
            "META_ACCESS_TOKEN=your_token_here\n"
            "META_AD_ACCOUNT_ID=act_123456789",
            language="bash",
        )
        return

    config = render_sidebar()
    date_preset = config["date_preset"]
    level = config["level"]

    st.title("Meta Ads Dashboard")
    st.caption("Live data from Meta Marketing API")

    account_id = config["account_id"]

    # ── Fetch main data ──
    with st.spinner("Loading data from Meta Ads API..."):
        df = load_insights(
            account_id, date_preset, level,
            since=config["custom_since"], until=config["custom_until"],
            attribution_windows=config["attr_windows"],
        )

    # ── Filter by campaign status ──
    campaigns_data = load_campaigns(account_id)
    if config["statuses"] and not df.empty and "campaign_id" in df.columns:
        active_ids = {
            c["id"] for c in campaigns_data
            if c.get("status") in config["statuses"]
        }
        if active_ids:
            df = df[df["campaign_id"].isin(active_ids)]

    if df.empty:
        # Check for API error first
        api_error = get_last_api_error()
        if api_error:
            st.error(f"Meta API Error: {api_error}")
        else:
            st.warning("No data returned for the selected filters.")

        # ── Diagnostics: help the user figure out why ──
        with st.expander("Troubleshooting Info", expanded=True):
            st.markdown(f"**Account ID:** `{account_id}`")
            date_label = date_preset or f"{config['custom_since']} → {config['custom_until']}"
            st.markdown(f"**Date range:** {date_label}")
            st.markdown(f"**Status filter:** {config['statuses'] if config['statuses'] else 'None (all)'}")

            if api_error:
                st.markdown(f"**API Error:** `{api_error}`")
                if "expired" in api_error.lower() or "token" in api_error.lower() or "190" in str(api_error):
                    st.error("Your Meta access token may have expired. Generate a new one at https://developers.facebook.com/tools/explorer/ and update Streamlit secrets.")
                elif "100" in str(api_error) or "permission" in api_error.lower():
                    st.error("Permission issue — the token may not have access to this ad account.")

            # Check what campaigns actually exist
            if campaigns_data:
                statuses_found = {}
                for c in campaigns_data:
                    s = c.get("status", "UNKNOWN")
                    statuses_found[s] = statuses_found.get(s, 0) + 1
                st.markdown(f"**Campaigns in account:** {len(campaigns_data)}")
                for s, count in statuses_found.items():
                    st.markdown(f"  - {s}: {count}")
            else:
                st.markdown("**Campaigns in account:** 0 (or API error)")

            st.markdown(f"**Raw insight rows (before status filter):** {len(df)}")

            if not api_error and len(df) == 0:
                st.info("The Meta API returned no data for this account and date range. The account may not have had any ad spend in this period.")
        return

    # ── Period-over-period comparison ──
    summary = summary_metrics(df)
    comparison = None

    if date_preset:
        cs, cu, ps, pu = get_comparison_dates(date_preset)
        if cs and ps:
            try:
                prev_df = load_insights(
                    account_id, None, level, since=ps, until=pu,
                    attribution_windows=config["attr_windows"],
                )
                if config["statuses"] and not prev_df.empty and "campaign_id" in prev_df.columns:
                    active_ids = {
                        c["id"] for c in campaigns_data
                        if c.get("status") in config["statuses"]
                    }
                    if active_ids:
                        prev_df = prev_df[prev_df["campaign_id"].isin(active_ids)]
                if not prev_df.empty:
                    prev_summary = summary_metrics(prev_df)
                    comparison = period_comparison(summary, prev_summary)
            except Exception:
                pass  # Comparison is optional, don't break the dashboard

    render_kpis(summary, comparison)

    # ── Slack report button ──
    webhook = config.get("slack_webhook", "")
    if webhook:
        col_slack, _ = st.columns([1, 5])
        with col_slack:
            if st.button("📤 Send to Slack"):
                accounts = get_accounts()
                acct = get_active_account()
                acct_name = accounts.get(acct, acct)
                date_label = date_preset or f"{config['custom_since']} to {config['custom_until']}"
                if send_slack_report(webhook, summary, acct_name, date_label):
                    st.success("Sent!")
                else:
                    st.error("Failed to send. Check your webhook URL.")

    st.markdown("---")

    # ── Tabs ──
    tab_overview, tab_analysis, tab_funnel, tab_campaigns, tab_creatives, tab_audience, tab_raw = st.tabs(
        ["Overview", "Analysis", "Funnel", "Campaigns", "Creatives", "Audience", "Raw Data"]
    )

    with tab_overview:
        render_overview(df, config)

    with tab_analysis:
        render_analysis(df, summary, comparison, config)

    with tab_funnel:
        render_funnel(df)

    with tab_campaigns:
        render_campaigns(df, campaigns_data)

    with tab_creatives:
        render_creatives(df, config)

    with tab_audience:
        render_audience(config)

    with tab_raw:
        render_raw_data(df)


if __name__ == "__main__":
    main()
