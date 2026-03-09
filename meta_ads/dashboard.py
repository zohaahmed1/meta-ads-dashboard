"""
Meta Ads Dashboard — Interactive Streamlit app for analyzing Meta ad performance.

Launch: streamlit run meta_ads/dashboard.py
   or: python3 meta_ads_tool.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Ensure meta_ads package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meta_ads.meta_api import (
    fetch_insights, fetch_campaigns, VALID_DATE_PRESETS,
    VALID_LEVELS, VALID_BREAKDOWNS, _check_credentials,
    get_accounts, set_active_account, get_active_account,
)
from meta_ads.metrics import (
    insights_to_dataframe, summary_metrics, daily_trend,
    campaign_comparison, creative_performance, audience_breakdown,
    format_currency, format_number, format_pct, format_roas,
)


# ── Page config ──
st.set_page_config(
    page_title="Meta Ads Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Cached data fetchers ──
@st.cache_data(ttl=300, show_spinner=False)
def load_insights(date_preset, level, time_increment=None):
    raw = fetch_insights(
        date_preset=date_preset, level=level, time_increment=time_increment
    )
    return insights_to_dataframe(raw)


@st.cache_data(ttl=300, show_spinner=False)
def load_insights_with_breakdown(date_preset, level, breakdown):
    raw = fetch_insights(
        date_preset=date_preset, level=level, breakdowns=[breakdown]
    )
    return insights_to_dataframe(raw)


@st.cache_data(ttl=600, show_spinner=False)
def load_campaigns():
    return fetch_campaigns()


# ── Sidebar ──
def render_sidebar():
    st.sidebar.title("Meta Ads Dashboard")
    st.sidebar.markdown("---")

    # Account selector
    accounts = get_accounts()
    if len(accounts) > 1:
        account_ids = list(accounts.keys())
        account_labels = [f"{accounts[a]}" for a in account_ids]
        selected_label = st.sidebar.selectbox("Ad Account", account_labels)
        selected_idx = account_labels.index(selected_label)
        set_active_account(account_ids[selected_idx])
    elif accounts:
        only_id = list(accounts.keys())[0]
        st.sidebar.text(f"Account: {accounts[only_id]}")
        set_active_account(only_id)

    st.sidebar.markdown("---")

    date_range = st.sidebar.selectbox(
        "Date Range",
        VALID_DATE_PRESETS,
        index=VALID_DATE_PRESETS.index("last_7d"),
        format_func=lambda x: x.replace("_", " ").title(),
    )

    level = st.sidebar.selectbox(
        "Reporting Level",
        VALID_LEVELS,
        format_func=lambda x: x.title(),
    )

    st.sidebar.markdown("---")

    if st.sidebar.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    return date_range, level


# ── KPI row ──
def render_kpis(summary):
    cols = st.columns(6)
    kpis = [
        ("Spend", format_currency(summary["total_spend"])),
        ("Impressions", format_number(summary["total_impressions"])),
        ("Clicks", format_number(summary["total_clicks"])),
        ("CTR", format_pct(summary["avg_ctr"])),
        ("Conversions", format_number(summary["total_conversions"])),
        ("ROAS", format_roas(summary["roas"])),
    ]
    for col, (label, value) in zip(cols, kpis):
        col.metric(label, value)


# ── Overview tab ──
def render_overview(df, date_range):
    if df.empty:
        st.info("No data for the selected date range.")
        return

    # Daily trends
    daily_df = load_insights(date_range, "campaign", time_increment=1)
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

    # Campaign spend bar chart
    camp_df = campaign_comparison(df)
    if not camp_df.empty:
        st.subheader("Spend by Campaign")
        fig = px.bar(
            camp_df, x="campaign_name", y="spend",
            color="spend", color_continuous_scale="Blues",
            labels={"campaign_name": "Campaign", "spend": "Spend ($)"},
        )
        fig.update_layout(
            showlegend=False, height=350,
            margin=dict(t=20, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)


# ── Campaigns tab ──
def render_campaigns(df):
    if df.empty:
        st.info("No campaign data available.")
        return

    camp_df = campaign_comparison(df)
    if camp_df.empty:
        st.info("No campaigns to compare.")
        return

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


# ── Creatives tab ──
def render_creatives(df, date_range):
    ad_df = load_insights(date_range, "ad")
    if ad_df.empty:
        st.info("No ad-level data available.")
        return

    creative_df = creative_performance(ad_df)
    if creative_df.empty:
        st.info("No creative data to analyze.")
        return

    st.subheader("Ad Creative Performance")

    display_df = creative_df[[
        "ad_name", "spend", "impressions", "clicks",
        "ctr", "total_conversions", "cost_per_conversion", "roas",
    ]].copy()
    display_df.columns = [
        "Ad", "Spend", "Impressions", "Clicks",
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
def render_audience(date_range):
    st.subheader("Audience Breakdowns")

    breakdown_choice = st.selectbox(
        "Select breakdown",
        ["age", "gender", "country", "publisher_platform"],
        format_func=lambda x: x.replace("_", " ").title(),
    )

    with st.spinner(f"Loading {breakdown_choice} breakdown..."):
        bd_df = load_insights_with_breakdown(date_range, "campaign", breakdown_choice)

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

    date_range, level = render_sidebar()

    st.title("Meta Ads Dashboard")
    st.caption("Live data from Meta Marketing API")

    # Fetch main data
    with st.spinner("Loading data from Meta Ads API..."):
        df = load_insights(date_range, level)

    if df.empty:
        st.warning("No data returned for the selected date range. Try a different range.")
        return

    summary = summary_metrics(df)
    render_kpis(summary)

    st.markdown("---")

    # Tabs
    tab_overview, tab_campaigns, tab_creatives, tab_audience, tab_raw = st.tabs(
        ["Overview", "Campaigns", "Creatives", "Audience", "Raw Data"]
    )

    with tab_overview:
        render_overview(df, date_range)

    with tab_campaigns:
        render_campaigns(df)

    with tab_creatives:
        render_creatives(df, date_range)

    with tab_audience:
        render_audience(date_range)

    with tab_raw:
        render_raw_data(df)


if __name__ == "__main__":
    main()
