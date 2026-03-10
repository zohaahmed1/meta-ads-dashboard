"""
Metric calculations, data transformation, and aggregation for Meta Ads data.
Converts raw API responses into analysis-ready pandas DataFrames.
"""

import pandas as pd
import numpy as np


def _safe_float(value):
    """Safely convert a Meta API value to float.

    Meta sometimes returns plain numbers, sometimes lists of
    {action_type, value} dicts for the same field across accounts.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    if isinstance(value, list):
        # Sum all values in the list (it's an actions-style array)
        total = 0.0
        for item in value:
            if isinstance(item, dict):
                total += float(item.get("value", 0))
            else:
                total += float(item)
        return total
    return 0.0


def extract_action(actions_list, action_type):
    """Extract a specific action value from Meta's actions array.

    Meta returns actions as: [{"action_type": "link_click", "value": "42"}, ...]
    This pulls out a single action type's value.

    Args:
        actions_list: list of dicts or None
        action_type: str, e.g. 'link_click', 'lead', 'purchase',
                     'offsite_conversion.fb_pixel_purchase'

    Returns: float (0.0 if not found)
    """
    if not actions_list:
        return 0.0
    for action in actions_list:
        if action.get("action_type") == action_type:
            return float(action.get("value", 0))
    return 0.0


def extract_all_actions(actions_list):
    """Flatten all actions into a dict keyed by action_type.

    Returns: dict like {"link_click": 42.0, "lead": 5.0, ...}
    """
    if not actions_list:
        return {}
    return {
        a.get("action_type"): float(a.get("value", 0))
        for a in actions_list
        if a.get("action_type")
    }


def insights_to_dataframe(raw_insights):
    """Convert raw Meta API insights to a clean pandas DataFrame.

    Flattens nested actions/action_values into individual columns and
    casts numeric fields to appropriate types.

    Args:
        raw_insights: list of dicts from fetch_insights()

    Returns: pd.DataFrame with typed columns
    """
    if not raw_insights:
        return pd.DataFrame()

    rows = []
    for row in raw_insights:
        actions = extract_all_actions(row.get("actions"))
        cost_per_actions = extract_all_actions(row.get("cost_per_action_type"))
        action_values = extract_all_actions(row.get("action_values"))

        # Calculate ROAS: total conversion value / spend
        spend = float(row.get("spend", 0))
        total_conversion_value = sum(action_values.values())
        roas = total_conversion_value / spend if spend > 0 else 0.0

        # Sum all conversion-type actions for total conversions
        conversion_types = [
            "offsite_conversion.fb_pixel_purchase",
            "offsite_conversion.fb_pixel_lead",
            "lead", "purchase", "complete_registration",
            "offsite_conversion.fb_pixel_complete_registration",
        ]
        total_conversions = sum(
            actions.get(ct, 0) for ct in conversion_types
        )
        # Fallback: if no specific conversion types, use "conversions" field
        if total_conversions == 0:
            total_conversions = _safe_float(row.get("conversions", 0))

        parsed = {
            "campaign_name": row.get("campaign_name", ""),
            "campaign_id": row.get("campaign_id", ""),
            "adset_name": row.get("adset_name", ""),
            "adset_id": row.get("adset_id", ""),
            "ad_name": row.get("ad_name", ""),
            "ad_id": row.get("ad_id", ""),
            "date_start": row.get("date_start", ""),
            "date_stop": row.get("date_stop", ""),
            "impressions": int(row.get("impressions", 0)),
            "reach": int(row.get("reach", 0)),
            "frequency": float(row.get("frequency", 0)),
            "clicks": int(row.get("clicks", 0)),
            "unique_clicks": int(row.get("unique_clicks", 0) or 0),
            "ctr": float(row.get("ctr", 0)),
            "unique_ctr": float(row.get("unique_ctr", 0) or 0),
            "cpc": float(row.get("cpc", 0) or 0),
            "cpm": float(row.get("cpm", 0) or 0),
            "spend": spend,
            "link_clicks": actions.get("link_click", 0),
            "landing_page_views": actions.get("landing_page_view", 0),
            "leads": actions.get("lead", 0) + actions.get("offsite_conversion.fb_pixel_lead", 0),
            "purchases": actions.get("purchase", 0) + actions.get("offsite_conversion.fb_pixel_purchase", 0),
            "registrations": actions.get("complete_registration", 0) + actions.get("offsite_conversion.fb_pixel_complete_registration", 0),
            "total_conversions": total_conversions,
            "cost_per_link_click": cost_per_actions.get("link_click", 0),
            "cost_per_lead": cost_per_actions.get("lead", 0) or cost_per_actions.get("offsite_conversion.fb_pixel_lead", 0),
            "cost_per_purchase": cost_per_actions.get("purchase", 0) or cost_per_actions.get("offsite_conversion.fb_pixel_purchase", 0),
            "cost_per_conversion": _safe_float(row.get("cost_per_conversion", 0)),
            "conversion_value": total_conversion_value,
            "roas": roas,
        }

        # Preserve breakdown columns (age, gender, country, etc.)
        for key in ["age", "gender", "country", "placement",
                     "device_platform", "publisher_platform",
                     "platform_position"]:
            if key in row:
                parsed[key] = row[key]

        rows.append(parsed)

    df = pd.DataFrame(rows)

    if "date_start" in df.columns and not df.empty:
        df["date_start"] = pd.to_datetime(df["date_start"], errors="coerce")
        df["date_stop"] = pd.to_datetime(df["date_stop"], errors="coerce")

    return df


def summary_metrics(df):
    """Calculate aggregate summary metrics across all rows.

    Returns: dict with total and average KPIs
    """
    if df.empty:
        return {
            "total_spend": 0, "total_impressions": 0, "total_reach": 0,
            "total_clicks": 0, "total_conversions": 0,
            "avg_ctr": 0, "avg_cpc": 0, "avg_cpm": 0,
            "cost_per_conversion": 0, "roas": 0, "avg_frequency": 0,
        }

    total_spend = df["spend"].sum()
    total_impressions = df["impressions"].sum()
    total_clicks = df["clicks"].sum()
    total_conversions = df["total_conversions"].sum()
    total_conversion_value = df["conversion_value"].sum()

    return {
        "total_spend": total_spend,
        "total_impressions": total_impressions,
        "total_reach": df["reach"].sum(),
        "total_clicks": total_clicks,
        "total_conversions": total_conversions,
        "avg_ctr": (total_clicks / total_impressions * 100) if total_impressions > 0 else 0,
        "avg_cpc": (total_spend / total_clicks) if total_clicks > 0 else 0,
        "avg_cpm": (total_spend / total_impressions * 1000) if total_impressions > 0 else 0,
        "cost_per_conversion": (total_spend / total_conversions) if total_conversions > 0 else 0,
        "roas": (total_conversion_value / total_spend) if total_spend > 0 else 0,
        "avg_frequency": df["frequency"].mean(),
    }


def daily_trend(df):
    """Aggregate metrics by date for time-series charts.

    Returns: DataFrame indexed by date with daily totals
    """
    if df.empty or "date_start" not in df.columns:
        return pd.DataFrame()

    daily = df.groupby("date_start").agg({
        "spend": "sum",
        "impressions": "sum",
        "reach": "sum",
        "clicks": "sum",
        "total_conversions": "sum",
        "conversion_value": "sum",
    }).reset_index()

    daily["ctr"] = (daily["clicks"] / daily["impressions"] * 100).fillna(0)
    daily["cpc"] = (daily["spend"] / daily["clicks"]).fillna(0)
    daily["roas"] = (daily["conversion_value"] / daily["spend"]).fillna(0)
    daily = daily.sort_values("date_start")
    return daily


def campaign_comparison(df):
    """Aggregate metrics per campaign for comparison table.

    Returns: DataFrame with one row per campaign, sorted by spend desc
    """
    if df.empty or "campaign_name" not in df.columns:
        return pd.DataFrame()

    grouped = df.groupby(["campaign_name", "campaign_id"]).agg({
        "spend": "sum",
        "impressions": "sum",
        "reach": "sum",
        "clicks": "sum",
        "total_conversions": "sum",
        "conversion_value": "sum",
    }).reset_index()

    grouped["ctr"] = (grouped["clicks"] / grouped["impressions"] * 100).fillna(0)
    grouped["cpc"] = (grouped["spend"] / grouped["clicks"]).fillna(0)
    grouped["cost_per_conversion"] = (grouped["spend"] / grouped["total_conversions"]).fillna(0)
    grouped["roas"] = (grouped["conversion_value"] / grouped["spend"]).fillna(0)
    grouped = grouped.sort_values("spend", ascending=False)
    return grouped


def creative_performance(df):
    """Aggregate metrics per ad (creative level) for comparison.

    Returns: DataFrame with one row per ad, sorted by conversions desc
    """
    if df.empty or "ad_name" not in df.columns:
        return pd.DataFrame()

    grouped = df.groupby(["ad_name", "ad_id"]).agg({
        "spend": "sum",
        "impressions": "sum",
        "clicks": "sum",
        "total_conversions": "sum",
        "conversion_value": "sum",
    }).reset_index()

    grouped["ctr"] = (grouped["clicks"] / grouped["impressions"] * 100).fillna(0)
    grouped["cpc"] = (grouped["spend"] / grouped["clicks"]).fillna(0)
    grouped["cost_per_conversion"] = (grouped["spend"] / grouped["total_conversions"]).fillna(0)
    grouped["roas"] = (grouped["conversion_value"] / grouped["spend"]).fillna(0)
    grouped = grouped.sort_values("total_conversions", ascending=False)
    return grouped


def audience_breakdown(df, breakdown_col):
    """Aggregate metrics by a breakdown dimension.

    Args:
        df: DataFrame that was fetched with the given breakdown
        breakdown_col: 'age', 'gender', 'country', 'placement', etc.

    Returns: DataFrame grouped by breakdown_col, sorted by spend desc
    """
    if df.empty or breakdown_col not in df.columns:
        return pd.DataFrame()

    grouped = df.groupby(breakdown_col).agg({
        "spend": "sum",
        "impressions": "sum",
        "clicks": "sum",
        "total_conversions": "sum",
        "conversion_value": "sum",
    }).reset_index()

    grouped["ctr"] = (grouped["clicks"] / grouped["impressions"] * 100).fillna(0)
    grouped["cpc"] = (grouped["spend"] / grouped["clicks"]).fillna(0)
    grouped["cost_per_conversion"] = (grouped["spend"] / grouped["total_conversions"]).fillna(0)
    grouped["roas"] = (grouped["conversion_value"] / grouped["spend"]).fillna(0)
    grouped = grouped.sort_values("spend", ascending=False)
    return grouped


def period_comparison(current_summary, previous_summary):
    """Calculate % change between current and previous period for each KPI.

    Returns: dict of {metric: {current, previous, change_pct, improved}}
    """
    # Metrics where lower is better
    lower_is_better = {"cost_per_conversion", "avg_cpc", "avg_cpm"}
    comparison = {}
    for key in current_summary:
        cur = current_summary[key]
        prev = previous_summary[key]
        if prev and prev != 0:
            pct = ((cur - prev) / abs(prev)) * 100
        else:
            pct = 0.0 if cur == 0 else 100.0
        improved = pct < 0 if key in lower_is_better else pct > 0
        comparison[key] = {
            "current": cur,
            "previous": prev,
            "change_pct": pct,
            "improved": improved,
        }
    return comparison


def funnel_metrics(df):
    """Build funnel data: impressions -> clicks -> landing page views -> conversions.

    Returns: list of (stage_name, value) tuples
    """
    if df.empty:
        return []
    stages = [
        ("Impressions", df["impressions"].sum()),
        ("Clicks", df["clicks"].sum()),
        ("Link Clicks", df["link_clicks"].sum()),
        ("Landing Page Views", df["landing_page_views"].sum()),
        ("Conversions", df["total_conversions"].sum()),
    ]
    # Filter out zero stages (except impressions)
    return [(name, val) for name, val in stages if val > 0 or name == "Impressions"]


def budget_pacing(campaigns, insights_df):
    """Calculate budget pacing: budget vs actual spend per campaign.

    Args:
        campaigns: list of campaign dicts from fetch_campaigns()
        insights_df: DataFrame from insights_to_dataframe()

    Returns: DataFrame with budget, spend, and pacing % per campaign
    """
    if not campaigns or insights_df.empty:
        return pd.DataFrame()

    spend_by_campaign = insights_df.groupby("campaign_id")["spend"].sum().to_dict()

    rows = []
    for camp in campaigns:
        cid = camp.get("id", "")
        # Meta returns budgets in cents
        daily_b = float(camp.get("daily_budget", 0) or 0) / 100
        lifetime_b = float(camp.get("lifetime_budget", 0) or 0) / 100
        actual = spend_by_campaign.get(cid, 0)
        budget = daily_b or lifetime_b
        pacing = (actual / budget * 100) if budget > 0 else 0

        rows.append({
            "campaign_name": camp.get("name", ""),
            "campaign_id": cid,
            "status": camp.get("status", ""),
            "daily_budget": daily_b,
            "lifetime_budget": lifetime_b,
            "budget": budget,
            "actual_spend": actual,
            "pacing_pct": pacing,
        })

    return pd.DataFrame(rows).sort_values("actual_spend", ascending=False)


def generate_recommendations(camp_df, summary, client_context=None):
    """Generate actionable optimization recommendations from campaign data.

    Args:
        camp_df: DataFrame from campaign_comparison()
        summary: dict from summary_metrics()
        client_context: optional dict with keys: name, industry, goal,
                        target_cpa, target_roas, monthly_budget, notes

    Returns: list of (icon, message) tuples sorted by severity
    """
    if camp_df.empty:
        return []

    ctx = client_context or {}
    target_cpa = ctx.get("target_cpa")
    target_roas = ctx.get("target_roas")
    monthly_budget = ctx.get("monthly_budget")
    goal = ctx.get("goal", "")
    client_name = ctx.get("name", "")

    recs = []
    avg_roas = summary.get("roas", 0)
    avg_cpc = summary.get("avg_cpc", 0)
    avg_ctr = summary.get("avg_ctr", 0)
    avg_cost_per_conv = summary.get("cost_per_conversion", 0)
    total_spend = summary.get("total_spend", 1)

    # ── Client target-based recommendations ──
    if target_roas and avg_roas > 0:
        if avg_roas < target_roas:
            gap_pct = ((target_roas - avg_roas) / target_roas) * 100
            recs.append((1, "🎯", f"Account ROAS ({avg_roas:.2f}x) is **{gap_pct:.0f}% below target** ({target_roas:.1f}x). Priority: cut low-ROAS campaigns, scale winners."))
        else:
            recs.append((4, "✅", f"Account ROAS ({avg_roas:.2f}x) is **above target** ({target_roas:.1f}x). Consider scaling spend to capture more volume."))

    if target_cpa and avg_cost_per_conv > 0:
        if avg_cost_per_conv > target_cpa:
            over_pct = ((avg_cost_per_conv - target_cpa) / target_cpa) * 100
            recs.append((1, "🎯", f"Avg CPA (${avg_cost_per_conv:,.2f}) is **{over_pct:.0f}% over target** (${target_cpa:,.2f}). Tighten audiences or pause expensive campaigns."))
        else:
            recs.append((4, "✅", f"Avg CPA (${avg_cost_per_conv:,.2f}) is **under target** (${target_cpa:,.2f}). Room to scale volume."))

    if monthly_budget and total_spend > 0:
        # Estimate monthly run rate from current spend period
        pacing_pct = (total_spend / monthly_budget) * 100
        if pacing_pct > 110:
            recs.append((1, "🔴", f"On track to **overspend** monthly budget (${monthly_budget:,.0f}). Current period spend is ${total_spend:,.0f} ({pacing_pct:.0f}% of monthly)."))
        elif pacing_pct < 60:
            recs.append((2, "🟡", f"**Underspending** vs monthly budget (${monthly_budget:,.0f}). Only ${total_spend:,.0f} spent ({pacing_pct:.0f}%). Increase budgets on top performers."))

    # ── Per-campaign recommendations ──
    for _, row in camp_df.iterrows():
        name = row["campaign_name"]
        roas = row.get("roas", 0)
        spend = row.get("spend", 0)
        ctr = row.get("ctr", 0)
        cost_per = row.get("cost_per_conversion", 0)
        convs = row.get("total_conversions", 0)
        spend_share = (spend / total_spend * 100) if total_spend > 0 else 0

        # Target CPA checks per campaign
        if target_cpa and cost_per > 0 and cost_per > target_cpa * 1.5:
            recs.append((1, "🔴", f"**{name}** — CPA ${cost_per:,.2f} is **{((cost_per/target_cpa)-1)*100:.0f}% over target** (${target_cpa:,.2f}). Cut budget or restructure."))
        elif target_cpa and cost_per > 0 and cost_per < target_cpa * 0.7:
            recs.append((3, "🟢", f"**{name}** — CPA ${cost_per:,.2f} is **well under target**. Scale this campaign."))

        # Target ROAS checks per campaign
        if target_roas and roas > 0 and roas < target_roas * 0.5 and spend_share > 10:
            recs.append((1, "🔴", f"**{name}** — ROAS {roas:.2f}x is **less than half of target** ({target_roas:.1f}x) while consuming {spend_share:.0f}% of budget. Pause or restructure."))

        # Generic checks (only if no client targets set)
        if not target_roas and not target_cpa:
            if roas < 1.0 and spend_share > 10:
                recs.append((1, "🔴", f"**{name}** — ROAS {roas:.2f}x on {spend_share:.0f}% of total spend. Consider pausing or cutting budget."))
            elif roas > avg_roas * 1.5 and roas > 1.5 and spend_share < 40:
                recs.append((3, "🟢", f"**{name}** — {roas:.2f}x ROAS, only {spend_share:.0f}% of budget. Strong candidate to scale."))

        # Low CTR → creative/targeting issue
        if ctr < avg_ctr * 0.5 and spend > 0:
            recs.append((2, "🟡", f"**{name}** — CTR {ctr:.2f}% is well below average ({avg_ctr:.2f}%). Test new creatives or tighten targeting."))

        # Expensive conversions (generic, only without target CPA)
        if not target_cpa and avg_cost_per_conv > 0 and cost_per > avg_cost_per_conv * 2 and convs > 0:
            recs.append((2, "🟡", f"**{name}** — Cost/conversion ${cost_per:,.2f} is 2x+ above account average. Review audiences."))

        # Zero conversions with meaningful spend
        if convs == 0 and spend > total_spend * 0.05:
            recs.append((1, "🔴", f"**{name}** — ${spend:,.0f} spent with zero conversions. Pause or restructure."))

    # ── Account-level insights ──
    if not target_roas and avg_roas > 0 and avg_roas < 1.0:
        recs.append((1, "🔴", f"Account-wide ROAS is {avg_roas:.2f}x — spending more than you're earning. Review all campaigns."))

    if avg_ctr < 1.0:
        recs.append((2, "🟡", f"Account CTR is {avg_ctr:.2f}% — below 1% benchmark. Test new ad formats or creative angles."))

    # ── Goal-specific recommendations ──
    if goal == "Scale Volume" and avg_roas > 1.5:
        recs.append((3, "🟢", f"ROAS is healthy ({avg_roas:.2f}x) — you have headroom to increase budgets 20-30% while maintaining profitability."))
    elif goal == "Brand Awareness":
        recs.append((3, "💡", f"For awareness, focus on CPM (${summary.get('avg_cpm', 0):,.2f}) and reach ({summary.get('total_reach', 0):,}) rather than conversions."))

    # Sort by priority (1=critical, 2=warning, 3=opportunity, 4=positive)
    recs.sort(key=lambda x: x[0])
    return [(icon, msg) for _, icon, msg in recs]


def efficiency_quadrant(camp_df):
    """Classify campaigns into efficiency quadrants based on ROAS and spend share.

    Quadrants:
      - Stars: High ROAS, high spend — scale winners
      - Question Marks: High ROAS, low spend — test & scale
      - Cash Cows: Low ROAS, high spend — optimize or cut
      - Dogs: Low ROAS, low spend — pause

    Returns: camp_df with added 'quadrant' column
    """
    if camp_df.empty:
        return camp_df

    df = camp_df.copy()
    median_roas = df["roas"].median()
    median_spend = df["spend"].median()

    def classify(row):
        high_roas = row["roas"] >= median_roas
        high_spend = row["spend"] >= median_spend
        if high_roas and high_spend:
            return "Stars"
        elif high_roas and not high_spend:
            return "Question Marks"
        elif not high_roas and high_spend:
            return "Cash Cows"
        else:
            return "Dogs"

    df["quadrant"] = df.apply(classify, axis=1)
    return df


def day_of_week_performance(df):
    """Aggregate metrics by day of week to find best/worst performing days.

    Returns: DataFrame with day name, spend, clicks, conversions, ctr, cpc, roas
    """
    if df.empty or "date_start" not in df.columns:
        return pd.DataFrame()

    df2 = df.copy()
    df2["day_of_week"] = pd.to_datetime(df2["date_start"]).dt.day_name()

    grouped = df2.groupby("day_of_week").agg({
        "spend": "sum",
        "impressions": "sum",
        "clicks": "sum",
        "total_conversions": "sum",
        "conversion_value": "sum",
    }).reset_index()

    grouped["ctr"] = (grouped["clicks"] / grouped["impressions"] * 100).fillna(0)
    grouped["cpc"] = (grouped["spend"] / grouped["clicks"]).fillna(0)
    grouped["cost_per_conversion"] = (grouped["spend"] / grouped["total_conversions"]).fillna(0)
    grouped["roas"] = (grouped["conversion_value"] / grouped["spend"]).fillna(0)

    # Sort by standard weekday order
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    grouped["day_order"] = grouped["day_of_week"].map({d: i for i, d in enumerate(day_order)})
    grouped = grouped.sort_values("day_order").drop(columns=["day_order"])
    return grouped


def spend_efficiency_curve(df):
    """Calculate cumulative spend vs cumulative conversions to show diminishing returns.

    Campaigns are sorted by cost_per_conversion (most efficient first).
    Returns: DataFrame with cumulative_spend, cumulative_conversions, campaign_name
    """
    if df.empty:
        return pd.DataFrame()

    camp = campaign_comparison(df)
    if camp.empty:
        return pd.DataFrame()

    # Sort by efficiency (lowest cost per conversion first)
    camp = camp[camp["total_conversions"] > 0].copy()
    if camp.empty:
        return pd.DataFrame()

    camp = camp.sort_values("cost_per_conversion", ascending=True)
    camp["cumulative_spend"] = camp["spend"].cumsum()
    camp["cumulative_conversions"] = camp["total_conversions"].cumsum()
    camp["marginal_cpa"] = camp["spend"] / camp["total_conversions"]
    return camp[["campaign_name", "spend", "total_conversions", "cost_per_conversion",
                  "cumulative_spend", "cumulative_conversions", "marginal_cpa"]]


def detect_anomalies(df, metric="spend", threshold=2.0):
    """Detect daily anomalies using z-score method.

    Args:
        df: daily trend DataFrame (must have date_start and the metric column)
        metric: which metric to check for anomalies
        threshold: z-score threshold (default 2.0 = ~95% confidence)

    Returns: DataFrame of anomalous days with z-scores
    """
    if df.empty or metric not in df.columns or len(df) < 3:
        return pd.DataFrame()

    mean = df[metric].mean()
    std = df[metric].std()
    if std == 0:
        return pd.DataFrame()

    df2 = df.copy()
    df2["z_score"] = (df2[metric] - mean) / std
    anomalies = df2[df2["z_score"].abs() > threshold].copy()
    anomalies["direction"] = anomalies["z_score"].apply(lambda z: "spike" if z > 0 else "drop")
    return anomalies


def spend_allocation_score(camp_df):
    """Score how well spend is allocated across campaigns (0-100).

    Perfect score = all spend goes to highest-ROAS campaigns.
    Low score = high spend on low-ROAS campaigns.

    Returns: dict with score, interpretation, and details
    """
    if camp_df.empty or len(camp_df) < 2:
        return {"score": 0, "interpretation": "Not enough campaigns to score", "details": []}

    df = camp_df.copy()
    total_spend = df["spend"].sum()
    if total_spend == 0:
        return {"score": 0, "interpretation": "No spend data", "details": []}

    df["spend_share"] = df["spend"] / total_spend
    df["roas_rank"] = df["roas"].rank(ascending=False, method="min")
    df["ideal_rank"] = df["spend"].rank(ascending=False, method="min")

    # Score: correlation between ROAS rank and spend rank
    # If highest ROAS campaigns get highest spend, correlation is high
    n = len(df)
    if n < 2:
        return {"score": 50, "interpretation": "Single campaign", "details": []}

    rank_diff = (df["roas_rank"] - df["ideal_rank"]).abs().sum()
    max_diff = n * (n - 1) / 2  # Maximum possible rank difference
    if max_diff == 0:
        score = 100
    else:
        score = max(0, 100 - (rank_diff / max_diff * 100))

    if score >= 80:
        interp = "Excellent — spend aligns well with ROAS"
    elif score >= 60:
        interp = "Good — mostly efficient, some room to reallocate"
    elif score >= 40:
        interp = "Fair — consider shifting budget to higher-ROAS campaigns"
    else:
        interp = "Poor — high spend on low-ROAS campaigns, reallocate urgently"

    # Details: which campaigns are misallocated
    details = []
    for _, row in df.iterrows():
        if row["spend_share"] > 0.15 and row["roas"] < df["roas"].median():
            details.append(f"**{row['campaign_name']}** gets {row['spend_share']*100:.0f}% of spend but ROAS is below median")
        elif row["spend_share"] < 0.10 and row["roas"] > df["roas"].median() * 1.5:
            details.append(f"**{row['campaign_name']}** has strong ROAS ({row['roas']:.2f}x) but only gets {row['spend_share']*100:.0f}% of spend — scale up")

    return {"score": score, "interpretation": interp, "details": details}


def creative_fatigue_check(df):
    """Check for creative fatigue by analyzing frequency vs CTR relationship.

    High frequency + declining CTR = audience seeing ads too often.
    Returns: list of (campaign_name, frequency, ctr, severity) tuples
    """
    if df.empty or "frequency" not in df.columns:
        return []

    fatigued = []
    for _, row in df.iterrows():
        freq = row.get("frequency", 0)
        ctr = row.get("ctr", 0)
        name = row.get("campaign_name", row.get("ad_name", "Unknown"))

        if freq >= 5.0:
            fatigued.append((name, freq, ctr, "critical"))
        elif freq >= 3.5:
            fatigued.append((name, freq, ctr, "warning"))
        elif freq >= 2.5 and ctr < 1.0:
            fatigued.append((name, freq, ctr, "watch"))

    fatigued.sort(key=lambda x: x[1], reverse=True)
    return fatigued


def trend_forecast(daily_df, days_ahead=7, monthly_budget=None):
    """Project metrics forward using linear regression on daily data.

    Args:
        daily_df: DataFrame from daily_trend() with date_start, spend, total_conversions, etc.
        days_ahead: number of days to project
        monthly_budget: optional monthly budget for pacing projection

    Returns: dict with forecast DataFrames and summary stats
    """
    if daily_df.empty or len(daily_df) < 3:
        return None

    df = daily_df.sort_values("date_start").copy()
    df["day_num"] = range(len(df))

    forecasts = {}
    for metric in ["spend", "total_conversions", "roas"]:
        if metric not in df.columns:
            continue
        y = df[metric].values
        x = df["day_num"].values

        # Linear regression
        coeffs = np.polyfit(x, y, 1)
        slope, intercept = coeffs

        # Project forward
        future_x = np.arange(len(df), len(df) + days_ahead)
        future_y = slope * future_x + intercept
        future_y = np.maximum(future_y, 0)  # No negatives

        last_date = df["date_start"].max()
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=days_ahead)

        forecast_df = pd.DataFrame({
            "date": future_dates,
            metric: future_y,
            "type": "forecast",
        })

        # Combine actual + forecast for charting
        actual_df = df[["date_start", metric]].copy()
        actual_df.columns = ["date", metric]
        actual_df["type"] = "actual"

        combined = pd.concat([actual_df, forecast_df], ignore_index=True)

        forecasts[metric] = {
            "combined": combined,
            "slope": slope,
            "projected_total": future_y.sum(),
            "daily_avg_actual": y.mean(),
            "daily_avg_forecast": future_y.mean(),
            "trend": "increasing" if slope > 0 else "decreasing" if slope < 0 else "flat",
        }

    # Monthly pacing projection
    result = {"metrics": forecasts}
    if monthly_budget and "spend" in forecasts:
        total_actual = df["spend"].sum()
        projected_remaining = forecasts["spend"]["projected_total"]
        projected_monthly = total_actual + projected_remaining
        result["budget_pacing"] = {
            "spent_so_far": total_actual,
            "projected_remaining": projected_remaining,
            "projected_monthly_total": projected_monthly,
            "monthly_budget": monthly_budget,
            "on_track": projected_monthly <= monthly_budget * 1.1,
            "pacing_pct": (projected_monthly / monthly_budget * 100) if monthly_budget > 0 else 0,
        }

    return result


def campaign_momentum(df, recent_days=3):
    """Compare each campaign's recent performance vs full period.

    Flags campaigns that are accelerating (getting better) or decaying
    (getting worse) over the most recent days.

    Args:
        df: raw insights DataFrame with date_start and campaign columns
        recent_days: number of most recent days to compare against full period

    Returns: list of dicts with campaign name, direction, and metric changes
    """
    if df.empty or "date_start" not in df.columns or "campaign_name" not in df.columns:
        return []

    df2 = df.copy()
    df2["date_start"] = pd.to_datetime(df2["date_start"])
    max_date = df2["date_start"].max()
    cutoff = max_date - pd.Timedelta(days=recent_days)

    results = []
    for name, group in df2.groupby("campaign_name"):
        full = group
        recent = group[group["date_start"] > cutoff]

        if full.empty or recent.empty or len(full) < recent_days + 1:
            continue

        full_days = (full["date_start"].max() - full["date_start"].min()).days + 1
        if full_days == 0:
            continue

        # Daily averages for full period vs recent
        full_daily_spend = full["spend"].sum() / full_days
        recent_daily_spend = recent["spend"].sum() / recent_days if recent_days > 0 else 0

        full_daily_conv = full["total_conversions"].sum() / full_days
        recent_daily_conv = recent["total_conversions"].sum() / recent_days if recent_days > 0 else 0

        full_ctr = (full["clicks"].sum() / full["impressions"].sum() * 100) if full["impressions"].sum() > 0 else 0
        recent_ctr = (recent["clicks"].sum() / recent["impressions"].sum() * 100) if recent["impressions"].sum() > 0 else 0

        full_cpa = (full["spend"].sum() / full["total_conversions"].sum()) if full["total_conversions"].sum() > 0 else 0
        recent_cpa = (recent["spend"].sum() / recent["total_conversions"].sum()) if recent["total_conversions"].sum() > 0 else 0

        # Calculate momentum signals
        conv_change = ((recent_daily_conv - full_daily_conv) / full_daily_conv * 100) if full_daily_conv > 0 else 0
        ctr_change = ((recent_ctr - full_ctr) / full_ctr * 100) if full_ctr > 0 else 0
        cpa_change = ((recent_cpa - full_cpa) / full_cpa * 100) if full_cpa > 0 else 0

        # Overall direction: weighted score of conversion growth and efficiency
        # Positive = accelerating, negative = decaying
        score = conv_change * 0.5 + ctr_change * 0.3 - cpa_change * 0.2

        if score > 15:
            direction = "accelerating"
            icon = "🚀"
        elif score > 5:
            direction = "improving"
            icon = "📈"
        elif score < -15:
            direction = "decaying"
            icon = "🔻"
        elif score < -5:
            direction = "slowing"
            icon = "📉"
        else:
            direction = "stable"
            icon = "➡️"

        results.append({
            "campaign_name": name,
            "direction": direction,
            "icon": icon,
            "score": score,
            "conv_change_pct": conv_change,
            "ctr_change_pct": ctr_change,
            "cpa_change_pct": cpa_change,
            "recent_daily_conv": recent_daily_conv,
            "full_daily_conv": full_daily_conv,
            "recent_cpa": recent_cpa,
            "full_cpa": full_cpa,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def budget_reallocation_impact(camp_df, reallocations):
    """Simulate the impact of reallocating budget between campaigns.

    Args:
        camp_df: DataFrame from campaign_comparison()
        reallocations: dict of {campaign_name: new_spend_pct} (0-100)
            Must sum to ~100

    Returns: dict with projected metrics under new allocation
    """
    if camp_df.empty:
        return {}

    total_spend = camp_df["spend"].sum()
    total_conversions = camp_df["total_conversions"].sum()
    total_value = camp_df["conversion_value"].sum()

    projected = []
    for _, row in camp_df.iterrows():
        name = row["campaign_name"]
        current_spend = row["spend"]
        current_conv = row["total_conversions"]
        current_value = row["conversion_value"]

        new_pct = reallocations.get(name, (current_spend / total_spend * 100) if total_spend > 0 else 0)
        new_spend = total_spend * new_pct / 100

        # Estimate new conversions using diminishing returns model
        # Assume efficiency decreases as spend increases beyond current level
        if current_spend > 0 and current_conv > 0:
            efficiency = current_conv / current_spend
            spend_ratio = new_spend / current_spend
            # Diminishing returns: sqrt scaling for increases, linear for decreases
            if spend_ratio > 1:
                adj_ratio = 1 + (spend_ratio - 1) ** 0.7
            else:
                adj_ratio = spend_ratio
            new_conv = efficiency * current_spend * adj_ratio
            value_per_conv = current_value / current_conv if current_conv > 0 else 0
            new_value = new_conv * value_per_conv
        else:
            new_conv = 0
            new_value = 0

        projected.append({
            "campaign_name": name,
            "current_spend": current_spend,
            "new_spend": new_spend,
            "spend_change": new_spend - current_spend,
            "current_conversions": current_conv,
            "projected_conversions": new_conv,
            "conv_change": new_conv - current_conv,
            "current_roas": row["roas"],
            "projected_roas": new_value / new_spend if new_spend > 0 else 0,
        })

    proj_df = pd.DataFrame(projected)
    return {
        "campaigns": proj_df,
        "total_current_conv": total_conversions,
        "total_projected_conv": proj_df["projected_conversions"].sum(),
        "total_current_roas": total_value / total_spend if total_spend > 0 else 0,
        "total_projected_roas": proj_df.apply(
            lambda r: r["new_spend"] * r["projected_roas"], axis=1
        ).sum() / total_spend if total_spend > 0 else 0,
        "conv_change_pct": ((proj_df["projected_conversions"].sum() - total_conversions) / total_conversions * 100) if total_conversions > 0 else 0,
    }


def executive_summary(summary, camp_df, comparison=None, client_context=None, fatigue=None, momentum=None):
    """Generate a plain-English executive summary of account performance.

    Returns: string with 3-5 sentences summarizing the account state
    """
    ctx = client_context or {}
    client_name = ctx.get("name", "The account")

    parts = []

    # Opening: overall state
    spend = summary.get("total_spend", 0)
    roas = summary.get("roas", 0)
    convs = summary.get("total_conversions", 0)
    cpa = summary.get("cost_per_conversion", 0)

    parts.append(
        f"{client_name} spent ${spend:,.2f} and generated "
        f"{convs:,.0f} conversion{'s' if convs != 1 else ''} "
        f"at a {roas:.2f}x ROAS"
        f"{f' (${cpa:,.2f} CPA)' if cpa > 0 else ''}."
    )

    # Period comparison
    if comparison:
        spend_chg = comparison.get("total_spend", {}).get("change_pct", 0)
        conv_chg = comparison.get("total_conversions", {}).get("change_pct", 0)
        roas_chg = comparison.get("roas", {}).get("change_pct", 0)

        if abs(conv_chg) > 5 or abs(roas_chg) > 5:
            direction = "up" if conv_chg > 0 else "down"
            parts.append(
                f"Compared to the previous period, conversions are {direction} "
                f"{abs(conv_chg):.0f}% and ROAS {'improved' if roas_chg > 0 else 'declined'} "
                f"{abs(roas_chg):.0f}%."
            )

    # Target comparison
    target_roas = ctx.get("target_roas")
    target_cpa = ctx.get("target_cpa")
    if target_roas:
        if roas >= target_roas:
            parts.append(f"ROAS is above the {target_roas:.1f}x target — there's room to scale.")
        else:
            gap = ((target_roas - roas) / target_roas * 100)
            parts.append(f"ROAS is {gap:.0f}% below the {target_roas:.1f}x target — optimization is needed.")
    if target_cpa and cpa > 0:
        if cpa <= target_cpa:
            parts.append(f"CPA of ${cpa:,.2f} is within the ${target_cpa:,.2f} target.")
        else:
            parts.append(f"CPA of ${cpa:,.2f} exceeds the ${target_cpa:,.2f} target by {((cpa - target_cpa) / target_cpa * 100):.0f}%.")

    # Winners and losers
    if not camp_df.empty and len(camp_df) > 1:
        best = camp_df.nlargest(1, "roas").iloc[0]
        worst = camp_df.nsmallest(1, "roas").iloc[0]
        parts.append(
            f"Top performer is \"{best['campaign_name']}\" "
            f"({best['roas']:.2f}x ROAS), while \"{worst['campaign_name']}\" "
            f"is the weakest ({worst['roas']:.2f}x)."
        )

    # Momentum
    if momentum:
        accel = [m for m in momentum if m["direction"] in ("accelerating", "improving")]
        decay = [m for m in momentum if m["direction"] in ("decaying", "slowing")]
        if accel:
            names = ", ".join(f'"{m["campaign_name"]}"' for m in accel[:2])
            parts.append(f"{names} {'is' if len(accel) == 1 else 'are'} trending upward over the last 3 days.")
        if decay:
            names = ", ".join(f'"{m["campaign_name"]}"' for m in decay[:2])
            parts.append(f"Watch {names} — performance is declining recently.")

    # Creative fatigue
    if fatigue:
        critical = [f for f in fatigue if f[3] == "critical"]
        if critical:
            parts.append(f"{len(critical)} campaign{'s' if len(critical) > 1 else ''} {'show' if len(critical) > 1 else 'shows'} creative fatigue (frequency 5+). Rotate creatives soon.")

    # Top action
    if roas < 1.0:
        parts.append("Priority action: cut losing campaigns immediately — the account is spending more than it earns.")
    elif not camp_df.empty:
        dogs = camp_df[camp_df["roas"] < camp_df["roas"].median() * 0.5]
        stars = camp_df[camp_df["roas"] > camp_df["roas"].median() * 1.5]
        if not dogs.empty and not stars.empty:
            parts.append(f"Priority action: shift budget from underperformers to top campaigns to improve overall efficiency.")

    return " ".join(parts)


def hourly_performance(df):
    """Aggregate metrics by hour of day from hourly breakdown data.

    Expects df to have an 'hourly_stats_aggregated_by_advertiser_time_zone'
    column, or to have been fetched with that breakdown.

    Returns: DataFrame with hour (0-23) and aggregated metrics
    """
    hour_col = "hourly_stats_aggregated_by_advertiser_time_zone"
    if df.empty or hour_col not in df.columns:
        return pd.DataFrame()

    df2 = df.copy()
    # Meta returns hour ranges like "00:00:00 - 00:59:59"
    # Extract the starting hour
    df2["hour"] = df2[hour_col].apply(
        lambda x: int(str(x).split(":")[0]) if pd.notna(x) else -1
    )
    df2 = df2[df2["hour"] >= 0]

    grouped = df2.groupby("hour").agg({
        "spend": "sum",
        "impressions": "sum",
        "clicks": "sum",
        "total_conversions": "sum",
        "conversion_value": "sum",
    }).reset_index()

    grouped["ctr"] = (grouped["clicks"] / grouped["impressions"] * 100).fillna(0)
    grouped["cpc"] = (grouped["spend"] / grouped["clicks"]).fillna(0)
    grouped["cost_per_conversion"] = (grouped["spend"] / grouped["total_conversions"]).fillna(0)
    grouped["roas"] = (grouped["conversion_value"] / grouped["spend"]).fillna(0)

    return grouped.sort_values("hour")


def audience_saturation(daily_df):
    """Track audience saturation by analyzing frequency and CTR trends over time.

    Rising frequency + falling CTR = audience exhaustion.

    Args:
        daily_df: raw insights DataFrame with date_start, frequency, ctr

    Returns: dict with saturation data and alerts
    """
    if daily_df.empty or "date_start" not in daily_df.columns:
        return None

    df = daily_df.copy()
    df["date_start"] = pd.to_datetime(df["date_start"])

    # Aggregate by date
    daily = df.groupby("date_start").agg({
        "impressions": "sum",
        "reach": "sum",
        "clicks": "sum",
        "spend": "sum",
    }).reset_index()

    daily = daily.sort_values("date_start")
    daily["frequency"] = (daily["impressions"] / daily["reach"]).fillna(0)
    daily["ctr"] = (daily["clicks"] / daily["impressions"] * 100).fillna(0)

    if len(daily) < 5:
        return None

    # Calculate rolling averages for smoothing
    daily["freq_ma"] = daily["frequency"].rolling(3, min_periods=1).mean()
    daily["ctr_ma"] = daily["ctr"].rolling(3, min_periods=1).mean()

    # Trend analysis: compare first half vs second half
    mid = len(daily) // 2
    first_half = daily.iloc[:mid]
    second_half = daily.iloc[mid:]

    avg_freq_first = first_half["frequency"].mean()
    avg_freq_second = second_half["frequency"].mean()
    avg_ctr_first = first_half["ctr"].mean()
    avg_ctr_second = second_half["ctr"].mean()

    freq_trend = ((avg_freq_second - avg_freq_first) / avg_freq_first * 100) if avg_freq_first > 0 else 0
    ctr_trend = ((avg_ctr_second - avg_ctr_first) / avg_ctr_first * 100) if avg_ctr_first > 0 else 0

    # Saturation score: high freq growth + CTR decline = saturation
    if freq_trend > 0 and ctr_trend < 0:
        saturation_score = min(100, abs(freq_trend) + abs(ctr_trend))
        if saturation_score > 50:
            severity = "critical"
            message = "Audience is heavily saturated — frequency rising sharply while CTR drops. Expand audiences or rotate creatives immediately."
        elif saturation_score > 25:
            severity = "warning"
            message = "Audience showing saturation signals — frequency increasing while engagement declines. Consider refreshing creatives."
        else:
            severity = "watch"
            message = "Early saturation signals detected. Monitor frequency and CTR trends."
    elif freq_trend > 20:
        saturation_score = min(50, freq_trend)
        severity = "watch"
        message = "Frequency is rising but CTR is holding. Watch for engagement drops."
    else:
        saturation_score = 0
        severity = "healthy"
        message = "No saturation signals. Audience engagement is stable."

    return {
        "daily": daily[["date_start", "frequency", "ctr", "freq_ma", "ctr_ma"]],
        "freq_trend_pct": freq_trend,
        "ctr_trend_pct": ctr_trend,
        "saturation_score": saturation_score,
        "severity": severity,
        "message": message,
        "avg_freq_first_half": avg_freq_first,
        "avg_freq_second_half": avg_freq_second,
        "avg_ctr_first_half": avg_ctr_first,
        "avg_ctr_second_half": avg_ctr_second,
    }


def top_performers(df, metric="roas", n=10, ascending=False):
    """Return top N rows by a given metric.

    Args:
        df: DataFrame
        metric: column to rank by
        n: number of results
        ascending: sort direction

    Returns: DataFrame, top N rows
    """
    if df.empty or metric not in df.columns:
        return df
    return df.nlargest(n, metric) if not ascending else df.nsmallest(n, metric)


# ── Formatting helpers ──

def format_currency(value):
    """Format float as currency: $1,234.56"""
    return f"${value:,.2f}"


def format_number(value):
    """Format number with commas: 1,234,567"""
    if isinstance(value, float):
        return f"{value:,.1f}" if value % 1 else f"{int(value):,}"
    return f"{value:,}"


def format_pct(value):
    """Format as percentage: 3.45%"""
    return f"{value:.2f}%"


def format_roas(value):
    """Format ROAS: 2.4x"""
    return f"{value:.2f}x"
