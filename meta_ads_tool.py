#!/usr/bin/env python3
"""
Meta Ads Reporting Tool: Pull live data from the Meta Marketing API,
generate reports, or launch an interactive dashboard.

Agency: Skip the Noise Media

Usage:
    python3 meta_ads_tool.py                                # Launch Streamlit dashboard
    python3 meta_ads_tool.py --report summary               # Print summary to terminal
    python3 meta_ads_tool.py --report csv                   # Export CSV report
    python3 meta_ads_tool.py --report csv --output report.csv
    python3 meta_ads_tool.py --date-range last_30d          # Specify date range
    python3 meta_ads_tool.py --level ad                     # campaign / adset / ad
    python3 meta_ads_tool.py --breakdown age                # age / gender / country / placement
"""

import argparse
import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path

# Ensure meta_ads package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from meta_ads.meta_api import (
    fetch_insights, fetch_campaigns, VALID_DATE_PRESETS,
    VALID_LEVELS, VALID_BREAKDOWNS, _check_credentials,
    get_accounts, set_active_account,
)
from meta_ads.metrics import (
    insights_to_dataframe, summary_metrics, campaign_comparison,
    creative_performance, audience_breakdown,
    format_currency, format_number, format_pct, format_roas,
)


def cli_summary_report(date_range, level, breakdowns):
    """Fetch data and print a formatted summary to the terminal."""
    print("=" * 64)
    print(f"  Meta Ads Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Date Range: {date_range} | Level: {level}")
    if breakdowns:
        print(f"  Breakdowns: {', '.join(breakdowns)}")
    print("=" * 64)
    print()

    print("[...] Fetching insights from Meta API...")
    raw = fetch_insights(date_preset=date_range, level=level, breakdowns=breakdowns)
    if not raw:
        print("[WARN] No data returned. Check your date range or ad account.")
        return

    df = insights_to_dataframe(raw)
    summary = summary_metrics(df)

    print(f"[OK] Fetched {len(raw)} rows")
    print()

    # ── Summary KPIs ──
    print("  SUMMARY")
    print(f"    Total Spend:          {format_currency(summary['total_spend'])}")
    print(f"    Total Impressions:    {format_number(summary['total_impressions'])}")
    print(f"    Total Reach:          {format_number(summary['total_reach'])}")
    print(f"    Total Clicks:         {format_number(summary['total_clicks'])}")
    print(f"    Avg CTR:              {format_pct(summary['avg_ctr'])}")
    print(f"    Avg CPC:              {format_currency(summary['avg_cpc'])}")
    print(f"    Avg CPM:              {format_currency(summary['avg_cpm'])}")
    print(f"    Total Conversions:    {format_number(summary['total_conversions'])}")
    print(f"    Cost/Conversion:      {format_currency(summary['cost_per_conversion'])}")
    print(f"    ROAS:                 {format_roas(summary['roas'])}")
    print(f"    Avg Frequency:        {summary['avg_frequency']:.2f}")
    print()

    # ── Campaign breakdown ──
    if level in ("campaign", "adset", "ad"):
        campaigns = campaign_comparison(df)
        if not campaigns.empty:
            print("  CAMPAIGNS")
            header = f"    {'Campaign':<35} {'Spend':>10} {'Impr':>10} {'Clicks':>8} {'CTR':>7} {'Conv':>6} {'CPA':>10} {'ROAS':>7}"
            print(header)
            print("    " + "-" * (len(header) - 4))
            for _, row in campaigns.iterrows():
                name = row["campaign_name"][:33]
                print(
                    f"    {name:<35} "
                    f"{format_currency(row['spend']):>10} "
                    f"{format_number(row['impressions']):>10} "
                    f"{format_number(row['clicks']):>8} "
                    f"{format_pct(row['ctr']):>7} "
                    f"{format_number(row['total_conversions']):>6} "
                    f"{format_currency(row['cost_per_conversion']):>10} "
                    f"{format_roas(row['roas']):>7}"
                )
            print()

    # ── Breakdown (if requested) ──
    if breakdowns:
        for bd in breakdowns:
            bd_df = audience_breakdown(df, bd)
            if not bd_df.empty:
                print(f"  BREAKDOWN: {bd.upper()}")
                header = f"    {bd.title():<20} {'Spend':>10} {'Impr':>10} {'Clicks':>8} {'CTR':>7} {'Conv':>6}"
                print(header)
                print("    " + "-" * (len(header) - 4))
                for _, row in bd_df.iterrows():
                    print(
                        f"    {str(row[bd]):<20} "
                        f"{format_currency(row['spend']):>10} "
                        f"{format_number(row['impressions']):>10} "
                        f"{format_number(row['clicks']):>8} "
                        f"{format_pct(row['ctr']):>7} "
                        f"{format_number(row['total_conversions']):>6}"
                    )
                print()

    print("=" * 64)


def cli_csv_report(date_range, level, breakdowns, output_path):
    """Fetch data and export to CSV."""
    print(f"[...] Fetching insights ({date_range}, {level} level)...")
    raw = fetch_insights(date_preset=date_range, level=level, breakdowns=breakdowns)
    if not raw:
        print("[WARN] No data returned.")
        return

    df = insights_to_dataframe(raw)
    df.to_csv(output_path, index=False)
    print(f"[OK] Exported {len(df)} rows to {output_path}")


def launch_dashboard():
    """Launch the Streamlit dashboard."""
    dashboard_path = Path(__file__).resolve().parent / "meta_ads" / "dashboard.py"
    if not dashboard_path.exists():
        print(f"[ERR] Dashboard not found at {dashboard_path}")
        sys.exit(1)

    print("[...] Launching Meta Ads Dashboard...")
    print("     Open http://localhost:8501 in your browser")
    print("     Press Ctrl+C to stop")
    print()
    try:
        subprocess.run(
            ["streamlit", "run", str(dashboard_path), "--server.headless", "true"],
            check=True,
        )
    except FileNotFoundError:
        print("[ERR] Streamlit not found. Install it with: pip install streamlit")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[OK] Dashboard stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Meta Ads Reporting Tool — Pull live data from Meta Marketing API"
    )
    parser.add_argument(
        "--report",
        choices=["summary", "csv"],
        help="Generate a CLI report (omit to launch dashboard)",
    )
    parser.add_argument(
        "--date-range",
        default="last_7d",
        choices=VALID_DATE_PRESETS,
        help="Date range for the report (default: last_7d)",
    )
    parser.add_argument(
        "--level",
        default="campaign",
        choices=VALID_LEVELS,
        help="Reporting level (default: campaign)",
    )
    parser.add_argument(
        "--breakdown",
        action="append",
        choices=VALID_BREAKDOWNS,
        help="Add a breakdown dimension (can be repeated)",
    )
    parser.add_argument(
        "--output",
        default="meta_ads_report.csv",
        help="CSV output file path (default: meta_ads_report.csv)",
    )
    accounts = get_accounts()
    if accounts:
        parser.add_argument(
            "--account",
            choices=list(accounts.keys()),
            help="Ad account ID (default: first configured account)",
        )

    args = parser.parse_args()

    # Set active account if specified
    if hasattr(args, "account") and args.account:
        set_active_account(args.account)

    if args.report is None:
        launch_dashboard()
    elif not _check_credentials():
        sys.exit(1)
    elif args.report == "summary":
        acc_name = accounts.get(args.account, "") if hasattr(args, "account") and args.account else ""
        if acc_name:
            print(f"  Account: {acc_name}")
        cli_summary_report(args.date_range, args.level, args.breakdown)
    elif args.report == "csv":
        cli_csv_report(args.date_range, args.level, args.breakdown, args.output)


if __name__ == "__main__":
    main()
