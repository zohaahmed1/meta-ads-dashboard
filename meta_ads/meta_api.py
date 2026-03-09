"""
Meta Marketing API client — handles auth, fetching, pagination, and rate limiting.
Supports multiple ad accounts via META_AD_ACCOUNT_IDS env var.
"""

import requests
import time
import os
import sys
import json
from pathlib import Path

# ── Load .env file if present (for local dev) ──
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── Load Streamlit Cloud secrets (if running on Streamlit Cloud) ──
try:
    import streamlit as st
    if hasattr(st, "secrets"):
        for key in ["META_ACCESS_TOKEN", "META_AD_ACCOUNT_IDS", "META_AD_ACCOUNT_NAMES"]:
            if key in st.secrets:
                os.environ.setdefault(key, st.secrets[key])
except Exception:
    pass

# ── Config ──
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")

# Multi-account support: comma-separated IDs and names
_raw_ids = os.environ.get("META_AD_ACCOUNT_IDS", os.environ.get("META_AD_ACCOUNT_ID", ""))
_raw_names = os.environ.get("META_AD_ACCOUNT_NAMES", "")

AD_ACCOUNTS = {}  # {account_id: display_name}
_ids = [x.strip() for x in _raw_ids.split(",") if x.strip()]
_names = [x.strip() for x in _raw_names.split(",") if x.strip()]
for i, acc_id in enumerate(_ids):
    name = _names[i] if i < len(_names) else acc_id
    AD_ACCOUNTS[acc_id] = name

# Default to first account
_active_account_id = _ids[0] if _ids else ""

INSIGHT_FIELDS = [
    "campaign_name", "campaign_id",
    "adset_name", "adset_id",
    "ad_name", "ad_id",
    "impressions", "reach", "frequency",
    "clicks", "unique_clicks",
    "ctr", "unique_ctr",
    "cpc", "cpm",
    "spend",
    "actions",
    "cost_per_action_type",
    "action_values",
    "conversions",
    "cost_per_conversion",
    "purchase_roas",
    "date_start", "date_stop",
]

VALID_DATE_PRESETS = [
    "today", "yesterday", "last_7d", "last_14d",
    "last_30d", "this_month", "last_month",
]

VALID_BREAKDOWNS = [
    "age", "gender", "country", "placement",
    "device_platform", "publisher_platform",
]

VALID_LEVELS = ["campaign", "adset", "ad"]


def get_active_account():
    """Return the currently active ad account ID."""
    return _active_account_id


def set_active_account(account_id):
    """Set the active ad account ID for all subsequent API calls."""
    global _active_account_id
    _active_account_id = account_id


def get_accounts():
    """Return dict of {account_id: display_name} for all configured accounts."""
    return dict(AD_ACCOUNTS)


def _check_credentials():
    """Verify that META_ACCESS_TOKEN and at least one account ID are set."""
    if not META_ACCESS_TOKEN:
        print("[ERR] META_ACCESS_TOKEN not set. Add it to .env or environment.")
        return False
    if not _active_account_id:
        print("[ERR] No ad account configured. Set META_AD_ACCOUNT_IDS in .env.")
        return False
    return True


def _handle_rate_limit(response):
    """Check Meta rate limit headers and back off if needed."""
    throttle = response.headers.get("x-fb-ads-insights-throttle")
    if not throttle:
        return
    try:
        throttle_info = json.loads(throttle)
        app_pct = throttle_info.get("app_id_util_pct", 0)
        acc_pct = throttle_info.get("acc_id_util_pct", 0)
        max_pct = max(float(app_pct), float(acc_pct))
        if max_pct >= 90:
            print(f"[RATE] Throttle at {max_pct}%, backing off 30s...")
            time.sleep(30)
        elif max_pct >= 75:
            print(f"[RATE] Throttle at {max_pct}%, backing off 2s...")
            time.sleep(2)
    except (json.JSONDecodeError, ValueError):
        pass


def meta_api_get(endpoint, params=None):
    """Make authenticated GET request to the Meta Graph API.

    Args:
        endpoint: relative path, e.g. 'act_123/insights'
        params: dict of query parameters

    Returns: dict (JSON response) or None on error
    """
    if not _check_credentials():
        return None

    if params is None:
        params = {}
    params["access_token"] = META_ACCESS_TOKEN

    url = f"{META_BASE_URL}/{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=60)
        _handle_rate_limit(resp)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        error_data = {}
        try:
            error_data = e.response.json()
        except Exception:
            pass
        error_msg = error_data.get("error", {}).get("message", str(e))
        error_code = error_data.get("error", {}).get("code", "")
        print(f"[ERR] Meta API {resp.status_code}: {error_msg} (code={error_code})")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[ERR] Meta API request failed: {e}")
        return None


def _paginate(initial_response):
    """Follow pagination cursors to collect all results.

    Returns: list of all data items across pages
    """
    if initial_response is None:
        return []

    all_data = list(initial_response.get("data", []))
    paging = initial_response.get("paging", {})

    while "next" in paging:
        next_url = paging["next"]
        try:
            resp = requests.get(next_url, timeout=60)
            resp.raise_for_status()
            page = resp.json()
            _handle_rate_limit(resp)
            all_data.extend(page.get("data", []))
            paging = page.get("paging", {})
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            print(f"[ERR] Pagination failed: {e}")
            break

    return all_data


def fetch_insights(date_preset="last_7d", level="campaign",
                   breakdowns=None, fields=None,
                   time_increment=None, limit=500,
                   account_id=None):
    """Fetch insights from the Meta Marketing API.

    Args:
        date_preset: one of VALID_DATE_PRESETS
        level: 'campaign', 'adset', or 'ad'
        breakdowns: list of breakdown dimensions, e.g. ['age', 'gender']
        fields: list of field names (defaults to INSIGHT_FIELDS)
        time_increment: int for daily (1) or weekly (7) grouping
        limit: results per page (max 500)
        account_id: override active account (optional)

    Returns: list of insight dicts
    """
    acc = account_id or _active_account_id
    if fields is None:
        fields = INSIGHT_FIELDS

    params = {
        "fields": ",".join(fields),
        "date_preset": date_preset,
        "level": level,
        "limit": limit,
    }

    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    if time_increment:
        params["time_increment"] = str(time_increment)

    response = meta_api_get(f"{acc}/insights", params)
    return _paginate(response)


def fetch_campaigns(account_id=None):
    """List all campaigns in the ad account.

    Returns: list of campaign dicts with id, name, status, objective, etc.
    """
    acc = account_id or _active_account_id
    fields = [
        "id", "name", "status", "objective",
        "daily_budget", "lifetime_budget",
        "start_time", "stop_time", "created_time",
    ]
    params = {"fields": ",".join(fields), "limit": 500}
    response = meta_api_get(f"{acc}/campaigns", params)
    return _paginate(response)


def fetch_adsets(campaign_id=None, account_id=None):
    """List ad sets, optionally filtered by campaign.

    Args:
        campaign_id: if provided, fetch ad sets under this campaign
        account_id: override active account (optional)

    Returns: list of ad set dicts
    """
    acc = account_id or _active_account_id
    fields = [
        "id", "name", "status", "campaign_id",
        "optimization_goal", "bid_strategy",
        "daily_budget", "start_time", "targeting",
    ]
    endpoint = f"{campaign_id}/adsets" if campaign_id else f"{acc}/adsets"
    params = {"fields": ",".join(fields), "limit": 500}
    response = meta_api_get(endpoint, params)
    return _paginate(response)


def fetch_ads(adset_id=None, account_id=None):
    """List ads with creative details.

    Args:
        adset_id: if provided, fetch ads under this ad set
        account_id: override active account (optional)

    Returns: list of ad dicts with creative info
    """
    acc = account_id or _active_account_id
    fields = [
        "id", "name", "status", "adset_id",
        "creative{id,title,body,image_url,thumbnail_url,video_id,call_to_action_type}",
    ]
    endpoint = f"{adset_id}/ads" if adset_id else f"{acc}/ads"
    params = {"fields": ",".join(fields), "limit": 500}
    response = meta_api_get(endpoint, params)
    return _paginate(response)


# ── Quick smoke test ──
if __name__ == "__main__":
    print("=" * 60)
    print("  Meta Ads API — Connection Test")
    print("=" * 60)

    accounts = get_accounts()
    print(f"[OK] {len(accounts)} ad account(s) configured:")
    for acc_id, name in accounts.items():
        print(f"  - {name} ({acc_id})")
    print()

    if not _check_credentials():
        sys.exit(1)

    for acc_id, name in accounts.items():
        print(f"── {name} ──")
        set_active_account(acc_id)

        campaigns = fetch_campaigns()
        if campaigns is None:
            print(f"  [ERR] Failed to fetch campaigns.")
        else:
            print(f"  [OK] {len(campaigns)} campaigns")
            for c in campaigns[:3]:
                print(f"    - {c.get('name')} ({c.get('status')})")

        insights = fetch_insights(date_preset="last_7d", level="campaign")
        print(f"  [OK] {len(insights)} insight rows (last 7 days)")
        print()

    print("=" * 60)
