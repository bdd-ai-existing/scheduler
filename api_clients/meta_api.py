# api_clients/meta_api.py
import requests
from config import settings
import json

META_URL = settings.FB_URI
META_APP_ID = settings.FB_APP_ID
META_APP_SECRET = settings.FB_APP_SECRET
META_VER = settings.FB_VER

def refresh_token(token:str):
    url = f"{META_URL}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": META_APP_ID,
        "client_secret": META_APP_SECRET,
        "fb_exchange_token": token
    }
    response = requests.get(url=url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Meta API error: {response.text}")

def debug_token(token:str):
    url = f"{META_URL}/debug_token"
    params = {
        "input_token": token,
        "access_token": f"{META_APP_ID}|{META_APP_SECRET}"
    }

    response = requests.get(url=url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Meta API error: {response.text}")

def fetch_meta_report_id(account_id, token):
    """Fetch insights for a given account ID."""
    url = f"{META_URL}/{META_VER}/{account_id}/insights"
    params = {
      "level": "campaign",
      "time_increment": 1,
      "time_range": json.dumps({
        "since": "2024-01-01",
        "until": "2024-01-07"
      }),
      "filtering": json.dumps([]),
      "breakdowns": json.dumps([]),
      "use_account_attribution_setting": True,
      "action_attribution_windows": json.dumps(["7d_click","1d_view"]),
      "use_unified_attribution_setting": True,
      "access_token": token,
      "fields": json.dumps(
        [    
          "ad_id",
          "ad_name",
          "campaign_id",
          "campaign_name",
          "adset_id",
          "adset_name",
          "objective",    
          "spend",
          "impressions",
          "clicks",
          "actions",
          "action_values",
          "catalog_segment_actions",
          "catalog_segment_value",
          "outbound_clicks",
          "video_15_sec_watched_actions",
          "video_p100_watched_actions",
          "video_play_actions",
          "attribution_setting",
          "estimated_ad_recallers",
      ]
      )
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    # return response.json().get("data", [])
    return response.json().get("report_run_id", [])
