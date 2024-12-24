# api_clients/meta_api.py
import requests
from config import settings
from schemas.meta_schema import MetaInsight, MetaReference, AdsInsightParam, TimeRange
import json
from utils.logging import setup_task_logger
import asyncio

META_URL = settings.FB_URI
META_APP_ID = settings.FB_APP_ID
META_APP_SECRET = settings.FB_APP_SECRET
META_VER = settings.FB_VER

METRICS_CHILD = [
    "omni_view_content",
    "omni_add_to_cart",
    "landing_page_view",
    "link_click",    
    "omni_purchase",
    "omni_initiated_checkout",
    "post_engagement",    
    "lead",
    "post_engagement",
    "onsite_conversion.post_save",
    "post_reaction",
    "comment",
    "post",
    "outbound_click",
    "video_view",
    "offsite_conversion.fb_pixel_custom",
    "omni_complete_registration",
    "submit_application_total",
    "onsite_conversion.messaging_first_reply",
    "onsite_conversion.messaging_block",
    "onsite_conversion.messaging_conversation_started_7d"
]

REQUEST_FILTER_ARR = [
    {"field":"action_type","operator":"IN","value":METRICS_CHILD},
]

METRICS_PARENT = [    
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

METRICS_PARENT_LIVE = [    
    "ad_id",
    "ad_name",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "objective",    
    "reach",
    "frequency",
]

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

def start_meta_async_job(account_id, date_start, date_end, level, token, is_live=False):
    """Fetch insights for a given account ID."""
    url = f"{META_URL}/{META_VER}/{account_id}/insights"
    params = AdsInsightParam(
        time_range=TimeRange(since=date_start,until=date_end),
        filtering=REQUEST_FILTER_ARR,
        level=level,
        breakdowns=['publisher_platform','platform_position','device_platform'] if level == 'ad' and not is_live else None, 
        access_token=token, 
        fields=METRICS_PARENT if not is_live else METRICS_PARENT_LIVE,
        time_increment=None if is_live else "1",
    )

    json_params = {}
    for key, value in params.dict().items():
        # convert all dict and list to json string
        if isinstance(value, dict) or isinstance(value, list):
            json_params[key] = json.dumps(value)
        else:
            json_params[key] = value
    
    response = requests.post(url, params=json_params)
    response.raise_for_status()
    # return response.json().get("data", [])
    return response.json().get("report_run_id", [])

def poll_meta_job_status(report_id, token):
    """Fetch insights for a given account ID."""
    url = f"{META_URL}/{META_VER}/{report_id}"
    headers = {
      "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    if response.json().get("async_status", None) == "Job Completed":
        return True
    else:
        return False
    

def fetch_meta_insight_from_reference_id(reference_id, token):
    """Fetch insights for a given account ID."""
    url = f"{META_URL}/{META_VER}/{reference_id}/insights"
    headers = {
      "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    paging = response.json().get("paging", {})
    data = response.json().get("data", [])

    while "next" in paging:
        response = requests.get(paging["next"], headers=headers)
        response.raise_for_status()
        data += response.json().get("data", [])
        paging = response.json().get("paging", {})

    return data

def fetch_meta_insights(report_id, token):
    """
    Fetch insights for a completed async job using requests.
    :param report_id: The report_run_id from the async job.
    :param token: The access token for the Meta API.
    :return: A list of all insights data across all pages.
    """
    url = f"{META_URL}/{META_VER}/{report_id}/insights"
    headers = {"Authorization": f"Bearer {token}"}
    all_insights = []

    while url:
        try:
            # Synchronous HTTP request
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Append current page data to the list
            all_insights.extend(data.get("data", []))

            # Get the next page URL, if it exists
            url = data.get("paging", {}).get("next")
        except requests.RequestException as e:
            raise Exception(f"Error fetching insights for report_id {report_id}: {e}")

    return all_insights

async def fetch_ad_preview(ad_id, token, publisher_platform=None, platform_position=None):
    """
    Fetch ad preview content from the Meta API.
    :param ad_id: The ID of the ad.
    :param token: Access token for the Meta API.
    :return: Preview content as a dictionary.
    """
    url = f"{META_URL}/{META_VER}/{ad_id}/previews"
    headers = {"Authorization": f"Bearer {token}"}

    if publisher_platform == "facebook" and platform_position == "feed":
      ad_format = 'MOBILE_FEED_STANDARD'
    elif publisher_platform == "facebook" and platform_position == "instant_article":
      ad_format = 'INSTANT_ARTICLE_STANDARD'
    elif publisher_platform == "facebook" and platform_position == "facebook_stories":
      ad_format = 'FACEBOOK_STORY_MOBILE'
    elif publisher_platform == "instagram" and platform_position == "feed":
      ad_format = 'INSTAGRAM_STANDARD'
    elif publisher_platform == "instagram" and platform_position == "instagram_stories":
      ad_format = 'INSTAGRAM_STORY'
    elif publisher_platform == "instagram" and platform_position == "instagram_explore":
      ad_format = 'INSTAGRAM_EXPLORE_CONTEXTUAL'
    else:
      ad_format = 'MOBILE_FEED_STANDARD'

    params = {
        # "access_token": token,
        "ad_format": ad_format,  # Adjust format as needed,
        "height": '550',
        "width": '350',
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("data", [])
