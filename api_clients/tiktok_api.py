import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json
from config import settings
from utils.utils import split_list
from collections import defaultdict

TIKTOK_URL = settings.TIKTOK_URI
TIKTOK_VER = settings.TIKTOK_VER

METRICS_PARENT = [
  "campaign_id",
  "campaign_name",
  "adgroup_id",
  "adgroup_name",
  "ad_id",
  "ad_name",
  "objective_type",
  "spend",
  "billed_cost",
  "cash_spend",
  "impressions",
  "gross_impressions",
  "clicks",
  "conversion",
  "real_time_conversion",
  "video_play_actions",
  "video_watched_2s",
  "video_watched_6s",
  "engaged_view",
  "engaged_view_15s",
  "video_views_p25",
  "video_views_p50",
  "video_views_p75",
  "video_views_p100",
  "engagements",
  "follows",
  "likes",
  "comments",
  "shares",
  "profile_visits",
  "clicks_on_music_disc",
  "duet_clicks",
  "stitch_clicks",
  "sound_usage_clicks",
  "anchor_clicks",
  "clicks_on_hashtag_challenge",
  "ix_video_views",
  "ix_video_views_p25",
  "ix_video_views_p50",
  "ix_video_views_p75",
  "ix_video_views_p100",
  "tt_playlist_visit",
  "interactive_add_on_impressions",
  "interactive_add_on_destination_clicks",
  "interactive_add_on_activity_clicks",
  "interactive_add_on_option_a_clicks",
  "interactive_add_on_option_b_clicks",
  "countdown_sticker_recall_clicks",
  "live_views",
  "live_unique_views",
  "live_effective_views",
  "live_product_clicks",
  "real_time_app_install",
  "real_time_app_install_cost",
  "app_install",
  "registration",
  "total_registration",
  "purchase",
  "total_purchase",
  "value_per_total_purchase",
  "total_purchase_value",
  "app_event_add_to_cart",
  "total_app_event_add_to_cart",
  "value_per_total_app_event_add_to_cart",
  "total_app_event_add_to_cart_value",
  "checkout",
  "total_checkout",
  "value_per_checkout",
  "total_checkout_value",
  "view_content",
  "total_view_content",
  "value_per_total_view_content",
  "total_view_content_value",
  "next_day_open",
  "total_next_day_open",
  "add_payment_info",
  "total_add_payment_info",
  "add_to_wishlist",
  "total_add_to_wishlist",
  "value_per_total_add_to_wishlist",
  "total_add_to_wishlist_value",
  "launch_app",
  "total_launch_app",
  "complete_tutorial",
  "total_complete_tutorial",
  "value_per_total_complete_tutorial",
  "total_complete_tutorial_value",
  "create_group",
  "total_create_group",
  "value_per_total_create_group",
  "total_create_group_value",
  "join_group",
  "total_join_group",
  "value_per_total_join_group",
  "total_join_group_value",
  "create_gamerole",
  "total_create_gamerole",
  "value_per_total_create_gamerole",
  "total_create_gamerole_value",
  "spend_credits",
  "total_spend_credits",
  "value_per_total_spend_credits",
  "total_spend_credits_value",
  "achieve_level",
  "total_achieve_level",
  "value_per_total_achieve_level",
  "total_achieve_level_value",
  "unlock_achievement",
  "total_unlock_achievement",
  "value_per_total_unlock_achievement",
  "total_unlock_achievement_value",
  "sales_lead",
  "total_sales_lead",
  "value_per_total_sales_lead",
  "total_sales_lead_value",
  "in_app_ad_click",
  "total_in_app_ad_click",
  "value_per_total_in_app_ad_click",
  "total_in_app_ad_click_value",
  "in_app_ad_impr",
  "total_in_app_ad_impr",
  "value_per_total_in_app_ad_impr",
  "total_in_app_ad_impr_value",
  "loan_apply",
  "total_loan_apply",
  "loan_credit",
  "total_loan_credit",
  "loan_disbursement",
  "total_loan_disbursement",
  "total_login",
  "ratings",
  "total_ratings",
  "value_per_total_ratings",
  "total_ratings_value",
  "search",
  "total_search",
  "start_trial",
  "total_start_trial",
  "subscribe",
  "total_subscribe",
  "value_per_total_subscribe",
  "total_subscribe_value",
  "unique_custom_app_events",
  "custom_app_events",
  "value_per_custom_app_event",
  "custom_app_events_value",
  "unique_ad_impression_events",
  "ads_impression_events",
  "value_per_ad_impression_event",
  "total_ad_impression_events_value",
  "vta_conversion",
  "vta_app_install",
  "vta_registration",
  "vta_purchase",
  "vta_complete_payment",
  "vta_complete_payment_value",
  "cta_conversion",
  "cta_app_install",
  "cta_registration",
  "cta_purchase",
  "engaged_view_through_conversions",
  "evta_app_install",
  "evta_registration",
  "evta_purchase",
  "evta_payments_completed",
  "complete_payment",
  "value_per_complete_payment",
  "total_landing_page_view",
  "total_pageview",
  "total_value_per_pageview",
  "button_click",
  "value_per_button_click",
  "total_button_click_value",
  "online_consult",
  "value_per_online_consult",
  "total_online_consult_value",
  "user_registration",
  "value_per_user_registration",
  "total_user_registration_value",
  "page_content_view_events",
  "value_per_page_content_view_event",
  "total_page_view_content_events_value",
  "product_details_page_browse",
  "value_per_product_details_page_browse",
  "total_product_details_page_browse_value",
  "web_event_add_to_cart",
  "value_per_web_event_add_to_cart",
  "total_web_event_add_to_cart_value",
  "on_web_order",
  "value_per_on_web_order",
  "total_on_web_order_value",
  "initiate_checkout",
  "value_per_initiate_checkout",
  "total_initiate_checkout_value",
  "add_billing",
  "value_per_add_billing",
  "total_add_billing_value",
  "page_event_search",
  "value_per_page_event_search",
  "total_page_event_search_value",
  "form",
  "value_per_form",
  "download_start_rate",
  "value_per_download_start",
  "total_download_start_value",
  "on_web_add_to_wishlist",
  "on_web_add_to_wishlist_per_click",
  "value_per_on_web_add_to_wishlist",
  "total_on_web_add_to_wishlist_value",
  "on_web_subscribe",
  "on_web_subscribe_per_click",
  "value_per_on_web_subscribe",
  "total_on_web_subscribe_value",
  "custom_page_events",
  "value_per_custom_page_event",
  "custom_page_events_value",
  "onsite_add_to_wishlist",
  "value_per_onsite_add_to_wishlist",
  "total_onsite_add_to_wishlist_value",
  "onsite_add_billing",
  "value_per_onsite_add_billing",
  "total_onsite_add_billing_value",
  "onsite_form",
  "value_per_onsite_form",
  "total_onsite_form_value",
  "onsite_download_start",
  "onsite_destination_visits",
  "cost_per_onsite_destination_visit",
  "onsite_destination_visit_rate",
  "ix_page_view_count",
  "ix_button_click_count",
  "ix_product_click_count",
  "onsite_shopping",
  "value_per_onsite_shopping",
  "total_onsite_shopping_value",
  "onsite_initiate_checkout_count",
  "value_per_onsite_initiate_checkout_count",
  "total_onsite_initiate_checkout_count_value",
  "onsite_on_web_detail",
  "value_per_onsite_on_web_detail",
  "total_onsite_on_web_detail_value",
  "onsite_on_web_cart",
  "value_per_onsite_on_web_cart",
  "total_onsite_on_web_cart_value",
]

METRICS_PARENT_LIVE = [
    "campaign_id",
    "campaign_name",
    "adgroup_id",
    "adgroup_name",
    "ad_id",
    "ad_name",
    "objective_type",
    "reach",
    "frequency",
    "cost_per_1000_reached",
    "average_video_play",
    "average_video_play_per_user",
    "onsite_download_start_rate",
    "ix_page_duration_avg",
    "ix_page_viewrate_avg",
]

def fetch_single_chunk(account_id, token, dimensions, metrics, data_level, date, retries=3):
    """
    Fetch a single metrics chunk with retry logic.
    """
    url = f"{TIKTOK_URL}/{TIKTOK_VER}/report/integrated/get"
    headers = {"Access-Token": token, "Content-Type": "application/json"}
    params = {
        "advertiser_id": account_id,
        "report_type": 'BASIC',
        "dimensions": json.dumps(dimensions),
        "data_level": data_level,
        "start_date": date.get("date_start"),
        "end_date": date.get("date_end"),
        "service_type": 'AUCTION',
        "query_mode": 'CHUNK',
        "metrics": json.dumps(metrics),
        "page_size": 1000
    }

    all_data = []
    next_page = None

    for attempt in range(retries):
        try:
            while True:
                if next_page:
                    params["page"] = next_page

                response = requests.get(url, headers=headers, params=params)
                
                # Rate limit handling
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    print(f"Rate limit hit. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue  # Retry immediately

                response.raise_for_status()
                data = response.json()

                if "data" in data and "list" in data["data"]:
                    all_data.extend(data["data"]["list"])
                    next_page = data["data"].get("page_info", {}).get("next_page")
                    if not next_page:
                        break
                else:
                    print(f"Unexpected response format: {data}")
                    break

            return all_data

        except requests.RequestException as e:
            print(f"Error: {e}. Retry attempt {attempt + 1}...")
            time.sleep(2 ** attempt)  # Exponential backoff
    return []

def fetch_ads_data(account_id, token, retries=3):
    """
    Fetch ad data (video_id and image_ids) from /ad/get endpoint.
    """
    url = f"{TIKTOK_URL}/{TIKTOK_VER}/ad/get/"
    headers = {"Access-Token": token, "Content-Type": "application/json"}
    params = {
      "advertiser_id": account_id, 
      "filtering": json.dumps({
        # creation filter start time will be the first day of the year and the creation filter end time will be the current date
        "creation_filter_start_time": datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d %H:%M:%S"),
        "creation_filter_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
      }), 
      "page_size": 1000
    }
    
    all_ads = []
    next_page = None

    for attempt in range(retries):
        try:
            while True:
                if next_page:
                    params["page"] = next_page

                response = requests.get(url, headers=headers, params=params)
                
                # Handle rate limit
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    print(f"Rate limit hit. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()  # Raise exception for other HTTP errors
                data = response.json()

                if "data" in data and "list" in data["data"]:
                    all_ads.extend(data["data"]["list"])
                    next_page = data["data"].get("page_info", {}).get("next_page")
                    if not next_page:
                        break
                else:
                    break

            return all_ads  # Return all retrieved ads
        except requests.RequestException as e:
            print(f"Error fetching ads data: {e}. Retry attempt {attempt + 1}...")
            time.sleep(2 ** attempt)  # Exponential backoff
    return []

def fetch_content_details(advertiser_id, ids, endpoint, token, retries=3):
    """
    Fetch video or image details using GET with advertiser_id.
    :param advertiser_id: TikTok advertiser ID.
    :param ids: List of IDs to fetch details for.
    :param endpoint: API endpoint for fetching content (e.g., file/video/ad/info or file/image/ad/info).
    :param token: TikTok access token.
    :param retries: Number of retries in case of errors.
    """
    url = f"{TIKTOK_URL}/{TIKTOK_VER}/{endpoint}"
    headers = {"Access-Token": token}
    results = []

    # Ensure no None values in IDs
    ids = [str(id) for id in ids if id]

    for chunk in split_list(ids, 20):  # Batch size of 50 IDs
        for attempt in range(retries):
            try:
                if 'image' in endpoint:
                  params = {
                      "advertiser_id": advertiser_id,
                      "image_ids": json.dumps(chunk),  # Join IDs into a comma-separated string
                  }
                else:
                  params = {
                      "advertiser_id": advertiser_id,
                      "video_ids": json.dumps(chunk),  # Join IDs into a comma-separated string
                  }
                response = requests.get(url, headers=headers, params=params)

                # Handle rate limit
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    print(f"Rate limit hit. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()  # Raise exception for HTTP errors
                data = response.json()
                results.extend(data.get("data", []).get("list", []))
                break
            except requests.RequestException as e:
                print(f"Error fetching content info for chunk {chunk}: {e}. Retry attempt {attempt + 1}...")
                time.sleep(2 ** attempt)  # Exponential backoff

    return results

def group_and_merge_metrics(data, unique_keys):
    """
    Group and merge metrics by unique keys.
    :param data: List of dictionaries containing chunked metrics.
    :param unique_keys: List of keys used to identify unique records (e.g., campaign_id, stat_time_day).
    :return: Merged and grouped list of dictionaries.
    """
    grouped_data = defaultdict(dict)
    
    for record in data:
        # Generate a unique key based on unique_keys
        key = tuple(record.get('dimensions').get(k) for k in unique_keys)
        key_name = key[0] + key[1] if len(key) > 1 else key[0]
        
        if key_name not in grouped_data:
          grouped_data[key_name] = {}
        for k, v in record.items():
            if k == 'metrics':
              if k not in grouped_data[key_name]:
                  grouped_data[key_name][k] = v
              grouped_data[key_name][k] = {
                  **grouped_data[key_name][k],
                  **v
              }
            else:
              grouped_data[key_name][k] = v

    return list(grouped_data.values())

def fetch_tiktok_metrics(account_id: str, token: str, level: str, date: dict, is_live=False):
    """
    Fetch TikTok metrics for a given account with concurrency and rate limit handling.
    """
    # Determine dimensions and data level based on level
    if level == 'account':
      data_level = 'AUCTION_ADVERTISER'
      dimensions = ["advertiser_id", "stat_time_day"]
      metrics_parent =  METRICS_PARENT
      unique_keys = ["advertiser_id", "stat_time_day"]

      if is_live:
        dimensions = ["advertiser_id"]
        metrics_parent = METRICS_PARENT_LIVE
        unique_keys = ["advertiser_id"]

      metrics = [m for m in metrics_parent if m not in ["advertiser_id", "campaign_id", "campaign_name", "adgroup_id", "adgroup_name", "ad_id", "ad_name"]]
    elif level == 'campaign':
      data_level = 'AUCTION_CAMPAIGN'
      dimensions = ["campaign_id", "stat_time_day"]
      metrics_parent = METRICS_PARENT
      unique_keys = ["campaign_id", "stat_time_day"]

      if is_live:
        dimensions = ["campaign_id"]
        metrics_parent = METRICS_PARENT_LIVE
        unique_keys = ["campaign_id"]
      
      metrics = [m for m in metrics_parent if m not in ["campaign_id", "adgroup_id", "adgroup_name", "ad_id", "ad_name"]]
    elif level == 'ad':
      data_level = 'AUCTION_AD'
      dimensions = ["ad_id", "stat_time_day"]
      metrics_parent = METRICS_PARENT
      unique_keys = ["ad_id", "stat_time_day"]

      if is_live:
        dimensions = ["ad_id"]
        metrics_parent = METRICS_PARENT_LIVE
        unique_keys = ["ad_id"]
      
      metrics = [m for m in metrics_parent if m not in ["ad_id"]]
    else:
        raise ValueError("Invalid level specified. Choose from 'account', 'campaign', or 'ad'.")
    
    # Split metrics into chunks of 50
    metric_chunks = list(split_list(metrics, 50))
    all_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:  # Limit concurrency to avoid rate limits
        futures = [
            executor.submit(fetch_single_chunk, account_id, token, dimensions, chunk, data_level, date)
            for chunk in metric_chunks
        ]

        for future in as_completed(futures):
            try:
                data = future.result()
                all_data.extend(data)
            except Exception as e:
                print(f"Error processing a chunk: {e}")

    # return all_data

    # Group and merge metrics
    merged_data = group_and_merge_metrics(all_data, unique_keys)
    return merged_data
