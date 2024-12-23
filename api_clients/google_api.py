import requests
from config import settings
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Replace these with your credentials
AUTH_URL = settings.GOOGLE_AUTH_URL
CLIENT_ID = settings.GOOG_CLIENT_ID_AUTO
CLIENT_SECRET = settings.GOOG_CLIENT_SECRET_AUTO

METRICS_PARENT = [
    "impressions",
    "clicks",
    "cost_micros",
    "engagements",
    "conversions",
    "all_conversions",
    "conversions_value",
    "all_conversions_value",
    "video_views",
    "interactions",
]

def debug_token(access_token):
    # Check token validity
    token_info_url = f"{AUTH_URL}/tokeninfo"
    params = {
        "access_token": access_token
    }
    response = requests.get(token_info_url, params=params)
    
    
    if response.status_code == 200:
        # print("Access Token is valid.")
        return True  # Return the valid token
    else:
        # print("Access Token is invalid or expired. Refreshing...")
        return False

def refresh_token(refresh_token):
    # Endpoint to refresh token
    token_url = f"{AUTH_URL}/token"
    
    # Data for the token refresh request
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    
    # Make the POST request
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        token_data = response.json()
        # new_access_token = token_data["access_token"]
        # print("New Access Token:", token_data)
        return token_data
    else:
        # print("Failed to refresh token:", response.json())
        return None

def get_google_ads_client(client_customer_id, access_token, refresh_token):
    """
    Creates and returns a Google Ads API client.
    
    Args:
        developer_token (str): Developer token for Google Ads API.
        client_customer_id (str): The Google Ads account's customer ID.
        access_token (str): OAuth access token for authentication.
        refresh_token (str): OAuth refresh token for token refresh.
    
    Returns:
        GoogleAdsClient: Configured Google Ads client instance.
    """
    try:
        google_ads_client = GoogleAdsClient.load_from_dict({
            "developer_token": settings.GOOG_DEVELOPER_TOKEN,
            "client_id": settings.GOOG_CLIENT_ID_AUTO,
            "client_secret": settings.GOOG_CLIENT_SECRET_AUTO,
            "login_customer_id": client_customer_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "use_proto_plus": True
        })
        return google_ads_client
    except Exception as e:
        raise Exception(f"Error creating Google Ads client: {e}")


def fetch_google_ads_metrics(client, customer_id, date_start, date_end, level, scheduler_type):
    """
    Fetch Google Ads metrics using the API.
    :param client: Google Ads API client.
    :param customer_id: Google Ads customer ID.
    :param date_start: Start date for the metrics query (YYYY-MM-DD).
    :param date_end: End date for the metrics query (YYYY-MM-DD).
    :param level: Level of granularity ('account', 'campaign', 'ad').
    :param scheduler_type: Scheduler type (e.g., 'live').
    :return: List of metrics data.
    """
    
    metrics = [f"metrics.{metric}" for metric in METRICS_PARENT]
    metrics = ", \n".join(metrics)

    query = f"""
    SELECT 
        campaign.id, 
        campaign.name, 
        campaign.advertising_channel_type, 
        segments.date, 
        {metrics}
    FROM campaign 
    WHERE segments.date BETWEEN '{date_start}' AND '{date_end}'
    """

    # # Adjust the query based on level
    # if level == 'campaign':
    #     query += " AND campaign.status = 'ENABLED'"

    metrics_data = []
    try:
        # Perform search
        google_ads_service = client.get_service("GoogleAdsService")
        response = google_ads_service.search(customer_id=customer_id, query=query)

        # Iterate through the rows in the response
        for row in response:
            metrics_data.append({
                "campaign": {
                    "id": row.campaign.id,
                    "name": row.campaign.name,
                    "advertising_channel_type": row.campaign.advertising_channel_type.name,
                },
                "segments": {
                    "date": row.segments.date,
                },
                "metrics": {
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "cost_micros": row.metrics.cost_micros,
                    "engagements": row.metrics.engagements,
                    "conversions": row.metrics.conversions,
                    "all_conversions": row.metrics.all_conversions,
                    "conversions_value": row.metrics.conversions_value,
                    "all_conversions_value": row.metrics.all_conversions_value,
                    "video_views": row.metrics.video_views,
                    "interactions": row.metrics.interactions,
                }
            })

    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"Google Ads API Error: {error.message}")
        raise

    return metrics_data

def fetch_google_ads_content(client, customer_id, date_start, date_end, channel_types):
    """
    Fetch Google Ads content for specified channel types.
    :param client: Google Ads API client.
    :param customer_id: Google Ads customer ID.
    :param date_start: Start date for the query.
    :param date_end: End date for the query.
    :param channel_types: List of channel types (e.g., ['SEARCH', 'VIDEO', 'DISPLAY']).
    :return: List of content data.
    """
    # query = f"""
    # SELECT
    #     ad.id,
    #     ad.type,
    #     ad.name,
    #     ad.final_urls,
    #     ad.responsive_search_ad.headlines,
    #     ad.responsive_search_ad.descriptions,
    #     ad.responsive_display_ad.marketing_images,
    #     campaign.id,
    #     campaign.name,
    #     campaign.advertising_channel_type,
    #     segments.date
    # FROM ad
    # WHERE segments.date BETWEEN '{date_start}' AND '{date_end}'
    #   AND campaign.advertising_channel_type IN ({", ".join(f"'{ct}'" for ct in channel_types)})
    # """
    query = f"""
        SELECT
            campaign.id, 
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.resource_name,
            ad_group_ad.status,
            ad_group_ad.ad_strength,
            ad_group_ad.ad.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.responsive_search_ad.descriptions,
            ad_group_ad.ad.responsive_search_ad.headlines,
            ad_group_ad.ad.video_ad.video.asset,
            ad_group_ad.ad.video_responsive_ad.videos,
            ad_group_ad.ad_strength,
            ad_group_ad.resource_name,
            ad_group_ad.status,
            ad_group_ad.ad.responsive_display_ad.accent_color,
            ad_group_ad.ad.responsive_display_ad.business_name,
            ad_group_ad.ad.responsive_display_ad.call_to_action_text,
            ad_group_ad.ad.responsive_display_ad.descriptions,
            ad_group_ad.ad.responsive_display_ad.format_setting,
            ad_group_ad.ad.responsive_display_ad.headlines,
            ad_group_ad.ad.responsive_display_ad.logo_images,
            ad_group_ad.ad.responsive_display_ad.long_headline,
            ad_group_ad.ad.responsive_display_ad.main_color,
            ad_group_ad.ad.responsive_display_ad.marketing_images,
            ad_group_ad.ad.responsive_display_ad.price_prefix,
            ad_group_ad.ad.responsive_display_ad.promo_text,
            ad_group_ad.ad.responsive_display_ad.square_logo_images,
            ad_group_ad.ad.responsive_display_ad.square_marketing_images,
            ad_group_ad.ad.responsive_display_ad.youtube_videos,
            campaign.advertising_channel_type
        FROM
            ad_group_ad
        WHERE
            campaign.advertising_channel_type IN ({", ".join(f"'{ct}'" for ct in channel_types)})
            AND ad_group_ad.status = 'ENABLED'
            AND segments.date BETWEEN '{date_start}' AND '{date_end}'
    """

    content_data = []
    try:
        response = client.get_service("GoogleAdsService").search(
            customer_id=customer_id, query=query
        )

        for row in response:
            content_data.append({
                "campaign": {
                    "id": row.campaign.id,
                    "name": row.campaign.name,
                    "advertising_channel_type": row.campaign.advertising_channel_type.name,
                    "resource_name": row.campaign.resource_name,
                },
                "ad_group": {
                    "id": row.ad_group.id,
                    "name": row.ad_group.name,
                    "resource_name": row.ad_group_ad.resource_name,
                },
                "ad_group_ad": {
                    "resource_name": row.ad_group_ad.resource_name,
                    "status": row.ad_group_ad.status.name,
                    "ad": {
                        "responsive_search_ad": {
                            "headlines": [headline.text for headline in row.ad_group_ad.ad.responsive_search_ad.headlines],
                            "descriptions": [description.text for description in row.ad_group_ad.ad.responsive_search_ad.descriptions],
                        },
                        "responsive_display_ad": {
                            "marketing_images": [marketing_image.asset for marketing_image in row.ad_group_ad.ad.responsive_display_ad.marketing_images],
                            "square_marketing_images": [square_marketing_image.asset for square_marketing_image in row.ad_group_ad.ad.responsive_display_ad.square_marketing_images],
                            "logo_images": [logo_image.asset for logo_image in row.ad_group_ad.ad.responsive_display_ad.logo_images],
                            "square_logo_images": [square_logo_image.asset for square_logo_image in row.ad_group_ad.ad.responsive_display_ad.square_logo_images],
                            "headlines": [headline.text for headline in row.ad_group_ad.ad.responsive_display_ad.headlines],
                            "long_headline": row.ad_group_ad.ad.responsive_display_ad.long_headline.text,
                            "descriptions": [description.text for description in row.ad_group_ad.ad.responsive_display_ad.descriptions],
                            "youtube_videos": [youtube_video.asset for youtube_video in row.ad_group_ad.ad.responsive_display_ad.youtube_videos],
                            "format_setting": row.ad_group_ad.ad.responsive_display_ad.format_setting,
                            "business_name": row.ad_group_ad.ad.responsive_display_ad.business_name,
                        },
                        "video_ad": {
                            "video": row.ad_group_ad.ad.video_ad.video.asset,
                        },
                        "resource_name": row.ad_group_ad.ad.resource_name,
                        "id": row.ad_group_ad.ad.id,
                    }
                }
            })
    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"Error: {error.message}")
        raise

    return content_data

