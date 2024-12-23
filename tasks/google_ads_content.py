from datetime import datetime, timedelta
from pymongo import ReplaceOne
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.batching import batch_processor
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from api_clients.google_api import get_google_ads_client, fetch_google_ads_content
from schemas.google_ads_schema import GoogleAdsContentData, SearchData, DisplayData, VideoData, GoogleAdsContent
from db.crud_mongodb import upsert_bulk_insights
import traceback
import json

async def fetch_and_store_google_ads_content(batch_size=10):
    """
    Fetch and store Google Ads content for specified channel types.
    """
    logger = setup_task_logger("fetch_and_store_google_ads_content")

    # Step 1: Fetch account IDs and tokens from SQL
    mysql_db = MySQLSessionLocal()
    accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 4)  # Replace 1 with the actual platform ID for Google Ads
    mysql_db.close()

    logger.info(f"Fetched {len(accounts)} accounts for platform Google Ads.")

    # Step 2: Batch process accounts to fetch content
    async def process_batch(account_batch):
        channel_types = ["SEARCH", "VIDEO", "DISPLAY"]
        date = {
            "date_start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "date_end": datetime.now().strftime("%Y-%m-%d"),
            # "date_start": "2024-02-01",
            # "date_end": "2024-02-28",
        }

        for account in account_batch:
            try:
                account_id, token, refresh_token = account.account_id, account.token, account.refresh_token

                # Initialize Google Ads client
                client = get_google_ads_client(account_id, token, refresh_token)

                # Step 3: Fetch Google Ads content for the account
                content_data = fetch_google_ads_content(
                    client=client,
                    customer_id=account_id,
                    date_start=date["date_start"],
                    date_end=date["date_end"],
                    channel_types=channel_types
                )

                # Step 4: Transform and prepare documents for MongoDB
                bulk_operations = []
                for content in content_data:
                    sem = SearchData(
                        headlines=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_search_ad", {}).get("headlines", None),
                        descriptions=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_search_ad", {}).get("descriptions", None),
                    )
                    gdn = DisplayData(
                        headlines=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_display_ad", {}).get("headlines", None),
                        descriptions=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_display_ad", {}).get("descriptions", None),
                        long_headline=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_display_ad", {}).get("long_headline", None),
                        business_name=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_display_ad", {}).get("business_name", None),
                        youtube_videos=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_display_ad", {}).get("youtube_videos", None),
                        images=content.get("ad_group_ad", {}).get("ad", {}).get("responsive_display_ad", {}).get("marketing_images", None),
                    )
                    video = VideoData(
                        video=content.get("ad_group_ad", {}).get("ad", {}).get("video_ad", {}).get("video", None),
                    )
                    data_contents = GoogleAdsContentData(
                        sem=sem,
                        gdn=gdn,
                        video=video,
                    )
                    replacement = GoogleAdsContent(
                        account_id=account_id,
                        campaign_id=str(content.get("campaign", {}).get("id", None)),
                        campaign_name=content.get("campaign", {}).get("name", None),
                        adset_id=str(content.get("ad_group", {}).get("id", None)),
                        adset_name=content.get("ad_group", {}).get("name", None),
                        ad_id=str(content.get("ad_group_ad", {}).get("ad", {}).get("id", None)),
                        ad_name=content.get("ad_group_ad", {}).get("ad", {}).get("name", None),
                        channel_type=content.get("campaign", {}).get("advertising_channel_type", None),
                        data=data_contents
                    ).dict()

                    filter = {"account_id": account_id, "campaign_id": str(content.get("campaign", {}).get("id", None)),"ad_id": str(content.get("ad_group_ad", {}).get("ad", {}).get("id", None))}

                    bulk_operations.append(
                        ReplaceOne(filter=filter, replacement=replacement, upsert=True)
                    )

                # Execute bulk upsert
                if bulk_operations:
                    upsert_bulk_insights("google_ads_content", bulk_operations)
                    logger.info(
                        f"Upserted {len(bulk_operations)} content items for account_id {account_id}."
                    )
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error processing account {account_id}: {e}")

    await batch_processor(accounts, process_batch, batch_size=batch_size)
