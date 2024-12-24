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
import asyncio

async def fetch_and_store_google_ads_content(batch_size=10, max_workers=5):
    """
    Fetch and store Google Ads content for specified channel types.
    """
    logger = setup_task_logger("fetch_and_store_google_ads_content")

    # Step 1: Fetch account IDs and tokens from SQL
    try:
        mysql_db = MySQLSessionLocal()
        accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 4)  # Google Ads Platform ID
        mysql_db.close()

        logger.info(f"Fetched {len(accounts)} accounts for Google Ads platform.")
    except Exception as e:
        logger.error(f"Error fetching accounts from SQL: {e}")
        return

    async def process_batch(account_batch):
        channel_types = ["SEARCH", "VIDEO", "DISPLAY"]
        date = {
            "date_start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "date_end": datetime.now().strftime("%Y-%m-%d"),
        }

        loop = asyncio.get_event_loop()
        bulk_operations = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_account = {
                executor.submit(fetch_account_content, account, date, channel_types): account
                for account in account_batch
            }

            for future in as_completed(future_to_account):
                account = future_to_account[future]
                try:
                    account_results = future.result()
                    if account_results:
                        bulk_operations.extend(account_results)
                except Exception as e:
                    logger.error(f"Error processing account {account.account_id}: {e}")

        # Execute bulk upsert
        if bulk_operations:
            upsert_bulk_insights("google_ads_content", bulk_operations)
            logger.info(f"Upserted {len(bulk_operations)} content items to MongoDB.")

    def fetch_account_content(account, date, channel_types):
        """Fetch content for a single account synchronously."""
        try:
            account_id, token, refresh_token = account.account_id, account.token, account.refresh_token

            # Initialize Google Ads client
            client = get_google_ads_client(account_id, token, refresh_token)

            # Fetch Google Ads content
            content_data = fetch_google_ads_content(
                client=client,
                customer_id=account_id,
                date_start=date["date_start"],
                date_end=date["date_end"],
                channel_types=channel_types,
            )

            # Transform and prepare documents
            account_bulk_operations = []
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
                    data=data_contents,
                ).dict()

                filter = {
                    "account_id": account_id,
                    "campaign_id": str(content.get("campaign", {}).get("id", None)),
                    "ad_id": str(content.get("ad_group_ad", {}).get("ad", {}).get("id", None)),
                }

                account_bulk_operations.append(
                    ReplaceOne(filter=filter, replacement=replacement, upsert=True)
                )

            return account_bulk_operations

        except Exception as e:
            logger.error(f"Error fetching content for account {account.account_id}: {e}")
            traceback.print_exc()
            return []

    # Step 3: Execute batch processing
    await batch_processor(accounts, process_batch, batch_size=batch_size)
