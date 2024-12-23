from api_clients.tiktok_api import fetch_ads_data, fetch_content_details
from pymongo import ReplaceOne
from utils.batching import batch_processor
from utils.logging import setup_task_logger
from config import settings
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from db.crud_mongodb import upsert_bulk_insights
from schemas.tiktok_schema import TikTokContentData, TikTokContent
import traceback
import json
from datetime import datetime

async def fetch_and_store_tiktok_ad_contents(batch_size=10):
    """
    Fetch and store TikTok ad content details in MongoDB.
    """
    logger = setup_task_logger("fetch_and_store_tiktok_ad_contents")

    # Step 1: Fetch account IDs and tokens from SQL
    try:
        mysql_db = MySQLSessionLocal()
        accounts = get_account_id_and_access_token_by_platform_id(mysql_db, platform_id=10)  # TikTok Platform ID
        mysql_db.close()

        logger.info(f"Fetched {len(accounts)} accounts for TikTok platform.")
    except Exception as e:
        logger.error(f"Error fetching accounts from SQL: {e}")
        return

    # Step 2: Batch process accounts
    async def process_batch(account_batch):
        for account in account_batch:
            try:
                account_id, token = account.account_id, account.token

                # Fetch ad data
                ads_data = fetch_ads_data(account_id, token)
                logger.info(f"Fetched {len(ads_data)} ads for account {account_id}.")

                # Extract unique video IDs and image IDs, filter out None values
                video_ids = list({ad.get("video_id") for ad in ads_data if ad.get("video_id")})
                image_ids = list({img_id for ad in ads_data if ad.get("image_ids") for img_id in ad["image_ids"] if img_id})

                # Fetch video details
                video_details = fetch_content_details(account_id, video_ids, "file/video/ad/info", token)

                # Fetch image details
                image_details = fetch_content_details(account_id, image_ids, "file/image/ad/info", token)

                # Step 4: Prepare bulk upsert operations for MongoDB
                bulk_operations = []
                for ad in ads_data:
                    # Find matching video or image details
                    video_data = next((video for video in video_details if video["video_id"] == ad.get("video_id")), None)
                    image_data = next((img for img in image_details if img["id"] in ad.get("image_ids", [])), None)

                    if video_data:
                      content = TikTokContentData(
                          content=video_data.get("preview_url"),
                          content_expire_time=video_data.get("preview_url_expire_time"),
                      )
                    elif image_data:
                      content = TikTokContentData(
                          content=image_data.get("image_url"),
                      )
                    else:
                      logger.warning(f"No video or image data found for ad {ad['ad_id']}. Skipping.")
                      continue

                    replacement = TikTokContent(
                      account_id=account_id,
                      ad_id=ad["ad_id"],
                      data=content
                    ).dict()

                    bulk_operations.append(
                        ReplaceOne(
                            filter={"account_id": str(account_id), "ad_id": str(ad["ad_id"])},
                            replacement=replacement,
                            upsert=True
                        )
                    )

                # Execute bulk operations
                if bulk_operations:
                    upsert_bulk_insights("tiktok_content", bulk_operations)
                    logger.info(f"Upserted {len(bulk_operations)} ad previews in batch.")

            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error processing account {account.account_id}: {e}")

    # Batch process accounts
    await batch_processor(accounts, process_batch, batch_size=batch_size)