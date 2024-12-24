import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.batching import batch_processor
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from db.crud_mongodb import get_data, upsert_bulk_insights
from api_clients.meta_api import fetch_ad_preview
from schemas.meta_schema import MetaContent, MetaContentData
from pymongo import ReplaceOne
import traceback


async def fetch_and_store_ad_previews(batch_size=10, max_workers=5):
    """
    Fetch and store ad preview content from the Meta API.

    :param batch_size: Number of accounts or ads to process in a batch.
    :param max_workers: Maximum number of concurrent threads for API calls.
    """
    logger = setup_task_logger("fetch_and_store_ad_previews")

    # Step 1: Fetch account IDs and tokens from SQL
    mysql_db = MySQLSessionLocal()
    accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 1)
    mysql_db.close()

    account_ids = [account.account_id for account in accounts]
    logger.info(f"Fetched {len(accounts)} accounts for platform Meta.")

    # Step 2: Retrieve ad IDs from the MongoDB Ad collection for these accounts
    ads = list(
        get_data(
            collection_name="meta_insights_ad",
            filter={"account_id": {"$in": account_ids}},
            projection={"ad_id": 1, "account_id": 1, "publisher_platform": 1, "platform_position": 1}
        )
    )
    logger.info(f"Fetched {len(ads)} ad records for specified accounts.")

    # Helper function to run async `fetch_ad_preview` in a synchronous context
    def run_async_fetch_preview(ad_id, token, publisher_platform, platform_position):
        return asyncio.run(fetch_ad_preview(ad_id, token, publisher_platform, platform_position))

    # Step 3: Batch process ad previews
    async def process_batch(ad_batch):
        bulk_operations = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    run_async_fetch_preview,
                    ad["ad_id"],
                    next((a.token for a in accounts if a.account_id == ad["account_id"]), None),
                    ad.get("publisher_platform"),
                    ad.get("platform_position"),
                ): ad
                for ad in ad_batch
            }

            for future in as_completed(futures):
                ad = futures[future]
                try:
                    preview_content = future.result()
                    preview_document = MetaContent(
                        ad_id=ad["ad_id"],
                        account_id=ad["account_id"],
                        data=MetaContentData(
                            content=preview_content[0].get("body") if preview_content else None,
                        ),
                    )
                    bulk_operations.append(
                        ReplaceOne(
                            filter={"ad_id": ad["ad_id"]},
                            replacement=preview_document.dict(),
                            upsert=True,
                        )
                    )
                    logger.info(f"Fetched ad preview for ad_id {ad['ad_id']}.")
                except Exception as e:
                    traceback.print_exc()
                    logger.error(f"Error fetching preview for ad_id {ad['ad_id']}: {e}")

        # Execute bulk operations
        if bulk_operations:
            upsert_bulk_insights("meta_content", bulk_operations)
            logger.info(f"Upserted {len(bulk_operations)} ad previews in batch.")

    await batch_processor(ads, process_batch, batch_size=batch_size)
    logger.info("Completed fetching and storing Meta ad previews.")
