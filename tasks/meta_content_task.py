from utils.batching import batch_processor
from datetime import datetime
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from db.crud_mongodb import get_data, upsert_bulk_insights
from api_clients.meta_api import fetch_ad_preview
from schemas.meta_schema import MetaContent, MetaContentData
from pymongo import ReplaceOne
import json
import traceback

async def fetch_and_store_ad_previews(batch_size=10):
    """
    Fetch and store ad preview content from the Meta API.
    :param platform_id: The platform ID for fetching accounts.
    :param batch_size: Number of accounts or ads to process in a batch.
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

    # Step 3: Batch process ad previews
    async def process_batch(ad_batch):
        bulk_operations = []

        for ad in ad_batch:
            try:
                ad_id = ad["ad_id"]
                account_id = ad["account_id"]

                # Find the access token for the account
                account = next((a for a in accounts if a.account_id == account_id), None)
                if not account:
                    logger.warning(f"No access token found for account {account_id}. Skipping.")
                    continue

                token = account.token

                # Fetch ad preview from the Meta API
                preview_content = await fetch_ad_preview(ad_id, token, ad.get("publisher_platform"), ad.get("platform_position"))
                preview_document = MetaContent(
                  ad_id=ad_id,
                  account_id=account_id,
                  data= MetaContentData(
                      content=preview_content[0].get("body") if preview_content else None,
                  )
                )
                bulk_operations.append(
                    ReplaceOne(
                      filter={"ad_id": ad_id},
                      replacement=preview_document.dict(),
                      upsert=True
                    )
                )
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error fetching preview for ad_id {ad['ad_id']}: {e}")
                # failed_collection.insert_one({"ad_id": ad["ad_id"], "error": str(e), "date": datetime.utcnow()})

        # Execute bulk operations
        if bulk_operations:
            upsert_bulk_insights("meta_content", bulk_operations)
            logger.info(f"Upserted {len(bulk_operations)} ad previews in batch.")

    await batch_processor(ads, process_batch, batch_size=batch_size)
