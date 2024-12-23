import traceback
from datetime import datetime, timedelta
from pymongo import ReplaceOne
from config import settings
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from utils.batching import batch_processor
from utils.logging import setup_task_logger
from api_clients.google_api import get_google_ads_client, fetch_google_ads_metrics
from schemas.google_ads_schema import GoogleAdsInsightData, GoogleAdsInsight
from db.crud_mongodb import upsert_bulk_insights
import json

async def fetch_and_store_google_ads_insights(level, scheduler_type: str = None, batch_size=10):
    """
    Fetch and store Google Ads insights for accounts.
    :param level: The granularity level ('account', 'campaign', 'ad').
    :param scheduler_type: Scheduler type, e.g., 'live'.
    :param batch_size: Number of accounts to process in a batch.
    """
    logger = setup_task_logger("fetch_and_store_google_ads_insights")

    # Step 1: Fetch account IDs and tokens from SQL
    try:
        mysql_db = MySQLSessionLocal()
        accounts = get_account_id_and_access_token_by_platform_id(mysql_db, platform_id=4)  # Google Ads Platform ID
        mysql_db.close()

        logger.info(f"Fetched {len(accounts)} accounts for Google Ads platform.")
    except Exception as e:
        logger.error(f"Error fetching accounts from SQL: {e}")
        return

    # Step 2: Batch process accounts
    async def process_batch(account_batch):
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
                google_ads_client = get_google_ads_client(account_id, token, refresh_token)

                # Step 3: Fetch Google Ads metrics for the account
                metrics_data = fetch_google_ads_metrics(
                    client=google_ads_client,
                    customer_id=account_id,
                    date_start=date["date_start"],
                    date_end=date["date_end"],
                    level=level,
                    scheduler_type=scheduler_type
                )

                logger.info(f"Fetched {len(metrics_data)} insights for account {account_id}.")

                # Step 4: Transform and prepare documents
                bulk_operations = []
                for insight in metrics_data:
                    data_metrics = GoogleAdsInsightData(
                        channel_type=insight.get("campaign", {}).get("advertising_channel_type", None),
                        **insight.get("metrics", {}),
                    )
                    
                    """
                      check all value in data, if all value is 0 in data_metrics except in objective key, skip this record
                    """
                    if all(value == 0 for key, value in data_metrics.dict().items() if key != "channel_type"):
                        continue

                    replacement = GoogleAdsInsight(
                      channel_type=insight.get("campaign", {}).get("advertising_channel_type", None),
                      account_id=account_id, 
                      campaign_id=str(insight.get("campaign", {}).get("id", None)),
                      campaign_name=insight.get("campaign", {}).get("name", None),
                      date=insight.get("segments", {}).get("date"),
                      data=data_metrics
                    ).dict()

                    filter = {"account_id": account_id, "date": datetime.strptime(insight.get("segments", {}).get("date"), "%Y-%m-%d")}

                    bulk_operations.append(
                      ReplaceOne(
                        filter=filter,
                        replacement=replacement,
                        upsert=True
                      ) 
                    )

                # Execute bulk upsert
                if bulk_operations:
                    upsert_bulk_insights(f"google_ads_insights_{level}" if not scheduler_type else f"google_ads_insights_{level}_{scheduler_type}", bulk_operations)

                    logger.info(f"Upserted {len(bulk_operations)} insights for account_id {account_id}.")

            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error processing account {account_id}: {e}")

    # Step 5: Batch processing
    await batch_processor(accounts, process_batch, batch_size=batch_size)