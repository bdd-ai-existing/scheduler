from concurrent.futures import ThreadPoolExecutor, as_completed
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


async def fetch_and_store_google_ads_insights(level, scheduler_type: str = None, batch_size=10, max_workers=5):
    """
    Fetch and store Google Ads insights for accounts with batch processing and thread pooling.

    :param level: The granularity level ('account', 'campaign', 'ad').
    :param scheduler_type: Scheduler type, e.g., 'live'.
    :param batch_size: Number of accounts to process in a batch.
    :param max_workers: Maximum number of threads for concurrent execution.
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

    async def process_batch(account_batch):
        """
        Process a batch of accounts concurrently.
        """
        date = {
            "date_start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "date_end": datetime.now().strftime("%Y-%m-%d"),
        }

        bulk_operations = []

        def process_account(account):
            """
            Process a single account to fetch and transform insights.
            """
            try:
                account_id, token, refresh_token = account.account_id, account.token, account.refresh_token

                # Initialize Google Ads client
                google_ads_client = get_google_ads_client(account_id, token, refresh_token)

                # Fetch Google Ads metrics
                metrics_data = fetch_google_ads_metrics(
                    client=google_ads_client,
                    customer_id=account_id,
                    date_start=date["date_start"],
                    date_end=date["date_end"],
                    level=level,
                    scheduler_type=scheduler_type
                )

                logger.info(f"Fetched {len(metrics_data)} insights for account {account_id}.")

                # Prepare bulk operations
                local_bulk_operations = []
                for insight in metrics_data:
                    data_metrics = GoogleAdsInsightData(
                        channel_type=insight.get("campaign", {}).get("advertising_channel_type", None),
                        **insight.get("metrics", {}),
                    )

                    # Skip records with all zero values in data_metrics
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

                    local_bulk_operations.append(
                        ReplaceOne(
                            filter=filter,
                            replacement=replacement,
                            upsert=True
                        )
                    )

                return local_bulk_operations

            except Exception as e:
                logger.error(f"Error processing account {account.account_id}: {e}")
                return []

        # Use ThreadPoolExecutor to process accounts in the batch concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_account, account): account for account in account_batch}

            for future in as_completed(futures):
                try:
                    account_bulk_operations = future.result()
                    bulk_operations.extend(account_bulk_operations)
                except Exception as e:
                    account = futures[future]
                    logger.error(f"Error processing account {account.account_id}: {e}")

        # Execute bulk upsert for the entire batch
        if bulk_operations:
            upsert_bulk_insights(
                f"google_ads_insights_{level}" if not scheduler_type else f"google_ads_insights_{level}_{scheduler_type}",
                bulk_operations
            )
            logger.info(f"Upserted {len(bulk_operations)} insights to MongoDB.")

    # Step 2: Execute batch processing
    await batch_processor(accounts, process_batch, batch_size=batch_size)
    logger.info("Completed fetching and storing Google Ads insights.")
