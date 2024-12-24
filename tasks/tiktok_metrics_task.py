import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.batching import batch_processor
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from db.crud_mongodb import upsert_bulk_insights
from api_clients.tiktok_api import fetch_tiktok_metrics
from schemas.tiktok_schema import TikTokInsightData, TikTokInsight, TikTokInsightDataLive, TikTokInsightLive
from pymongo import ReplaceOne
import traceback

async def fetch_and_store_tiktok_metrics(level, scheduler_type: str = None, batch_size=10, max_workers=5):
    """
    Optimized function to fetch and store TikTok metrics for accounts.

    :param level: Data granularity level (e.g., account, campaign, ad).
    :param scheduler_type: Optional scheduler type (e.g., 'live').
    :param batch_size: Number of accounts to process in a batch.
    :param max_workers: Maximum number of concurrent threads for API calls.
    """
    logger = setup_task_logger("fetch_and_store_tiktok_metrics")

    # Step 1: Fetch account IDs and tokens from SQL
    try:
        mysql_db = MySQLSessionLocal()
        accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 10)
        mysql_db.close()
        logger.info(f"Fetched {len(accounts)} accounts for platform TikTok.")
    except Exception as e:
        logger.error(f"Error fetching accounts from SQL: {e}")
        return

    if not accounts:
        logger.warning("No accounts found for TikTok metrics.")
        return

    date = {
        "date_start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "date_end": datetime.now().strftime("%Y-%m-%d"),
    }

    # Helper function to run async `fetch_tiktok_metrics` in a synchronous context
    def run_async_fetch_metrics(account_id, token):
        return asyncio.run(fetch_tiktok_metrics(account_id, token, level, date, is_live=(scheduler_type == "live")))

    # Step 2: Batch process accounts to fetch metrics
    async def process_batch(account_batch):
        bulk_operations = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(run_async_fetch_metrics, account.account_id, account.token): account
                for account in account_batch
            }

            for future in as_completed(futures):
                account = futures[future]
                try:
                    metrics_data = future.result()

                    if not metrics_data:
                        logger.info(f"No metrics data for account_id {account.account_id}.")
                        continue

                    # Transform and prepare documents for bulk upsert
                    for insight in metrics_data:
                        data_metrics = TikTokInsightData(
                            **insight.get('metrics', {}),
                            objective=insight.get('metrics', {}).get("objective_type", ""),
                        ) if scheduler_type != "live" else TikTokInsightDataLive(
                            **insight.get('metrics', {}),
                            objective=insight.get('metrics', {}).get("objective_type", ""),
                        )

                        # Skip records with all zero values (excluding objective)
                        if all(value == 0 for key, value in data_metrics.dict().items() if key != "objective"):
                            continue

                        replacement = TikTokInsight(
                          **insight.get("dimensions", {}),
                          **insight.get("metrics", {}),
                          adset_id=insight.get('metrics', {}).get("adgroup_id", None),
                          adset_name=insight.get('metrics', {}).get("adgroup_name", None),
                          objective=insight.get('metrics', {}).get("objective_type", None),
                          account_id=account.account_id, 
                          date=insight.get("dimensions", {}).get("stat_time_day"), 
                          data=data_metrics
                        ).dict() if scheduler_type != "live" else TikTokInsightLive(
                          **insight.get("dimensions", {}),
                          **insight.get("metrics", {}),
                          adset_id=insight.get('metrics', {}).get("adgroup_id", None),
                          adset_name=insight.get('metrics', {}).get("adgroup_name", None),
                          objective=insight.get('metrics', {}).get("objective_type", None),
                          account_id=account.account_id, 
                          date_start=date.get("date_start"), 
                          date_end=date.get("date_end"), 
                          data=data_metrics
                        ).dict()

                        # Define MongoDB filter
                        if level == "account":
                            if scheduler_type == "live":
                                filter = {"account_id": account.account_id, "date_start": date.get("date_start"), "date_end": date.get("date_end")}
                            else:
                              filter = {"account_id": account.account_id, "date": datetime.strptime(insight.get("dimensions", {}).get("stat_time_day"), "%Y-%m-%d %H:%M:%S")}
                        elif level == "campaign":
                            if scheduler_type == "live":
                                filter = {"account_id": account.account_id, "campaign_id": insight.get("dimensions", {}).get("campaign_id"), "date_start": date.get("date_start"), "date_end": date.get("date_end")}
                            else:
                              filter = {"account_id": account.account_id, "campaign_id": insight.get("dimensions", {}).get("campaign_id"), "date": datetime.strptime(insight.get("dimensions", {}).get("stat_time_day"), "%Y-%m-%d %H:%M:%S")}
                        elif level == "ad":
                            if scheduler_type == "live":
                                filter = {"account_id": account.account_id, "ad_id": insight.get("dimensions", {}).get("ad_id"), "date_start": date.get("date_start"), "date_end": date.get("date_end")}
                            else:
                              filter = {"account_id": account.account_id, "ad_id": insight.get("dimensions", {}).get("ad_id"), "date": datetime.strptime(insight.get("dimensions", {}).get("stat_time_day"), "%Y-%m-%d %H:%M:%S")}

                        bulk_operations.append(
                            ReplaceOne(
                                filter=filter,
                                replacement=replacement,
                                upsert=True
                            )
                        )

                except Exception as e:
                    traceback.print_exc()
                    logger.error(f"Error fetching TikTok metrics for account {account.account_id}: {e}")

        # Execute bulk upsert
        if bulk_operations:
            collection_name = f"tiktok_insights_{level}" if not scheduler_type else f"tiktok_insights_{level}_{scheduler_type}"
            upsert_bulk_insights(collection_name, bulk_operations)
            logger.info(f"Upserted {len(bulk_operations)} records into {collection_name}.")

    # Execute batch processing
    await batch_processor(accounts, process_batch, batch_size=batch_size)
    logger.info("Completed fetching and storing TikTok metrics.")
