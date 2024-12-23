from utils.batching import batch_processor
from datetime import datetime, timedelta
from api_clients.tiktok_api import fetch_tiktok_metrics
from utils.logging import setup_task_logger
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from schemas.tiktok_schema import TikTokInsightData, TikTokInsight, TikTokInsightDataLive, TikTokInsightLive
from db.crud_mongodb import upsert_bulk_insights
from pymongo import ReplaceOne
import json
import traceback

async def fetch_and_store_tiktok_metrics(level, scheduler_type: str = None, batch_size = 10):
    """
    Fetch and store TikTok metrics for accounts.
    :param platform_id: The platform ID for fetching accounts.
    :param batch_size: Number of accounts to process in a batch.
    """
    logger = setup_task_logger("fetch_and_store_report_id")

    # Step 1: Fetch account IDs and tokens from SQL
    try:
      mysql_db = MySQLSessionLocal()

      accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 10)
      mysql_db.close()

      logger.info(f"Fetched {len(accounts)} accounts for platform Tiktok.")
    except Exception as e:
      logger.error(f"Error fetching accounts from SQL: {e}")
      return

    # Step 2: Batch process accounts to fetch metrics
    async def process_batch(account_batch):
        date = {
            "date_start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "date_end": datetime.now().strftime("%Y-%m-%d"),
        }

        for account in account_batch:
            try:
                account_id, token = account.account_id, account.token

                # Step 3: Fetch TikTok metrics for the account
                metrics_data = fetch_tiktok_metrics(
                  account_id = account_id, 
                  token = token, 
                  level = level,  
                  date = date,
                  is_live=False if scheduler_type != "live" else True
                )
                
                # Step 4: Transform and prepare documents
                bulk_operations = []
                for insight in metrics_data:
                    data_metrics = TikTokInsightData(
                        **insight.get('metrics', {}),
                        objective=insight.get('metrics', {}).get("objective_type", ""),
                    ) if scheduler_type != "live" else TikTokInsightDataLive(
                        **insight.get('metrics', {}),
                        objective=insight.get('metrics', {}).get("objective_type", ""),
                    )

                    """
                      check all value in data, if all value is 0 in data_metrics except in objective key, skip this record
                    """
                    if all(value == 0 for key, value in data_metrics.dict().items() if key != "objective"):
                        continue
                    
                    replacement = TikTokInsight(
                      **insight.get("dimensions", {}),
                      **insight.get("metrics", {}),
                      adset_id=insight.get('metrics', {}).get("adgroup_id", None),
                      adset_name=insight.get('metrics', {}).get("adgroup_name", None),
                      objective=insight.get('metrics', {}).get("objective_type", None),
                      account_id=account_id, 
                      date=insight.get("dimensions", {}).get("stat_time_day"), 
                      data=data_metrics
                    ).dict() if scheduler_type != "live" else TikTokInsightLive(
                      **insight.get("dimensions", {}),
                      **insight.get("metrics", {}),
                      adset_id=insight.get('metrics', {}).get("adgroup_id", None),
                      adset_name=insight.get('metrics', {}).get("adgroup_name", None),
                      objective=insight.get('metrics', {}).get("objective_type", None),
                      account_id=account_id, 
                      date_start=date.get("date_start"), 
                      date_end=date.get("date_end"), 
                      data=data_metrics
                    ).dict()

                    if level == "account":
                        if scheduler_type == "live":
                            filter = {"account_id": account_id, "date_start": date.get("date_start"), "date_end": date.get("date_end")}
                        else:
                          filter = {"account_id": account_id, "date": datetime.strptime(insight.get("dimensions", {}).get("stat_time_day"), "%Y-%m-%d %H:%M:%S")}
                    elif level == "campaign":
                        if scheduler_type == "live":
                            filter = {"account_id": account_id, "campaign_id": insight.get("dimensions", {}).get("campaign_id"), "date_start": date.get("date_start"), "date_end": date.get("date_end")}
                        else:
                          filter = {"account_id": account_id, "campaign_id": insight.get("dimensions", {}).get("campaign_id"), "date": datetime.strptime(insight.get("dimensions", {}).get("stat_time_day"), "%Y-%m-%d %H:%M:%S")}
                    elif level == "ad":
                        if scheduler_type == "live":
                            filter = {"account_id": account_id, "ad_id": insight.get("dimensions", {}).get("ad_id"), "date_start": date.get("date_start"), "date_end": date.get("date_end")}
                        else:
                          filter = {"account_id": account_id, "ad_id": insight.get("dimensions", {}).get("ad_id"), "date": datetime.strptime(insight.get("dimensions", {}).get("stat_time_day"), "%Y-%m-%d %H:%M:%S")}

                    bulk_operations.append(
                      ReplaceOne(
                        filter=filter,
                        replacement=replacement,
                        upsert=True
                      ) 
                    )

                # Execute bulk upsert
                if bulk_operations:
                    upsert_bulk_insights(f"tiktok_insights_{level}" if not scheduler_type else f"tiktok_insights_{level}_{scheduler_type}", bulk_operations)
                    logger.info(f"Upserted {len(bulk_operations)} insights for account_id {account_id}.")
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error fetching TikTok metrics for account {account_id}: {e}")
                # failed_collection.insert_one({
                #     "account_id": account_id,
                #     "error": str(e),
                #     "date": datetime.utcnow()
                # })

    await batch_processor(accounts, process_batch, batch_size=batch_size)
