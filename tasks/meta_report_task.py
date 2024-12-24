from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from pymongo import ReplaceOne
from db.crud_mysql import get_account_id_and_access_token_by_platform_id
from db.crud_mongodb import upsert_bulk_insights
from api_clients.meta_api import start_meta_async_job
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from utils.batching import batch_processor
import traceback

async def fetch_and_store_report_id(level, scheduler_type: str = None, batch_size=10, max_workers=5):
    """
    Fetch and store Meta report IDs.

    :param batch_size: Number of accounts to process in a batch.
    :param max_workers: Maximum number of concurrent threads for API calls.
    """
    logger = setup_task_logger("meta_daily_references")

    # Step 1: Fetch accounts from SQL
    mysql_db = MySQLSessionLocal()

    accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 1)
    mysql_db.close()

    if not accounts:
        logger.warning("No accounts found for Meta daily references.")
        return

    logger.info(f"Fetched {len(accounts)} accounts for Meta daily references.")

    # Step 2: Batch processing function
    async def process_batch(account_batch):
        date_ = {
          "date_start": (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
          "date_end": date.today().strftime("%Y-%m-%d"),
        }

        is_live=False if scheduler_type != "live" else True

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(start_meta_async_job, account.account_id, date_['date_start'], date_['date_end'], level, account.token, is_live): account 
                for account in account_batch
            }

            bulk_operations = []

            for future in as_completed(futures):
                account = futures[future]
                try:
                    # Step 3: API call to fetch report IDs
                    report_ids = future.result()

                    if not report_ids:
                        logger.info(f"No report IDs found for account {account.account_id}.")
                        continue

                    # Step 4: Prepare MongoDB upsert operations
                    bulk_operations.append(
                      ReplaceOne(
                          filter = {"account_id": account.account_id, "report_id": report_ids},
                          replacement = {
                              "account_id": account.account_id,
                              "access_token": account.token,
                              "level": level,
                              "reference": report_ids,
                              "date_start": date_['date_start'],
                              "date_end": date_['date_end'],
                              "created_at": datetime.now(),
                              "status": 0  # 0 = pending
                          },
                          upsert=True
                      )
                    )

                    logger.info(f"Fetched {len(report_ids)} report IDs for account {account.account_id}.")

                except Exception as e:
                    traceback.print_exc()
                    logger.error(f"Error processing account {account.account_id}: {e}")

            # Step 5: Bulk upsert into MongoDB
            if bulk_operations:
                upsert_bulk_insights("meta_references" if not scheduler_type else f"meta_references_{scheduler_type}", bulk_operations)
                logger.info(f"Upserted {len(bulk_operations)} report IDs to MongoDB.")

    # Step 6: Execute batch processing
    await batch_processor(accounts, process_batch, batch_size=batch_size)
    logger.info("Completed fetching and storing Meta report IDs.")
