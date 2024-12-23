import datetime
import threading
from utils.batching import batch_processor
from db.crud_mongodb import upsert_bulk_insights, insert_bulk_data, get_data, delete_data
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials, get_account_id_and_access_token_by_platform_id
from api_clients.meta_api import start_meta_async_job
from utils.logging import setup_task_logger
import traceback

async def fetch_and_store_report_id(level, scheduler_type: str = None, batch_size: int = 10):
    """
    Fetch and store report IDs in MongoDB.
    :param platform_id: The platform ID for fetching accounts.
    :param level: The level for the insights data (e.g., 'account', 'campaign').
    :param token: Access token for Meta API.
    :param batch_size: Number of accounts to process in a batch.
    """
    logger = setup_task_logger("fetch_and_store_report_id")
    # Step 1: Fetch account IDs and tokens from SQL
    mysql_db = MySQLSessionLocal()

    accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 1)
    mysql_db.close()

    logger.info(f"Fetched {len(accounts)} accounts for platform Meta.")

    # Step 2: Batch process accounts
    async def process_batch(account_batch):
        date_today = datetime.date.today().strftime("%Y-%m-%d")
        date_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        documents = []
        for account in account_batch:
            try:
                account_id, token = account.account_id, account.token
                report_id = start_meta_async_job(
                  account_id = account_id, 
                  date_start = date_yesterday, 
                  date_end = date_today, 
                  level = level, 
                  token = token,
                  is_live=False if scheduler_type != "live" else True
                )
                
                documents.append({
                    "account_id": account_id,
                    "access_token": token,
                    "level": level,
                    "reference": report_id,
                    "date_start": str(date_yesterday),
                    "date_end": str(date_today),
                    "created_at": datetime.datetime.utcnow(),
                    "status": 0  # 0 = pending
                })
                logger.info(f"Inserted report_id {report_id} for account {account_id}.")
            except Exception as e:
                logger.error(f"Error processing account {account['account_id']}: {e}")
                # failed_collection = get_mongo_collection("meta_failed_references")
                # failed_collection.insert_one({"account_id": account["account_id"], "error": str(e), "level": level})
        if documents:
            insert_bulk_data("meta_references" if not scheduler_type else f"meta_references_{scheduler_type}", documents)

    await batch_processor(accounts, process_batch, batch_size=batch_size)
