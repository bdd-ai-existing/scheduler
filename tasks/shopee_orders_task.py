from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List
import traceback
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id, batch_update_user_credentials_by_userid_account_type
from utils.logging import setup_task_logger
from api_clients.shopee_api import fetch_order_list, refresh_token as refresh_token_shopee, get_shop_info as debug_token

ENV = "production"

async def fetch_and_store_shopee_orders(batch_size=10, max_workers=5):
    """
    Fetch and store Shopee orders data.

    :param batch_size: Number of accounts to process in a batch.
    :param max_workers: Maximum number of concurrent threads for API calls.
    """
    logger = setup_task_logger("fetch_and_store_shopee_orders")

    # Step 1: Fetch accounts from SQL
    mysql_db = MySQLSessionLocal()

    accounts = get_account_id_and_access_token_by_platform_id(mysql_db, 9)
    mysql_db.close()

    if not accounts:
        logger.warning("No accounts found for Shopee orders.")
        return

    logger.info(f"Fetched {len(accounts)} accounts for Shopee orders.")

    # Step 2: Batch processing function
    async def process_batch(account_batch):
        
        data_update_token = []
        for account in account_batch:
            if account.account_id != '284908788':
                continue
            
            print(account)
            account_id, access_token, refresh_token = account.account_id, account.token, account.refresh_token
            try:
                # debug the token
                response, status_code = debug_token(shop_id = account_id, env = ENV, access_token = access_token)

                if status_code == 401 or status_code == 403:
                    # refresh the token
                    access_token, new_refresh_token, token_expiry = refresh_token_shopee(account_id, ENV, refresh_token)
                    print(access_token, new_refresh_token, token_expiry)

                    if "error" in access_token:
                        logger.error(f"Error refreshing token from account {account_id} 😡: {access_token}")
                        continue

                    # Calculate expiration date
                    current_time = datetime.now()
                    expiry_date = current_time + timedelta(seconds=token_expiry)

                    data_update_token.append({
                        "user_id": account.user_id,
                        "account_type": 9,
                        "token": access_token,
                        "refresh_token": new_refresh_token,
                        "updated_at": current_time,
                        "token_expiry": expiry_date,
                        "flag": 1
                    })

                # Step 3: API call to fetch order list
                order_list = await fetch_order_list(account.token, account.account_id, "2022-01-01", "2022-01-31")
                print(order_list)

                # if not order_list:
                #     logger.info(f"No orders found for account {account.account_id}.")
                #     continue

                # # Step 4: Store orders data in MongoDB
                # await upsert_bulk_orders(order_list)
            except Exception as e:
                logger.error(f"Error processing account {account.account_id}: {str(e)}")
                logger.error(traceback.format_exc())

        if data_update_token:
            batch_update_user_credentials_by_userid_account_type(mysql_db, data_update_token)
            logger.info(f"{len(data_update_token)} tokens updated 👍.")
            
    # Step 3: Batch process accounts
    for i in range(0, len(accounts), batch_size):
        await process_batch(accounts[i:i + batch_size])