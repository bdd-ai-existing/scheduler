import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from pymongo import ReplaceOne
from utils.logging import setup_task_logger
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_account_id_and_access_token_by_platform_id, batch_update_user_credentials_by_userid_account_type
from db.crud_mongodb import upsert_bulk_insights
from api_clients.shopee_api import get_shop_info as debug_token, refresh_token as refresh_token_shopee, fetch_order_list, fetch_order_details
from utils.batching import batch_processor
from db.models_mysql import UserAdAccountCredentialInformation
import traceback

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

    async def process_batch(account_batch):
        date_ = {
          "date_start": (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
          "date_end": date.today().strftime("%Y-%m-%d"),
        }

        data_update_token = []
        all_bulk_operations = []

        def fetch_orders_chunk(args):
            """Helper function to fetch orders in chunks."""
            access_token, account_id, chunk_start, chunk_end = args
            return fetch_order_list(
                access_token,
                account_id,
                chunk_start.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for account in account_batch:
                account_id, access_token, refresh_token = account.account_id, account.token, account.refresh_token

                try:
                    # Debug the token
                    response, status_code = debug_token(shop_id=account_id, env=ENV, access_token=access_token)

                    if status_code in [401, 403]:
                        # Refresh the token
                        access_token, new_refresh_token, token_expiry = refresh_token_shopee(account_id, ENV, refresh_token)
                        logger.info(f"Refreshed token for account {account_id}.")

                        if "error" in access_token:
                            logger.error(f"Error refreshing token for account {account_id} 😡: {access_token}")
                            continue

                        # Calculate expiration date
                        current_time = datetime.now()
                        expiry_date = current_time + timedelta(seconds=token_expiry)

                        data_update_token.append(UserAdAccountCredentialInformation(
                            user_id=account.user_id,
                            account_type=9,
                            token=access_token,
                            refresh_token=new_refresh_token,
                            updated_at=current_time,
                            token_expiry=expiry_date,
                            flag=1
                        ).dict())

                    # Split date range into 15-day chunks
                    date_start = datetime.strptime(date_['date_start'], "%Y-%m-%d")
                    date_end = datetime.strptime(date_['date_end'], "%Y-%m-%d")
                    delta = (date_end - date_start).days

                    tasks = []
                    for start_offset in range(0, delta + 1, 15):
                        chunk_start = date_start + timedelta(days=start_offset)
                        chunk_end = min(chunk_start + timedelta(days=14), date_end)
                        tasks.append((access_token, account_id, chunk_start, chunk_end))

                    # Fetch order chunks concurrently
                    all_orders = []
                    futures = {executor.submit(fetch_orders_chunk, task): task for task in tasks}
                    for future in as_completed(futures):
                        try:
                            orders_chunk = future.result()
                            all_orders.extend(orders_chunk)
                        except Exception as e:
                            logger.error(f"Error fetching orders for account {account_id}: {str(e)}")

                    logger.info(f"Fetched {len(all_orders)} orders for account {account_id}.")

                    # Fetch order details concurrently
                    order_details_futures = {
                        executor.submit(fetch_order_details, access_token, account_id, order_sn): order_sn
                        for order_sn in all_orders
                    }

                    bulk_operations = []
                    for future in as_completed(order_details_futures):
                        try:
                            order_details = future.result()
                            bulk_operations.append(
                                ReplaceOne(
                                    filter={"order_sn": order_details["order_sn"]},
                                    replacement={
                                        "order_sn": order_details["order_sn"],
                                        "data": order_details,
                                        "last_updated": datetime.utcnow(),
                                    },
                                    upsert=True,
                                )
                            )
                        except Exception as e:
                            logger.error(f"Error fetching details for order: {order_details_futures[future]}: {str(e)}")

                    all_bulk_operations.extend(bulk_operations)

                except Exception as e:
                    logger.error(f"Error processing account {account_id}: {str(e)}")
                    logger.error(traceback.format_exc())

        # Step 5: Upsert all bulk operations into MongoDB
        if all_bulk_operations:
            upsert_bulk_insights("shopee_orders", all_bulk_operations)
            logger.info(f"Upserted {len(all_bulk_operations)} orders into MongoDB.")

        # Step 6: Update tokens in batch
        if data_update_token:
            batch_update_user_credentials_by_userid_account_type(mysql_db, data_update_token)
            logger.info(f"{len(data_update_token)} tokens updated 👍.")

    # Step 7: Batch process accounts
    await batch_processor(accounts, process_batch, batch_size=batch_size)
