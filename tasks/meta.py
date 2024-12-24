# tasks/meta.py
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials
from db.base_mysql import MySQLSessionLocal
from utils.utils import platform_name_correction
from api_clients.meta_api import refresh_token, debug_token
from datetime import datetime, timedelta
from utils.logging import setup_task_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import traceback

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import traceback

def refresh_meta_token(max_workers=5):
    """
    Refresh Meta tokens with optimized concurrency and better time complexity.
    :param max_workers: Number of threads for concurrent token refresh.
    """
    # Initialize task-specific logger
    logger = setup_task_logger("meta_refresh_token")

    logger.info("Starting 'refresh_meta_token' task. 🏁")
    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("meta")

        # Get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # Get all access tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info(f"Fetched {len(tokens)} access tokens for Meta platform.")

        if not tokens:
            logger.warning("No tokens found for Meta platform. Exiting.")
            return

        data = []

        def refresh_single_token(token):
            """
            Refresh a single token and return the updated data.
            """
            try:
                response = refresh_token(token.token)
                logger.info(f"Refreshing token for user_id {token.user_id}...")

                if response.get("error"):
                    logger.error(f"Error refreshing token for user_id {token.user_id}: {response.get('error').get('message')}")
                    return None

                # Calculate expiration date
                current_time = datetime.now()
                expiry_date = current_time + timedelta(seconds=response.get("expires_in", 0))

                return {
                    "id": token.id,
                    "token": response.get("access_token"),
                    "refresh_token": None,
                    "updated_at": current_time,
                    "token_expiry": expiry_date,
                    "flag": 1
                }
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error processing token for user_id {token.user_id}: {e}")
                return None

        # Use a thread pool to process token refreshes concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(refresh_single_token, token): token for token in tokens}

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        data.append(result)
                except Exception as e:
                    logger.error(f"Error processing token: {e}")

        # Batch update the tokens
        if data:
            batch_update_user_credentials(mysql_db, data)
            logger.info(f"{len(data)} tokens updated successfully.")

        mysql_db.close()
        logger.info("Meta token refresh task completed successfully. 😉")

    except Exception as e:
        traceback.print_exc()
        logger.error(f"An error occurred during Meta token refresh: {e}")

def check_meta_token_validity(max_workers=5):
    """
    Check the validity of Meta platform tokens with optimized concurrency.
    :param batch_size: Number of tokens to process in a batch.
    :param max_workers: Maximum number of threads for parallel processing.
    """
    logger = setup_task_logger("meta_check_token_validity")

    logger.info("Starting 'check_meta_token_validity' task. 🏁")

    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("meta")

        # Get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # Get all access tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info(f"Fetched {len(tokens)} access tokens for Meta platform.")

        data = []

        def process_token(token):
            """
            Process a single token to check its validity and return its update data.
            """
            try:
                if token.token_expiry < datetime.now():
                    logger.warning(f"Token for user {token.user_id} is expired. 😓")
                    return {"id": token.id, "flag": 0}

                response = debug_token(token.token)

                if response.get("error"):
                    logger.error(f"Error debugging token for user {token.user_id} 😡: {response.get('error').get('message')}")
                    return None

                if not response.get("data", {}).get("is_valid"):
                    logger.warning(f"Token for user {token.user_id} is invalid. 😓")
                    return {"id": token.id, "flag": 0}
                
                logger.info(f"Token for user {token.user_id} is valid. 👍")
                return {"id": token.id, "flag": 1}
            except Exception as e:
                logger.error(f"Error processing token for user {token.user_id}: {e}")
                return None

        # Use a ThreadPoolExecutor for concurrent token processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_token = {executor.submit(process_token, token): token for token in tokens}

            for future in as_completed(future_to_token):
                token = future_to_token[future]
                try:
                    result = future.result()
                    if result:
                        data.append(result)
                except Exception as e:
                    logger.error(f"Error processing token for user {token.user_id}: {e}")

        # Update the tokens in batch
        if data:
            batch_update_user_credentials(mysql_db, data)
            logger.info(f"{len(data)} tokens' flag updated 👍.")

        mysql_db.close()

        logger.info("Meta token validity check completed. 😉")
    except Exception as e:
        traceback.print_exc()
        logger.error(f"An error occurred when checking the meta token validity 😡: {e}")
