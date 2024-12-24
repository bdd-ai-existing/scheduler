# tasks/google.py
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials
from db.base_mysql import MySQLSessionLocal
from utils.utils import platform_name_correction
from datetime import datetime, timedelta
from api_clients.google_api import refresh_token, debug_token
from utils.logging import setup_task_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

def refresh_google_ads_token(max_workers=5):
    """
    Refresh Google Ads tokens concurrently and update their information in the database.
    :param max_workers: Maximum number of threads for parallel processing.
    """
    # Initialize task-specific logger
    logger = setup_task_logger("google_ads_refresh_token")

    logger.info("Starting 'refresh_google_ads_token' task 🏁.")

    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("googleads")

        # Get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # Get all refresh tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info(f"Fetched {len(tokens)} refresh tokens for Google Ads platform.")

        def process_token(token):
            """
            Refresh a single token and return the updated data.
            """
            try:
                logger.info(f"Refreshing token for user_id {token.user_id}...")

                # Refresh the token
                response = refresh_token(token.refresh_token)
                if not response:
                    logger.error(f"Error refreshing token for user_id {token.user_id} 😡")
                    return None

                # Calculate expiration date
                current_time = datetime.now()
                expiry_date = current_time + timedelta(seconds=response.get("expires_in"))

                return {
                    "id": token.id,
                    "token": response.get("access_token"),
                    "refresh_token": token.refresh_token,
                    "updated_at": current_time,
                    "token_expiry": expiry_date,
                    "flag": 1,
                }
            except Exception as e:
                logger.error(f"Error processing token for user_id {token.user_id}: {e}")
                traceback.print_exc()
                return None

        # Use ThreadPoolExecutor for concurrent token refresh
        data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_token = {executor.submit(process_token, token): token for token in tokens}

            for future in as_completed(future_to_token):
                result = future.result()
                if result:
                    data.append(result)

        # Update the tokens in the database
        if data:
            batch_update_user_credentials(mysql_db, data)
            logger.info(f"{len(data)} tokens updated 👍.")
        else:
            logger.warning("No tokens were updated.")

        mysql_db.close()
        logger.info("Google Ads token refresh completed. 😉")

    except Exception as e:
        traceback.print_exc()
        logger.error(f"An error occurred while refreshing Google Ads tokens 😡: {e}")

def check_google_ads_token_validity(max_workers=5):
    """
    Check the validity of Google Ads tokens concurrently and update their flags in the database.
    :param max_workers: Maximum number of threads for parallel processing.
    """
    # Initialize task-specific logger
    logger = setup_task_logger("google_ads_check_token_validity")

    logger.info("Starting 'check_google_ads_token_validity' task 🏁.")

    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("googleads")

        # Get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # Get all access tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info(f"Fetched {len(tokens)} tokens for Google Ads platform.")

        def process_token(token):
            """
            Check and update the validity of a single token.
            """
            try:
                if token.token_expiry < datetime.now():
                    logger.warning(f"Token for user {token.user_id} is expired. 😓")
                    return {"id": token.id, "flag": 0}

                access_token = debug_token(token.token)
                if not access_token:
                    logger.warning(f"Token for user {token.user_id} is invalid. 😓")
                    return {"id": token.id, "flag": 0}

                logger.info(f"Token for user {token.user_id} is valid. 👍")
                return {"id": token.id, "flag": 1}
            except Exception as e:
                logger.error(f"Error processing token for user {token.user_id}: {e}")
                return None

        # Use ThreadPoolExecutor for concurrent token checks
        data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_token = {executor.submit(process_token, token): token for token in tokens}

            for future in as_completed(future_to_token):
                result = future.result()
                if result:
                    data.append(result)

        # Update the tokens in the database
        if data:
            batch_update_user_credentials(mysql_db, data)
            logger.info(f"{len(data)} tokens' flags updated 👍.")
        else:
            logger.warning("No tokens were updated.")

        mysql_db.close()
        logger.info("Google Ads token validity check completed. 😉")

    except Exception as e:
        traceback.print_exc()
        logger.error(f"An error occurred while checking the Google Ads token validity 😡: {e}")

def refresh_google_analytics_token(max_workers=5):
    """
    Refresh Google Analytics tokens concurrently and update their information in the database.
    :param max_workers: Maximum number of threads for parallel processing.
    """
    # Initialize task-specific logger
    logger = setup_task_logger("google_analytics_refresh_token")

    logger.info("Starting 'refresh_google_analytics_token' task 🏁.")

    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("googleanalytics")

        # Get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # Get all refresh tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info(f"Fetched {len(tokens)} refresh tokens for Google Analytics platform.")

        def process_token(token):
            """
            Refresh a single token and return the updated data.
            """
            try:
                logger.info(f"Refreshing token for user_id {token.user_id}...")

                # Refresh the token
                response = refresh_token(token.refresh_token)
                if not response:
                    logger.error(f"Error refreshing token for user_id {token.user_id} 😡")
                    return None

                # Calculate expiration date
                current_time = datetime.now()
                expiry_date = current_time + timedelta(seconds=response.get("expires_in"))

                return {
                    "id": token.id,
                    "token": response.get("access_token"),
                    "refresh_token": token.refresh_token,
                    "updated_at": current_time,
                    "token_expiry": expiry_date,
                    "flag": 1,
                }
            except Exception as e:
                logger.error(f"Error processing token for user_id {token.user_id}: {e}")
                traceback.print_exc()
                return None

        # Use ThreadPoolExecutor for concurrent token refresh
        data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_token = {executor.submit(process_token, token): token for token in tokens}

            for future in as_completed(future_to_token):
                result = future.result()
                if result:
                    data.append(result)

        # Update the tokens in the database
        if data:
            batch_update_user_credentials(mysql_db, data)
            logger.info(f"{len(data)} tokens updated 👍.")
        else:
            logger.warning("No tokens were updated.")

        mysql_db.close()
        logger.info("Google Analytics token refresh completed. 😉")

    except Exception as e:
        traceback.print_exc()
        logger.error(f"An error occurred while refreshing Google Analytics tokens 😡: {e}")

def check_google_analytics_token_validity(max_workers=5):
    """
    Check the validity of Google Analytics tokens concurrently and update their flags in the database.
    :param max_workers: Maximum number of threads for parallel processing.
    """
    # Initialize task-specific logger
    logger = setup_task_logger("google_analytics_check_token_validity")

    logger.info("Starting 'check_google_analytics_token_validity' task 🏁.")

    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("googleanalytics")

        # Get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # Get all access tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info(f"Fetched {len(tokens)} tokens for Google Analytics platform.")

        def process_token(token):
            """
            Check and update the validity of a single token.
            """
            try:
                if token.token_expiry < datetime.now():
                    logger.warning(f"Token for user {token.user_id} is expired. 😓")
                    return {"id": token.id, "flag": 0}

                access_token = debug_token(token.token)
                if not access_token:
                    logger.warning(f"Token for user {token.user_id} is invalid. 😓")
                    return {"id": token.id, "flag": 0}

                logger.info(f"Token for user {token.user_id} is valid. 👍")
                return {"id": token.id, "flag": 1}
            except Exception as e:
                logger.error(f"Error processing token for user {token.user_id}: {e}")
                return None

        # Use ThreadPoolExecutor for concurrent token checks
        data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_token = {executor.submit(process_token, token): token for token in tokens}

            for future in as_completed(future_to_token):
                result = future.result()
                if result:
                    data.append(result)

        # Update the tokens in the database
        if data:
            batch_update_user_credentials(mysql_db, data)
            logger.info(f"{len(data)} tokens' flags updated 👍.")
        else:
            logger.warning("No tokens were updated.")

        mysql_db.close()
        logger.info("Google Analytics token validity check completed. 😉")

    except Exception as e:
        traceback.print_exc()
        logger.error(f"An error occurred while checking the Google Ads token validity 😡: {e}")