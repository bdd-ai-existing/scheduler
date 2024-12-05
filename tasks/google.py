# tasks/google.py
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials
from db.base_mysql import MySQLSessionLocal
from utils.utils import platform_name_correction
from datetime import datetime, timedelta
from api_clients.google_api import refresh_token, debug_token
from utils.logging import setup_task_logger

def refresh_google_ads_token():
    # Initialize task-specific logger
    logger = setup_task_logger("google_ads_refresh_token")

    logger.info("Starting 'refresh_google_ads_token' task 🏁.")

    try:

      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("googleads")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)
      logger.info("Getting all access tokens for Google Ads platform.")

      data = []
      for token in tokens:
          # refresh the token
          response = refresh_token(token.refresh_token)
          logger.info(f"Refreshing token for user_id {token.user_id}...")

          if not response:
              logger.error(f"Error refreshing token from user_id {token.user_id} 😡")
              continue

          # Calculate expiration date
          current_time = datetime.now()
          expiry_date = current_time + timedelta(seconds=response.get("expires_in"))

          data.append({
              "id": token.id,
              "token": response.get("access_token"),
              "refresh_token": token.refresh_token,
              "updated_at": current_time,
              "token_expiry": expiry_date,
              "flag": 1
          })

      # update the tokens
      batch_update_user_credentials(mysql_db, data)
      logger.info(f"{len(data)} tokens updated 👍.")

      mysql_db.close()

      logger.info("Google Ads token refresh completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when refresh the Google Ads token 😡: {e}")

def check_google_ads_token_validity():
    # Initialize task-specific logger
    logger = setup_task_logger("google_ads_check_token_validity")

    logger.info("Starting 'check_google_ads_token_validity' task 🏁.")

    try:
      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("googleads")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)
      logger.info("Getting all access tokens for Google Ads platform.")
      
      data = []
      for token in tokens:
          if token.token_expiry < datetime.now():
              data.append({
                  "id": token.id,
                  "flag": 0
              })
              logger.warning(f"Token for user {token.user_id} is expired. 😓")
          else:
              # debug the token
              access_token = debug_token(token.token)
              
              if not access_token:
                  # update flag to 0
                  data.append({
                      "id": token.id,
                      "flag": 0
                  })
                  logger.warning(f"Token for user {token.user_id} is invalid. 😓")
              else:
                  data.append({
                      "id": token.id,
                      "flag": 1
                  })
                  logger.info(f"Token for user {token.user_id} is valid. 👍")

      # update the tokens
      batch_update_user_credentials(mysql_db, data)
      logger.info(f"{len(data)} tokens' flag updated 👍.")
      
      mysql_db.close()

      logger.info("Google Ads token validity check completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when checking the Google Ads token validity 😡: {e}")

def refresh_google_analytics_token():
    # Initialize task-specific logger
    logger = setup_task_logger("google_analytics_refresh_token")

    logger.info("Starting 'refresh_google_analytics_token' task. 🏁")

    try:
      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("googleanalytics")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)
      logger.info("Getting all access tokens for Google Analytics platform.")

      data = []
      for token in tokens:
          # refresh the token
          response = refresh_token(token.refresh_token)
          logger.info(f"Refreshing token for user_id {token.user_id}...")

          if not response:
              logger.error(f"Error refreshing token from user_id {token.user_id} 😡")
              continue

          # Calculate expiration date
          current_time = datetime.now()
          expiry_date = current_time + timedelta(seconds=response.get("expires_in"))

          data.append({
              "id": token.id,
              "token": response.get("access_token"),
              "refresh_token": token.refresh_token,
              "updated_at": current_time,
              "token_expiry": expiry_date,
              "flag": 1
          })

      # update the tokens
      batch_update_user_credentials(mysql_db, data)
      logger.info(f"{len(data)} tokens updated 👍.")

      mysql_db.close()

      logger.info("Google Analytics token refresh completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when refresh the Google Analytics token 😡: {e}")

def check_google_analytics_token_validity():
    # Initialize task-specific logger
    logger = setup_task_logger("google_analytics_check_token_validity")

    logger.info("Starting 'check_google_analytics_token_validity' task. 🏁")

    try:
      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("googleanalytics")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)
      logger.info("Getting all access tokens for Google Analytics platform.")
      
      data = []
      for token in tokens:
          if token.token_expiry < datetime.now():
              data.append({
                  "id": token.id,
                  "flag": 0
              })
              logger.warning(f"Token for user {token.user_id} is expired. 😓")
          else:
              # debug the token
              access_token = debug_token(token.token)
              
              if not access_token:
                  # update flag to 0
                  data.append({
                      "id": token.id,
                      "flag": 0
                  })
                  logger.warning(f"Token for user {token.user_id} is invalid. 😓")
              else:
                  data.append({
                      "id": token.id,
                      "flag": 1
                  })
                  logger.info(f"Token for user {token.user_id} is valid. 👍")

      # update the tokens
      batch_update_user_credentials(mysql_db, data)
      logger.info(f"{len(data)} tokens' flag updated 👍.")
      
      mysql_db.close()

      logger.info("Google Analytics token validity check completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when checking the Google Analytics token validity 😡: {e}")