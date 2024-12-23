# tasks/meta.py
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials
from db.base_mysql import MySQLSessionLocal
from utils.utils import platform_name_correction
from api_clients.meta_api import refresh_token, debug_token
from datetime import datetime, timedelta
from utils.logging import setup_task_logger
import json

def refresh_meta_token():
    # Initialize task-specific logger
    logger = setup_task_logger("meta_refresh_token")

    logger.info("Starting 'refresh_meta_token' task. 🏁")
    try:
        mysql_db = MySQLSessionLocal()

        platform_name = platform_name_correction("meta")

        # get the platform
        platform = get_ad_account_platform_by_name(mysql_db, platform_name)

        # get all access tokens for the platform
        tokens = get_access_tokens(mysql_db, platform.id)
        logger.info("Getting all access tokens for Meta platform.")

        data = []
        for token in tokens:
            # refresh the token
            response = refresh_token(token.token)
            logger.info("Refreshing token for user_id {token.user_id}...")

            if response.get("error"):
                logger.error(f"Error refreshing token from user_id {token.user_id} 😡: {response.get('error').get('message')}")
                continue

            # Calculate expiration date
            current_time = datetime.now()
            expiry_date = current_time + timedelta(seconds=response.get("expires_in"))

            data.append({
                "id": token.id,
                "token": response.get("access_token"),
                "refresh_token": None,
                "updated_at": current_time,
                "token_expiry": expiry_date,
                "flag": 1
            })

        # update the tokens
        batch_update_user_credentials(mysql_db, data)
        logger.info(f"{len(data)} tokens updated 👍.")

        mysql_db.close()

        logger.info("Meta token refresh completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when refresh the meta token 😡: {e}")

def check_meta_token_validity():
    # Initialize task-specific logger
    logger = setup_task_logger("meta_check_token_validity")

    logger.info("Starting 'check_meta_token_validity' task. 🏁")

    try:
      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("meta")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)
      logger.info("Getting all access tokens for Meta platform.")
      
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
              response = debug_token(token.token)

              if response.get("error"):
                  logger.error(f"Error debugging token for user {token.user_id} 😡: {response.get('error').get('message')}")
                  continue
              
              if not response.get("data").get("is_valid"):
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

      logger.info("Meta token validity check completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when checking the meta token validity 😡: {e}")
