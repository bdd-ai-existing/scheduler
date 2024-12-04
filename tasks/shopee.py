# tasks/meta.py
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials, get_account_ids_by_platform_id_and_user_id
from db.base_mysql import MySQLSessionLocal
from utils.utils import platform_name_correction
from api_clients.shopee_api import get_shop_info as debug_token, refresh_token
from datetime import datetime, timedelta
from utils.logging import setup_task_logger

ENV = "testing"

def refresh_shopee_token():
    # Initialize task-specific logger
    logger = setup_task_logger("shopee_refresh_token")

    logger.info("Starting 'refresh_shopee_token' task.")

    try:
      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("shopee")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)

      data = []
      for token in tokens:
          account_ids = get_account_ids_by_platform_id_and_user_id(mysql_db, platform.id, token.user_id)

          for account_id in account_ids:
            # refresh the token
            access_token, new_refresh_token, token_expiry = refresh_token(account_id.account_id, ENV, token.refresh_token)

            if "error" in access_token:
                logger.error(f"Error refreshing token from user_id {token.user_id} 😡: {access_token}")
                continue

            # Calculate expiration date
            current_time = datetime.now()
            expiry_date = current_time + timedelta(seconds=token_expiry)

            data.append({
                "id": token.id,
                "token": access_token,
                "refresh_token": new_refresh_token,
                "updated_at": current_time,
                "token_expiry": expiry_date,
                "flag": 1
            })

      # update the tokens
      batch_update_user_credentials(mysql_db, data)
      logger.info(f"{len(data)} tokens updated 👍. Data: {data}")

      mysql_db.close()

      logger.info("Shopee token refresh completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when refreshing the Shopee token 😡: {e}")

def check_shopee_token_validity():
    # Initialize task-specific logger
    logger = setup_task_logger("shopee_check_token_validity")

    logger.info("Starting 'check_shopee_token_validity' task.")
    
    try:
      mysql_db = MySQLSessionLocal()

      platform_name = platform_name_correction("shopee")

      # get the platform
      platform = get_ad_account_platform_by_name(mysql_db, platform_name)

      # get all access tokens for the platform
      tokens = get_access_tokens(mysql_db, platform.id)
      
      data = []
      for token in tokens:    
          if token.token_expiry < datetime.now():
              data.append({
                  "id": token.id,
                  "flag": 0
              })
              logger.warning(f"Token for user {token.user_id} is expired. 😓")
          else:
              account_ids = get_account_ids_by_platform_id_and_user_id(mysql_db, platform.id, token.user_id)

              for account_id in account_ids:
                  
                # debug the token
                response, status_code = debug_token(shop_id = account_id.account_id, env = ENV, access_token = token.token)

                if status_code == 200:
                    data.append({
                        "id": token.id,
                        "flag": 1
                    })
                    logger.info(f"Token for user {token.user_id} is valid. 👍")
                elif status_code == 401:
                    data.append({
                        "id": token.id,
                        "flag": 0
                    })
                    logger.warning(f"Token for user {token.user_id} is invalid. 😓")
                else:
                    logger.error(f"Error debugging token for user {token.user_id} 😡: {status_code}")

      # update the tokens
      batch_update_user_credentials(mysql_db, data)
      logger.info(f"{len(data)} tokens updated 👍. Data: {data}")

      mysql_db.close()

      logger.info("Shopee token validity check completed. 😉")
    except Exception as e:
        logger.error(f"An error occurred when checking the Shopee token validity 😡: {e}")