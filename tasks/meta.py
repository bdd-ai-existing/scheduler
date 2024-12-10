# tasks/meta.py
from db.crud_mysql import get_access_tokens, get_ad_account_platform_by_name, batch_update_user_credentials, get_account_id_and_access_token_by_platform_id
from db.base_mysql import MySQLSessionLocal
from utils.utils import platform_name_correction
from api_clients.meta_api import refresh_token, debug_token
from datetime import datetime, timedelta
from utils.logging import setup_task_logger
import json

from concurrent.futures import ThreadPoolExecutor, as_completed
from api_clients.meta_api import fetch_meta_report_id
from db.crud_mongodb import upsert_bulk_insights, insert_bulk_data
import logging
from schemas.meta_schema import MetaInsight, MetaReference

BATCH_SIZE = 10  # Process accounts in batches
MAX_WORKERS = 5  # Number of threads for parallel processing

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

def daily_fetch_data_meta():
    mysql_db = MySQLSessionLocal()

    account_data = get_account_id_and_access_token_by_platform_id(mysql_db, 1)
    mysql_db.close()

    fetch_and_store_report_id(account_data)

def process_report_id_of_the_account(account_id, token):
    logger = setup_task_logger("fetch_report_id_of_the_account")
    """
    Fetch insights for a single account, validate using Pydantic, and prepare for MongoDB insertion.
    :param account_id: The ID of the Meta ad account.
    :param token: The access token for the ad account.
    :return: A list of validated and structured documents ready for bulk insertion into MongoDB.
    """
    logger.info(f"Fetching insights for account ID: {account_id}")
    try:
        # Fetch raw data from Meta API
        insights = fetch_meta_report_id(account_id, token)  # Assume this function is implemented
        bulk_data = []

        # Prepare the raw data with additional fields
        raw_data = {
            "account_id": account_id,
            "access_token": token,
            "level": "campaign",
            "reference": insights,
            "date_start": "2024-01-01",  # Assuming 'date_start' exists in the response
            "date_end": "2024-01-07",
            "created_at": datetime.now(),
        }

        # Validate and sanitize data using Pydantic
        try:
            validated_data = MetaReference(**raw_data).dict()
            bulk_data.append(validated_data)
        except Exception as validation_error:
            logger.error(f"Validation failed for account ID {account_id}: {validation_error}", exc_info=True)
        
        return bulk_data

    except Exception as e:
        logger.error(f"Error fetching insights for account ID {account_id}: {e}", exc_info=True)
        return []
    
def process_account(account_id, token):
    logger = setup_task_logger("process_report_id_of_the_account")
    """
    Fetch insights for a single account, validate using Pydantic, and prepare for MongoDB insertion.
    :param account_id: The ID of the Meta ad account.
    :param token: The access token for the ad account.
    :return: A list of validated and structured documents ready for bulk insertion into MongoDB.
    """
    print(f"Fetching insights for account ID: {account_id}")
    try:
        # Fetch raw data from Meta API
        insights = fetch_meta_report_id(account_id, token)  # Assume this function is implemented
        bulk_data = []

        for insight in insights:
            # Prepare the raw data with additional fields
            actions = {action['action_type']: action.get('value', 0) for action in insight.get('actions', [])}
            action_values = {value['action_type']: value.get('value', 0) for value in insight.get('action_values', [])}
            catalog_segment_actions = {action['action_type']: action.get('value', 0) for action in insight.get('catalog_segment_actions', [])}
            catalog_segment_values = {value['action_type']: value.get('value', 0) for value in insight.get('catalog_segment_values', [])}
            
            raw_data = {
                "ad_account": account_id,
                "campaign_name": insight.get("campaign_name"),
                "campaign_id": insight.get("campaign_id"),
                "date": insight.get("date_start"),  # Assuming 'date_start' exists in the response
                "objective": insight.get("objective"),
                "data": {
                    **insight,  # Include all raw data,
                    **actions,  # actions,
                    **action_values,  # action_values,
                    **catalog_segment_actions,  # catalog_segment_actions,
                    **catalog_segment_values,  # catalog_segment_values
                },
            }

            # Validate and sanitize data using Pydantic
            try:
                validated_data = MetaInsight(**raw_data).dict()
                bulk_data.append({
                    "update_one": {
                        "filter": {"ad_account": account_id, "date": validated_data["date"]},
                        "update": {"$set": validated_data},
                        "upsert": True,
                    }
                })
            except Exception as validation_error:
                print(f"Validation failed for account ID {account_id}: {validation_error}")
        print(json.dumps(bulk_data, indent=2))
        return bulk_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        # print(f"Error fetching insights for account ID {account_id}: {e}", exc_info=True)
        print(f"Error fetching insights for account ID {account_id}: {e}")
        return []


def process_batch_report_id(account_data):
    logger = setup_task_logger("fetch_report_id_of_the_account")
    """Fetch and store insights for a batch of accounts."""
    logger.info(f"Processing batch of {len(account_data)} accounts...")
    bulk_data = []

    # Use ThreadPoolExecutor for parallel API calls
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_account = {executor.submit(process_report_id_of_the_account, dt.account_id, dt.token): dt for dt in account_data}

        for future in as_completed(future_to_account):
            dt = future_to_account[future]
            try:
                insights = future.result()
                bulk_data.extend(insights)
            except Exception as e:
                logging.error(f"Error in future for account ID {dt}: {e}", exc_info=True)
    
    # Perform bulk upsert in MongoDB
    if bulk_data:
        insert_bulk_data("queue-facebook", bulk_data)
        logger.info(f"Successfully stored insights for batch of {len(account_data)} accounts.")

def fetch_and_store_report_id(account_data):
    logger = setup_task_logger("fetch_report_id_of_the_account")
    """Fetch and store insights for all accounts."""
    logger.info(f"Starting to fetch and store insights for {len(account_data)} accounts...")

    # Split account IDs into batches
    for i in range(0, len(account_data), BATCH_SIZE):
        batch = account_data[i:i + BATCH_SIZE]
        process_batch_report_id(batch)
