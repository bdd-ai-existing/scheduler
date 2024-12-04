from datetime import datetime
from db.base_mysql import MySQLSessionLocal
from db.crud_mysql import get_all_access_tokens, get_account_ids_by_platform_id_and_user_id
from utils.utils import platform_name_correction_reverse
from db.crud_mysql import get_ad_account_platform_by_id
from api_clients.meta_api import debug_token as meta_debug_token
from api_clients.google_api import debug_token as google_debug_token
from api_clients.shopee_api import get_shop_info as shopee_debug_token
import traceback
import math
from utils.email import send_batch_emails
from config import settings

ENV = "testing"
BATCH_SIZE = 100  # Define the number of users per batch

META_BINDING_URL = settings.META_BINDING_URL
TIKTOK_BINDING_URL = settings.TIKTOK_BINDING_URL
GOOGLE_ADS_BINDING_URL = settings.GOOGLE_ADS_BINDING_URL
GOOGLE_ANALYTICS_BINDING_URL = settings.GOOGLE_ANALYTICS_BINDING_URL
SHOPEE_BINDING_URL = settings.SHOPEE_BINDING_URL

def notification_user_tokens_exp():
    """
    Notify users about expired tokens.
    """
    try:
        mysql_db = MySQLSessionLocal()

        # Notify Meta token expiration
        tokens = get_all_access_tokens(mysql_db)

        users = []

        for token in tokens:
            account_type = token.account_type

            account_platform = get_ad_account_platform_by_id(mysql_db, account_type)
            account_platform_name = platform_name_correction_reverse(account_platform.name)

            access_token = token.token

            if token.token_expiry < datetime.now():
                users.append({
                    "user_id": token.user_id,
                    "user_name": token.name,
                    "user_email": token.email,
                    "platform": account_platform_name,
                })
            else:
              if account_platform_name.casefold() == "meta":
                  # debug meta token expiry
                  debug_meta = meta_debug_token(access_token)
                  
                  if debug_meta.get("error"):
                      users.append({
                          "user_id": token.user_id,
                          "user_name": token.name,
                          "user_email": token.email,
                          "platform": account_platform_name,
                      })
                      continue
                  
                  if not debug_meta.get("data").get("is_valid"):
                      users.append({
                          "user_id": token.user_id,
                          "user_name": token.name,
                          "user_email": token.email,
                          "platform": account_platform_name,
                      })

              elif account_platform_name.casefold() in ["googleads", "googleanalytics"]:
                  # debug google ads and analytics token expiry
                  debug_google = google_debug_token(access_token)

                  if not debug_google:
                      users.append({
                          "user_id": token.user_id,
                          "user_name": token.name,
                          "user_email": token.email,
                          "platform": account_platform_name,
                      })

              elif account_platform_name.casefold() == "shopee":
                  account_ids = get_account_ids_by_platform_id_and_user_id(mysql_db, account_type, token.user_id)

                  for account_id in account_ids:  
                    # debug shopee token expiry
                    response, status_code = shopee_debug_token(shop_id = account_id.account_id, env = ENV, access_token = token.token)

                    if status_code != 200:
                        users.append({
                            "user_id": token.user_id,
                            "user_name": token.name,
                            "user_email": token.email,
                            "platform": account_platform_name,
                        })

        """
        group users by user_id and format the data become like this:
        [
            {
                "user_id": 1,
                "user_name": "user1",
                "user_email": "user1@email.com",
                "platforms": [
                    {
                        "platform": "meta",
                        "link": "https://meta.com"
                    },
                    {
                        "platform": "googleads",
                        "link": "https://googleads.com"
                    }
                ]
        ]
        """
        users_grouped = {}
        for user in users:
            if user["user_id"] not in users_grouped:
                users_grouped[user["user_id"]] = {
                    "user_id": user["user_id"],
                    "user_name": user["user_name"],
                    "user_email": user["user_email"],
                    "platforms": []
                }

            link = ""
            if user["platform"].casefold() == "meta":
                link = META_BINDING_URL
            elif user["platform"].casefold() == "googleads":
                link = GOOGLE_ADS_BINDING_URL
            elif user["platform"].casefold() == "googleanalytics":
                link = GOOGLE_ANALYTICS_BINDING_URL
            elif user["platform"].casefold() == "shopee":
                link = SHOPEE_BINDING_URL

            users_grouped[user["user_id"]]["platforms"].append({
                "platform": user["platform"],
                "link": link
            })

        list_users = [
            user for user in users_grouped.values()
        ]

        notify_users_in_batches(list_users)

        mysql_db.close()
    except Exception as e:
        traceback.print_exc()
        print(f"An error occurred when notifying users about expired tokens 😡: {e}")

def batch_users(users, batch_size):
    """
    Split users into batches of a given size.
    :param users: List of user dictionaries.
    :param batch_size: Number of users per batch.
    :return: List of batches.
    """
    total_batches = math.ceil(len(users) / batch_size)
    return [users[i * batch_size:(i + 1) * batch_size] for i in range(total_batches)]

def notify_users_in_batches(users):
    """
    Notify users in batches about token expiration.
    :param users: List of user dictionaries.
    """
    user_batches = batch_users(users, BATCH_SIZE)

    for batch in user_batches:
        email_data_list = [
            {
                "user_email": user["user_email"],
                "subject": "Token Expiration Notification",
                "template_name": "token_expiration_notification.html",
                "template_data": {
                    "user_name": user["user_name"],
                    "platforms": user["platforms"]
                },
            }
            for user in batch
        ]

        send_batch_emails(email_data_list)
