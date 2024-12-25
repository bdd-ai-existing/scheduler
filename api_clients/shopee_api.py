from config import settings
import requests
import json
import time
import hmac
import hashlib
from datetime import datetime

SHOPEEV2_BASE_URL = settings.SHOPEEV2_BASE_URL
SHOPEEV2_BASE_URL_TEST = settings.SHOPEEV2_BASE_URL_TEST
SHOPEEV2_SECRET_KEY = settings.SHOPEEV2_SECRET_KEY
SHOPEEV2_SECRET_KEY_TEST = settings.SHOPEEV2_SECRET_KEY_TEST
SHOPEEV2_PARTNER_ID = settings.SHOPEEV2_PARTNER_ID
SHOPEEV2_PARTNER_ID_TEST = settings.SHOPEEV2_PARTNER_ID_TEST
SHOPEEV2_VER = settings.SHOPEEV2_VER

def get_shop_info(shop_id: int, env: str, access_token: str):
  # get shop info
  url, params = create_v2_signature(
      path = f"/api/{SHOPEEV2_VER}/shop/get_shop_info",
      access_token = access_token,
      shop_id = shop_id,
      api_type = env
  )

  response = requests.get(url = url, params=params)

  shop_info = response.json()

  return shop_info, response.status_code

def refresh_token(shop_id: int, env: str, refresh_token: str):
    partner_id = SHOPEEV2_PARTNER_ID
    if env == "testing":
        partner_id = SHOPEEV2_PARTNER_ID_TEST

    # get token and access token
    url, params = create_v2_signature(
        path = f"/api/{SHOPEEV2_VER}/auth/access_token/get",
        api_type= env,
    )

    body = {
        "shop_id": int(shop_id), 
        "refresh_token": refresh_token,
        "partner_id":int(partner_id)
    }

    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, json=body, headers=headers, params=params)
    ret = json.loads(resp.content)
    access_token = ret.get("access_token")
    new_refresh_token = ret.get("refresh_token")
    token_expiry = ret.get("expire_in")

    if not access_token or not new_refresh_token:
        return ret, ret, ret

    return access_token, new_refresh_token, token_expiry

async def fetch_order_list(access_token, shop_id, date_start, date_end):
    """
    Fetch the list of orders from Shopee API within a given date range.
    """
    from_time = int(datetime.strptime(date_start, "%Y-%m-%d").timestamp())
    to_time = int(datetime.strptime(date_end, "%Y-%m-%d").timestamp())

    order_list = []
    cursor = None

    while True:
        url, params = create_v2_signature(
            path="/api/v2/order/get_order_list",
            access_token=access_token,
            shop_id=shop_id
        )
        params.update({
            "time_from": from_time,
            "time_to": to_time,
            "page_size": 20,
            "time_range_field": "create_time",
        })
        if cursor:
            params["cursor"] = cursor

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            raise Exception(f"Error fetching order list: {data['error']}")

        order_list.extend(order["order_sn"] for order in data["response"].get("order_list", []))

        if not data["response"].get("more"):
            break

        cursor = data["response"].get("next_cursor")

    return order_list

def create_v2_signature(path, access_token=None, shop_id=None, api_type=None):
    """
    Generate a signed URL and parameters for Shopee API requests.
    """
    timestamp = int(datetime.now().timestamp())
    base_url = SHOPEEV2_BASE_URL
    partner_id = SHOPEEV2_PARTNER_ID
    secret_key = SHOPEEV2_SECRET_KEY.encode()

    if api_type == "testing":
        base_url = SHOPEEV2_BASE_URL_TEST
        partner_id = SHOPEEV2_PARTNER_ID_TEST
        secret_key = SHOPEEV2_SECRET_KEY_TEST

    # Create the base string for signing
    base_string = f"{partner_id}{path}{timestamp}{access_token or ''}{shop_id or ''}"
    sign = hmac.new(secret_key, base_string.encode(), hashlib.sha256).hexdigest()

    params = {
        "partner_id": partner_id,
        "timestamp": timestamp,
        "sign": sign,
    }

    if shop_id and access_token:
        params.update({
            "shop_id": shop_id,
            "access_token": access_token,
        })

    return base_url + path, params

def createv2Signature(path: str, access_token: str = None, shop_id: int = None, redirect_uri: str = None, api_type: str = None):
    timest = int(time.time())

    host = SHOPEEV2_BASE_URL
    partner_id = SHOPEEV2_PARTNER_ID
    tmp_partner_key = SHOPEEV2_SECRET_KEY
    if api_type == "testing":
        host = SHOPEEV2_BASE_URL_TEST
        partner_id = SHOPEEV2_PARTNER_ID_TEST
        tmp_partner_key = SHOPEEV2_SECRET_KEY_TEST

    partner_key = tmp_partner_key.encode()
    # tmp_base_string = "%s%s%s" % (partner_id, path, timest)
    tmp_base_string = f"{partner_id}{path}{timest}{access_token or ''}{shop_id or ''}"
    base_string = tmp_base_string.encode()
    sign = hmac.new(partner_key, base_string, hashlib.sha256).hexdigest()
    ##generate api
    params = {}
    url = host + path
    if redirect_uri:
        params = {
            "partner_id" : partner_id,
            "timestamp" : timest,
            "sign" : sign,
            "redirect" : redirect_uri
        }
    elif shop_id and access_token:
        params = {
            "partner_id" : partner_id,
            "timestamp" : timest,
            "sign" : sign,
            "shop_id" : shop_id,
            "access_token" : access_token
        }
    else:
        params = {
            "partner_id" : partner_id,
            "timestamp" : timest,
            "sign" : sign
        }

    return url, params