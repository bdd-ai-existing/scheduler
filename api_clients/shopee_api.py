from config import settings
import requests
import json
import time
import hmac
import hashlib
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.utils import split_list

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

def get_session_with_retries():
    session = requests.Session()
    retries = Retry(
        total=5,  # Retry up to 5 times
        backoff_factor=1,  # Exponential backoff: 1, 2, 4, 8, ...
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP statuses
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_order_list(access_token, shop_id, date_start, date_end):
    """
    Fetch the list of orders from Shopee API within a given date range.
    """
    from_time = int(datetime.strptime(date_start, "%Y-%m-%d").timestamp())
    to_time = int(datetime.strptime(date_end, "%Y-%m-%d").timestamp())

    order_list = []
    cursor = None
    session = get_session_with_retries()

    while True:
        try:
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

            response = session.get(url, params=params, timeout=10)  # Add timeout
            response.raise_for_status()  # Raise HTTPError for bad responses
            data = response.json()

            if data.get("error"):
                raise Exception(f"Error fetching order list: {data['error']}")

            order_list.extend(order["order_sn"] for order in data["response"].get("order_list", []))

            if not data["response"].get("more"):
                break

            cursor = data["response"].get("next_cursor")

        except requests.exceptions.Timeout:
            print("Request timed out. Retrying...")
            continue  # Retry the same request
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            raise

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

def fetch_order_details(access_token: str, shop_id: int, order_sn_list: list):
    """
    Fetch detailed information for a list of orders from Shopee API.
    """
    response_optional_fields = (
        "order_sn,region,currency,cod,total_amount,order_status,shipping_carrier,payment_method," 
        "estimated_shipping_fee,message_to_seller,create_time,update_time,days_to_ship,ship_by_date," 
        "buyer_user_id,buyer_username,recipient_address,actual_shipping_fee,goods_to_declare,note," 
        "note_update_time,item_list,pay_time,dropshipper,dropshipper_phone,split_up,buyer_cancel_reason," 
        "cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag," 
        "pickup_done_time,package_list,invoice_data,checkout_shipping_carrier,reverse_shipping_fee," 
        "order_chargeable_weight_gram"
    )

    list_order_detail = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for order_sn_chunk in list(split_list(order_sn_list, 50)):
            auth_url, params = create_v2_signature(
                path="/api/v2/order/get_order_detail",
                access_token=access_token,
                shop_id=shop_id
            )
            params.update({
                "order_sn_list": ",".join(order_sn_chunk),
                "response_optional_fields": response_optional_fields,
            })

            futures[executor.submit(requests.get, auth_url, params=params)] = order_sn_chunk

        for future in as_completed(futures):
            try:
                response = future.result()
                response.raise_for_status()
                resp = response.json()

                if "response" in resp:
                    list_order_detail.extend(resp["response"].get("order_list", []))
                else:
                    raise Exception(f"Error fetching details for chunk: {futures[future]} - {resp.get('error', 'Unknown error')}")

            except Exception as e:
                raise Exception(f"Error fetching order details for chunk: {futures[future]} - {e}")

    return list_order_detail