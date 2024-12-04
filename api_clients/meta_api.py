# api_clients/meta_api.py
import requests
from config import settings

META_URL = settings.FB_URI
META_APP_ID = settings.FB_APP_ID
META_APP_SECRET = settings.FB_APP_SECRET

def refresh_token(token:str):
    url = f"{META_URL}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": META_APP_ID,
        "client_secret": META_APP_SECRET,
        "fb_exchange_token": token
    }
    response = requests.get(url=url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Meta API error: {response.text}")

def debug_token(token:str):
    url = f"{META_URL}/debug_token"
    params = {
        "input_token": token,
        "access_token": f"{META_APP_ID}|{META_APP_SECRET}"
    }

    response = requests.get(url=url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Meta API error: {response.text}")