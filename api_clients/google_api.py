import requests
from config import settings

# Replace these with your credentials
AUTH_URL = settings.GOOGLE_AUTH_URL
CLIENT_ID = settings.GOOG_CLIENT_ID_AUTO
CLIENT_SECRET = settings.GOOG_CLIENT_SECRET_AUTO

def debug_token(access_token):
    # Check token validity
    token_info_url = f"{AUTH_URL}/tokeninfo"
    params = {
        "access_token": access_token
    }
    response = requests.get(token_info_url, params=params)
    
    
    if response.status_code == 200:
        # print("Access Token is valid.")
        return True  # Return the valid token
    else:
        # print("Access Token is invalid or expired. Refreshing...")
        return False

def refresh_token(refresh_token):
    # Endpoint to refresh token
    token_url = f"{AUTH_URL}/token"
    
    # Data for the token refresh request
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    
    # Make the POST request
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        token_data = response.json()
        # new_access_token = token_data["access_token"]
        # print("New Access Token:", token_data)
        return token_data
    else:
        # print("Failed to refresh token:", response.json())
        return None

# # Example usage
# current_access_token = "your_existing_access_token"
# current_access_token = check_access_token_and_refresh(current_access_token)
