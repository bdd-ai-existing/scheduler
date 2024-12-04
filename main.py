
import sys
from tasks.meta import refresh_meta_token, check_meta_token_validity
from tasks.google import refresh_google_ads_token, check_google_ads_token_validity, refresh_google_analytics_token, check_google_analytics_token_validity
from tasks.shopee import refresh_shopee_token, check_shopee_token_validity
from tasks.notification import notification_user_tokens_exp

def main():
  func_name = sys.argv[1]

  mapFn = {
      "refresh_meta_token": refresh_meta_token,
      "check_meta_token_validity": check_meta_token_validity,

      "refresh_google_ads_token": refresh_google_ads_token,
      "check_google_ads_token_validity": check_google_ads_token_validity,

      "refresh_google_analytics_token": refresh_google_analytics_token,
      "check_google_analytics_token_validity": check_google_analytics_token_validity,

      "refresh_shopee_token": refresh_shopee_token,
      "check_shopee_token_validity": check_shopee_token_validity,

      "token_expired_notification": notification_user_tokens_exp
  }

  # Execute the function
  if func_name in mapFn:
      mapFn[func_name]()

# fn = mapFn.get(func_name)

if __name__ == "__main__":
  main()