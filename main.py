
import sys
from tasks.meta import refresh_meta_token, check_meta_token_validity
from tasks.google import refresh_google_ads_token, check_google_ads_token_validity, refresh_google_analytics_token, check_google_analytics_token_validity
from tasks.shopee import refresh_shopee_token, check_shopee_token_validity
from tasks.notification import notification_user_tokens_exp
from tasks.meta_report_task import fetch_and_store_report_id
from tasks.meta_insights_task import fetch_and_store_insights
from tasks.meta_content_task import fetch_and_store_ad_previews
from tasks.tiktok_metrics_task import fetch_and_store_tiktok_metrics
from tasks.tiktok_content_task import fetch_and_store_tiktok_ad_contents
from tasks.google_ads_insights_task import fetch_and_store_google_ads_insights
from tasks.google_ads_content import fetch_and_store_google_ads_content
import asyncio

async def main():
  func_name = sys.argv[1]

  mapFn = {
      "refresh_meta_token": refresh_meta_token,
      "check_meta_token_validity": check_meta_token_validity,
      "meta_daily_references": fetch_and_store_report_id,
      "meta_daily_insights": fetch_and_store_insights,
      "meta_content": fetch_and_store_ad_previews,

      "refresh_google_ads_token": refresh_google_ads_token,
      "check_google_ads_token_validity": check_google_ads_token_validity,
      "google_ads_daily": fetch_and_store_google_ads_insights,
      "google_ads_content": fetch_and_store_google_ads_content,

      "refresh_google_analytics_token": refresh_google_analytics_token,
      "check_google_analytics_token_validity": check_google_analytics_token_validity,

      "refresh_shopee_token": refresh_shopee_token,
      "check_shopee_token_validity": check_shopee_token_validity,

      "tiktok_daily": fetch_and_store_tiktok_metrics,
      "tiktok_content": fetch_and_store_tiktok_ad_contents,

      "token_expired_notification": notification_user_tokens_exp
  }

  # Execute the function
  if func_name in mapFn:
      print(f"Executing {func_name}")

      fn = mapFn.get(func_name)

      if func_name in ["meta_daily_references", "meta_daily_insights", "tiktok_daily", "google_ads_daily"]:
          if asyncio.iscoroutinefunction(fn):  # Check if fn is coroutine
              if len(sys.argv) == 3:
                await fn(sys.argv[2])
              elif len(sys.argv) == 4:
                await fn(sys.argv[2], sys.argv[3])  # Await for async functions
          else:
              if len(sys.argv) == 3:
                fn(sys.argv[2])
              elif len(sys.argv) == 4:
                fn(sys.argv[2], sys.argv[3])  # Call directly if not async
      else:
          if asyncio.iscoroutinefunction(fn):  # Check if fn is coroutine
              await fn()  # Await for async functions
          else:
              fn()  # Call directly if not async
  else:
      print(f"Function {func_name} not found in mapFn")
      

# fn = mapFn.get(func_name)

if __name__ == "__main__":
  asyncio.run(main())