def platform_name_correction(
  platform_name: str
):
  if platform_name.casefold() == "meta":
    platform_name = "facebook"
  elif platform_name.casefold() == "googleads" or platform_name.casefold() == "gads":
    platform_name = "gadwords"
  elif platform_name.casefold() == "googleanalytics" or platform_name.casefold() == "ganalytics":
    platform_name = "ganalytics"

  return platform_name

def platform_name_correction_reverse(
  platform_name: str
):
  if platform_name.casefold() == "facebook":
    platform_name = "meta"
  elif platform_name.casefold() == "gadwords":
    platform_name = "googleAds"
  elif platform_name.casefold() == "ganalytics":
    platform_name = "googleAnalytics"

  return platform_name


def split_list(lst, chunk_size):
    """Split a list into smaller chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]