import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Ad Account API"
    VERSION: str = "1.0.0"
    SECRET_KEY: str = "supersecretkey"

    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "password"
    MYSQL_HOST: str = "localhost"
    MYSQL_NAME: str = "ad_accounts"

    FB_URI: str = "https://graph.facebook.com"
    FB_VER: str = "v21.0"
    FB_APP_ID: str = "your_app_id"
    FB_APP_SECRET: str = "your_app_secret"

    TIKTOK_APP_ID: str = "your_app_id"
    TIKTOK_SECRET_KEY: str = "your_secret_key"
    TIKTOK_URI: str = "https://business-api.tiktok.com/open_api"
    TIKTOK_VER: str = "v1.3"

    GOOG_REDIRECT_URI_ADS: str = "https://redirect_uri_ads"
    GOOG_REDIRECT_URI_ADS_STAG: str = "https://redirect_uri_ads"
    GOOG_REDIRECT_URI_ANALYTICS: str = "https://redirect_uri_ads"
    GOOG_REDIRECT_URI_ANALYTICS_STAG: str = "https://redirect_uri_ads"
    GOOG_CLIENT_ID_AUTO: str = "your_client_id"
    GOOG_CLIENT_SECRET_AUTO: str = "your_client_secret"
    GOOG_DEVELOPER_TOKEN: str = "your_developer_token"

    GOOGLE_VERSION: str = "v18"
    GOOGLE_BASE_URL: str = "https://googleads.googleapis.com"

    GOOGLE_AUTH_URL: str = "https://oauth2.googleapis.com"

    GA_URL: str = "https://www.googleapis.com/analytics"
    GA_VER: str = "v3"

    GA4_URL: str = "https://analyticsadmin.googleapis.com"
    GA4_VER: str = "v1beta"

    SHOPEEV2_BASE_URL: str = "https://partner.shopeemobile.com"
    SHOPEEV2_PARTNER_ID: str = "your_partner_id"
    SHOPEEV2_SECRET_KEY: str = "your_secret_key"
    SHOPEEV2_REDIRECT_URI: str = "https://redirect_uri"
    SHOPEEV2_BASE_URL_TEST: str = "https://partner.test-stable.shopeemobile.com"
    SHOPEEV2_PARTNER_ID_TEST: str = "your_partner_id"
    SHOPEEV2_SECRET_KEY_TEST: str = "your_secret_key"
    SHOPEEV2_REDIRECT_URI_STAG: str = "https://redirect_uri"
    SHOPEEV2_VER: str = "v2"

    META_BINDING_URL: str = "https://binding.com"
    TIKTOK_BINDING_URL: str = "https://binding.com"
    GOOGLE_ADS_BINDING_URL: str = "https://binding.com"
    GOOGLE_ANALYTICS_BINDING_URL: str = "https://binding.com"
    SHOPEE_BINDING_URL: str = "https://binding.com"

    LOG_DIR: str = "./"

    SMTP_USER: str = "the@sender.com"
    SMTP_PASSWORD: str = "thesecret"

    @property
    def DATABASE_URL(self):
        return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}/{self.MYSQL_NAME}"

    class Config:
        env_file = ".env"

settings = Settings()
