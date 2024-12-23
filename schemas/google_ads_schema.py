from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class SearchData(BaseModel):
    headlines: Optional[list[str]] = Field(None, title="Headlines")
    descriptions: Optional[list[str]] = Field(None, title="Descriptions")

class DisplayData(BaseModel):
    headlines: Optional[list[str]] = Field(None, title="Headlines")
    descriptions: Optional[list[str]] = Field(None, title="Descriptions")
    long_headline: Optional[str] = Field(None, title="Long Headline")
    business_name: Optional[str] = Field(None, title="Business Name")
    youtube_videos: Optional[list[str]] = Field(None, title="YouTube Videos")
    images: Optional[list[str]] = Field(None, title="Images")

class VideoData(BaseModel):
    video: Optional[str] = Field(None, title="Video")

class GoogleAdsContentData(BaseModel):
    sem: Optional[SearchData] = Field(None, title="SEM")
    gdn: Optional[DisplayData] = Field(None, title="GDN")
    youtube: Optional[VideoData] = Field(None, title="YouTube")

class GoogleAdsInsightData(BaseModel):
    channel_type : Optional[str] = Field(None, title="Channel Type")
    impressions : Optional[int] = Field(0, title="Impressions")
    clicks : Optional[int] = Field(0, title="Clicks")
    cost_micros : Optional[float] = Field(0, title="Cost Micros")
    engagements : Optional[int] = Field(0, title="Engagements")
    conversions : Optional[int] = Field(0, title="Conversions")
    all_conversions : Optional[float] = Field(0, title="All Conversions")
    conversions_value : Optional[float] = Field(0, title="Conversions Value")
    all_conversions_value : Optional[float] = Field(0, title="All Conversions Value")
    video_views : Optional[int] = Field(0, title="Video Views")
    interactions : Optional[int] = Field(0, title="Interactions")

class GoogleAdsDataLive(BaseModel):
    """Schema for nested 'data' field."""
    channel_type: Optional[str] = Field(None, description="The type of channel")

class GoogleAdsInsight(BaseModel):
    """Schema for the main document."""
    account_id: str = Field(..., description="The ID of the ad account")
    campaign_name: Optional[str] = Field(None, description="The name of the campaign")
    campaign_id: Optional[str] = Field(None, description="The ID of the campaign")
    adset_name: Optional[str] = Field(None, description="The name of the ad set")
    adset_id: Optional[str] = Field(None, description="The ID of the ad set")
    ad_name: Optional[str] = Field(None, description="The name of the ad")
    ad_id: Optional[str] = Field(None, description="The ID of the ad")
    date: datetime = Field(..., description="The start date of the data")
    date_inserted: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    channel_type: Optional[str] = Field(None, description="The type of channel")
    data: Optional[GoogleAdsInsightData] = Field(..., description="Nested data containing metrics")

class GoogleAdsContent(BaseModel):
    """Schema for the main document."""
    account_id: str = Field(..., description="The ID of the ad account")
    campaign_name: Optional[str] = Field(None, description="The name of the campaign")
    campaign_id: Optional[str] = Field(None, description="The ID of the campaign")
    adset_name: Optional[str] = Field(None, description="The name of the ad set")
    adset_id: Optional[str] = Field(None, description="The ID of the ad set")
    ad_name: Optional[str] = Field(None, description="The name of the ad")
    ad_id: Optional[str] = Field(None, description="The ID of the ad")
    date_inserted: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    channel_type: Optional[str] = Field(None, description="The type of channel")
    data: Optional[GoogleAdsContentData] = Field(..., description="Nested data containing metrics")

class GoogleAdsInsightLive(BaseModel):
    """Schema for the main document."""
    account_id: str = Field(..., description="The ID of the ad account")
    campaign_name: Optional[str] = Field(None, description="The name of the campaign")
    campaign_id: Optional[str] = Field(None, description="The ID of the campaign")
    adset_name: Optional[str] = Field(None, description="The name of the ad set")
    adset_id: Optional[str] = Field(None, description="The ID of the ad set")
    ad_name: Optional[str] = Field(None, description="The name of the ad")
    ad_id: Optional[str] = Field(None, description="The ID of the ad")
    date_start: datetime = Field(..., description="The start date of the data")
    date_end: datetime = Field(..., description="The end date of the data")
    date_inserted: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    channel_type: Optional[str] = Field(None, description="The type of channel")
    data: Optional[GoogleAdsDataLive] = Field(..., description="Nested data containing metrics")