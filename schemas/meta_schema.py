from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class MetaInsightData(BaseModel):
    """Schema for nested 'data' field."""
    objective: Optional[str] = Field(None, description="The campaign objective")
    spend: Optional[float] = Field(0, description="The total spend")
    impressions: Optional[int] = Field(0, description="The total impressions")
    clicks: Optional[int] = Field(0, description="The total clicks")
    link_click: Optional[int] = Field(0, description="The total link clicks")
    landing_page_view: Optional[int] = Field(0, description="The total landing page views")
    add_to_cart: Optional[int] = Field(0, description="The total add to cart actions")
    add_to_cart_value: Optional[float] = Field(0, description="The total value of add to cart actions")
    purchase: Optional[int] = Field(0, description="The total purchase actions")
    purchase_value: Optional[float] = Field(0, description="The total value of purchase actions")
    initiate_checkout: Optional[int] = Field(0, description="The total initiate checkout actions")
    post_engagement: Optional[int] = Field(0, description="The total post engagement actions")
    add_to_cart_shared_item: Optional[int] = Field(0, description="The total add to cart actions for shared items")
    add_to_cart_value_shared_item: Optional[float] = Field(0, description="The total value of add to cart actions for shared items")
    purchase_shared_item: Optional[int] = Field(0, description="The total purchase actions for shared items")
    purchase_value_shared_item: Optional[float] = Field(0, description="The total value of purchase actions for shared items")
    content_view_shared_item: Optional[int] = Field(0, description="The total content view actions for shared items")
    content_view_value_shared_item: Optional[float] = Field(0, description="The total value of content view actions for shared items")
    lead: Optional[int] = Field(0, description="The total lead actions")
    application_submitted: Optional[int] = Field(0, description="The total application submitted actions")
    application_submitted_custom: Optional[int] = Field(0, description="The total application submitted custom actions")
    registration_completed: Optional[int] = Field(0, description="The total registration completed actions")
    estimated_ad_recallers: Optional[int] = Field(0, description="The total estimated ad recallers")
    post_engagement: Optional[int] = Field(0, description="The total post engagement actions")
    post_saves: Optional[int] = Field(0, description="The total post saves actions")
    post_reaction: Optional[int] = Field(0, description="The total post reaction actions")
    post_comment: Optional[int] = Field(0, description="The total post comment actions")
    post_share: Optional[int] = Field(0, description="The total post share actions")
    outbound_click: Optional[int] = Field(0, description="The total outbound click actions")
    thruplays: Optional[int] = Field(0, description="The total thruplays")
    video_play: Optional[int] = Field(0, description="The total video play actions")
    video_play_100: Optional[int] = Field(0, description="The total video play 100% actions")
    messaging_new: Optional[int] = Field(0, description="The total messaging new actions")
    messaging_blocked: Optional[int] = Field(0, description="The total messaging blocked actions")
    messaging_conv_started: Optional[int] = Field(0, description="The total messaging conversation started actions")

class MetaInsightDataLive(BaseModel):
    """Schema for nested 'data' field."""
    objective: Optional[str] = Field(None, description="The campaign objective")
    reach: Optional[int] = Field(0, description="The total reach")
    frequency: Optional[float] = Field(0, description="The total frequency")

class MetaContentData(BaseModel):
    """Schema for nested 'data' field."""
    content: Optional[str] = Field(None, description="The content of the ad")

class MetaInsight(BaseModel):
    """Schema for the main document."""
    account_id: str = Field(..., description="The ID of the ad account")
    campaign_name: Optional[str] = Field(None, description="The name of the campaign")
    campaign_id: Optional[str] = Field(None, description="The ID of the campaign")
    adset_name: Optional[str] = Field(None, description="The name of the ad set")
    adset_id: Optional[str] = Field(None, description="The ID of the ad set")
    ad_name: Optional[str] = Field(None, description="The name of the ad")
    ad_id: Optional[str] = Field(None, description="The ID of the ad")
    publisher_platform: Optional[str] = Field(None, description="The publisher platform")
    platform_position: Optional[str] = Field(None, description="The platform position")
    date: datetime = Field(..., description="The start date of the data")
    date_inserted: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    objective: Optional[str] = Field(None, description="The campaign objective")
    data: Optional[MetaInsightData] = Field(..., description="Nested data containing metrics")

class MetaInsightLive(BaseModel):
    """Schema for the main document."""
    account_id: str = Field(..., description="The ID of the ad account")
    campaign_name: Optional[str] = Field(None, description="The name of the campaign")
    campaign_id: Optional[str] = Field(None, description="The ID of the campaign")
    adset_name: Optional[str] = Field(None, description="The name of the ad set")
    adset_id: Optional[str] = Field(None, description="The ID of the ad set")
    ad_name: Optional[str] = Field(None, description="The name of the ad")
    ad_id: Optional[str] = Field(None, description="The ID of the ad")
    publisher_platform: Optional[str] = Field(None, description="The publisher platform")
    platform_position: Optional[str] = Field(None, description="The platform position")
    date_start: datetime = Field(..., description="The start date of the data")
    date_end: datetime = Field(..., description="The end date of the data")
    date_inserted: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    objective: Optional[str] = Field(None, description="The campaign objective")
    data: Optional[MetaInsightDataLive] = Field(..., description="Nested data containing metrics")

class MetaContent(BaseModel):
    """Schema for the main document."""
    account_id: str = Field(..., description="The ID of the ad account")
    ad_id: Optional[str] = Field(None, description="The ID of the ad")
    date_inserted: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    data: Optional[MetaContentData] = Field(..., description="Nested data containing content")

class MetaReference(BaseModel):
    """Schema for the reference document."""
    account_id: str = Field(..., description="The ID of the account")
    access_token: str = Field(..., description="The access token")
    level: str = Field(..., description="The level of the data")
    reference: str = Field(..., description="The reference ID")
    date_start: str = Field(..., description="The start date of the data")
    date_end: str = Field(..., description="The end date of the data")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="The timestamp when the data was inserted")
    status: int = Field(0, description="The status of the data")

class TimeRange(BaseModel):
    since: str
    until: str

class AdsInsightParam(BaseModel):
    level: Optional[str] = "campaign"
    time_increment: Optional[str] = None
    time_range: Optional[TimeRange] = None
    date_preset: Optional[str] = None
    filtering: Optional[list[dict]] = None    
    breakdowns: Optional[list[str]] = None
    use_account_attribution_setting: bool = True
    action_attribution_windows: list[str] = ["7d_click","1d_view"]
    use_unified_attribution_setting: bool = True
    access_token: str = None
    fields: Optional[list[str]] = None

class MetricsList(BaseModel):
    objective: Optional[str] = "objective"
    spend: Optional[str] = "spend"
    impressions: Optional[str] = "impressions"
    clicks_all: Optional[str] = "clicks"
    actions_link_click: Optional[str] = "link_click"
    actions_landing_page_view: Optional[str] = "landing_page_view"
    actions_omni_add_to_cart: Optional[str] = "add_to_cart"
    action_values_omni_add_to_cart: Optional[str] = "add_to_cart_value"
    actions_omni_purchase: Optional[str] = "purchase"
    action_values_omni_purchase: Optional[str] = "purchase_value"
    actions_omni_initiated_checkout: Optional[str] = "initiate_checkout"
    actions_post_engagement: Optional[str] = "post_engagement"
    catalog_segment_actions_omni_add_to_cart: Optional[str] = "add_to_cart_shared_item"
    catalog_segment_value_omni_add_to_cart: Optional[str] = "add_to_cart_value_shared_item"
    catalog_segment_actions_omni_purchase: Optional[str] = "purchase_shared_item"
    catalog_segment_value_omni_purchase: Optional[str] = "purchase_value_shared_item"
    catalog_segment_actions_omni_view_content: Optional[str] = "content_view_shared_item"
    catalog_segment_value_omni_view_content: Optional[str] = "content_view_value_shared_item"
    actions_lead: Optional[str] = "lead"
    actions_submit_application_total: Optional[str] = "application_submitted"
    actions_fb_pixel_custom: Optional[str] = "application_submitted_custom"
    actions_omni_complete_registration: Optional[str] = "registration_completed"
    estimated_ad_recallers: Optional[str] = "estimated_ad_recallers"
    actions_post_engagement: Optional[str] = "post_engagement"
    actions_post_save: Optional[str] = "post_saves"
    actions_post_reaction: Optional[str] = "post_reaction"
    actions_comment: Optional[str] = "post_comment"
    actions_post: Optional[str] = "post_share"
    outbound_clicks_outbound_click: Optional[str] = "outbound_click"
    video_15_sec_watched_actions_video_view: Optional[str] = "thruplays"
    video_play_actions_video_view: Optional[str] = "video_play"
    video_p100_watched_actions_video_view: Optional[str] = "video_play_100"
    actions_messaging_first_reply: Optional[str] = "messaging_new"
    actions_messaging_block: Optional[str] = "messaging_blocked"
    actions_messaging_conversation_started_7d: Optional[str] = "messaging_conv_started"

class MetricsListLive(BaseModel):
    objective: Optional[str] = "objective"
    reach: Optional[str] = "reach"
    frequency: Optional[str] = "frequency"
