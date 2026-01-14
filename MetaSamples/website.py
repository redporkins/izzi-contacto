import time

from facebook_business.adobjects.serverside.content import Content
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.gender import Gender
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi

access_token = '<ACCESS_TOKEN>'
pixel_id = '<ADS_PIXEL_ID>'

FacebookAdsApi.init(access_token=access_token)

user_data_0 = UserData(
    emails=["7b17fb0bd173f625b58636fb796407c22b3d16fc78302d79f0fd30c2fc2fc068"],
    phones=[]
)
custom_data_0 = CustomData(
    value=142.52,
    currency="USD"
)
event_0 = Event(
    event_name="Purchase",
    event_time=1764780314,
    user_data=user_data_0,
    custom_data=custom_data_0,
    action_source="website"
)

events = [event_0]
event_request = EventRequest(
    events=events,
    pixel_id=pixel_id
)
event_response = event_request.execute()