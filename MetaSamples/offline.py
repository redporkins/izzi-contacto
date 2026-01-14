import time

from facebook_business.adobjects.serverside.content import Content
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.gender import Gender
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi

access_token = 'EAAMCZCGhHcggBQEGwrN711hss0ZBYo8jSrZCTjHzssGgUAfWDrZC6NQvSf7lf7hh5ZCuP8xo2f3r7WZBgvZANphlKl8E4Y15497p9gP7tHZCERoNFWhbPKUndpb0ZAQOFT7IwRpn65E2ZBOtCkuZByOZB8rdJg98TnOh4aG2BpIvgeDXETDXdU5Mm3ZA2lBB3nHcSuAZDZD'
pixel_id = '777472967223169'

FacebookAdsApi.init(access_token=access_token)

user_data_0 = UserData(
    emails=["7b17fb0bd173f625b58636fb796407c22b3d16fc78302d79f0fd30c2fc2fc068"],
    phones=["d36e83082288d9f2c98b3f3f87cd317a31e95527cb09972090d3456a7430ad4d"]
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
    action_source="physical_store"
)

events = [event_0]
event_request = EventRequest(
    events=events,
    pixel_id=pixel_id
)
event_response = event_request.execute()