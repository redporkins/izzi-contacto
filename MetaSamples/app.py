from flask import Flask, request, jsonify
import time

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.action_source import ActionSource

ACCESS_TOKEN = "EAAMCZCGhHcggBQEGwrN711hss0ZBYo8jSrZCTjHzssGgUAfWDrZC6NQvSf7lf7hh5ZCuP8xo2f3r7WZBgvZANphlKl8E4Y15497p9gP7tHZCERoNFWhbPKUndpb0ZAQOFT7IwRpn65E2ZBOtCkuZByOZB8rdJg98TnOh4aG2BpIvgeDXETDXdU5Mm3ZA2lBB3nHcSuAZDZD"
PIXEL_ID = "777472967223"

FacebookAdsApi.init(access_token=ACCESS_TOKEN)

app = Flask(__name__)

@app.post("/hibot/lead")
def hibot_lead():
    body = request.json or {}

    email = body.get("email")
    phone = body.get("phone")
    name  = body.get("name")  # opcional

    user_data = UserData(
        emails=[email] if email else None,
        phones=[phone] if phone else None,
    )

    custom_data = CustomData(
        value=0,
        currency="USD",
    )

    event = Event(
        event_name="Lead",
        event_time=int(time.time()),
        user_data=user_data,
        custom_data=custom_data,
        action_source=ActionSource.BUSINESS_MESSAGING,
    )

    request_fb = EventRequest(pixel_id=PIXEL_ID, events=[event])
    fb_response = request_fb.execute()

    return jsonify({"status": "ok", "fb": fb_response}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
