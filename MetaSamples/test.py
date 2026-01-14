import os
import time
import uuid
import hashlib
import requests

PIXEL_ID = os.environ["META_PIXEL_ID"]          # e.g. "1234567890"
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]  # Events Manager > Settings > Conversions API
API_VERSION = "v19.0"  # pick a current Graph API version you use in your stack
TEST_EVENT_CODE = os.getenv("META_TEST_EVENT_CODE")  # optional, from Events Manager > Test Events

def sha256_normalized(value: str) -> str:
    """
    Meta expects SHA-256 of normalized (trimmed, lowercased) strings for PII fields.
    (Keep raw IP + user agent unhashed.)
    """
    v = value.strip().lower().encode("utf-8")
    return hashlib.sha256(v).hexdigest()

def send_purchase_event(
    email: str,
    phone: str,
    value: float,
    currency: str,
    event_source_url: str,
    client_ip: str,
    client_user_agent: str,
    order_id: str,
):
    event_id = str(uuid.uuid4())  # IMPORTANT: reuse this same ID if you also fire Pixel in the browser

    payload = {
        "data": [
            {
                "event_name": "Purchase",
                "event_time": int(time.time()),            # unix seconds :contentReference[oaicite:11]{index=11}
                "event_id": event_id,                      # for dedup with Pixel :contentReference[oaicite:12]{index=12}
                "action_source": "website",                # required :contentReference[oaicite:13]{index=13}
                "event_source_url": event_source_url,      # required for web events :contentReference[oaicite:14]{index=14}
                "user_data": {
                    "em": [sha256_normalized(email)],
                    "ph": [sha256_normalized(phone)],
                    "client_ip_address": client_ip,
                    "client_user_agent": client_user_agent,  # required for web events :contentReference[oaicite:15]{index=15}
                },
                "custom_data": {
                    "currency": currency,
                    "value": value,
                    "order_id": order_id,
                },
            }
        ]
    }

    # If you're testing, pass test_event_code so it shows in Events Manager > Test Events
    # (Meta’s playbook recommends starting with a test event flow.) :contentReference[oaicite:16]{index=16}
    if TEST_EVENT_CODE:
        payload["test_event_code"] = TEST_EVENT_CODE

    url = f"https://graph.facebook.com/{API_VERSION}/{PIXEL_ID}/events"
    resp = requests.post(url, params={"access_token": ACCESS_TOKEN}, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json(), event_id

if __name__ == "__main__":
    result, event_id = send_purchase_event(
        email="customer@example.com",
        phone="+1 555 555 5555",
        value=49.99,
        currency="USD",
        event_source_url="https://example.com/thank-you",
        client_ip="203.0.113.10",
        client_user_agent="Mozilla/5.0 ...",
        order_id="ORDER-10001",
    )
    print("Sent. event_id =", event_id)
    print(result)
