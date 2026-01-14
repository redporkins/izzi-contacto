from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.lead import Lead

ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"
AD_ACCOUNT_ID = "act_1234567890"  # your ad account id

access_token = 'EAAMCZCGhHcggBQEGwrN711hss0ZBYo8jSrZCTjHzssGgUAfWDrZC6NQvSf7lf7hh5ZCuP8xo2f3r7WZBgvZANphlKl8E4Y15497p9gP7tHZCERoNFWhbPKUndpb0ZAQOFT7IwRpn65E2ZBOtCkuZByOZB8rdJg98TnOh4aG2BpIvgeDXETDXdU5Mm3ZA2lBB3nHcSuAZDZD'
pixel_id = '777472967223169'

FacebookAdsApi.init(access_token=ACCESS_TOKEN)

account = AdAccount(AD_ACCOUNT_ID)

lead_fields = ["id", "created_time", "field_data"]

# This gives you all leads on the account (you can add filters by date, form, etc.)
leads = account.get_leads(fields=lead_fields)

for lead in leads:
    print("Lead ID:", lead["id"])
    print("Created:", lead["created_time"])
    # field_data contains the answers to your form questions
    for field in lead["field_data"]:
        name = field.get("name")
        value = field.get("values", [""])[0]
        print(f"  {name}: {value}")
    print("-" * 40)
