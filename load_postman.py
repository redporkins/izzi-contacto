import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

print("Fetching Postman collection...")

headers = {"X-Api-Key": os.getenv("POSTMAN_API_KEY")}

# Get collection (HiBot)
col = requests.get(f"{os.getenv('POSTMAN_BASE')}/collections/{os.getenv('POSTMAN_META_COLLECTION_ID')}", headers=headers).json()
with open("JSON/collection_meta.json", "w") as f:
    json.dump(col["collection"], f, indent=2)

# Get environment (HiBot -if any-)
env = requests.get(f"{os.getenv('POSTMAN_BASE')}/environments/{os.getenv('POSTMAN_META_ENVIRONMENT_ID')}", headers=headers).json()
with open("JSON/environment_meta.json", "w") as f:
    json.dump(env["environment"], f, indent=2)

print("Saved collection.json and environment.json")


