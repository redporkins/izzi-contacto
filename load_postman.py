import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
def load_postman_collection(collection_id: str, environment_id: str, name: str) -> None:
    print("Fetching Postman collection...")

    headers = {"X-Api-Key": os.getenv("POSTMAN_API_KEY")}

    # Get collection (HiBot)
    col = requests.get(f"{os.getenv('POSTMAN_BASE')}/collections/{collection_id}", headers=headers).json()
    with open(f"JSON/collection_{name}.json", "w") as f:
        json.dump(col["collection"], f, indent=2)

    # Get environment (HiBot -if any-)
    env = requests.get(f"{os.getenv('POSTMAN_BASE')}/environments/{environment_id}", headers=headers).json()
    with open(f"JSON/environment_{name}.json", "w") as f:
        json.dump(env["environment"], f, indent=2)

    print(f"Saved collection_{name}.json and environment_{name}.json")
    
def main():
    load_postman_collection(
        os.getenv("POSTMAN_META_COLLECTION_ID"),
        os.getenv("POSTMAN_META_ENVIRONMENT_ID"),
        "meta"
    )
    
    load_postman_collection(
        os.getenv("POSTMAN_HIBOT_COLLECTION_ID"),
        os.getenv("POSTMAN_HIBOT_ENVIRONMENT_ID"),
        "hibot"
    )

if __name__ == "__main__":
    main()