import os
import csv
import time
import hashlib
import requests
from datetime import datetime, timezone
import json
from typing import Dict
from pathlib import Path

"""
SELECT * FROM Venta.VentasRegistradas
SELECT * FROM Venta.Servicio

Se desean obetner los siguientes campos para alimentar al Pixel con informacion más completa en la compra de un producto/servicio:

Para ubicar la campaña, conjunto de anuncios y anuncio por donde llegó el usuario, se requieren los siguientes campos:
- campaign_id: identificador de la campaña
- adset_id: identificador del conjunto de anuncios
- ad_id: identificador del anuncio

Para tener mejor noción del usuario, se requieren los siguientes campos:
- fn (first name): nombre (separar los nombres)
- ln (last name): apellido (separar los apellidos)
- ge (gender): género
- db (date of birth): fecha de nacimiento
- lead_id (lead id): identificador del lead
- email: correo electrónico (aunque ya hay un campo en SQL no se está utilizando)


Revisar el siguiente link para constuir eventos personalizados: 
https://developers.facebook.com/docs/marketing-api/conversions-api/payload-helper?data=[%7B%22event_name%22%3A%22Purchase%22%2C%22event_time%22%3A1767974571%2C%22action_source%22%3A%22physical_store%22%2C%22user_data%22%3A%7B%22em%22%3A%227b17fb0bd173f625b58636fb796407c22b3d16fc78302d79f0fd30c2fc2fc068%22%2C%22ph%22%3A%22d36e83082288d9f2c98b3f3f87cd317a31e95527cb09972090d3456a7430ad4d%22%7D%2C%22attribution_data%22%3A%7B%22attribution_share%22%3A%220.3%22%7D%2C%22custom_data%22%3A%7B%22currency%22%3A%22USD%22%2C%22value%22%3A%22142.52%22%7D%2C%22original_event_data%22%3A%7B%22event_name%22%3A%22Purchase%22%2C%22event_time%22%3A1767974571%7D%7D]&selectedProduct=Offline
"""

# ---------- Config loading ----------
def load_postman_collection_variables(col_json: Path) -> Dict[str, str]:
    """
    Load key/values from a Postman collection export (collection.json).
    Args: 
      { "variable": [ {"key":"base_url", "value":"..."}, ... ] }
    Returns: 
      Dict of key/values
    """
    if not col_json.exists():
        return {}
    data = json.loads(col_json.read_text(encoding="utf-8"))
    result = {}
    variables = data.get("variable") or []
    for item in variables:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        val = item.get("value")
        if key and val is not None:
            result[key] = val
    return result

def load_postman_environment_values(env_json: Path) -> Dict[str, str]:
    """
    Load key/values from a Postman environment export (environment.json).
    Args: 
      { "values": [ {"key":"base_url", "value":"...", ...}, ... ] }
    Returns: 
      Dict of key/values
    """
    if not env_json.exists():
        return {}
    data = json.loads(env_json.read_text(encoding="utf-8"))
    result = {}
    values = data.get("values") or []
    for item in values:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        val = item.get("value")
        if key and val is not None:
            result[key] = str(val)
    return result

def sha256_normalized(s: str) -> str:
    s = s.strip().lower()
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def parse_dt_to_unix(dt_str: str) -> int:
    # Example input: "2025-12-30 08:54:39.573537"
    dt = datetime.fromisoformat(dt_str)
    # Treat as local time if naive; adjust if you store timezone elsewhere
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def normalize_phone_mx(raw: str) -> str | None:
    """
    Ideally convert to E.164 (+52...). If you don't know the country or format,
    at least strip non-digits. For best match, use python-phonenumbers library.
    """
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 8:
        return None
    # If you *know* these are MX numbers and already 10 digits, prepend country code:
    if len(digits) == 10:
        return "+52" + digits
    if digits.startswith("52") and len(digits) in (12, 13):
        return "+" + digits
    return "+" + digits  # fallback

# def send_capi_events(events: list[dict]) -> dict:
#     url = f"https://graph.facebook.com/{API_VERSION}/{PIXEL_ID}/events"
#     resp = requests.post(url, params={"access_token": ACCESS_TOKEN}, json={"data": events}, timeout=15)
#     resp.raise_for_status()
#     return resp.json()

# def send_capi_events(events: list[dict], creds: dict) -> dict:
#     url = f"https://graph.facebook.com/{creds['API_VERSION']}/{creds['PIXEL_ID']}/events"
#     resp = requests.post(url, params={"access_token": creds["ACCESS_TOKEN"]}, json={"data": events}, timeout=15)
#     resp.raise_for_status()
#     return resp.json()

def send_capi_events(events: list[dict], creds: dict) -> dict:
    url = f"https://graph.facebook.com/{creds['API_VERSION']}/{creds['PIXEL_ID']}/events"
    payload = {
        "data": events,
        "test_event_code": "TEST12345"  # paste from Events Manager
    }
    resp = requests.post(
        url,
        params={"access_token": creds["ACCESS_TOKEN"]},
        json=payload,
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()

def build_purchase_event(row: dict) -> dict | None:
    """
    Sends the purchase orders from the SQL export to Meta CAPI.
    Args:
        row: Dict representing a row from the CSV.
    Returns:
        Dict representing the event for Meta CAPI, or None to skip.
    
    """
    # Skip non-finalized orders if you want (example filter)
    if row.get("EstadoOrden") and row["EstadoOrden"].strip().upper() != "DONE":
        # Your sample says NOT DONE; decide your business logic here
        pass

    event_time = parse_dt_to_unix(row["FechaDeCreacionPakoa"])
    # Meta rejects events older than ~7 days; ensure you're within the window. :contentReference[oaicite:5]{index=5}
    if int(time.time()) - event_time > 7 * 24 * 3600:
        return None

    email = (row.get("Email") or "").strip()
    phone = normalize_phone_mx(row.get("Telefono") or row.get("Telefono2") or "")
    name = (row.get("Nombre") or "").strip()
    city = (row.get("DeleoMuni") or "").strip()
    state = (row.get("Estado") or "").strip()
    zipcode = (row.get("CodigoPostal") or "").strip()
    colonia = (row.get("Colonia") or "").strip()

    user_data = {}
    if email:
        user_data["em"] = [sha256_normalized(email)]
    if phone:
        user_data["ph"] = [sha256_normalized(phone)]
    if name: 
        user_data["fn"] = [sha256_normalized(name)]
    if city:
        user_data["ct"] = [sha256_normalized(city)]
    if state:
        user_data["st"] = [sha256_normalized(state)]
    if zipcode:
        user_data["zp"] = [sha256_normalized(zipcode)]
    
    # All users are in Mexico for this dataset    
    user_data["country"] = [sha256_normalized("mx")]

    # If you have neither email nor phone, match rate will be poor.
    if not user_data:
        return None

    # For non-web/offline CRM events, action_source is required; web-only fields
    # like event_source_url/client_user_agent apply to website events. :contentReference[oaicite:6]{index=6}
    value = float(row.get("Costo") or 0)
    
    # Determine content_category based on NoRGU and append the Descipcion if available
    tipo = row.get("Tipo", "").strip()

    content_category = {
        1: f"1 Play {tipo}",
        2: f"2 Play {tipo}",
        3: f"3 Play {tipo}",
    }.get(row.get("NoRGU"), f"Other {tipo}" if tipo else "Other")


    return {
        "event_name": "Purchase",
        "event_time": event_time,
        "action_source": "system_generated",
        "event_id": "purchase",
        "user_data": user_data,
        "custom_data": {
            "currency": "MXN",
            "value": value,
            "order_id": row.get("NoContrato"),
            "content_category": content_category,
            "content_name": row.get("Descripcion"),
            "status": row.get("EstadoOrden"),
        },
    }

def build_lead_event_tiktok(row: dict) -> dict | None:
    # Implement similar to build_purchase_event if needed
    
    event_time = parse_dt_to_unix(row["Creation time"])
    # Meta rejects events older than ~7 days; ensure you're within the window. :contentReference[oaicite:5]{index=5}
    if int(time.time()) - event_time > 7 * 24 * 3600:
        return None

    # email = (row.get("Email") or "").strip()
    phone = normalize_phone_mx(row.get("Phone number") or "")
    name = (row.get("Name") or "").strip()
    lead_id = (row.get("Lead ID") or "").strip()
    # city = (row.get("DeleoMuni") or "").strip()
    # state = (row.get("Estado") or "").strip()
    # zipcode = (row.get("CodigoPostal") or "").strip()
    # colonia = (row.get("Colonia") or "").strip()

    user_data = {}
    # if email:
    #     user_data["em"] = [sha256_normalized(email)]
    if phone:
        user_data["ph"] = [sha256_normalized(phone)]
    if name: 
        user_data["fn"] = [sha256_normalized(name)]
    if lead_id:
        user_data["external_id"] = [sha256_normalized(lead_id)]
    # if city:
    #     user_data["ct"] = [sha256_normalized(city)]
    # if state:
    #     user_data["st"] = [sha256_normalized(state)]
    # if zipcode:
    #     user_data["zp"] = [sha256_normalized(zipcode)]
    
    # All users are in Mexico for this dataset    
    user_data["country"] = [sha256_normalized("mx")]

    # If you have neither email nor phone, match rate will be poor.
    if not user_data:
        return None

    return {
        "event_name": "Lead",
        "event_time": event_time,
        "action_source": "system_generated",
        "event_id": "formFill",
        "user_data": user_data,
        "custom_data": [
            {
                "name": "Form Page ID",
                "value": row.get("Form ID")
            },
            {
                "name": "Form Name",
                "value": row.get("Form name")
            },
            {
                "name": "Campaign ID",
                "value": row.get("Campaign ID")
            },
            {
                "name": "Campaign Name",
                "value": row.get("Campaign name")
            },
            {
                "name": "Adgroup ID",
                "value": row.get("Ad group ID")
            },
            {
                "name": "Adgroup Name",
                "value": row.get("Ad group name")
            },
            {
                "name": "Ad ID",
                "value": row.get("Ad ID")
            },
            {
                "name": "Ad Name",
                "value": row.get("Ad name")
            },
            {
                "name": "lead_source",
                "value": row.get("lead_source")
            },
            {
                "name": "advertiser_id",
                "value": row.get("advertiser_id")
            },
            {
                "name": "advertiser_name",
                "value": row.get("advertiser_name")
            },
            {
                "name": "library_id", 
                "value": row.get("library_id")
            },
            {
                "name": "platform",
                "value": "TikTok"
            }
        ],
        "action_source": "other"
    }
    return None

def read_csv_events(csv_path: str, event_type: str, creds: dict) -> None:
    print(f"Processing CSV: {csv_path} | Event Type: {event_type}")
    
    batch = []
    sent = 0
    skipped = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if event_type == "Purchase":
                ev = build_purchase_event(row)
            elif event_type == "Lead":
                ev = build_lead_event_tiktok(row)
            if not ev:
                skipped += 1
                continue

            batch.append(ev)

            # Meta supports batching; keep it reasonable (Meta mentions up to 1,000). :contentReference[oaicite:7]{index=7}
            if len(batch) >= 500:
                # print(send_capi_events(batch))
                print(send_capi_events(batch, creds))
                sent += len(batch)
                batch = []

    if batch:
        print(send_capi_events(batch, creds))
        sent += len(batch)

    print(f"Sent: {sent} | Skipped: {skipped}")
    
    return None

# def main(csv_path: str):
def main(CSVs: dict) -> None:
    # Get current directory
    here = Path(__file__).parent
    print("Path:", here)
    
    
    # Load Postman collection and environment exports
    pm_coll_vals = load_postman_collection_variables(here / "JSON/collection_meta.json")
    pm_env = load_postman_environment_values(here / "JSON/environment_meta.json")
    
    creds = {
        "PIXEL_ID": pm_env.get("META_PIXEL01_ID"),
        "ACCESS_TOKEN": pm_env.get("META_API_TOKEN"),
        "API_VERSION": pm_env.get("VERSION")  # choose the Graph API version you use
    }
    
    print(f"PIXEL_ID: {creds["PIXEL_ID"]}")
    print(f"ACCESS_TOKEN: {creds["ACCESS_TOKEN"][:8]}...")
    print(f"API_VERSION: {creds["API_VERSION"]}")
    
    for event_type, paths in CSVs.items():
        print(f"CSV: {paths} | Event Type: {event_type}")
        # Check if paths is a list or a single string
        if isinstance(paths, list) and len(paths) > 0:
            for path in paths:
                read_csv_events(path, event_type, creds)
        else:
            read_csv_events(paths, event_type, creds)
    
    
    # batch = []
    # sent = 0
    # skipped = 0

    # with open(csv_path, newline="", encoding="utf-8") as f:
    #     reader = csv.DictReader(f)
    #     for row in reader:
    #         ev = build_purchase_event(row)
    #         if not ev:
    #             skipped += 1
    #             continue

    #         batch.append(ev)

    #         # Meta supports batching; keep it reasonable (Meta mentions up to 1,000). :contentReference[oaicite:7]{index=7}
    #         if len(batch) >= 500:
    #             # print(send_capi_events(batch))
    #             print(send_capi_events(batch, creds))
    #             sent += len(batch)
    #             batch = []

    # if batch:
    #     print(send_capi_events(batch, creds))
    #     sent += len(batch)

    # print(f"Sent: {sent} | Skipped: {skipped}")
    
    return None

if __name__ == "__main__":
    CSVs = {
        "Purchase": "CSV/filtered_sql_sales_export.csv",    
        # "Lead":  {"tiktok" : "filtered_tiktok_export.csv", "hibot": "filtered_hibot_export.csv"},
        # "Lead":  "filtered_tiktok_export.csv",
        }
    # main("filtered_sql_export.csv")
    main(CSVs)
    
# TEST90305
# "A4gDLIU1cRrGz37HGCb9jpu"
