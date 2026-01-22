import requests 
import json
import csv
import asyncio
import aiohttp
import time
import random
import base64
import argparse
import pandas as pd
from aiohttp import ClientTimeout
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

HIBOT_COLUMNS = [
    "active","agentName","assigned","assignmentType","attentionHour","campaignName",
    "channel","channelId","chatId","client","clientId","closed","contact_account",
    "contact_exclusive_agent_id","contact_exclusive_agent_name","contact_exclusive_agents_count",
    "contact_exclusive_agents_json","contact_exclusive_campaign_id","contact_exclusive_campaign_name",
    "contact_id","contact_name","contact_tags","contacts_count","created","delegate","delegated",
    "duration","id","inactivityCounterByAgent","initFromAgent","isTransfer","note","oldConversationId",
    "outOfTime","parentConversationAgent","postId","projectName","responseTime","sendAck","tags",
    "typeChannel","typing","unknownContact","waitTime"
]


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

# ---------- Helper functions ----------
def join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"

def jwt_info(token: str) -> dict:
    """
    Decode JWT token payload without verification.
    Assumes token is in the format header.payload.signature
    
    Args:
        token (str): JWT token
    Returns:
        dict: Decoded payload
    """
    payload_b64 = token.split(".")[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    return json.loads(base64.urlsafe_b64decode(payload_b64))

def jwt_expired(token: str) -> bool:
    """
    Check if a JWT token is expired based on its "exp" claim.
    Returns True if expired, False otherwise.
    Assumes token is in the format header.payload.signature
    
    Args: 
      token (str): JWT token
    Returns:
      bool: True if expired, False otherwise
    """
    try:
        payload_b64 = token.split(".")[1]
        # pad base64
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = payload.get("exp")
        if not exp:
            return False
        exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
        print("Token exp (UTC):", exp_dt.isoformat())
        return datetime.now(timezone.utc) >= exp_dt
    except Exception as e:
        print("Could not decode token exp:", e)
        return False

def date_to_iso_z(dt: str | datetime) -> str:
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"

def _flatten_contact(contact: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    out["contact_id"] = contact.get("contactId")
    out["contact_account"] = contact.get("account")
    out["contact_name"] = contact.get("name")

    # tags sometimes is a list of ids
    tags = contact.get("tags", [])
    if isinstance(tags, list):
        # out["contact_tags"] = ",".join(str(t) for t in tags)
        out["contact_tags"] = json.dumps(tags, ensure_ascii=False) if isinstance(tags, list) else (str(tags) if tags is not None else "")
        # out["contact_tags"] = "|".join(str(t) for t in tags)

    else:
        out["contact_tags"] = str(tags) if tags is not None else ""

    # exclusiveAgents is a list of dicts
    ex = contact.get("exclusiveAgents", [])
    out["contact_exclusive_agents_count"] = len(ex) if isinstance(ex, list) else 0

    # Flatten FIRST exclusive agent (common case)
    first_ex = ex[0] if isinstance(ex, list) and ex else {}
    out["contact_exclusive_agent_id"] = first_ex.get("agentId")
    out["contact_exclusive_campaign_id"] = first_ex.get("campaignId")
    out["contact_exclusive_agent_name"] = first_ex.get("agent")
    out["contact_exclusive_campaign_name"] = first_ex.get("campaign")

    # Keep full exclusiveAgents as JSON (optional but useful)
    out["contact_exclusive_agents_json"] = json.dumps(ex, ensure_ascii=False) if isinstance(ex, list) else ""

    return out

def infer_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("content"), list):
            return payload["content"]
        if isinstance(payload.get("items"), list):
            return payload["items"]
        for v in payload.values():
            if isinstance(v, list):
                return v
    return []

def has_more(payload: Any, page_idx: int, page_size: int, got_count: int) -> bool:
    if isinstance(payload, dict):
        if isinstance(payload.get("last"), bool):
            return not payload["last"]
        if isinstance(payload.get("totalPages"), int) and isinstance(payload.get("number"), int):
            return (page_idx + 1) < payload["totalPages"]
    # fallback if server doesn't tell us
    return got_count == page_size

def flatten_conversation_rows(conversations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for conv in conversations:
        row = dict(conv)

        contacts = row.pop("contacts", [])
        row["contacts_count"] = len(contacts) if isinstance(contacts, list) else 0
        first_contact = contacts[0] if isinstance(contacts, list) and contacts else {}
        row.update(_flatten_contact(first_contact))

        if "tags" in row and isinstance(row["tags"], (list, dict)):
            row["tags"] = json.dumps(row["tags"], ensure_ascii=False)

        for k, v in list(row.items()):
            if isinstance(v, (list, dict)):
                row[k] = json.dumps(v, ensure_ascii=False)

        rows.append(row)
    return rows

# def append_rows_csv(file_path: str, rows: List[Dict[str, Any]], fieldnames: List[str], write_header: bool) -> None:
#     with open(file_path, "a", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
#         if write_header:
#             writer.writeheader()
#         writer.writerows(rows)
def append_rows_csv(file_path: str, rows: List[Dict[str, Any]], fieldnames: List[str], write_header: bool) -> None:
    import csv
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=HIBOT_COLUMNS,
            extrasaction="ignore",
            restval="",
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,   # quotes only when needed (commas, quotes, newlines)
            escapechar="\\",             # helps if weird characters appear
            doublequote=True
        )
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


CONNECT_TIMEOUT = 5
READ_TIMEOUT = 120

# async def resilient_get_json(
#     session: aiohttp.ClientSession,
#     url: str,
#     headers: Dict[str, str],
#     params: Dict[str, Any],
#     page: int,
# ) -> Any:
#     attempt = 0
#     while True:
#         try:
#             async with session.get(url, headers=headers, params=params) as resp:
#                 if resp.status in (429,) or 500 <= resp.status < 600:
#                     wait = min(2 ** attempt, 60) + random.uniform(0, 0.5)
#                     print(f"HTTP {resp.status} on page {page}, retrying in {wait:.1f}s...")
#                     await asyncio.sleep(wait)
#                     attempt += 1
#                     continue

#                 resp.raise_for_status()
#                 return await resp.json()

#         except (asyncio.TimeoutError, aiohttp.ClientConnectionError):
#             wait = min(2 ** attempt, 60) + random.uniform(0, 0.5)
#             print(f"Request Timeout/connection error on page {page} (attempt {attempt+1}), retrying in {wait:.1f}s...")
#             await asyncio.sleep(wait)
#             attempt += 1
#             continue

#         except aiohttp.ClientResponseError as e:
#             # For non-retryable 4xx (except 429), usually fail fast
#             if 400 <= e.status < 500 and e.status != 429:
#                 raise
#             wait = min(2 ** attempt, 60) + random.uniform(0, 0.5)
#             print(f"HTTP error {e.status} on page {page}, retrying in {wait:.1f}s...")
#             await asyncio.sleep(wait)
#             attempt += 1
#             continue

async def resilient_request_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    headers: Dict[str, str],
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    page: int = 0,
) -> Any:
    attempt = 0
    while True:
        try:
            async with session.request(method, url, headers=headers, params=params, json=json_body) as resp:
                # Helpful debug if it fails
                if resp.status >= 400:
                    text = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status} {resp.url}\n{text}")

                if resp.status in (429,) or 500 <= resp.status < 600:
                    wait = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                    print(f"HTTP {resp.status} on page {page}, retrying in {wait:.1f}s... {url}\n{text[:500]}")
                    await asyncio.sleep(wait)
                    attempt += 1
                    continue

                resp.raise_for_status()
                return await resp.json()

        except (asyncio.TimeoutError, aiohttp.ClientConnectionError):
            wait = min(2 ** attempt, 60) + random.uniform(0, 0.5)
            print(f"Timeout/connection error on page {page} (attempt {attempt+1}), retrying in {wait:.1f}s...")
            await asyncio.sleep(wait)
            attempt += 1
            continue

        except aiohttp.ClientResponseError as e:
            if 400 <= e.status < 500 and e.status != 429:
                # include body snippet if we grabbed it
                raise
            wait = min(2 ** attempt, 60) + random.uniform(0, 0.5)
            print(f"HTTP error {e.status} on page {page}, retrying in {wait:.1f}s...")
            await asyncio.sleep(wait)
            attempt += 1
            continue

# ---------- Functions ----------
# async def fetch_conversations_page(
#     session: aiohttp.ClientSession,
#     base_url: str,
#     core_reports_path: str,
#     token: str,
#     zone_id: str,
#     start_iso_z: str,
#     end_iso_z: str,
#     page: int,
#     size: int,
#     time_unit: str = "seconds",
# ) -> Tuple[int, List[Dict[str, Any]], Dict[str, Any]]:
#     range_value = f"{start_iso_z},{end_iso_z}"
#     url = join_url(base_url, join_url(core_reports_path, "reportauditory"))

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Accept": "application/json",
#         "zoneid": zone_id,
#         "Origin": "https://pdn-interactions.hibot.us",
#         "Referer": "https://pdn-interactions.hibot.us/",
#         "User-Agent": "Mozilla/5.0",
#     }

#     params = {
#         "startDate": range_value,
#         "dateRange": range_value,
#         "timeUnit": time_unit,
#         "page": str(page),
#         "size": str(size),
#         "sort": "",
#     }

#     data = await resilient_get_json(session, url, headers, params, page)
#     items = infer_items(data)  # will use data["content"]
#     return page, items, data

# async def fetch_conversations_page(
#     session: aiohttp.ClientSession,
#     base_url: str,
#     core_reports_path: str,
#     token: str,
#     zone_id: str,
#     start_iso_z: str,
#     end_iso_z: str,
#     page: int,
#     size: int,
#     time_unit: str = "seconds",
# ) -> Tuple[int, List[Dict[str, Any]], Dict[str, Any]]:

#     url = join_url(base_url, join_url(core_reports_path, "reportauditory/search"))

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Accept": "application/json",
#         "Content-Type": "application/json",
#         "zoneid": zone_id,
#         "Origin": "https://pdn-interactions.hibot.us",
#         "Referer": "https://pdn-interactions.hibot.us/",
#         "User-Agent": "Mozilla/5.0",
#         # If the UI sends tenant as a header and the API requires it, add it:
#         # "tenant": tenant_id,
#     }

#     # IMPORTANT: this is the part that must match the UI's REQUEST PAYLOAD.
#     # Since you didn't paste the UI request body, we’ll use the most common schema.
#     # If this returns 400, copy the DevTools "Payload" and we’ll mirror it exactly.
#     body = {
#         "dateRange": {
#             "startDate": start_iso_z,
#             "endDate": end_iso_z,
#         },
#         "timeUnit": time_unit,
#         "page": page,
#         "size": size,
#         "sort": [{"field": "created", "direction": "DESC"}],
#     }

#     data = await resilient_request_json(
#         session,
#         "POST",
#         url,
#         headers,
#         json_body=body,
#         page=page,
#     )

#     items = infer_items(data)  # still works (data["content"])
#     return page, items, data

# async def fetch_conversations_page(
#     session: aiohttp.ClientSession,
#     base_url: str,
#     core_reports_path: str,
#     token: str,
#     zone_id: str,
#     tenant_id: str,
#     start_iso_z: str,
#     end_iso_z: str,
#     page: int,
#     size: int,
#     time_unit: str = "seconds",
# ) -> Tuple[int, List[Dict[str, Any]], Dict[str, Any]]:

#     url = join_url(base_url, join_url(core_reports_path, "reportauditory/search"))

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Accept": "application/json",
#         "Content-Type": "application/json",
#         "zoneid": zone_id,
#         "tenant": tenant_id,
#         "Origin": "https://pdn-interactions.hibot.us",
#         "Referer": "https://pdn-interactions.hibot.us/",
#         "User-Agent": "Mozilla/5.0",
#     }
    
#     range_value = f"{start_iso_z},{end_iso_z}"
    
#     params = {  # keep if backend still reads query
#     "startDate": range_value,
#     # "startDate": start_iso_z,
#     # "endDate": end_iso_z,
#     "dateRange": range_value,
#     "timeUnit": time_unit,
#     "tenant": tenant_id,
#     "page": str(page),
#     "size": str(size),
#     "sort": "createdDate",
#     }
    
#     # body = {
#     #     "startDate": range_value,
#     #     "dateRange": range_value,
#     #     "timeUnit": time_unit,
#     #     "page": page,
#     #     "size": size,
#     #     "sort": "createdDate",
#     # }
    
#     body = {  # send body too (this is likely required)
#         "startDate": start_iso_z,
#         "endDate": end_iso_z,
#         "timeUnit": time_unit,
#         "tenant": tenant_id,
#         "page": page,
#         "size": size,
#         "sort": "createdDate",
#     }

#     data = await resilient_request_json(
#         session,
#         "POST",
#         url,
#         headers,
#         params=params,
#         json_body=body,
#         page=page,
#     )
#     items = infer_items(data)
#     return page, items, data

async def fetch_conversations_page(
    session: aiohttp.ClientSession,
    base_url: str,
    core_reports_path: str,
    token: str,
    zone_id: str,
    tenant_id: str,
    start_iso_z: str,
    end_iso_z: str,
    page: int,
    size: int,
    time_unit: str = "seconds",
) -> Tuple[int, List[Dict[str, Any]], Dict[str, Any]]:

    url = join_url(base_url, join_url(core_reports_path, "reportauditory/search"))

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "zoneid": zone_id,
        "tenant": tenant_id,
        "Origin": "https://pdn-interactions.hibot.us",
        "Referer": "https://pdn-interactions.hibot.us/",
        "User-Agent": "Mozilla/5.0",
    }

    # keep query params minimal (or omit entirely)
    params = {
        "tenant": tenant_id,
        "page": str(page),
        "size": str(size),
    }

    body = {
        "dateRange": {"startDate": start_iso_z, "endDate": end_iso_z},
        "timeUnit": time_unit,
        "page": page,
        "size": size,
        "sort": "",              # IMPORTANT: UI uses "" not "createdDate"
        "filters": [],
        "dynamicFields": [],
    }

    data = await resilient_request_json(
        session,
        "POST",
        url,
        headers,
        params=params,
        json_body=body,
        page=page,
    )

    items = infer_items(data)
    return page, items, data


async def fetch_all_conversations_async_to_csv(
    base_url: str,
    core_reports_path: str,
    tenant_id: str,
    token: str,
    zone_id: str,
    start_iso_z: str,
    end_iso_z: str,
    directory: str,
    page_size: int = 50,
    concurrency: int = 8,
    batch_write_every: int = 5,
    time_unit: str = "seconds",
    start_page: int = 0,
) -> None:
    # reset file
    if os.path.exists(directory):
        os.remove(directory)

    timeout = ClientTimeout(sock_connect=CONNECT_TIMEOUT, sock_read=READ_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=concurrency * 2, force_close=True)

    next_page = start_page
    stop_page: Optional[int] = None  # once we find the first empty/last page
    lock = asyncio.Lock()

    gathered_rows: List[Dict[str, Any]] = []
    wrote_header = False
    fieldnames: Optional[List[str]] = None
    pages_done = 0

    async def worker(worker_id: int, session: aiohttp.ClientSession):
        nonlocal next_page, stop_page, gathered_rows, wrote_header, fieldnames, pages_done

        while True:
            async with lock:
                # If we already discovered stop_page, don't schedule beyond it
                if stop_page is not None and next_page > stop_page:
                    return
                page = next_page
                next_page += 1

            p, items, data = await fetch_conversations_page(
                session=session,
                base_url=base_url,
                core_reports_path=core_reports_path,
                token=token,
                zone_id=zone_id,
                tenant_id=tenant_id,
                start_iso_z=start_iso_z,
                end_iso_z=end_iso_z,
                page=page,
                size=page_size,
                time_unit=time_unit,
            )

            got = len(items)
            print(f"[w{worker_id}] page {p}: {got} items")

            # Detect stop condition
            more = has_more(data, p, page_size, got)
            if got == 0 or not more:
                async with lock:
                    # stop_page should be the FIRST page that indicates stopping
                    if stop_page is None or p < stop_page:
                        stop_page = p

            if got:
                rows = flatten_conversation_rows(items)
                async with lock:
                    gathered_rows.extend(rows)

                    # initialize fieldnames once (from first non-empty batch)
                    # if fieldnames is None:
                    #     fieldnames = sorted({k for r in gathered_rows for k in r.keys()})
                    fieldnames = HIBOT_COLUMNS

            async with lock:
                pages_done += 1
                should_flush = (pages_done % batch_write_every == 0) or (stop_page is not None and page >= stop_page)
                if should_flush and gathered_rows:
                    # # ensure fieldnames includes any new keys seen in gathered_rows
                    # if fieldnames is None:
                    #     fieldnames = sorted({k for r in gathered_rows for k in r.keys()})
                    # else:
                    #     new_keys = {k for r in gathered_rows for k in r.keys()}
                    #     if not set(fieldnames).issuperset(new_keys):
                    #         fieldnames = sorted(set(fieldnames).union(new_keys))

                    # append_rows_csv(directory, gathered_rows, fieldnames, write_header=(not wrote_header))
                    # append_rows_csv(directory, gathered_rows, write_header=(not wrote_header))
                    append_rows_csv(directory, gathered_rows, HIBOT_COLUMNS, write_header=(not wrote_header))

                    wrote_header = True
                    gathered_rows = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        start_time = time.time()
        workers = [asyncio.create_task(worker(i + 1, session)) for i in range(concurrency)]
        await asyncio.gather(*workers)

        # final flush
        # if gathered_rows and fieldnames is not None:
        if gathered_rows:
            # append_rows_csv(directory, gathered_rows, fieldnames, write_header=(not wrote_header))
            append_rows_csv(directory, gathered_rows, HIBOT_COLUMNS, write_header=(not wrote_header))


        print(f"Done. CSV: {directory}")
        print(f"⏱ Total time: {time.time() - start_time:.2f}s")

# ---------- Main execution ----------
def main():
    # Get current directory
    here = Path(__file__).parent
    print("Path:", here)
    
    directory = "CSV/filtered_hibot_export.csv"

    # Load Postman collection and environment exports
    pm_coll_vals = load_postman_collection_variables(here / "JSON/collection_hibot.json")
    pm_env = load_postman_environment_values(here / "JSON/environment_hibot.json")
    
    # Extract relevant variables from environment 
    token = pm_env.get("HIBOT_API_TOKEN")
    base_url = pm_env.get("BASE_URL")
    core_reports_url = pm_env.get("CORE_REPORTS")
    api_interactions_endpoint = pm_env.get("API_INTERACTIONS")
    api_url = pm_env.get("API_URL")
    app_id = pm_env.get("APP_ID")
    secret_key = pm_env.get("CLAVE_SECRETA")
    
    # Check that environment variables are set
    if not token or not base_url or not core_reports_url:
        raise RuntimeError("Missing required env vars: HIBOT_API_TOKEN, BASE_URL, CORE_REPORTS")
    
    info = jwt_info(token)
    print("Token aud:", info.get("aud"))
    print("Token iss:", info.get("iss"))
    print("Token exp:", datetime.fromtimestamp(info.get("exp", 0), tz=timezone.utc))
    print("Token iat:", datetime.fromtimestamp(info.get("iat", 0), tz=timezone.utc))
    print()
    
    # Check if token is expired
    if jwt_expired(token):
        raise RuntimeError("Your token is expired. Get a fresh HIBOT_API_TOKEN from the app/Postman.")

    # Extract relevant variables from collection 
    client_id = pm_coll_vals.get("client_id")
    project_id = pm_coll_vals.get("project_id")
    zone_id = pm_coll_vals.get("zone_id")
    tenant_id = pm_coll_vals.get("tenant_id")
    user_id = pm_coll_vals.get("user_id")
    
    # Print configurations
    print("----HiBot API Environment----")
    print(f"Token: {token[:20]}, length={len(token)}")
    print("Base URL:", base_url)
    print("Core Reports URL:", core_reports_url)
    print("API Interactions Endpoint:", api_interactions_endpoint)
    print("API URL:", api_url)
    print("App ID:", app_id)
    print(f"Secret Key: {secret_key[:10]}, length={len(secret_key)}")
    print("-----------------------------")
    print("----HiBot API Collection Variables----")
    print("Client ID:", client_id[:5])
    print("Zone ID:", zone_id)
    print("Project ID:", project_id[:5])
    print("Tenant ID:", tenant_id[:5])
    print("User ID:", user_id[:5])
    print("------------------------------")
    
    
    ap = argparse.ArgumentParser()
    # python3 get_hibot_data.py --from "2026-01-14 00:00:00" --to "2026-12-31 23:59:59"
    ap.add_argument("--from", dest="fecha_inicio", required=True, help="YYYY-MM-DD HH:MM:SS")
    ap.add_argument("--to", dest="fecha_fin", required=True, help="YYYY-MM-DD HH:MM:SS")
    args = ap.parse_args()
    
    start_date = args.fecha_inicio 
    end_date = args.fecha_fin 
    print("Start date:", start_date)
    print("End date:", end_date)
    
    # start_date = date_to_iso_z(datetime(2025, 12, 10, 0, 0, 0))  # "2025-12-10 00:00:00"
    # end_date = date_to_iso_z(datetime(2025, 12, 16, 23, 59, 59)) # "2025-12-16 23:59:59"
    start_date = date_to_iso_z(start_date)  # "2025-12-10 00:00:00"
    end_date = date_to_iso_z(end_date) # "2025-12-16 23:59:59"
    print("Start date (ISO Z):", start_date)
    print("End date (ISO Z):", end_date)
    
    # Fetch HiBot conversations until no more pages are left
    asyncio.run(
    fetch_all_conversations_async_to_csv(
        base_url=base_url,
        core_reports_path=core_reports_url,
        tenant_id=tenant_id,
        token=token,
        zone_id=zone_id,
        start_iso_z=start_date,
        end_iso_z=end_date,
        directory=directory,
        page_size=50,
        concurrency=12,         # tune this (start 4–8)
        batch_write_every=5,   # write every 5 completed pages
        )
    )
    
    # Get conversations for each unique contact_id using pandas and resave
    df = pd.read_csv(directory)
    df = df[df["contact_id"].notna()]
    df = df.drop_duplicates(subset=["contact_id"], keep="first").reset_index(drop=True)
    df.to_csv(directory, index=False)
       
    print("----Done fetching HiBot conversations----")
    print("------------------------------")

if __name__ == "__main__":
    import os
    main()