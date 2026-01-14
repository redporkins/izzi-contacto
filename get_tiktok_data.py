import requests
import pandas as pd
import argparse
from datetime import date
from dotenv import load_dotenv
import os

def fetch_tiktok_data(base_url: str, parameters: dict, directory: str) -> None:
    """
    Fetch TikTok data from a Google Sheets document and save it as a CSV file.
    1. Constructs the URL for exporting the Google Sheet as a CSV.
    2. Sends a GET request to the URL.
    3. Saves the content of the response to a file named 'sheet_export.csv'.
    4. Prints a confirmation message upon successful save.
    5. Raises an exception if the request fails.
    6. Uses a timeout of 30 seconds for the request.
    Args:
       - base_url (str): The base URL of the Google Sheets document.
       - parameters (dict): A dictionary containing 'spreadsheet_id' and 'gid'.
         - directory (str): The directory where the CSV file will be saved.
    Returns: None
    """
    url = f"{base_url}/d/{parameters['spreadsheet_id']}/export?format=csv&gid={parameters['gid']}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    with open(directory, "wb") as f:
        f.write(r.content)

    print(f"Saved {directory}")

def fetch_tiktok_rows(fecha_inicio: str, fecha_fin: str, directory: str, directory1: str) -> None:
    """
    Fetch TikTok data from a Google Sheets document and save it as a CSV file.
    Filters the TikTok data based on a date range and saves the filtered data to a new CSV file.
    1. Reads the TikTok data from a CSV file into a pandas DataFrame.
    2. Cleans the DataFrame headers by removing any BOM characters and whitespace.
    3. Parses the "Creation time" column to datetime format.
    4. Filters the DataFrame to include only rows where "Creation time" falls within the specified date range.
    5. Cleans the "Phone number" column to retain only the last 10 digits.
    6. Sorts the filtered DataFrame by "Creation time" in ascending order.
    7. Saves the filtered DataFrame to a new CSV file.
    Args:
       - fecha_inicio (str): The start date for filtering (YYYY-MM-DD).
       - fecha_fin (str): The end date for filtering (YYYY-MM-DD). If not provided, defaults to today's date.
       - directory (str): The directory of the input CSV file.
       - directory1 (str): The directory where the filtered CSV file will be saved.
    Returns: None
    """
    if not fecha_inicio:
        print("Fecha inicio is required.")
        return

    if not fecha_fin:
        fecha_fin = date.today().strftime('%Y-%m-%d')
        print("Fecha fin not provided. Using today's date:", fecha_fin)

    df = pd.read_csv(directory, low_memory=False)

    # Clean headers (BOM/whitespace)
    df.columns = df.columns.str.replace("\ufeff", "").str.strip()

    col = "Creation time"
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found. Columns: {list(df.columns)}")

    # Parse: MM/DD/YYYY H:MM:SS
    df[col] = df[col].astype(str).str.strip()
    df[col] = pd.to_datetime(df[col], format="%m/%d/%Y %H:%M:%S", errors="coerce")
    
    start_dt = pd.to_datetime(fecha_inicio)
    end_dt = pd.to_datetime(fecha_fin)

    mask = df[col].between(start_dt, end_dt, inclusive="both")
    df_filtered = df.loc[mask].copy()

    # DEBUG: prove whether anything is out of range
    out_of_range = df_filtered[(df_filtered[col] < start_dt) | (df_filtered[col] > end_dt)]
    print("Filtered min/max:", df_filtered[col].min(), "->", df_filtered[col].max())
    print("Out-of-range rows in output:", len(out_of_range))
    if len(out_of_range) > 0:
        print(out_of_range[[col, "Name", "Lead ID"]].head(20))
        
    # Get only the the last 10 digits of the phone number and remove non-numeric characters
    df_filtered["Phone number"] = (
        df_filtered["Phone number"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str[-10:]
    )

    df_filtered = df_filtered.sort_values(by="Creation time", ascending=True)
    df_filtered.to_csv(directory1, index=False)
    print(f"Saved CSV/tiktok_data_filtered.csv with {len(df_filtered)} rows.")
   
def main() -> None:
    load_dotenv()
    base_url = os.getenv("BASE_URL")
    parameters = {
        "spreadsheet_id": os.getenv("SPREADSHEET_ID"),
        "gid": os.getenv("GID"),
    }
    directory = "CSV/tiktok_export.csv"
    directory1 = "CSV/filtered_tiktok_export.csv"
    
    fetch_tiktok_data(base_url, parameters, directory)
    
    ap = argparse.ArgumentParser()
    # python3 get_tiktok_data.py --from "2025-11-01 00:00:00" --to "2026-01-09 23:59:59"

    ap.add_argument("--from", dest="fecha_inicio", required=True, help="YYYY-MM-DD HH:MM:SS")
    # Make the 'to' argument optional
    ap.add_argument("--to", dest="fecha_fin", required=False, help="YYYY-MM-DD HH:MM:SS")
    args = ap.parse_args()
    
    start_date = args.fecha_inicio 
    end_date = args.fecha_fin 
    print("Start date:", start_date)
    print("End date:", end_date)

    fetch_tiktok_rows(start_date, end_date, directory, directory1)

if __name__ == "__main__":
    main()