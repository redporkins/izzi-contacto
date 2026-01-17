import argparse
from datetime import date
import pymssql
import csv
import pandas as pd
from dataclasses import dataclass
from typing import Callable, Any, Optional
from dotenv import load_dotenv
import slugify

def fetch_rows(conn) -> list[dict]:
    """
    Fetch rows from the database based on date range and estados filter.
    Args:
        conn: pymssql connection object
    Returns:
        list of dict: Fetched rows
    """
    
    sql = f"""
        SELECT 
        * 
        FROM OneContactDb.Venta.servicio
        WHERE 
            Activo = 1;
    """
    
    cur = conn.cursor(as_dict=True)
    cur.execute(sql)
    return cur.fetchall()

# ------------------------------------------------------------------------------------

@dataclass(frozen=True)
class FieldSpec:
    required: bool
    mapper: Optional[Callable[[dict], Any]] = None
    default: Any = ""

def as_bool(v) -> bool:
    # handles True/False, "True"/"False", 1/0
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y")
    return False

def money_mxn(value) -> str:
    if value in (None, ""):
        return ""
    return f"{float(value):.2f} MXN"

def safe_str(v, default="") -> str:
    if v in (None, ""):
        return default
    return str(v)

def build_title(row: dict) -> str:
    # Keep <= 200 chars
    # return f"{safe_str(row.get('Tipo'))} - {safe_str(row.get('Descripcion'))}"[:200]
    return f"{safe_str(row.get('Descripcion'))}"[:200]

def build_description(row: dict) -> str:
    parts = [
        f"Tipo: {safe_str(row.get('Tipo'))}",
        # f"Servicio: {safe_str(row.get('Descripcion'))}",
    ]
    if row.get("NoRGU") not in (None, "") and row.get("NoRGU") == 1:
        parts.append(f"Incluye ({safe_str(row.get('NoRGU'))}): TV")
    elif row.get("NoRGU") not in (None, "") and row.get("NoRGU") == 2:
        parts.append(f"Incluye ({safe_str(row.get('NoRGU'))}): Internet y Telefonía")
    elif row.get("NoRGU") not in (None, "") and row.get("NoRGU") == 3:
        parts.append(f"Incluye ({safe_str(row.get('NoRGU'))}): Internet, Telefonía y TV")
    return " | ".join(parts)[:9999]

BASE_PRODUCT_URL = "https://redporkins.github.io/izzi-contacto/"
BASE_IMAGE_URL = "https://res.cloudinary.com/dl5h1i0up/image/upload/w_500,h_500,c_fill/"

def product_link(row: dict) -> str:
    return (
        f"{BASE_PRODUCT_URL}"
        f"?producto={slugify.slugify(row['Descripcion'])}"
        f"&tipo={row['Tipo'].lower()}"
        f"&id={row['Id']}"
    )

def image_link(row: dict) -> str:
    # Internet: https://res.cloudinary.com/dl5h1i0up/image/upload/v1768320093/Internet_q8nhk2.jpg
    # Internet y telefonía: https://res.cloudinary.com/dl5h1i0up/image/upload/v1768320093/Internet_telefonia_g5i4nx.jpg
    # Internet, telefonía y TV: https://res.cloudinary.com/dl5h1i0up/image/upload/v1768320093/Internet_telefonia_tv_dhueff.png
    
    if row.get("NoRGU") not in (None, "") and row.get("NoRGU") == 1:
        return f"{BASE_IMAGE_URL}v1768320093/Internet_q8nhk2.jpg"
    elif row.get("NoRGU") not in (None, "") and row.get("NoRGU") == 2:
        return f"{BASE_IMAGE_URL}v1768320093/Internet_telefonia_g5i4nx.jpg"
    elif row.get("NoRGU") not in (None, "") and row.get("NoRGU") == 3:
        return f"{BASE_IMAGE_URL}v1768320093/Internet_telefonia_tv_dhueff.png"
    else: 
        return None
    
# REGIONS = {
#     "NORTE": {
#         "BAJA CALIFORNIA", "BAJA CALIFORNIA SUR", "SONORA", "CHIHUAHUA",
#         "COAHUILA", "NUEVO LEON", "TAMAULIPAS", "DURANGO", "SINALOA"
#     },
#     "CENTRO": {
#         "AGUASCALIENTES", "GUANAJUATO", "QUERETARO", "SAN LUIS POTOSI",
#         "JALISCO", "MICHOACAN", "ZACATECAS", "COLIMA", "NAYARIT"
#     },
#     "VALLE_MX": {
#         "CIUDAD DE MEXICO", "ESTADO DE MEXICO", "HIDALGO", "MORELOS", "TLAXCALA", "PUEBLA"
#     },
#     "SUR": {
#         "VERACRUZ", "GUERRERO", "OAXACA", "CHIAPAS", "TABASCO"
#     },
#     "PENINSULA": {
#         "CAMPECHE", "YUCATAN", "QUINTANA ROO"
#     }
# }

# def build_region_zip_strings(zips_csv_path: str) -> dict[str, str]:
#     z = pd.read_csv(zips_csv_path)

#     # normalize columns
#     z["State"] = z["State"].astype(str).str.upper().str.strip()
#     z["Zip Code"] = z["Zip Code"].astype(str).str.extract(r"(\d{5})", expand=False).str.zfill(5)

#     region_to_cps: dict[str, str] = {}
#     for region, states in REGIONS.items():
#         cps = z[z["State"].isin(states)]["Zip Code"].dropna().unique().tolist()
#         cps = sorted(set(cps))
#         region_to_cps[region] = "|".join(cps)

#     return region_to_cps
        
# def availability_postal_codes(row: dict) -> str:
#     # Read the postal codes from the database or another source
#     zips = pd.read_csv("CSV/states_municipalities_zips.csv")

#     availability_postal_codes = "|".join(
#         sorted(set(zips["Zip Code"].astype(str).str.zfill(5)))
#     )
#     return availability_postal_codes

FB_CATALOG_SCHEMA: dict[str, FieldSpec] = {
    # REQUIRED
    "id": FieldSpec(True,  mapper=lambda r: str(r["Id"])[:100]),
    "title": FieldSpec(True, mapper=build_title),
    "description": FieldSpec(True, mapper=build_description),
    "availability": FieldSpec(True, mapper=lambda r: "in stock" if as_bool(r.get("Activo")) else "out of stock"),
    "condition": FieldSpec(True, mapper=lambda r: "new"),
    "price": FieldSpec(True, mapper=lambda r: money_mxn(r.get("Monto"))),
    "link": FieldSpec(True, mapper=product_link),
    "image_link": FieldSpec(True, mapper=image_link),
    "brand": FieldSpec(True, mapper=lambda r: "izzi"),
    
    # LOCALITY via circle (nationwide)
    "availability_circle_origin.latitude": FieldSpec(True, default="19.4326"),
    "availability_circle_origin.longitude": FieldSpec(True, default="-99.1332"),
    "availability_circle_radius_unit": FieldSpec(True, default="km"),
    "availability_circle_radius": FieldSpec(True, default="2000"),



    # OPTIONAL (you can populate if you want)
    "google_product_category": FieldSpec(False, mapper=lambda r: "Electronics > Communications > Telephony > Phone Services"),
    "fb_product_category": FieldSpec(False, mapper=lambda r: "other"),
    "quantity_to_sell_on_facebook": FieldSpec(False),
    # "sale_price": FieldSpec(False, mapper=lambda r: money_mxn(r.get("MontoAnterior"))),  # if you treat old price as sale price (optional)
    "sale_price": FieldSpec(False),  # if you treat old price as sale price (optional)
    "sale_price_effective_date": FieldSpec(False),
    "item_group_id": FieldSpec(False, mapper=lambda r: str(r.get("IdTipoVenta") or "")),
    "gender": FieldSpec(False, mapper=lambda r: "unisex"),
    "color": FieldSpec(False),
    "size": FieldSpec(False),
    "age_group": FieldSpec(False, mapper=lambda r: "adult"),
    "material": FieldSpec(False),
    "pattern": FieldSpec(False),
    "shipping": FieldSpec(False),
    "shipping_weight": FieldSpec(False),
    "[video][0].url]": FieldSpec(False, mapper=lambda r: "https://www.youtube.com/watch?v=PywT6TtlR-g"),
    "[video][0].tag[0]": FieldSpec(False),
    "[gtin]": FieldSpec(False),
    "[product_tags][0]": FieldSpec(False, mapper=lambda r: safe_str(r.get("Tipo"))),
    "[product_tags][1]": FieldSpec(False, mapper=lambda r: safe_str(r.get("IdTipoVenta"))),
    "[style][0]": FieldSpec(False),
}

def fb_catalog_header() -> list[str]:
    return list(FB_CATALOG_SCHEMA.keys())

def format_fb_catalog_row(db_row: dict) -> dict:
    out = {}
    missing = []

    for field, spec in FB_CATALOG_SCHEMA.items():
        value = spec.default
        if spec.mapper:
            mapped = spec.mapper(db_row)
            if mapped not in (None, ""):
                value = mapped
        out[field] = value

        if spec.required and out[field] in (None, ""):
            missing.append(field)

    if missing:
        raise ValueError(f"Missing required fields {missing} for Id={db_row.get('Id')}")
    return out

# ------------------------------------------------------------------------------------

# def fetch_rows_to_csv(conn, directory: str) -> None:
#     rows = fetch_rows(conn)
#     print(f"Rows: {len(rows)}")
#     with open(directory, "w", newline='', encoding="utf-8") as f:
#         if rows:
#             writer = csv.DictWriter(f, fieldnames=rows[0].keys())
#             writer.writeheader()
#             writer.writerows(rows)
#     print(f"Wrote {len(rows)} rows to {directory}")

def fetch_rows_to_csv(conn, directory: str) -> None:
    raw_rows = fetch_rows(conn)
    print(f"Raw rows: {len(raw_rows)}")

    if not raw_rows:
        print("No rows to export")
        return

    # 🔹 Convert SQL rows → Facebook catalog rows
    formatted_rows = []
    for r in raw_rows:
        try:
            formatted_rows.append(format_fb_catalog_row(r))
        except ValueError as e:
            # Optional: log and skip bad rows
            print(f"Skipping row Id={r.get('Id')}: {e}")

    if not formatted_rows:
        print("No valid Facebook catalog rows generated")
        return

    with open(directory, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fb_catalog_header()
        )
        writer.writeheader()
        writer.writerows(formatted_rows)

    print(f"Wrote {len(formatted_rows)} Facebook catalog rows to {directory}")

# def fetch_rows_to_csv(conn, directory: str) -> None:
#     raw_rows = fetch_rows(conn)
#     print(f"Raw rows: {len(raw_rows)}")
#     if not raw_rows:
#         print("No rows to export")
#         return

#     # Build region CP lists ONCE
#     region_zip = build_region_zip_strings("CSV/states_municipalities_zips.csv")

#     formatted_rows = []
#     for r in raw_rows:
#         for region, cps in region_zip.items():
#             row_out = format_fb_catalog_row(r)

#             # ✅ make row unique per region
#             row_out["id"] = f"{row_out['id']}_{region}"
#             row_out["title"] = f"{row_out['title']} - {region}"

#             # ✅ locality (smaller string per row)
#             row_out["availability_postal_codes"] = cps

#             formatted_rows.append(row_out)

#     with open(directory, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=fb_catalog_header())
#         writer.writeheader()
#         writer.writerows(formatted_rows)

#     print(f"Wrote {len(formatted_rows)} Facebook catalog rows to {directory}")

    
def main() -> None:
    load_dotenv()
    
    directory = "CSV/filtered_sql_catalog_export.csv"

    conn = pymssql.connect(
        server=os.getenv("SCS_DB01_HOST"),
        user=os.getenv("SCS_DB01_USER"),
        password=os.getenv("SCS_DB01_PASSWORD"),
        database="OneContactDb",
    )
    
    try:
        fetch_rows_to_csv(conn, directory)
        # do whatever: write CSV, etc.
    finally:
        conn.close()

if __name__ == "__main__":
    import os
    main()
