import argparse
from datetime import date
import pymssql
import csv
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

# ---- IMPORTANT: set these to your real URLs/patterns ----
# BASE_PRODUCT_URL = "https://tusitio.com/servicios"   # <-- change
# BASE_IMAGE_URL = "https://tusitio.com/images"        # <-- change (or use a static image)

# def product_link(row: dict) -> str:
#     # Example: https://tusitio.com/servicios/16
#     return f"{BASE_PRODUCT_URL}/{row['Id']}"

# def image_link(row: dict) -> str:
#     # Option A: 1 generic image per Tipo
#     tipo = safe_str(row.get("Tipo")).strip().lower()
#     if "negocio" in tipo:
#         return f"{BASE_IMAGE_URL}/izzi-negocios.png"
#     return f"{BASE_IMAGE_URL}/izzi-residencial.png"

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
    # if row.get("NoRGU") not in (None, ""):
    #     parts.append(f"RGUs: {safe_str(row.get('NoRGU'))}")
    # if row.get("ComisionNeta") not in (None, ""):
    #     parts.append(f"Comisión neta estimada: {money_mxn(row.get('ComisionNeta')).replace(' MXN','')} MXN")
    # if row.get("MontoAnterior") not in (None, ""):
    #     parts.append(f"Precio anterior: {money_mxn(row.get('MontoAnterior'))}")
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
    "[video][0].url]": FieldSpec(False),
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
