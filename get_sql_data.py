import argparse
from datetime import date
import pymssql
import csv
from dotenv import load_dotenv

def fetch_rows(conn, fecha_inicio: str, fecha_fin: str, estados: list[str]) -> list[dict]:
    """
    Fetch rows from the database based on date range and estados filter.
    Args:
        conn: pymssql connection object
        fecha_inicio (str): Start date in 'YYYY-MM-DD' format
        fecha_fin (str): End date in 'YYYY-MM-DD' format
        estados (list[str]): List of estados to filter on
    Returns:
        list of dict: Fetched rows
    """
    
    estados = [e for e in estados if e]  # drop empty
    has_estados = 1 if estados else 0

    in_clause = ", ".join(["%s"] * len(estados)) if estados else "NULL"
    sql = f"""
    SELECT 
        TRY_CAST(VR.NoContrato AS NUMERIC(18,0)) AS NoContrato,
        VR.CreationTime AS FechaDeCreacionPakoa,
        VR.UnidadPresupuesto,
        VR.Comentarios,
        VR.NoRGU,
        VR.ComentariosCancelacion,
        VR.NoRGU,
        VS.Descripcion,
        VS.Tipo,
        VR.Nombre, 
        VR.IdConversacion,
        VR.Sipre,
        VR.DeleoMuni, 
        VE.Name AS Estado,
        VR.CodigoPostal, 
        VR.Colonia, 
        VR.Costo,
        VR.Email,
        CASE 
            WHEN LEN(VR.Telefono) = 10 AND VR.Telefono NOT LIKE '%[^0-9]%' 
                THEN TRY_CAST(VR.Telefono AS NUMERIC(18,0))
            ELSE NULL
        END AS Telefono,
        TRY_CAST(VR.Telefono2 AS NUMERIC(18,0)) AS Telefono2,
        TRY_CAST(VR.TelefonoAtiende AS NUMERIC(18,0)) AS TelefonoAtiende,
        TRY_CAST(NM.fechaCierre AS DATE)     AS FechaDeInstalacion,
        TRY_CAST(NM.FechaGenerada AS DATE)   AS FechaCreacionOC,
        NM.Estatus                            AS EstadoOrden,
        CC.Nombre                             AS EstatusConfirmacionPakoa
    FROM OneContactDb.Venta.VentasRegistradas VR
    LEFT JOIN ArchivosIZZI.dbo.NM_BaseNacional NM 
        ON VR.NoContrato = NM.Contrato
    LEFT JOIN OneContactDb.Venta.VentasConfirmacion VC 
        ON VR.Id = VC.IdVenta
    LEFT JOIN OneContactDb.Venta.Servicio VS
        ON VR.IdProducto = VS.Id
    LEFT JOIN OneContactDb.Catalogo.Confirmacion CC 
        ON VC.IdConfirmacion = CC.Id
    LEFT JOIN OneContactDb.Catalogo.Estado VE
        ON VR.IdEstado = VE.Id
    WHERE 
        VR.NoContrato IS NOT NULL
        AND CAST(VR.CreationTime AS DATE) BETWEEN %s AND %s
        AND (
            %s = 0
            OR NM.Estatus IN ({in_clause})
        );
    """

    params = [fecha_inicio, fecha_fin, has_estados] + estados
    cur = conn.cursor(as_dict=True)
    cur.execute(sql, params)
    return cur.fetchall()


def fetch_rows_to_csv(conn, fecha_inicio: str, fecha_fin: str, estados: list[str], directory: str) -> None:
    rows = fetch_rows(conn, fecha_inicio, fecha_fin, estados)
    print(f"Rows: {len(rows)}")
    with open(directory, "w", newline='', encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {directory}")
    
def main() -> None:
    load_dotenv()
    
    directory = "CSV/filtered_sql_sales_export.csv"

    conn = pymssql.connect(
        server=os.getenv("SCS_DB01_HOST"),
        user=os.getenv("SCS_DB01_USER"),
        password=os.getenv("SCS_DB01_PASSWORD"),
        database="OneContactDb",
    )
    
    ap = argparse.ArgumentParser()
    # python3 get_sql_data.py --from 2024-01-01 --to 2024-01-31 --estado CANCELADO --estado "NOT DONE"
    # If no --estado provided, fetch all estados
    # python3 get_sql_data.py --from 2024-01-01 --to 2024-01-31
    ap.add_argument("--from", dest="fecha_inicio", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="fecha_fin", required=True, help="YYYY-MM-DD")
    ap.add_argument("--estado", action="append", default=[], help="Repeatable. e.g. --estado CANCELADO --estado 'NOT DONE'")
    args = ap.parse_args()
    
    try:
        fetch_rows_to_csv(conn, args.fecha_inicio, args.fecha_fin, args.estado, directory)
        # do whatever: write CSV, etc.
    finally:
        conn.close()

if __name__ == "__main__":
    import os
    main()
