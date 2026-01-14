DECLARE @FechaInicio DATE = '2026-01-01';
DECLARE @FechaFin    DATE = '2026-12-31';
-- Para una sola fecha, pon @FechaInicio = @FechaFin, por ejemplo:
-- DECLARE @FechaInicio DATE = '2025-11-15';
-- DECLARE @FechaFin    DATE = '2025-11-15';

-- Agrega más variables si las necesitas

SELECT 
    TRY_CAST(VR.NoContrato AS NUMERIC(18,0)) AS NoContrato,
    VR.CreationTime AS FechaDeCreacionPakoa,
    VR.UnidadPresupuesto,
    VR.Comentarios,
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
    -- NoContrato: usa la columna original, el alias del SELECT no sirve en el WHERE
    VR.NoContrato IS NOT NULL
    -- Rango de fechas (cast a DATE para ignorar la hora)
    AND CAST(VR.CreationTime AS DATE) BETWEEN @FechaInicio AND @FechaFin;
