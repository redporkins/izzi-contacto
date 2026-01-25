SELECT  
	vr.Costo,
	vr.NoRGU,
	vr.CreatorUserId,
	vr.CreationTime
-- SUM(vr.Costo) AS MontoVentas,
-- SUM(vr.NoRGU) AS CantidadRGU,
-- COUNT(vr.CreatorUserId) AS RecuentoVentas
FROM   OnecontactDb.Venta.VentasRegistradas vr 
WHERE 
	vr.CreationTime BETWEEN '2026-01-01' AND '2026-12-31'
	AND vr.VentaOrigen = 3
    ORDER BY vr.CreationTime DESC; 
	