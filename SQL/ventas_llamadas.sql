SELECT  
	* 
	-- vr.Costo,
	-- vr.NoRGU,
	-- vr.CreatorUserId,
	-- vr.CreationTime
	-- SUM(vr.Costo) AS MontoVentas,
	-- AVG(vr.Costo) AS PromedioVenta,
	-- SUM(vr.CostoComplemento) AS MontoComplemento,
	-- AVG(vr.CostoComplemento) AS PromedioComplemento,
	-- SUM(vr.NoRGU) AS CantidadRGU,
	-- AVG(vr.NoRGU) AS PromedioRGU,
	-- COUNT(vr.Id) AS CantidadVentas,
	-- COUNT(vr.CreatorUserId) AS RecuentoVentas
FROM   OnecontactDb.Venta.VentasRegistradas vr 
WHERE 
	vr.CreationTime BETWEEN '2026-01-01' AND '2026-12-31'
	-- AND vr.VentaOrigen = 3
    -- ORDER BY vr.CreationTime DESC; 
	