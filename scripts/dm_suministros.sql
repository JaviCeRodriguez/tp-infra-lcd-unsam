-- Fact table venta
SELECT * FROM usm-infra-grupo1.data_warehouse.fact_venta AS venta;

-- Fact table Stock
SELECT * FROM usm-infra-grupo1.data_warehouse.fact_stock AS stock;

-- Dim table Productos
SELECT * FROM usm-infra-grupo1.data_warehouse.dim_producto AS producto;

-- Dim table Sucursal
SELECT * FROM usm-infra-grupo1.data_warehouse.dim_sucursal AS sucursal;

-- Data Mart Suministros
SELECT
	stock.codigo_sucursal,
	sucursal.provincia,
	stock.stock_unidades,
	stock.fecha_cierre_comercial,
	stock.SKU_descripcion,
	stock.n_distribuidor
FROM
	usm-infra-grupo1.data_warehouse.fact_stock AS stock
	INNER JOIN usm-infra-grupo1.data_warehouse.dim_sucursal AS sucursal ON stock.codigo_sucursal = sucursal.codigo_sucursal;