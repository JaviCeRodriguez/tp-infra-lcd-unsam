-- Fact table venta
SELECT * FROM usm-infra-grupo1.data_warehouse.fact_venta AS venta;

-- Fact table Stock
SELECT * FROM usm-infra-grupo1.data_warehouse.fact_stock AS stock;

-- Dim table Productos
SELECT * FROM usm-infra-grupo1.data_warehouse.dim_producto AS producto;

-- Data Mart Suministros
SELECT
	stock.stock_unidades,
	venta.venta_unidades,
	venta.fecha_cierre_comercial,
	producto.SKU_descripcion,
	stock.n_distribuidor
FROM
	usm-infra-grupo1.data_warehouse.fact_venta AS venta
	INNER JOIN usm-infra-grupo1.data_warehouse.fact_stock AS stock ON
		venta.fecha_cierre_comercial = stock.fecha_cierre_comercial AND
		venta.codigo_sucursal = stock.codigo_sucursal
	INNER JOIN usm-infra-grupo1.data_warehouse.dim_producto AS producto ON venta.SKU_codigo = producto.SKU_codigo;
