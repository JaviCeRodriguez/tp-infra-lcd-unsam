-- Fact table venta
SELECT * FROM usm-infra-grupo1.data_warehouse.fact_venta AS venta;

-- Dim table Cliente
SELECT * FROM usm-infra-grupo1.data_warehouse.dim_cliente AS cliente;

-- Dim table Productos
SELECT * FROM usm-infra-grupo1.data_warehouse.dim_producto AS producto;

-- Data Mart Marketing
SELECT
	venta.codigo_cliente,
	venta.venta_unidades,
	venta.venta_importe,
	venta.fecha_cierre_comercial,
	producto.SKU_descripcion,
	cliente.provincia,
	cliente.ciudad,
	cliente.n_distribuidor,
FROM
	usm-infra-grupo1.data_warehouse.fact_venta AS venta
	INNER JOIN usm-infra-grupo1.data_warehouse.dim_cliente AS cliente ON venta.codigo_cliente = cliente.codigo_cliente
	INNER JOIN usm-infra-grupo1.data_warehouse.dim_producto AS producto ON venta.SKU_codigo = producto.SKU_codigo;


