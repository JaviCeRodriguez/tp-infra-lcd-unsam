-- Fact table Deuda
SELECT * FROM usm-infra-grupo1.data_warehouse.fact_deuda AS deuda;

-- Dim table Cliente
SELECT * FROM usm-infra-grupo1.data_warehouse.dim_cliente AS cliente;

-- Data Mart Suministros
SELECT
	cliente.codigo_cliente,
	cliente.ciudad,
	cliente.provincia,
	cliente.estado,
	cliente.fecha_alta,
	cliente.fecha_baja,
	cliente.tipo_negocio,
	cliente.lat,
	cliente.long,
	deuda.deuda_vencida,
	deuda.deuda_tota,
	deuda.n_distribuidor
FROM
	usm-infra-grupo1.data_warehouse.dim_cliente AS cliente
	INNER JOIN usm-infra-grupo1.data_warehouse.fact_deuda AS deuda ON cliente.codigo_cliente = deuda.codigo_cliente;
