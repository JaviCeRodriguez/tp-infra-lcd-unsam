from google.cloud import bigquery


def create_table(PROJECT_ID, TARGET_TABLE_ID, QUERY, WRITE_METHOD):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination=TARGET_TABLE_ID,
        write_disposition=WRITE_METHOD
    )

    query_job = client.query(QUERY, job_config=job_config)

    try:
        query_job.result()
        print("Query: tuki")
    except Exception as err:
        print('Fallo la query:', err)

if __name__ == '__main__':
    PROJECT_ID = "usm-infra-grupo1"
    DW_DATASET = "data_warehouse"
    datamarts = ['finanzas','marketing','suministros']
    ds_dwh = ['dmt_finanzas','dmt_marketing','dmt_suministros']

    direcciones = dict(zip(datamarts, ds_dwh))
    print(direcciones)
    
    sql_finanzas = f"""
        SELECT
            stock.stock_unidades,
            venta.venta_unidades,
            venta.fecha_cierre_comercial,
            producto.SKU_descripcion,
            stock.n_distribuidor
        FROM
            {PROJECT_ID}.{DW_DATASET}.fact_venta AS venta
            INNER JOIN {PROJECT_ID}.{DW_DATASET}.fact_stock AS stock ON
                venta.fecha_cierre_comercial = stock.fecha_cierre_comercial AND
                venta.codigo_sucursal = stock.codigo_sucursal
            INNER JOIN {PROJECT_ID}.{DW_DATASET}.dim_producto AS producto ON venta.SKU_codigo = producto.SKU_codigo;
    """
    
    sql_marketing = f"""
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
            {PROJECT_ID}.{DW_DATASET}.fact_venta AS venta
            INNER JOIN {PROJECT_ID}.{DW_DATASET}.dim_cliente AS cliente ON venta.codigo_cliente = cliente.codigo_cliente
            INNER JOIN {PROJECT_ID}.{DW_DATASET}.dim_producto AS producto ON venta.SKU_codigo = producto.SKU_codigo;
    """

    sql_suministros = f"""
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
            {PROJECT_ID}.{DW_DATASET}.dim_cliente AS cliente
            INNER JOIN {PROJECT_ID}.{DW_DATASET}.fact_deuda AS deuda ON cliente.codigo_cliente = deuda.codigo_cliente
    """

    for fact in datamarts:
        TABLE_ID = f"{PROJECT_ID}.{direcciones[fact]}.{fact}"
        print("---> DM TABLE", TABLE_ID)
        create_table(
            PROJECT_ID=PROJECT_ID,
            TARGET_TABLE_ID=TABLE_ID,
            QUERY=locals()[f"sql_{fact}"],
            WRITE_METHOD="WRITE_APPEND"
        )

    