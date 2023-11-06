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
    fact_tables = ['fact_venta', 'fact_stock', 'fact_deuda']
    dim_tables = ['dim_producto','dim_cliente', 'dim_sucursal']
    ds_dwh = "data_warehouse"

    sql_fact_venta = f"""SELECT * FROM {PROJECT_ID}.data_raw.venta;"""

    sql_fact_stock = f"""SELECT * FROM {PROJECT_ID}.data_raw.stock
    ;"""

    sql_fact_deuda = f"""SELECT * FROM {PROJECT_ID}.data_raw.deuda;
    """

    sql_dim_producto = f"""SELECT distinct(stock.SKU_codigo), stock.SKU_descripcion
        FROM {PROJECT_ID}.data_raw.stock as stock
    ;"""

    sql_dim_cliente = f"""SELECT * FROM {PROJECT_ID}.data_raw.cliente
    """

    sql_dim_sucursal = f"""
        SELECT
            DISTINCT(cliente.codigo_sucursal),
            cliente.provincia,
        FROM {PROJECT_ID}.data_raw.cliente as cliente;
    """

    for fact in fact_tables:
        TABLE_ID = f"{PROJECT_ID}.{ds_dwh}.{fact}"
        print("---> FACT TABLE", TABLE_ID)
        create_table(
            PROJECT_ID=PROJECT_ID,
            TARGET_TABLE_ID=TABLE_ID,
            QUERY=locals()[f"sql_{fact}"],
            WRITE_METHOD="WRITE_APPEND"
        )

    for dim in dim_tables:
        TABLE_ID = f"{PROJECT_ID}.{ds_dwh}.{dim}"
        print("---> DIM TABLE", TABLE_ID)
        create_table(
            PROJECT_ID=PROJECT_ID,
            TARGET_TABLE_ID=TABLE_ID,
            QUERY=locals()[f"sql_{dim}"],
            WRITE_METHOD="WRITE_TRUNCATE"
        )