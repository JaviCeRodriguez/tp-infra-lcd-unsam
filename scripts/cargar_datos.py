from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound
from datetime import datetime, timedelta

def load_gcs_to_bigquery_event_data(GCS_URI, TABLE_ID, table_schema):
    job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition = 'WRITE_APPEND',
            skip_leading_rows=1
        )

    load_job = client.load_table_from_uri(
        GCS_URI, TABLE_ID, job_config=job_config
    )

    load_job.result()
    table = client.get_table(TABLE_ID)

    print("Loaded {} rows to table {}".format(table.num_rows, TABLE_ID))


bq_table_schema_stock = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("SKU_codigo", "STRING"),
    bigquery.SchemaField("SKU_descripcion", "STRING"),
    bigquery.SchemaField("stock_unidades", "INTEGER"),
    bigquery.SchemaField("unidad", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

bq_table_schema_venta = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("SKU_codigo", "STRING"),
    bigquery.SchemaField("venta_unidades", "INTEGER"),
    bigquery.SchemaField("venta_importe", "FLOAT"),
    bigquery.SchemaField("condicion_venta", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

bq_table_schema_deuda = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("deuda_vencida", "FLOAT"),
    bigquery.SchemaField("deuda_tota", "FLOAT"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

bq_table_schema_cliente = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("ciudad", "STRING"),
    bigquery.SchemaField("provincia", "STRING"),
    bigquery.SchemaField("estado", "STRING"),
    bigquery.SchemaField("nombre_cliente", "STRING"),
    bigquery.SchemaField("cuit", "INTEGER"),
    bigquery.SchemaField("razon_social", "STRING"),
    bigquery.SchemaField("direccion", "STRING"),
    bigquery.SchemaField("dias_visita", "STRING"),
    bigquery.SchemaField("telefono", "STRING"),
    bigquery.SchemaField("fecha_alta", "DATETIME"),
    bigquery.SchemaField("fecha_baja", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("long", "FLOAT"),
    bigquery.SchemaField("condicion_venta", "STRING"),
    bigquery.SchemaField("deuda_vencida", "FLOAT"),
    bigquery.SchemaField("tipo_negocio", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

client = bigquery.Client()

if __name__ == '__main__':
    cant_dist = 5
    cant_dias = 15
    PROJECT_ID = "usm-infra-grupo1"
    dist_string = "g1_distribuidor_"
    storage = "gs://"
    tables = ['stock', 'venta', 'deuda', 'cliente']
    fecha_actual = datetime.now()
    ds_raw = "data_raw"

    for distribuidor in range(1, cant_dist):
        print(f"\n======= DISTRIBUIDOR {distribuidor} =======")
        for d in range(0, cant_dias):
            try:
                fecha_cierre = fecha_actual - timedelta(days=d)
                fecha_cierre_str = f'{fecha_cierre.year}-{fecha_cierre.month:02d}-{fecha_cierre.day:02d}'
                for table in tables:
                    TABLE_ID = f"{PROJECT_ID}.{ds_raw}.{table}"
                    GCS_URI = f"{storage}{dist_string}{distribuidor}/{table}/{fecha_cierre_str}.csv"
                    print("---> Loading:", GCS_URI)
                    load_gcs_to_bigquery_event_data(GCS_URI, TABLE_ID, locals()[f"bq_table_schema_{table}"])
            except BadRequest as err:
                print('BadRequest', err)
            except NotFound as err:
                print('NotFound', err)
            except:
                print('Problema con el siguiente Blob: ', GCS_URI)