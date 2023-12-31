from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def create_bigquery_dataset(dataset_name):
    """Create bigquery dataset. Check first if the dataset exists
        Args:
            dataset_name: String
    """

    dataset_id = "{}.{}".format(client.project, dataset_name)

    try:
        client.get_dataset(dataset_id)
        print("Dataset {} already exists".format(dataset_id))
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


client = bigquery.Client()

datasets_name = ['data_raw','data_warehouse','dmt_finanzas','dmt_marketing','dmt_suministros']
location = 'US'

for name in datasets_name:
    create_bigquery_dataset(name)