import configparser

import requests

from enriched_customer_events_data_product.main import main
from tests.utils.s3_utils import create_bucket
from tests.utils.spark_utils import read_table, spark_session

spark = spark_session()

data_product_url = "http://localhost:8000"

config = configparser.ConfigParser()
config.read('../enriched_customer_events_data_product/config.ini')

s3_output_port = config['test']['s3_output_port']
bucket_name = config['test']['s3_bucket']

expected_data = spark.createDataFrame([
    (1, 'login', '2022-08-22 16:29:57', '1992-05-12'),
    (2, 'login', '2022-08-22 16:49:57', '1987-11-05'),
], ['customer_id', 'action', 'timestamp', 'date_of_birth'])


def test_enriched_customer_events_are_accessible_as_parquet_files():
    create_bucket(bucket_name)

    response = requests.post(f"{data_product_url}/run", )
    main('env')
    file_format = 'parquet'

    if response.status_code == 200:
        if data_product_has_finished(response.json()['execution_id']):
            output_port_data = read_table(spark, s3_output_port, file_format)
            assert (output_port_data.collect() == expected_data.collect())
    else:
        assert False, "Enriched Customer Events DP could not run"


def data_product_has_finished(execution_id: str) -> bool:
    response = requests.get(f"{data_product_url}/runs/{execution_id}")
    if response.json()['status'] == 'finished':
        return True
    return False
