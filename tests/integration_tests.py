from enriched_customer_events_data_product.main import publish_table_output_port
from tests.utils.s3_utils import delete_bucket, create_bucket
from tests.utils.spark_utils import read_table, spark_session

spark = spark_session()

test_data = spark.createDataFrame([
    (1, 'login', '2022-08-22 16:29:57', '1992-05-12'),
    (2, 'login', '2022-08-22 16:49:57', '1987-11-05'),
], ['customer_id', 'action', 'timestamp', 'date_of_birth'])

bucket_name = "test"
s3_output_port = f"s3a://{bucket_name}/domains/customers/enriched_customer_events_output_port"


def test_error_when_s3_bucket_does_not_exist():
    # setup
    delete_bucket(bucket_name)
    file_format = 'parquet'

    # exercise
    try:
        publish_table_output_port(test_data, s3_output_port, file_format)
        assert False, f"S3 output port should not be published since the bucket does not exist"
    except Exception:
        # verify
        assert True


def test_dataframe_is_written_as_parquet_in_s3():
    # setup
    create_bucket(bucket_name)
    file_format = 'parquet'

    # exercise
    try:
        publish_table_output_port(test_data, s3_output_port, file_format)
    except Exception as exc:
        assert False, f"S3 output port could not be published: {exc}"

    # verify
    published_data = read_table(spark, s3_output_port, file_format)
    assert (published_data.collect() == test_data.collect())
