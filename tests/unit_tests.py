from enriched_customer_events_data_product.main import enrich_customer_events
from tests.utils.spark_utils import spark_session

spark = spark_session()


def test_customer_login_events_are_enriched_only_with_date_of_birth():
    customer_events = spark.createDataFrame([
        (1, 'login', '2022-08-22 16:29:57'),
        (1, 'checkout', '2022-08-22 17:29:57'),
        (2, 'login', '2022-08-22 16:49:57')
    ], ['customer_id', 'action', 'timestamp'])

    customer_data = spark.createDataFrame([
        (1, 'Chris', '1992-05-12'),
        (2, 'Angela', '1987-11-05'),
        (3, 'Bob', '2001-12-10')
    ], ['id', 'name', 'date_of_birth'])

    login_events_with_date_of_birth = spark.createDataFrame([
        (1, 'login', '2022-08-22 16:29:57', '1992-05-12'),
        (2, 'login', '2022-08-22 16:49:57', '1987-11-05'),
    ], ['customer_id', 'action', 'timestamp', 'date_of_birth'])

    enriched_customer_events = enrich_customer_events(
        customer_events, customer_data)

    assert (enriched_customer_events.collect() ==
            login_events_with_date_of_birth.collect())
