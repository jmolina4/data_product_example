import os

from pyspark.sql import SparkSession, DataFrame


def main(argv):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("EnrichedCustomerEventsDP") \
        .getOrCreate()

    customer_events_raw = [
        (1, 'login', '2022-08-22 16:29:57'),
        (1, 'checkout', '2022-08-22 17:29:57'),
        (2, 'login', '2022-08-22 16:49:57'),
    ]

    customer_data_raw = [
        (1, 'Chris', '1992-05-12'),
        (2, 'Angela', '1987-11-05')
    ]

    customer_events = spark.createDataFrame(
        customer_events_raw, ['customer_id', 'action', 'timestamp'])
    customer_data = spark.createDataFrame(
        customer_data_raw, ['id', 'name', 'date_of_birth'])

    events = enrich_customer_events(customer_events, customer_data)

    environment = 'test'
    import configparser
    config = configparser.ConfigParser()
    config.read('../data_product_example/config.ini')
    s3_uri = str(config[environment]['s3_output_port'])

    publish_table_output_port(events, s3_uri, 'parquet')


def publish_table_output_port(data: DataFrame, location: str, file_format: str):
    print(location)
    data.write.format(file_format).save(location)


def enrich_customer_events(customer_events: DataFrame, customer_data: DataFrame) -> DataFrame:
    login_events = customer_events.filter(customer_events.action == 'login')
    enriched_events_with_all_fields = login_events.join(
        customer_data, customer_events.customer_id == customer_data.id)
    enriched_events = enriched_events_with_all_fields.select(
        "customer_id", "action", "timestamp", "date_of_birth")

    return enriched_events


if __name__ == "__main__":
    main()
