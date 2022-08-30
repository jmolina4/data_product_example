from pyspark.sql import SparkSession


def spark_session() -> SparkSession:
    s = SparkSession.builder \
        .master("local[1]") \
        .appName("EnrichedCustomerEventsDPTests") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "foo") \
        .config("spark.hadoop.fs.s3a.secret.key", "foo") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    return s

def read_table(spark: SparkSession, location: str, file_format: str):
    return spark \
        .read \
        .format(file_format) \
        .load(location)
