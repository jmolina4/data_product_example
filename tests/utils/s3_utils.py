import logging

import boto3
from botocore.exceptions import ClientError

import configparser

config = configparser.ConfigParser()
config.read('../enriched_customer_events_data_product/config.ini')

ACCESS_ID = config['test']['aws_access_key_id']
ACCESS_KEY = config['test']['aws_secret_access_key']


def create_bucket(name: str):
    try:
        s3_client = boto3.client('s3', region_name='eu-west-1', endpoint_url='http://localhost:4566',
                                 aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_KEY)
        s3 = boto3.resource('s3', region_name='eu-west-1', endpoint_url='http://localhost:4566',
                            aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_KEY)
        location = {'LocationConstraint': 'eu-west-1'}
        try:
            s3_client.head_bucket(Bucket=name)
            bucket = s3.Bucket(name)
            bucket.objects.all().delete()

            s3_client.delete_bucket(Bucket=name)
        except ClientError:
            print("Bucket already exists")
        s3_client.create_bucket(Bucket=name,
                                CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_bucket(name: str):
    s3_client = boto3.client('s3', region_name='eu-west-1', endpoint_url='http://localhost:4566',
                             aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_KEY)
    s3 = boto3.resource('s3', region_name='eu-west-1', endpoint_url='http://localhost:4566',
                        aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_KEY)
    bucket = s3.Bucket(name)
    bucket.objects.all().delete()
    s3_client.delete_bucket(Bucket=name)
