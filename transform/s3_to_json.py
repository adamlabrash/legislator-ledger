import json
from typing import Any
import boto3
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

# S3 Bucket credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_BUCKET_NAME = os.environ['AWS_BUCKET_NAME']


# TODO lru cache/check if it already exists before downloading from s3
def get_expenditures_json_s3() -> Any:
    client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    client.download_file(AWS_BUCKET_NAME, 'expenditures.json', 'transform/expenditures.json')

    with open(file='transform/expenditures.json', encoding='utf-8-sig') as json_file:
        data = json.load(json_file)
    return data
