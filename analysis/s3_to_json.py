import json
from typing import Any

import boto3

from configs import AWS_ACCESS_KEY_ID, AWS_BUCKET_NAME, AWS_SECRET_ACCESS_KEY


def write_s3_data_to_expenditures_json() -> Any:
    client = boto3.client(
        's3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    client.download_file(AWS_BUCKET_NAME, 'expenditures.json', 'transform/expenditures.json')

    with open(file='transform/expenditures.json', encoding='utf-8-sig') as json_file:
        data = json.load(json_file)
    return data
