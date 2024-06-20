import csv
import json
from typing import Any, Iterator

import boto3
from analysis.items import Location
from configs import AWS_ACCESS_KEY_ID, AWS_BUCKET_NAME, AWS_SECRET_ACCESS_KEY
from extraction.items import ExpenditureItem, MemberTravelClaim


def write_s3_data_to_expenditures_json() -> Any:
    client = boto3.client(
        's3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    client.download_file(AWS_BUCKET_NAME, 'expenditures.json', 'transform/expenditures.json')

    with open(file='transform/expenditures.json', encoding='utf-8-sig') as json_file:
        data = json.load(json_file)
    return data


def load_locations_csv() -> Iterator[Location]:
    reader = csv.reader(open('analysis/data/locations.csv', 'r'))
    next(reader)  # skip header
    for row in reader:
        yield Location.from_csv_row(row)


def load_member_travel_claims_from_csv() -> list[dict[Any, Any]]:

    with open('expenditures.json', encoding='utf-8-sig') as f:
        expenditure_data = json.load(f)

    travel_expenditures: list[dict] = []
    for expenditure in expenditure_data:
        expenditure = ExpenditureItem.model_validate(expenditure)
        if isinstance(expenditure.claim, MemberTravelClaim):
            for travel_event_data in expenditure.claim.as_dicts():
                flattened_data = travel_event_data | expenditure.as_dict()
                travel_expenditures.append(flattened_data)
    return travel_expenditures
