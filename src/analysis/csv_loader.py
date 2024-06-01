import csv
import json
from typing import Any, Iterator

from analysis.items import Location
from extraction.items import ExpenditureItem, MemberTravelClaim


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
