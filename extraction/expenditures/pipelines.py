import csv
from datetime import datetime
from io import StringIO
from typing import Iterator
from expenditures.enums import Institution
from expenditures.items import ContractClaim, ExpenditureItem, HospitalityClaim, MemberTravelClaim, TravelEvent

import json


class MemberExpenditureSpiderPipeline:
    def open_spider(self, spider):
        self.file = open("items.json", "w")

    def close_spider(self, spider):
        self.file.close()

    def extract_url_parts(self, url: str) -> dict:
        url_parts = url.split('/')
        return {
            'category': url_parts[-5],
            'year': int(url_parts[-4]) - 1,  # url year is off by one
            'quarter': url_parts[-3],
            'mp_id': url_parts[-2],
            'download_url': url,
        }

    def extract_travel_claims_from_csv(self, csv_data: Iterator[list[str]]) -> Iterator[MemberTravelClaim]:
        travel_claim = MemberTravelClaim.from_csv_row(next(csv_data))
        for row in csv_data:
            if row[0] == travel_claim.claim_id:
                travel_event = TravelEvent.from_csv_row(row)
                travel_claim.travel_events.append(travel_event)
            else:
                yield travel_claim
                try:
                    travel_claim = MemberTravelClaim.from_csv_row(row)  # TODO error handling *****START HERE ******
                except Exception as e:
                    import pdb

                    pdb.set_trace()

    def process_item(self, item, spider):  # item is a csv file
        csv_data = csv.reader(StringIO(item['csv'], newline='\r\n'))

        metadata = {
            'csv_title': next(csv_data)[0],
            'extracted_at': datetime.now(),
            'institution': Institution.MEMBERS_OF_PARLIAMENT,
            'caucus': item['caucus'],
            'constituency': item['constituency'],
            'name': item['name'],
        } | self.extract_url_parts(item['download_url'])

        next(csv_data)  # skip header row

        claims = []
        if metadata['category'] == 'hospitality':
            claims = [HospitalityClaim.from_csv_row(claim_row) for claim_row in csv_data]
        elif metadata['category'] == 'contract':
            claims = [ContractClaim.from_csv_row(claim_row) for claim_row in csv_data]
        elif metadata['category'] == 'travel':
            claims = self.extract_travel_claims_from_csv(csv_data)

        for claim in claims:
            expenditure_item = ExpenditureItem.model_validate(metadata | {'claim': claim})
            line = json.dumps(expenditure_item.model_dump(mode='json'))
            self.file.write(line + '\n')

        return item

        # TODO error handling
        # TODO convert list to set back to list
        # TODO model dump enum names not values

        # raise DropItem(f"Validation error(s) in {item}: {err}")
