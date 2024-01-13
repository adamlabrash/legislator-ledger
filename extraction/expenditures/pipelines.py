import csv
from datetime import datetime
from io import StringIO
import json
from typing import Iterator
from extraction.expenditures.enums import Institution

from urllib.parse import unquote

from extraction.expenditures.items import (
    ContractClaim,
    ExpenditureItem,
    HospitalityClaim,
    MemberTravelClaim,
    TravelEvent,
)


class MemberExpenditureSpiderPipeline:
    def open_spider(self, spider) -> None:
        self.file = open("expenditures.json", "w", encoding='utf-8-sig')
        self.file.write('[')
        self.is_first_item_written = False

    def close_spider(self, spider) -> None:
        self.file.write(']')
        self.file.close()

    def process_item(self, item, spider) -> list[dict]:  # item is a csv file + metadata
        csv_data = csv.reader(StringIO(item['csv'].decode('utf-8-sig'), newline='\r\n'))

        metadata = {
            'csv_title': unquote(next(csv_data)[0]),
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

        expenditure_items = [ExpenditureItem.model_validate(metadata | {'claim': claim}) for claim in claims]

        for expenditure in expenditure_items:
            line = json.dumps(expenditure.model_dump(mode='json', exclude_none=True), ensure_ascii=False, indent=4)
            if self.is_first_item_written:
                line = ',\n' + line
            else:
                self.is_first_item_written = True  # we don't want a comma before the first item

            self.file.write(line)

    def extract_url_parts(self, url: str) -> dict:
        url_parts = url.split('/')
        return {
            'category': url_parts[-5],
            'year': int(url_parts[-4]) - 1,  # url year is off by one
            'quarter': url_parts[-3],
            'mp_id': url_parts[-2],
            'download_url': url,
        }

    def extract_travel_claims_from_csv(self, csv_data: Iterator[list[str]]) -> list[MemberTravelClaim]:
        travel_events: list[tuple[str, TravelEvent]] = []
        travel_claims: list[MemberTravelClaim] = []

        for row in csv_data:
            try:
                travel_event = TravelEvent.from_csv_row(row)
                claim_id = row[0].strip()
                travel_events.append((claim_id, travel_event))
            except ValueError as e:
                try:
                    travel_claim = MemberTravelClaim.from_csv_row(row)
                    travel_claims.append(travel_claim)
                except ValueError as e:  # invalid row
                    print(row, e)
                    continue

        for travel_claim in travel_claims:
            for claim_id, travel_event in travel_events:
                if travel_claim.claim_id == claim_id:
                    travel_claim.travel_events.append(travel_event)

        return travel_claims
