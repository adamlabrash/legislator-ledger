import csv
import json
from io import StringIO
from typing import Any, Iterator
from urllib.parse import unquote

from loguru import logger

from extraction.enums import Institution
from extraction.items import (
    EXPENDITURE_CLAIM,
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

    def process_item(self, item: dict[str, Any], spider) -> list:  # item is a csv file + metadata
        csv_data = csv.reader(StringIO(item['csv'].decode('utf-8-sig'), newline='\r\n'))
        try:
            metadata = {
                'csv_title': unquote(next(csv_data)[0]),
                'institution': Institution.MEMBERS_OF_PARLIAMENT,
            } | self.extract_metadata_from_url(item['download_url'])

            next(csv_data)  # skip header row

            # perform basic validation before uploading data.
            claims = self.extract_expenditure_claims_from_csv_data(metadata['category'], csv_data)
            expenditure_items = [
                ExpenditureItem.model_validate(metadata | {'claim': claim} | item)
                for claim in claims
            ]

            self.write_expenditure_items_to_json(expenditure_items)
            logger.success(
                f'Extracted {len(expenditure_items)} expenditures for {item["download_url"]}'
            )
            return expenditure_items

        except Exception as e:
            logger.warning(e)

        return []

    def write_expenditure_items_to_json(self, expenditure_items: list[ExpenditureItem]) -> None:
        for expenditure in expenditure_items:
            line = json.dumps(
                expenditure.model_dump(mode='json', exclude_none=True), ensure_ascii=False, indent=4
            )
            if self.is_first_item_written:
                line = ',\n' + line
            else:
                self.is_first_item_written = True  # we don't want a comma before the first item

            self.file.write(line)

    def extract_metadata_from_url(self, url: str) -> dict:
        url_parts = url.split('/')
        return {
            'category': url_parts[-5],
            'year': int(url_parts[-4]) - 1,  # url year is off by one
            'quarter': url_parts[-3],
            'mp_id': url_parts[-2],
            'download_url': url,
        }

    def extract_expenditure_claims_from_csv_data(
        self, category: str, csv_data
    ) -> list[EXPENDITURE_CLAIM]:
        if category == 'hospitality':
            return [HospitalityClaim.from_csv_row(claim_row) for claim_row in csv_data]
        elif category == 'contract':
            return [ContractClaim.from_csv_row(claim_row) for claim_row in csv_data]
        elif category == 'travel':
            return self.extract_travel_claims_from_csv(csv_data)  # type: ignore
        raise ValueError('Unable to extract claim data')

    def extract_travel_claims_from_csv(
        self, csv_row: Iterator[list[str]]
    ) -> list[MemberTravelClaim]:

        travel_events: list[tuple[str, TravelEvent]] = []
        travel_claims: list[MemberTravelClaim] = []

        for row in csv_row:
            try:
                travel_event = TravelEvent.from_csv_row(row)
                claim_id = row[0].strip()
                travel_events.append((claim_id, travel_event))
            except ValueError:
                try:
                    travel_claim = MemberTravelClaim.from_csv_row(row)
                    travel_claims.append(travel_claim)
                except ValueError as e:  # invalid row
                    logger.warning(f'Invalid row: {e}')
                    continue

        for travel_claim in travel_claims:
            for claim_id, travel_event in travel_events:
                if travel_claim.claim_id == claim_id:
                    travel_claim.travel_events.append(travel_event)

        return travel_claims
