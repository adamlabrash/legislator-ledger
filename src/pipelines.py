import csv
import json
from io import StringIO
from typing import Any

from loguru import logger

from items import ExpenditureItem
from validation import build_metadata_dict, extract_expenditure_claims_from_csv_data


class MemberExpenditureSpiderPipeline:

    def __init__(self):
        self.is_first_item_written = False

    def open_spider(self, spider) -> None:
        self.file = open("expenditures.json", "w", encoding='utf-8-sig')
        self.file.write('[')

    def close_spider(self, spider) -> None:
        self.file.write(']')
        self.file.close()

    def process_item(self, item: dict[str, Any], spider) -> list:  # item is a csv file + metadata
        csv_data = csv.reader(StringIO(item['csv'].decode('utf-8-sig'), newline='\r\n'))
        try:
            metadata = build_metadata_dict(csv_data, item)
            next(csv_data)  # skip header row

            # perform basic validation before uploading data.
            claims = extract_expenditure_claims_from_csv_data(metadata['category'], csv_data)
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
