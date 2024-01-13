import csv
from datetime import datetime
from io import StringIO
from urllib.parse import unquote


class ConflictsOfInterestPipeline:
    def extract_url_parts(self, url: str) -> dict:
        url_parts = url.split('/')
        return {
            'mp_id': url_parts[-2],
            'download_url': url,
        }

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

        return [expenditure.model_dump(mode='json', exclude_none=True) for expenditure in expenditure_items]
