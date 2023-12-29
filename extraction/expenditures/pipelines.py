from datetime import datetime
from expenditures.enums import Institution
from pydantic import ValidationError
from scrapy.exceptions import DropItem
from expenditures.items import ExpenditureItem, MemberTravelClaim, TravelEvent
from urllib.parse import unquote


class MemberExpenditureSpiderPipeline:
    def __init__(self) -> None:
        self.ids_seen = set()

    def process_item(self, item, spider):
        # TODO convert list to set back to list
        # TODO move this to model validation
        # TODO model dump enum names not values

        url_parts = item['download_url'].split('/')
        metadata = {
            'category': url_parts[-5],
            'year': url_parts[-4],
            'quarter': url_parts[-3],
            'mp_id': url_parts[-2],
            'institution': Institution.MEMBERS_OF_PARLIAMENT,
            'extracted_at': datetime.now(),
            'mp_caucus': item['member_data'][2],
            'mp_constituency': item['member_data'][1],
            'mp_name': item['member_data'][0],
            'download_url': item['download_url'],
            'start_date': datetime.strptime(item['claim_row'][1], '%Y/%m/%d').date(),
            'end_date': datetime.strptime(item['claim_row'][1], '%Y/%m/%d').date(),
            'total_cost': item['claim_row'][-1],
        }

        try:
            claim = MemberTravelClaim.from_csv_row(item['claim_row'])
            for travel_event in item['travel_event_rows']:
                try:
                    claim.travel_events.append(TravelEvent.from_csv_row(travel_event))
                except ValueError as err:
                    print(err)
                    # TODO log item and err
                    continue

            expenditure = ExpenditureItem.model_validate(metadata | {'claim': claim.model_dump()})
        except ValidationError as err:
            print(err)
            # TODO log item and err
            raise DropItem(f"Validation error(s) in {item}: {err}")

        return expenditure.model_dump(mode='json')
