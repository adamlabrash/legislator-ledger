import csv
from shared_models.items import ExpenditureItem, MemberTravelClaim
from transform.s3_to_json import get_expenditures_json_s3

# TODO init rows
travel_events_writer = csv.writer(open('transform/travel_events.csv', 'w'))
travel_claims_writer = csv.writer(open('transform/travel_claims.csv', 'w'))


# TODO apply carbon estimations
def build_travel_csvs() -> None:
    for row in get_expenditures_json_s3():
        expenditure = ExpenditureItem.model_validate(row)

        # TODO validate no duplicate ids, each travel_event has a matching travel claim, location etc
        if isinstance(expenditure.claim, MemberTravelClaim):
            for travel_event in expenditure.claim.travel_events:
                travel_events_writer.writerow(travel_event.model_dump(mode='json'))
            travel_claims_writer.writerow(expenditure.claim.model_dump(mode='json', exclude={'travel_events'}))
