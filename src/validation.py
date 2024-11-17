from typing import Any, Iterator
from urllib.parse import unquote

from loguru import logger

from enums import ExpenditureCategory, Institution
from items import (
    ContractClaim,
    HospitalityClaim,
    MemberTravelClaim,
    TravelEvent,
)


# TODO improve typing
# TODO improve documentation
# TODO build tests
def extract_expenditure_claims_from_csv_data(
    category: ExpenditureCategory, csv_data
) -> list[HospitalityClaim | ContractClaim | MemberTravelClaim]:
    if category is ExpenditureCategory.HOSPITALITY:
        return [HospitalityClaim.from_csv_row(claim_row) for claim_row in csv_data]
    elif category is ExpenditureCategory.CONTRACTS:
        return [ContractClaim.from_csv_row(claim_row) for claim_row in csv_data]
    elif category is ExpenditureCategory.TRAVEL:
        return extract_travel_claims_from_csv(csv_data)
    raise ValueError('Unable to extract claim data')


def build_metadata_dict(csv_data, item) -> dict[str, Any]:
    return {
        'csv_title': unquote(next(csv_data)[0]),
        'institution': Institution.MEMBERS_OF_PARLIAMENT,
    } | extract_metadata_from_url(item['download_url'])


def extract_metadata_from_url(url: str) -> dict:
    url_parts = url.split('/')
    return {
        'category': url_parts[-5],
        'year': int(url_parts[-4]) - 1,  # url year is off by one
        'quarter': url_parts[-3],
        'mp_id': url_parts[-2],
        'download_url': url,
    }


def extract_travel_claims_from_csv(csv_rows: Iterator[list[str]]) -> list[MemberTravelClaim]:

    travel_events: list[tuple[str, TravelEvent]] = []
    travel_claims: list[MemberTravelClaim] = []

    for row in csv_rows:
        try:
            travel_event = TravelEvent.from_csv_row(row)
            claim_id = row[0].strip()
            travel_events.append((claim_id, travel_event))
        except ValueError:
            try:
                travel_claim = MemberTravelClaim.from_csv_row(row)
                travel_claims.append(travel_claim)
            except ValueError as e:  # this means that the MP has formatted the claim incorrectly
                logger.warning(f'Invalid row: {e}')
                continue

    for travel_claim in travel_claims:
        for claim_id, travel_event in travel_events:
            if travel_claim.claim_id == claim_id:
                travel_claim.travel_events.append(travel_event)

    return travel_claims
