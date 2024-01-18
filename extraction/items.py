from decimal import Decimal
from typing import Any, Iterator
from urllib.parse import unquote
from extraction.enums import (
    Caucus,
    ExpenditureCategory,
    HospitalityEventType,
    Institution,
    TravellerType,
    HospitalityPurpose,
    TravelPurpose,
)
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from datetime import date, datetime


###### THIRD PARTY CONTRACTS
class ContractClaim(BaseModel):
    supplier: str
    description: str
    date: date
    total_cost: Decimal = Field(..., decimal_places=2)

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'ContractClaim':
        row = [unquote(cell).strip() for cell in row]
        return ContractClaim.model_validate(
            {
                'supplier': row[0],
                'description': row[1],
                'date': datetime.strptime(row[2], '%Y/%m/%d').date(),
                'total_cost': row[3],
            }
        )

####### HOSPITALITY
class HospitalityClaim(BaseModel):
    claim_id: str
    date: date
    location: str
    num_attendees: int = Field(ge=0)
    purpose_of_hospitality: HospitalityPurpose
    event_type: HospitalityEventType
    supplier: str
    total_cost: Decimal = Field(..., decimal_places=2)

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'HospitalityClaim':
        row = [unquote(cell).strip() for cell in row]

        return HospitalityClaim.model_validate(
            {
                'claim_id': row[5],
                'date': datetime.strptime(row[0], '%Y/%m/%d').date(),
                'location': row[1],
                'num_attendees': row[2],
                'purpose_of_hospitality': row[3],
                'event_type': row[4],
                'supplier': row[6],
                'total_cost': row[7],
            }
        )


class AdminHospitality(HospitalityClaim):
    host: str


####### TRAVEL
class TravelClaim(BaseModel):
    claim_id: str
    transport_cost: Decimal = Field(..., decimal_places=2)
    accommodation_cost: Decimal = Field(..., decimal_places=2)
    meals_and_incidentals_cost: Decimal = Field(..., decimal_places=2)

    @field_validator('transport_cost', 'accommodation_cost', 'meals_and_incidentals_cost')
    def round_decimals(cls, v: Decimal) -> Decimal:
        return v.quantize(Decimal('1.00'))


class TravelEvent(BaseModel):
    traveller_name: str
    traveller_type: TravellerType
    purpose_of_travel: TravelPurpose
    date: date
    departure: str
    destination: str

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'TravelEvent':
        row = [unquote(cell).strip() for cell in row]

        return TravelEvent.model_validate(
            {
                'traveller_name': row[3],
                'traveller_type': row[4],
                'purpose_of_travel': row[5],
                'date': datetime.strptime(row[6], '%Y/%m/%d').date(),
                'departure': row[7],
                'destination': row[8],
            }
        )

    @field_validator('departure', 'destination')
    def validate_title_case(cls, v: str) -> str:
        if v.isupper():
            return v.title()
        return v

    @field_validator('purpose_of_travel', mode='before')
    def validate_member_case(cls, v: str) -> str:
        if v == 'To unite the family with the member':  # typo edgecase in csv
            return 'To unite the family with the Member'
        return v

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump() | {
            'traveller_type': self.traveller_type.value,
            'purpose_of_travel': self.purpose_of_travel.value,
        }


class MemberTravelClaim(TravelClaim):
    reg_points_used: Decimal = Field(..., decimal_places=1, multiple_of=0.5)
    special_points_used: Decimal = Field(..., decimal_places=1, multiple_of=0.5)
    USA_points_used: Decimal = Field(..., decimal_places=1, multiple_of=0.5)
    travel_events: list[TravelEvent] = []

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'MemberTravelClaim':
        row = [unquote(cell).strip() for cell in row]
        return MemberTravelClaim.model_validate(
            {
                'claim_id': row[0],
                'from_date': datetime.strptime(row[2], '%Y/%m/%d').date() if row[1] else None,
                'end_date': datetime.strptime(row[2], '%Y/%m/%d').date() if row[2] else None,
                'transport_cost': row[9],
                'accommodation_cost': row[10],
                'meals_and_incidentals_cost': row[11],
                'reg_points_used': row[12],
                'special_points_used': row[13],
                'USA_points_used': row[14],
                'total_cost': row[15],
            }
        )

    @field_validator('USA_points_used', 'special_points_used', 'reg_points_used')
    def round_decimal_fields(cls, v: Decimal) -> Decimal:
        if v == 0.1:  # rounding error in report
            v = Decimal(1)

        return v.quantize(Decimal('1.0'))

    def as_dicts(self) -> Iterator[dict]:
        for event in self.travel_events:
            yield self.model_dump(exclude={'travel_events'}) | event.as_dict() | {
                'total_travel_events_in_claim': len(self.travel_events)
            }


EXPENDITURE_CLAIM = ContractClaim | HospitalityClaim | MemberTravelClaim


class ExpenditureItem(BaseModel, revalidate_instances='always'):
    model_config = ConfigDict(str_strip_whitespace=True)

    # url parts
    category: ExpenditureCategory
    year: int
    quarter: int = Field(ge=1, le=4)
    mp_id: str
    download_url: str

    csv_title: str
    extracted_at: datetime

    institution: Institution

    caucus: Caucus
    name: str
    constituency: str

    claim: EXPENDITURE_CLAIM

    @model_validator(mode='before')
    def strip_whitespaces(cls, v: dict) -> dict:
        return {key: value.strip() if isinstance(value, str) else value for key, value in v.items()}

    def as_dict(self) -> dict:
        return self.model_dump(exclude={'claim'}) | {
            'category': self.category.value,
            'institution': self.institution.value,
            'caucus': self.caucus.value,
        }
