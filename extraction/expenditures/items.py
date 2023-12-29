from decimal import Decimal
from urllib.parse import unquote
from expenditures.enums import (
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


class HouseAdminContractClaim(ContractClaim):
    original_contract_value: Decimal = Field(..., decimal_places=2)
    amended_contract_value: Decimal = Field(..., decimal_places=2)


####### HOSPITALITY
class HospitalityClaim(BaseModel):
    claim_id: str
    location: str
    num_attendees: int = Field(ge=0)
    purpose_of_hospitality: HospitalityPurpose
    event_type: HospitalityEventType
    supplier: str


class AdminHospitality(HospitalityClaim):
    host: str


####### TRAVEL
class TravelClaim(BaseModel):
    claim_id: str
    transport_cost: Decimal = Field(..., decimal_places=2)
    accommodation_cost: Decimal = Field(..., decimal_places=2)
    meals_and_incidentals_cost: Decimal = Field(..., decimal_places=2)

    @field_validator('transport_cost', 'accommodation_cost', 'meals_and_incidentals_cost')
    def round_decimal_fields(cls, v: Decimal) -> Decimal:
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

        return cls.model_validate(
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
        return v.title()


class MemberTravelClaim(TravelClaim):
    reg_points_used: Decimal = Field(..., decimal_places=1, multiple_of=0.5, ge=0)
    special_points_used: Decimal = Field(..., decimal_places=1, multiple_of=0.5, ge=0)
    USA_points_used: Decimal = Field(..., decimal_places=1, multiple_of=0.5, ge=0)
    travel_events: list[TravelEvent] = []

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'MemberTravelClaim':
        row = [unquote(cell).strip() for cell in row]
        return cls.model_validate(
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


class HouseOfficerTravelClaim(TravelClaim):
    traveller_type: TravellerType
    purpose_of_travel: TravelPurpose
    itenerary: str
    role: str


class AdminTravelClaim(TravelClaim):
    purpose_of_travel: TravelPurpose


class CommitteeTravelClaim(TravelClaim):
    other: Decimal = Field(..., decimal_places=2)
    per_diems: Decimal = Field(..., decimal_places=2)


EXPENDITURE_CLAIM = ContractClaim | HospitalityClaim | MemberTravelClaim | HouseOfficerTravelClaim | AdminTravelClaim


class ExpenditureItem(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, use_enum_values=True)

    category: ExpenditureCategory
    institution: Institution
    mp_caucus: Caucus

    download_url: str
    quarter: int = Field(ge=1, le=4)
    year: int
    extracted_at: datetime

    mp_id: str | None
    mp_name: str
    mp_constituency: str

    start_date: date
    end_date: date

    total_cost: Decimal = Field(..., decimal_places=2, gt=0)
    claim: EXPENDITURE_CLAIM

    @model_validator(mode='before')
    def strip_whitespaces(cls, v: dict) -> dict:
        return {key: value.strip() if isinstance(value, str) else value for key, value in v.items()}
