from decimal import Decimal
from expenditures.enums import (
    Caucus,
    ExpenditureCategory,
    HospitalityEventType,
    Institution,
    TravellerType,
    HospitalityPurpose,
    TravelPurpose,
)
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import date, datetime


###### THIRD PARTY CONTRACTS
class ContractClaim(BaseModel):
    supplier: str
    description: str


class HouseAdminContractClaim(ContractClaim):
    original_contract_value: Decimal = Field(decimal_places=2)
    amended_contract_value: Decimal = Field(decimal_places=2)


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
    transport_cost: Decimal = Field(decimal_places=2)
    accommodation_cost: Decimal = Field(decimal_places=2)
    meals_and_incidentals_cost: Decimal = Field(decimal_places=2)

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
        return cls.model_validate(
            {
                'traveller_name': row[1],
                'traveller_type': row[2],
                'purpose_of_travel': row[3],
                'date': datetime.strptime(row[4], '%Y/%m/%d').date(),
                'departure': row[5],
                'destination': row[6],
            }
        )


class MemberTravelClaim(TravelClaim):
    reg_points_used: float = Field(decimal_places=1, multiple_of=0.5, ge=0)
    special_points_used: float = Field(decimal_places=1, multiple_of=0.5, ge=0)
    USA_points_used: float = Field(decimal_places=1, multiple_of=0.5, ge=0)
    travel_events: list[TravelEvent]

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'TravelClaim':
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
    other: Decimal = Field(decimal_places=2)
    per_diems: Decimal = Field(decimal_places=2)


EXPENDITURE_CLAIM = ContractClaim | HospitalityClaim | MemberTravelClaim | HouseOfficerTravelClaim | AdminTravelClaim


class ExpenditureItem(BaseModel):
    category: ExpenditureCategory
    institution: Institution
    caucus: Caucus

    download_url: str
    extracted_at: datetime

    mp_id: str | None
    mp_office: str
    mp_constituency: str

    # TODO review dates
    start_date: date | None = Field(serialization_alias='date')  # TODO check this -> start_date, date, end_date
    end_date: date | None = Field(serialization_alias='date')
    quarter: int = Field(ge=1, le=4)
    year: int

    total_cost: Decimal = Field(decimal_places=2, gt=0)
    claim: EXPENDITURE_CLAIM

    @field_validator('total_cost')
    def round_decimal_fields(cls, v: Decimal) -> Decimal:
        return v.quantize(Decimal('1.00'))

    # TODO -> drop events in claim if they don't match claim_id
    @model_validator(mode='before')
    def validate_model():
        pass
