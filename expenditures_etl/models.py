from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, field_validator


class ExpenditureCategory(Enum):
    CONTRACTS = 'contract'
    TRAVEL = 'travel'
    HOSPITALITY = 'hospitality'

    @property
    def file_name(self) -> str:
        return f'gov_{self.value}_expenditures.csv'


class TravelPurpose(Enum):
    ATTEND_CONSTITUENCY_EVENT = 'To attend a constituency or community event'
    OFFICE_BUSINESS = 'To conduct constituency office business'
    UNITE_WITH_FAMILY = 'To unite the family with the Member'
    TRAVEL_BETWEEN_CONSTITUENCY_OTTAWA = ' To travel to/from constituency and Ottawa'
    ATTEND_MEETING_CONSTITUENCY_ISSUES = 'To attend meetings about constituency issues'
    ATTEND_FUNERAL = 'To attend an authorized funeral'
    DRIVE_MEMBER_AIRPORT_TERMINAL = 'To drive Member to/from terminal'


class TravellerType(Enum):
    MEMBER = 'Member'
    DESIGNATED_TRAVELLER = 'Designated Traveller'
    EMPLOYEE = 'Employee'
    DEPENDANT = 'Dependant'
    HOUSE_OFFICER_EMPLOYEE = 'House Officer Employee'


class TravelEvent(BaseModel):
    claim_id: str
    traveller_name: str
    traveller_type: TravellerType
    purpose_of_travel: str
    date: date
    departure: str
    destination: str
    mp_office: str

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'TravelEvent':
        return cls.model_validate(
            {
                'claim_id': row[0],
                'traveller_name': row[1],
                'traveller_type': row[2],
                'purpose_of_travel': row[3],
                'data': datetime.strptime(row[4], '%Y/%m/%d').date(),
                'departure': row[5],
                'destination': row[6],
            }
        )


class TravelClaim(BaseModel):
    id: str
    from_date: date | None = None
    end_date: date | None = None
    transport_cost: Decimal
    accommodation_cost: Decimal
    meals_and_incidentals_cost: Decimal
    reg_points_used: Decimal
    special_points_used: Decimal
    USA_points_used: Decimal
    total_cost: Decimal
    mp_office: str

    @field_validator('transport_cost', 'accommodation_cost', 'meals_and_incidentals_cost', 'total_cost')
    def round_decimal_fields(cls, v: Decimal) -> Decimal:
        return v.quantize(Decimal('1.00'))

    @classmethod
    def from_csv_row(cls, row: list[str]) -> 'TravelClaim':
        return cls.model_validate(
            {
                'id': row[0],
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
