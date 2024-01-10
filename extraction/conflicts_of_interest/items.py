from datetime import date
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel


class RegulatoryAct(Enum):
    CONFLICT_OF_INTEREST_ACT = 'Conflict of Interest Act'
    CONFLICT_OF_INTEREST_CODE_HOUSE_OF_COMMONS = 'Conflict of Interest Code for Members of the House of Commons'


class DisclosureType(Enum):
    DISCLOSURE_SUMMARY = 'Disclosure Summary'
    SUMMARY_STATEMENT = 'Summary Statement'
    SPONSORED_TRAVEL = 'Sponsored Travel'
    GIFTS = 'Gifts'
    ASSETS = 'Assets'
    OUTSIDE_ACTIVITIES = 'Outside Activities'
    LIABILITIES = 'Liabilities'
    RECUSAL = 'Recusal'
    TRAVEL = 'Travel'
    FORFEITED_GIFTS = 'Forfeited Gifts'
    MATERIAL_CHANGES = 'Material Changes'
    COMPLIANCE_MEASURES = 'Compliance Measures'
    PRIVATE_INTEREST = 'Private Interest'
    OTHER = 'Other'  # Other Appropriate Documents


class BasePublicDeclaration(BaseModel):
    disclosure_date: date
    member: str
    is_still_applicable: bool
    regulatory_act: RegulatoryAct
    disclosure_type: DisclosureType
    url: str
    disclosure: str


class SponsoredTravelDisclosure(BasePublicDeclaration):
    accompanying_traveller: list[str]
    destinations: list[str]
    purpose_of_trip: str
    trip_sponsor: str
    travel_dates: str

    gifts_recieved_description: str
    gifts_recieved_value: Decimal

    transportation_recieved_description: str
    transportation_recieved_value: Decimal

    accommodations_recieved_description: str
    accommodations_recieved_value: Decimal

    other_recieved_description: str
    other_recieved_value: Decimal

    supporting_documents_url: str


class PublicDeclarationGifts(BasePublicDeclaration):
    nature: str
    source: str
    circumstance: str
    gift_received_date: date | None


# TODO include statuatory requirements description
class PublicDisclosureSummary(BasePublicDeclaration):
    assets: list[str]
    other_sources_of_income_prev_12_months: list[str]
    other_sources_of_income_next_12_months: list[str]
    investment_private_corporations: list[str]
    liabilities: list[str]

    spouse_assets: list[str]
    spouse_sources_of_income: list[str]  # TODO Last and next 12 months:
    spouse_liabilities: list[str]
    spouse_private_corporations_investments: list[str]
    spouse_activities: list[str]

    raw_text: str


# TODO
class NoticeMaterialChange(BaseModel):
    pass
