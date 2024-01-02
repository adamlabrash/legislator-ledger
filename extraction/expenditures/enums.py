from enum import Enum


class Caucus(Enum):
    CONSERVATIVE = 'Conservative'
    LIBERAL = 'Liberal'
    NDP = 'New Democratic Party'
    BLOC = 'Bloc Québécois'
    GREEN = 'Green Party'
    INDEPENDENT = 'Independent'
    ADMINISTRATION = 'Administration'


class ExpenditureCategory(Enum):
    CONTRACTS = 'contract'
    TRAVEL = 'travel'
    HOSPITALITY = 'hospitality'


class Institution(Enum):
    PRESIDING_OFFICERS_AND_HOUSE_OFFICERS = 'Presiding Officers and House Officers'
    MEMBERS_OF_PARLIAMENT = 'Members'
    HOUSE_OF_COMMONS_ADMIN = 'House of Commons Administration'
    PARLIAMENTARY_COMMITTEES = 'Parliamentary Committees'
    PARLIAMENTARY_DIPLOMACY = 'Parliamentary Diplomacy'


###### Hospitality
class HospitalityEventType(Enum):
    COMMUNITY_ACTIVITY_OR_EVENT = 'Community activity or event'
    HOSTING_A_RECEPTION_OR_OPEN_HOUSE = 'Hosting a reception or open house'
    HOSTING_A_MEETING = 'Hosting a meeting'
    GALA_RECEPTION_OR_CEREMONY = 'Gala, reception or ceremony'
    MEAL_AT_GALA_RECEPTION_OR_CEREMONY = 'Meal at a gala, reception or ceremony'
    MEAL_AT_A_COMMUNITY_ACTIVITY_OR_EVENT = 'Meal at a community activity or event'
    HOSTING_A_TOWN_HALL_OR_COMMUNITY_EVENT = 'Hosting a town hall or community event'
    HOSTING_A_STAFF_EVENT = 'Hosting a staff event'


class HospitalityPurpose(Enum):
    MEET_VISITORS_OF_MEMBER_OFFICE = 'To meet visitors of Member’s office'
    ATTEND_STAFF_EVENTS_INCLUDING_TRAINING = 'To attend staff events including training'
    DISCUSS_CONSTITUENCY_ISSUES_WITH_STAKEHOLDERS = 'To discuss constituency issues with stakeholders'
    DISCUSS_BUSINESS_OF_THE_HOUSE_CAUCUS_AND_COMMITTEES = 'To discuss business of the House, caucus and committees'
    PLAN_MEMBERS_PRIORITIES_AND_ACTIVITIES = "To plan Member's priorities and activities"
    EXCHANGE_WITH_DIGNITARIES = 'To exchange with dignitaries'
    MEET_CONSTITUENTS = 'To meet constituents'
    CELEBRATE_A_SIGNIFICANT_EVENT = 'To celebrate a significant event'


###### Travel
class TravelPurpose(Enum):
    ATTEND_TRAINING = 'To attend training'
    ATTEND_EVENT_PROTOCOL = 'To attend an event as a matter of protocol'
    ATTEND_MEETINGS_WASHINGTON = 'To attend meetings in Washington D.C'
    MEALS_INCIDENTALS = 'Meals and Incidentals'
    ATTEND_MEETINGS_CONSTITUENCY_ISSUES = 'To attend meetings about constituency issues'
    ATTEND_CONSTITUENCY_COMMUNITY_EVENT = 'To attend a constituency or community event'
    SUPPORT_HOUSE_OFFICER_FUNCTIONS = 'To support a House Officer\'s functions'
    SUPPORT_PARLIAMENTARY_EXCHANGE = 'To support a parliamentary exchange'
    ATTEND_CONFERENCE = 'To attend a conference'
    ATTEND_LANGUAGE_TRAINING = 'To attend language training'
    REPRESENT_MEMBER_EVENT = 'To represent the Member at an event'
    CONDUCT_CONSTITUENCY_OFFICE_BUSINESS = 'To conduct constituency office business'
    TRIP_CANCELLATION_EXPENSES = 'Trip cancellation expenses due to unforeseen circumstance'
    MAPLE_LEAF_LOUNGE = 'Maple Leaf Lounge'
    TRANSPORTATION = 'Transportation'
    ATTEND_REGIONAL_PROVINCIAL_CAUCUS_MEETING = 'To attend a regional or provincial caucus meeting'
    ATTEND_NATIONAL_CAUCUS_MEETING = 'To attend a national caucus meeting'
    UNITE_FAMILY_MEMBER = 'To unite the family with the Member'
    ATTEND_AUTHORIZED_FUNERAL = 'To attend an authorized funeral'
    ATTEND_EVENT_MEMBER = 'Attending event with Member'
    PARTICIPATE_GOVERNMENT_PROGRAM_INITIATIVE = 'To participate in a government program initiative'
    ATTEND_EVENT_GUEST_SPEAKER = 'To attend an event as a guest speaker'
    SUPPORT_PARLIAMENTARY_ASSOCIATION = 'To support a parliamentary association'
    ATTEND_MEETINGS_UN_OFFICIALS_NYC = 'To attend meetings with United Nations’ officials in New York City'
    SUPPORT_PARLIAMENTARY_COMMITTEE = 'To support a parliamentary committee'
    SECONDARY_RESIDENCE = 'Secondary Residence'
    DRIVE_MEMBER_AIRPORT_TERMINAL = 'To drive Member to/from terminal'
    ATTEND_MEETINGS_STAKEHOLDERS_BUSINESS_HOUSE = 'To attend meetings with stakeholders about business of the House'
    ATTEND_CONSTITUENCY_EVENT = 'To attend a constituency event'
    SUPPORT_HOUSE_OFFICER = 'To support the House Officer'
    SUPPORT_MEMBER_NATIONAL_CAUCUS_RESEARCH = 'To support the Member responsible of the National Caucus Research Office'
    ATTEND_AN_EVENT = 'To attend an event'
    ATTEND_BUSINESS_MEETING = 'To attend a business meeting'
    TRAVEL_BETWEEN_CONSTITUENCY_AND_OTTAWA = 'To travel to/from constituency and Ottawa'
    ACCOMMODATION = 'Accommodation'


class TravellerType(Enum):
    MEMBER = 'Member'
    DESIGNATED_TRAVELLER = 'Designated Traveller'
    EMPLOYEE = 'Employee'
    DEPENDANT = 'Dependant'
    HOUSE_OFFICER_EMPLOYEE = 'House Officer Employee'
    PARLIAMENTARY_INTERN = 'Parliamentary Intern'
    HOUSE_OFFICER = 'House Officer'
