import csv
from typing import Iterator, Set

import json
from shared_models.items import ExpenditureItem, MemberTravelClaim
from geopy.geocoders import Nominatim
from geopy.location import Location
from haversine import haversine
from pydantic import BaseModel, model_validator

'''
This script extracts the unique locations from the travel expenditure reports 
and then uses the geopy library to find the nearest airport to each location. The results are written to a locations.csv file (which is ultimately uploaded to supabase web application).
This location data is also used with the carbon_calculator module to calculate the carbon footprint of each travel event.
'''


class Airport(BaseModel):
    iata_code: str
    lat: float
    lon: float
    name: str

    @model_validator(mode='before')
    @classmethod
    def model_validate(cls, data: dict) -> dict:
        data['lon'], data['lat'] = data['lonlat'][0], data['lonlat'][1]
        return data

    def distance_from_coordinates(self, lat: float, lon: float) -> float:
        return haversine((self.lat, self.lon), (lat, lon))


def get_unique_locations_from_travel_expenditures() -> Set[str]:
    # load json data
    with open(file='transform/expenditures.json', encoding='utf-8-sig') as json_file:
        data = json.load(json_file)

    locations = set()
    for row in data:
        expenditure = ExpenditureItem.model_validate(row)
        if isinstance(expenditure.claim, MemberTravelClaim):
            locations.update(expenditure.claim.locations_traveled)
    return locations


def load_airport_data_from_json_file() -> list[Airport]:
    airports = []
    with open('transform/emissions_analysis/carbon_calculator/sources/airport.json') as file:
        data = json.load(file)
        for airport in data.values():
            if airport['icao_region_code'] != 'NARNAS':
                continue

            airports.append(Airport(**airport))

    return airports


# TODO check for existing locations.csv file, only get locations that are missing
def get_location_data_from_travel_events_csv() -> Iterator[Location]:
    geo_api = Nominatim(user_agent="GetLoc")
    airport_data = load_airport_data_from_json_file()

    for location in get_unique_locations_from_travel_expenditures():
        location_data: Location | None = geo_api.geocode(location, country_codes='CA', exactly_one=True)  # type: ignore
        if location_data is None:
            # TODO further validation, multithreading, flag missing
            # replace brackets, and try again etc
            continue

        closest_airport_to_location = min(
            airport_data,
            key=lambda airport: airport.distance_from_coordinates(location_data.latitude, location_data.longitude),
        )
        yield [
            location,
            location.latitude,
            location.longitude,
            location.address,
            closest_airport_to_location.iata_code,
        ]


# TODO init csv rows
def build_locations_csv_from_travel_expenditures():
    locations_csv_writer = csv.writer(open('transform/locations.csv', 'w'))
    for location_row in get_location_data_from_travel_events_csv():
        locations_csv_writer.writerow(location_row)
