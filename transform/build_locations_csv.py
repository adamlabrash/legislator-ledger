import csv
from typing import Iterator, Set

import json
from shared_models.items import ExpenditureItem, MemberTravelClaim
from geopy.geocoders import Nominatim
from geopy.location import Location
from pydantic import BaseModel, model_validator

'''
This script extracts the unique locations from the travel expenditure reports 
and then uses the geopy library to find the nearest airport to each location. The results are written to a locations.csv file (which is ultimately uploaded to supabase web application).
This location data is also used with the carbon_calculator module to calculate the carbon footprint of each travel event.
'''


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
