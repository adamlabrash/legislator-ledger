import csv
import json
from typing import Iterator

from geopy.geocoders import Nominatim
from geopy.location import Location
from haversine import haversine
from pydantic import BaseModel, model_validator

'''
This script extracts the unique locations from the travel expenditure reports 
and then uses the geopy library to find the nearest airport to each location. The results are written to a locations.csv file.
This location data is then used with the carbon_calculator module to calculate the carbon footprint of each travel event.
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


def extract_unique_locations_from_travel_events_csv() -> set[str]:
    locations = set()
    with open('nextjs/data/travel_events_2.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        next(csvreader)  # skip header
        for row in csvreader:
            # trip cancellation events have no location
            if row[-2] == '' or row[-3] == '':
                continue

            locations.add(row[-2])
            locations.add(row[-3])

    return locations


def load_airport_data_from_json_file() -> list[Airport]:
    airports = []
    with open('emissions_analysis/carbon_calculator/sources/airport.json') as file:
        data = json.load(file)
        for airport in data.values():
            if airport['icao_region_code'] != 'NARNAS':
                continue

            airports.append(Airport(**airport))

    return airports


def get_location_data_from_travel_events_csv() -> Iterator[Location]:
    geo_api = Nominatim(user_agent="GetLoc")

    for location in extract_unique_locations_from_travel_events_csv():
        if location_data := geo_api.geocode(location, country_codes='CA', exactly_one=True):
            yield location_data


def map_airport_data_to_travel_event_locations() -> Iterator[list[str]]:
    airports = load_airport_data_from_json_file()
    for location in get_location_data_from_travel_events_csv():
        closest_airport = min(
            airports, key=lambda airport: airport.distance_from_coordinates(location.latitude, location.longitude)
        )

        yield [
            location,
            location.latitude,
            location.longitude,
            closest_airport.iata_code,
            location.address,
        ]


# if __name__ == '__main__':
#     with open('locations.csv', 'a') as csvfile:
#         csvwriter = csv.writer(csvfile, delimiter=',')
#         csvwriter.writerow(['location', 'latitude', 'longitude', 'nearest_airport', 'full_address'])

#         for airport_data in map_airport_data_to_travel_event_locations():
#             csvwriter.writerow(airport_data)


import csv


def get_extracted_locations():
    locations = set()
    with open('emissions_analysis/locations.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            locations.add(row[0])
    return locations


# e_locs = extract_unique_locations_from_travel_events_csv()
# locs = get_extracted_locations()

# # TODO start here and manually add remaining locations
# for loc in e_locs:
#     if loc not in locs:
#         print(loc)
