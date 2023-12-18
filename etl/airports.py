import csv
import json
from typing import Iterator
from geopy.geocoders import Nominatim
from geopy.location import Location
from pydantic import BaseModel, model_validator
from haversine import haversine

'''
This script extracts the unique locations from the travel expenditure reports 
and then uses the geopy library to find the nearest airport to each location. The results are written to a locations.csv file.
This location data is then used with the carbon_calculator module to calculate the carbon footprint of each travel event.
'''


def extract_unique_locations_from_travel_events_csv() -> set[str]:
    locations = set()
    with open('data/travel_events.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        next(csvreader)  # skip header
        for row in csvreader:
            # trip cancellation events have no location
            if row[-2] == '' or row[-3] == '':
                continue

            locations.add(row[-2])
            locations.add(row[-3])

    return locations


loc = Nominatim(user_agent="GetLoc")


def get_geo_data_of_location(location: str) -> Location | None:
    return loc.geocode(location, country_codes='CA', exactly_one=True)


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


def load_airport_data_from_json() -> list[Airport]:
    airports = []
    with open('carbon_calculator/sources/airport.json') as file:
        data = json.load(file)
        for airport in data.values():
            if airport['icao_region_code'] != 'NARNAS':
                continue

            airports.append(Airport(**airport))

    return airports


airports = load_airport_data_from_json()


def get_closest_airport_to_coordinates(lat: float, lon: float) -> Airport:
    return min(airports, key=lambda airport: airport.distance_from_coordinates(lat, lon))


def map_airport_data_to_travel_event_locations() -> Iterator[list[str]]:
    for location in extract_unique_locations_from_travel_events_csv():
        if location_data := get_geo_data_of_location(location):
            closest_airport = get_closest_airport_to_coordinates(location_data.latitude, location_data.longitude)

            yield [
                location,
                location_data.latitude,
                location_data.longitude,
                closest_airport.iata_code,
                location_data.address,
            ]


if __name__ == '__main__':
    with open('locations.csv', 'a') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(['location', 'latitude', 'longitude', 'nearest_airport', 'full_address'])

        for row in map_airport_data_to_travel_event_locations():
            csvwriter.writerow(row)

locations = set()
with open('locations.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        locations.add(row[0])


locations_new = []
with open('etl/data/travel_events_2.csv', 'r') as eventsfile:
    eventsreader = csv.reader(eventsfile)
    next(eventsreader)
    missing_locations = set()
    for row in eventsreader:
        if row[-2] not in locations:
            missing_locations.add(row[-2])
        if row[-3] not in locations:
            missing_locations.add(row[-3])

    for location in missing_locations:
        print(f'Processing location: {location}')
        if location:
            if location_data := get_geo_data_of_location(location):
                closest_airport = get_closest_airport_to_coordinates(
                    airports, location_data.latitude, location_data.longitude
                )
                locations_new.append(
                    [
                        location,
                        location_data.latitude,
                        location_data.longitude,
                        closest_airport.iata_code,
                        location_data.address,
                    ]
                )
try:
    with open('locations.csv', 'a') as locationfile:
        writer = csv.writer(locationfile)
        for location_new in locations_new:
            print(location_new)
            writer.writerow(location_new)
except:
    import pdb

    pdb.set_trace()
