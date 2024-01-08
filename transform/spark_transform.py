from decimal import Decimal
import json
from typing import Set
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper
import csv
from shared_models.items import TravelEvent
from transform.airport import determine_nearest_airport_to_location
from transform.emissions_analysis.carbon_calculator.carbon_flight import CarbonFlight
from haversine import haversine
from geopy.geocoders import Nominatim
from geopy.location import Location as GeoLocation

from emissions_analysis.carbon_calculator.carbon_flight import CarbonFlight


def normalize_location_str(
    location: str,
) -> str:  # for matching expenditure locations with existing locations in dataset
    return (
        location.replace('-', ' ')
        .replace(' (Ville)', '')
        .replace(' (City / Ville)', '')
        .replace(' (Village)', '')
        .replace(' (District Municipality / Municipalité De District)', '')
        .replace('é', 'e')
        .upper()
        .strip()
    )


class TravelLocation(BaseModel):
    name: str
    latitude: float
    longitude: float
    address: str
    nearest_airport: str

    @classmethod
    def from_csv_row(cls, row: list) -> 'TravelLocation':
        return cls(
            name=row[0],
            latitude=row[1],
            longitude=row[2],
            nearest_airport=row[3],
            address=row[4],
        )

    def distance_from_coordinates(self, lat: float, lon: float) -> float:
        return haversine((self.latitude, self.longitude), (lat, lon))


class Flight(TravelEvent):
    departure: TravelLocation
    destination: TravelLocation
    cost: Decimal

    @property
    def est_carbon_footprint(self) -> int:
        return CarbonFlight().calculate_co2_from_airports(
            self.departure.nearest_airport, self.destination.nearest_airport, 'economy-class', trip_type='one-way'
        )


def get_location_data_from_csv() -> dict[str, TravelLocation]:
    locations: dict[str, TravelLocation] = {}

    with open('transform/locations.csv') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            location = TravelLocation.from_csv_row(row)
            locations[normalize_location_str(location.name)] = location

    return locations


locations_dict = get_location_data_from_csv()


# check if a direct match of normalized string is in locations.csv
def get_direct_match_travel_location(location_str: str) -> TravelLocation | None:
    normalized_location = normalize_location_str(location_str)
    if normalized_location in locations_dict:
        return locations_dict[normalized_location]

    return None


# check if a partial match of original string is in address fields in locations.csv
def get_match_in_address_string(location_str: str) -> TravelLocation | None:
    for _, location in locations_dict.items():
        if location_str in location.address:
            return location


# def get_match_
# for key, address in locations_dict.items():
#     if key in location_str.split(' '):
#         print(location)
#         print(key)
#         print(address)
#         print()
#         return Location.from_csv_row(locations_dict[key])

new_locations: Set[TravelLocation] = set()
geo_api = Nominatim(user_agent="GetLoc")


def get_geo_api_location(location_str: str) -> TravelLocation | None:
    geo_location: GeoLocation = geo_api.geocode(location_str, country_codes='CA', exactly_one=True)
    if geo_location is None:
        print("Unable to find location for:", location_str)
        return None

    closest_airport = determine_nearest_airport_to_location(geo_location.latitude, geo_location.longitude)
    travel_location = TravelLocation(
        name=location_str,
        latitude=geo_location.latitude,
        longitude=geo_location.longitude,
        address=geo_location.address,
        nearest_airport=closest_airport.iata_code,
    )
    new_locations.add(travel_location)


# update locations.csv
with open('transform/locations.csv', 'a') as f:
    writer = csv.writer(f)
    for new_location in new_locations:
        writer.writerow(new_location.model_dump(mode='csv'))


def determine_geo_data_of_location(location_str: str) -> TravelLocation | None:
    return (
        get_direct_match_travel_location(location_str)
        or get_match_in_address_string(location_str)
        or get_geo_api_location(location_str)
    )


with open('result.json', encoding='utf-8-sig') as f:
    expenditure_data = json.load(f)

flights: list[Flight] = []
for expenditure in expenditure_data:
    if expenditure['category'] == 'travel':
        for event in expenditure['claim']['travel_events']:
            if event['departure'] == '' or event['destination'] == '':
                continue
            flight_data = event | {
                'departure': determine_geo_data_of_location(event['departure']),
                'destination': determine_geo_data_of_location(event['destination']),
                'cost': expenditure['claim']['travel_cost'],
            }
            flights.append(Flight.model_validate(flight_data))

# write flight data to json
with open('transform/flights.json', 'a') as f:
    for flight in flights:
        json.dump(flight.model_dump(mode='json'), f)  # TODO include carbon calculations


'''
#TODO fix .title() in expenditures dataset Mcmurray => McMurray
#TODO implement spark
'''

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("result.json", multiLine=True)
# df.printSchema()
# df.show()
import pdb

pdb.set_trace()
df.withColumn('UPPER', upper(df.caucus)).show(n=5)
