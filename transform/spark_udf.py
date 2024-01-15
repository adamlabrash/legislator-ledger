from enum import Enum
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, FloatType
from transform.carbon_calculator.carbon_flight import CarbonFlight
from haversine import haversine
from geopy.location import Location as GeoLocation
from geopy.geocoders import Nominatim


@udf(returnType=StringType())
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
        .replace('Sainte', 'Saint')  # Replace ste with st? Saint with st?
        .upper()
        .strip()
    )


@udf(returnType=IntegerType())
def calc_carbon_emissions(departure_airport: str, destination_airport: str, passenger_class: str) -> int:
    return CarbonFlight().calculate_co2_from_airports(
        departure_airport, destination_airport, passenger_class, trip_type='one-way'
    )


@udf(returnType=FloatType())
def calc_distance_between_coordinates(
    departure_lat: float, departure_lon: float, destination_lat: float, destination_lon: float
) -> float:
    return haversine((departure_lat, departure_lon), (destination_lat, destination_lon))


@udf(returnType=StringType())
def calc_passenger_class(distance_travelled: float, traveller_type: str) -> str:
    # https://www.ourcommons.ca/Content/MAS/mas-e.pdf chapter 6, pg 8
    if (
        distance_travelled < 1274  # 1274km = 2hr flight time #TODO reduced vs full fare economy
        or traveller_type == 'Employee'
        or traveller_type == 'Parliamentary Intern'
        or traveller_type == 'House Officer Employee'
    ):
        return 'economy-class'
    return 'business-class'


geo_api = Nominatim(user_agent="GetLoc")


@udf()
def get_geo_api_location_data(location_str: str) -> dict | None:
    geo_location: GeoLocation = geo_api.geocode(location_str, country_codes='CA', exactly_one=True)  # type: ignore
    if geo_location is None:
        print("Unable to find location for:", location_str)
        return None
    return {
        'name': location_str,
        'latitude': geo_location.latitude,
        'longitude': geo_location.longitude,
        'address': geo_location.address,
    }
