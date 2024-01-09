from enum import Enum
import json
from pyspark.sql import SparkSession
from transform.airport import determine_nearest_airport_to_location
from transform.emissions_analysis.carbon_calculator.carbon_flight import CarbonFlight
from haversine import haversine
from geopy.geocoders import Nominatim
from geopy.location import Location as GeoLocation
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame


class PassengerClass(Enum):
    BUSINESS = 'business-class'
    ECONOMY = 'economy-class'


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
def calc_carbon_emissions(departure_airport: str, destination_airport: str, passenger_class: PassengerClass) -> int:
    return CarbonFlight().calculate_co2_from_airports(
        departure_airport, destination_airport, passenger_class.value, trip_type='one-way'
    )


@udf(returnType=FloatType())
def calc_distance_between_coordinates(
    departure_lat: float, departure_lon: float, destination_lat: float, destination_lon: float
) -> float:
    # TODO fix this
    return haversine((float(departure_lat), float(departure_lon)), (float(destination_lat), float(destination_lon)))


geo_api = Nominatim(user_agent="GetLoc")


def get_geo_api_location_data(location_str: str) -> dict | None:
    geo_location: GeoLocation = geo_api.geocode(location_str, country_codes='CA', exactly_one=True)  # type: ignore
    if geo_location is None:
        print("Unable to find location for:", location_str)
        return None

    closest_airport = determine_nearest_airport_to_location(geo_location.latitude, geo_location.longitude)
    return {
        'name': location_str,
        'latitude': geo_location.latitude,
        'longitude': geo_location.longitude,
        'address': geo_location.address,
        'nearest_airport': closest_airport.iata_code,
    }


spark = SparkSession.builder.getOrCreate()


def create_initial_dataframe() -> DataFrame:  # flatten travel expenditures from json
    with open('expenditures.json', encoding='utf-8-sig') as f:
        expenditure_data = json.load(f)

    travel_expenditures: list[dict] = []
    for expenditure in expenditure_data:
        if expenditure['category'] == 'travel':
            for event in expenditure['claim']['travel_events']:
                travel_expenditures.append(expenditure | expenditure['claim'] | event)

    # TODO make schema/struct or validate -> strings to ints etc
    return spark.createDataFrame(data=travel_expenditures)


def create_locations_dataframe() -> DataFrame:
    return spark.read.csv('transform/locations.csv', header=True)


# create initial travel dataframe and eliminate irrelevant columns
travels_df = create_initial_dataframe()


travels_df = travels_df.drop(
    'claim', 'category', 'travel_events', 'accommodation_cost', 'meals_and_incidentals_cost', 'year', 'quarter'
)
travels_df.filter((travels_df.departure != '') | (travels_df.destination != ''))

# normalize location strings
travels_df = travels_df.withColumn('departure_normalized', normalize_location_str(travels_df.departure))
travels_df = travels_df.withColumn('destination_normalized', normalize_location_str(travels_df.destination))

locations_df = create_locations_dataframe()
locations_df = locations_df.withColumn('location_normalized', normalize_location_str(locations_df.location))


# map departure geolocation data
travels_df = travels_df.join(
    locations_df, travels_df.departure_normalized == locations_df.location_normalized, how='left_outer'
)
for location_col in locations_df.columns:
    travels_df = travels_df.withColumnRenamed(location_col, f'departure_{location_col}')

# map destination geolocation data
travels_df = travels_df.join(
    locations_df, travels_df.destination_normalized == locations_df.location_normalized, how='left_outer'
)
for location_col in locations_df.columns:
    travels_df = travels_df.withColumnRenamed(location_col, f'destination_{location_col}')


# TODO handle remaining null values -> get geopy data
travels_df = travels_df.filter(
    travels_df.destination_nearest_airport.isNotNull() & travels_df.departure_nearest_airport.isNotNull()
)


#### eliminate non-flight travel events

# departure and destination cannot be the same
travels_df.filter(travels_df.departure != travels_df.destination)

flight_points_were_used = (
    (travels_df.reg_points_used > 0) | (travels_df.special_points_used > 0) | (travels_df.USA_points_used > 0)
)

travels_df.filter(flight_points_were_used)
# TODO look at other travel events in claim, determine if flight was used
# is_flight_pattern = (travels_df.distance_travelled > 50) & (travels_df.transport_cost > 200)
# price & distance, min distance of 50km, major airports (?)

# calculate distance travelled
travels_df = travels_df.withColumn(
    'distance_travelled',
    calc_distance_between_coordinates(
        travels_df.departure_latitude,
        travels_df.departure_longitude,
        travels_df.destination_latitude,
        travels_df.destination_longitude,
    ),
)

# TODO estimate time of flight and calculate business/economy class


travels_df = travels_df.withColumn(
    'est_carbon_emissions',
    calc_carbon_emissions(travels_df.departure_nearest_airport, travels_df.destination_nearest_airport),
)


from pyspark.sql.functions import year, col, sum, desc

# query highest carbon emitters in 2022
travels_df = travels_df.withColumn('year', year(travels_df.date))
travels_df = travels_df.filter(travels_df.year == 2022)

travels_df = travels_df.withColumn('est_carbon_emissions', travels_df['est_carbon_emissions'].cast(IntegerType()))

result = (
    travels_df.groupBy('name')
    .agg(sum(col('est_carbon_emissions')).alias('total_carbon_emissions'))
    .orderBy(desc('total_carbon_emissions'))
    .limit(10)
    .collect()
)


import pdb

pdb.set_trace()
# TODO dump values into json dataset & supabase csv files
travels_df.show(n=1)

'''
#TODO fix .title() in expenditures dataset Mcmurray => McMurray
'''
