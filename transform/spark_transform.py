import json
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from transform.spark_udf import (
    calc_carbon_emissions,
    calc_distance_between_coordinates,
    calc_passenger_class,
    get_geo_api_location_data,
    normalize_location_str,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

from extraction.expenditures.items import ExpenditureItem, MemberTravelClaim


def initialize_flights_dataframe() -> DataFrame:
    '''
    Loads, flattens and validates data from raw expenditures json file
    '''

    with open('transform/result.json', encoding='utf-8-sig') as f:
        expenditure_data = json.load(f)

    travel_expenditures: list[dict] = []
    for expenditure in expenditure_data:
        # if expenditure['category'] == 'travel':
        #     for event in expenditure['claim']['travel_events']:
        #         travel_expenditures.append(expenditure | expenditure['claim'] | event)

        expenditure = ExpenditureItem.model_validate(expenditure)
        if isinstance(expenditure.claim, MemberTravelClaim):
            # TODO start here -> make schema/struct or validate -> strings to ints etc
            for travel_event_data in expenditure.claim.as_flattened_dicts():
                flattened_data = travel_event_data | expenditure.model_dump()
                travel_expenditures.append(flattened_data)

    flights_df = spark.createDataFrame(data=travel_expenditures)
    flights_df = flights_df.drop(
        'claim', 'category', 'travel_events', 'accommodation_cost', 'meals_and_incidentals_cost', 'year', 'quarter'
    )

    # add normalized location columns
    flights_df = flights_df.withColumn('departure_normalized', normalize_location_str(flights_df.departure))
    flights_df = flights_df.withColumn('destination_normalized', normalize_location_str(flights_df.destination))

    return flights_df


def initialize_locations_dataframe() -> DataFrame:
    locations_df = spark.read.csv('transform/locations.csv', header=True)

    # add normalized location column
    locations_df = locations_df.withColumn('location_normalized', normalize_location_str(locations_df.location))
    return locations_df


def initialize_airport_dataframe() -> DataFrame:
    airports = []
    with open('transform/emissions_analysis/carbon_calculator/sources/airports.json') as file:
        json_dict = json.loads(file.read())
        for value in json_dict.values():
            value['airport_longitude'] = float(value['lonlat'][0])
            value['airport_latitude'] = float(value['lonlat'][1])
            del value['lonlat']
            airports.append(value)

    airport_df = spark.createDataFrame(data=airports)

    # NOTE change this when there are international flights
    airport_df.filter(airport_df.icao_region_code != 'NARNAS')

    return airport_df


def filter_non_flight_travel_events(flights_df: DataFrame) -> DataFrame:
    flights_df.filter((flights_df.departure != '') | (flights_df.destination != ''))  # no locations reported
    flights_df.filter(flights_df.departure != flights_df.destination)  # locations are the same -> not a flight

    flight_points_were_used = (
        (flights_df.reg_points_used > 0) | (flights_df.special_points_used > 0) | (flights_df.USA_points_used > 0)
    )

    flights_df.filter(flight_points_were_used)

    return flights_df


def get_missing_locations(flights_df: DataFrame, locations_df: DataFrame) -> DataFrame:
    # get expenditure locations with missing geolocation/airport data
    missing_departures = flights_df.join(
        locations_df, flights_df.departure_normalized == locations_df.location_normalized, how='left_anti'
    ).select('departure_normalized')
    missing_destinations = flights_df.join(
        locations_df, flights_df.destination_normalized == locations_df.location_normalized, how='left_anti'
    ).select(col('destination_normalized').alias('location_normalized'))

    # TODO apply additional matching logic
    return missing_destinations.union(missing_departures).distinct()


def get_geo_location_data_of_missing_locations(missing_locations_df: DataFrame) -> DataFrame:
    missing_locations_df = missing_locations_df.withColumn(
        'location_data', get_geo_api_location_data(missing_locations_df.location)
    )

    # TODO flag location values we couldn't find geolocation data for
    missing_locations_df.filter(missing_locations_df.location_data.isNull())
    return missing_locations_df


def map_nearest_airport_to_missing_locations(missing_locations_df: DataFrame, airport_df: DataFrame) -> DataFrame:
    combined_df = missing_locations_df.crossJoin(airport_df)
    combined_df = combined_df.withColumn(
        'distance_to_airport',
        calc_distance_between_coordinates(
            combined_df.latitude, combined_df.longitude, combined_df.airport_latitude, combined_df.airport_longitude
        ),
    )
    window_spec = Window.partitionBy('location')
    combined_df = combined_df.withColumn('min_result', F.min('distance_to_airport').over(window_spec))
    combined_df = combined_df.filter(F.col('distance_to_airport') == F.col('min_result')).drop('min_result')
    combined_df.cache()
    return combined_df


def apply_carbon_calculations_to_flight_data(flights_df: DataFrame) -> DataFrame:
    # calculate distance travelled
    flights_df = flights_df.withColumn(
        'distance_travelled',
        calc_distance_between_coordinates(
            flights_df.departure_latitude,
            flights_df.departure_longitude,
            flights_df.destination_latitude,
            flights_df.destination_longitude,
        ),
    )

    # calculate passenger flight class
    flights_df = flights_df.withColumn(
        'passenger_class', calc_passenger_class(flights_df.distance_travelled, flights_df.traveller_type)
    )

    # calculate carbon emissions
    flights_df = flights_df.withColumn(
        'est_carbon_emissions',
        calc_carbon_emissions(
            flights_df.departure_nearest_airport, flights_df.destination_nearest_airport, flights_df.passenger_class
        ),
    )

    return flights_df


def map_locations_to_flights(locations_df: DataFrame, flights_df: DataFrame) -> DataFrame:
    # departures
    flights_df = flights_df.join(
        locations_df, flights_df.departure_normalized == locations_df.location_normalized, how='left_outer'
    )
    for location_col in locations_df.columns:
        flights_df = flights_df.withColumnRenamed(location_col, f'departure_{location_col}')

    # destinations
    flights_df = flights_df.join(
        locations_df, flights_df.destination_normalized == locations_df.location_normalized, how='left_outer'
    )
    for location_col in locations_df.columns:
        flights_df = flights_df.withColumnRenamed(location_col, f'destination_{location_col}')

    # remove flights with no geolocation data
    flights_df = flights_df.filter(
        flights_df.destination_nearest_airport.isNotNull() & flights_df.departure_nearest_airport.isNotNull()
    )

    return flights_df


if __name__ == '__main__':
    flights_df = initialize_flights_dataframe()
    flights_df = filter_non_flight_travel_events(flights_df)

    locations_df = initialize_locations_dataframe()
    missing_locations_df = get_missing_locations(flights_df, locations_df)
    airport_df = initialize_airport_dataframe()
    new_locations_df = map_nearest_airport_to_missing_locations(missing_locations_df, airport_df)

    locations_df = locations_df.union(new_locations_df).distinct()
    flights_df = map_locations_to_flights(locations_df, flights_df)
    flights_df = apply_carbon_calculations_to_flight_data(flights_df)

    flights_df.write.json('transform/flights.json', mode='overwrite')

    locations_df.write.csv('transform/locations.csv', mode='overwrite', header=True)


# query highest carbon emitters in 2022
# flights_df = flights_df.withColumn('year', year(flights_df.date))
# flights_df = flights_df.filter(flights_df.year == 2022)

# flights_df = flights_df.withColumn('est_carbon_emissions', flights_df['est_carbon_emissions'].cast(IntegerType()))

# result = (
#     flights_df.groupBy('name')
#     .agg(sum(col('est_carbon_emissions')).alias('total_carbon_emissions'))
#     .orderBy(desc('total_carbon_emissions'))
#     .limit(10)
#     .collect()
# )


import pdb

pdb.set_trace()

# flights_df.show(n=1)

'''
BUG fix .title() in expenditures dataset Mcmurray => McMurray
TODO validate values before dumping into dataset
TODO dump values into json dataset & supabase csv files

TODO look at other travel events in claim, determine if flight was used
# is_flight_pattern = (travels_df.distance_travelled > 50) & (travels_df.transport_cost > 200)
# price & distance, min distance of 50km, major airports (?)
'''
