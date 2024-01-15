import csv
from pyspark.sql import DataFrame
from transform.dataframe_loader import (
    initialize_airport_dataframe,
    initialize_travel_dataframe,
    initialize_locations_dataframe,
)
from transform.missing_locations import update_locations_dataset
from transform.spark_udf import (
    calc_carbon_emissions,
    calc_distance_between_coordinates,
    calc_passenger_class,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def filter_non_flight_travel_events(travel_df: DataFrame) -> DataFrame:
    '''
    This is the algorithm that looks at a travel event and determines if it was a flight or not

    Future improvements:
    -Go through travel events -> estimate cost per event with driving
    -
    '''

    # no locations reported
    travel_df = travel_df.filter((travel_df.departure != '') | (travel_df.destination != ''))

    # locations are the same -> not a flight
    travel_df = travel_df.filter(travel_df.departure != travel_df.destination)

    # TODO fix this
    travel_df = travel_df.withColumn(
        'distance_between_airports',
        calc_distance_between_coordinates(
            travel_df.departure_latitude,
            travel_df.departure_longitude,
            travel_df.destination_latitude,
            travel_df.destination_longitude,
        ),
    )

    # exclude travel events where distance travelled is less than 125km
    travel_df = travel_df.filter(travel_df.distance_between_airports > 125)

    # TODO
    # exclude travel events where distance between destination and nearest airport is greater than total distance travelled/3
    # travel_df = travel_df.filter(travel_df.departure_nearest_airport > travel_df.distance_travelled)

    # exclude travel events where distance between departure and nearest airport is greater than total distance travelled/3
    # travel_df = travel_df.filter(travel_df.destination_nearest_airport > travel_df.distance_travelled)

    travel_df = travel_df.withColumn(
        'avg_cost_per_event', travel_df.transport_cost / travel_df.total_travel_events_in_claim
    )
    travel_df = travel_df.filter(travel_df.avg_cost_per_event > 100)

    travel_df = travel_df.cache()

    travel_df.show(vertical=True, n=2)
    import pdb

    pdb.set_trace()
    # T0215029 -> test case
    return travel_df


def apply_carbon_calculations_to_flight_data(travel_df: DataFrame) -> DataFrame:
    # calculate passenger flight class
    travel_df = travel_df.withColumn(
        'passenger_class', calc_passenger_class(travel_df.distance_between_airports, travel_df.traveller_type)
    )

    # calculate carbon emissions
    travel_df = travel_df.withColumn(
        'est_carbon_emissions',
        calc_carbon_emissions(
            travel_df.departure_nearest_airport, travel_df.destination_nearest_airport, travel_df.passenger_class
        ),
    )

    return travel_df


def map_locations_to_flights(locations_df: DataFrame, travel_df: DataFrame) -> DataFrame:
    # departures
    travel_df = travel_df.join(
        locations_df, travel_df.departure_normalized == locations_df.location_normalized, how='left_outer'
    )
    for location_col in locations_df.columns:
        travel_df = travel_df.withColumnRenamed(location_col, f'departure_{location_col}')

    # destinations
    travel_df = travel_df.join(
        locations_df, travel_df.destination_normalized == locations_df.location_normalized, how='left_outer'
    )
    for location_col in locations_df.columns:
        travel_df = travel_df.withColumnRenamed(location_col, f'destination_{location_col}')

    # remove flights with no geolocation data
    travel_df = travel_df.filter(
        travel_df.destination_nearest_airport.isNotNull() & travel_df.departure_nearest_airport.isNotNull()
    )

    return travel_df


def update_csv_file(file_name: str, df: DataFrame):
    with open(f'{file_name}.csv', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(df.columns)
        for row in df.collect():
            csv_writer.writerow(row)


if __name__ == '__main__':
    travel_df = initialize_travel_dataframe()
    locations_df = initialize_locations_dataframe()
    locations_df = update_locations_dataset(travel_df, locations_df)
    update_csv_file('locations', locations_df)

    # travel_df = map_locations_to_flights(locations_df, travel_df)
    # travel_df = filter_non_flight_travel_events(travel_df)
    # travel_df = apply_carbon_calculations_to_flight_data(travel_df)

    # TODO airplane csv file,
    # TODO locations csv file
    # TODO flight csv file
