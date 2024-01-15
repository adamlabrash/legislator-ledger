from pyspark.sql import DataFrame
from transform.dataframe_loader import initialize_airport_dataframe
from transform.spark_udf import (
    calc_distance_between_coordinates,
    get_geo_api_location_data,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def update_locations_dataset(travel_df: DataFrame, locations_df: DataFrame) -> DataFrame:
    # missing_locations_df = get_locations_not_in_geolocation_dataset(travel_df, locations_df)
    # missing_locations_df = get_geo_location_data_of_missing_locations(missing_locations_df)
    missing_locations_df = map_nearest_airport_to_missing_locations(missing_locations_df)

    missing_locations_df = missing_locations_df.drop('icao_region_code', 'iata_code', 'location_normalized')
    missing_locations_df = locations_df.union(missing_locations_df).distinct()
    return missing_locations_df


def get_locations_not_in_geolocation_dataset(travel_df: DataFrame, locations_df: DataFrame) -> DataFrame:
    missing_departures = travel_df.join(
        locations_df, travel_df.departure_normalized == locations_df.location_normalized, how='left_anti'
    ).select('departure_normalized')
    missing_destinations = travel_df.join(
        locations_df, travel_df.destination_normalized == locations_df.location_normalized, how='left_anti'
    ).select(col('destination_normalized').alias('location_normalized'))

    # TODO apply additional matching logic
    return missing_destinations.union(missing_departures).distinct()


def map_nearest_airport_to_missing_locations(missing_locations_df: DataFrame) -> DataFrame:
    airport_df = initialize_airport_dataframe()
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


def get_geo_location_data_of_missing_locations(missing_locations_df: DataFrame) -> DataFrame:
    missing_locations_df = missing_locations_df.withColumn(
        'location_data', get_geo_api_location_data(missing_locations_df.location)
    )

    # TODO flag location values we couldn't find geolocation data for
    return missing_locations_df.filter(missing_locations_df.location_data.isNull())
