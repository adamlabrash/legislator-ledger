from functools import lru_cache

from analysis.carbon_calculations import apply_carbon_calculations_to_travel_df
from analysis.dataframe_loader import (
    initialize_locations_dataframe,
    initialize_travel_dataframe,
)
from analysis.flight_identification import identify_flight_travel_events
from geopy.geocoders import Nominatim
from geopy.location import Location as GeoLocation
from pyspark.sql import DataFrame


@lru_cache(maxsize=1)
def get_geo_api_client() -> Nominatim:
    return Nominatim(user_agent="GetLoc")


def get_geo_api_location_data(location_str: str) -> dict | None:
    '''
    Gets latitude and longitude of a give location using open source geo-api.
    This information is used to build the locations.csv. Each travel expenditure should map to a location.
    '''

    geo_api = get_geo_api_client()
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


def join_locations_to_travel_events(locations_df: DataFrame, travel_df: DataFrame) -> DataFrame:
    for col in [travel_df.destination_normalized, travel_df.departure_normalized]:
        travel_df = travel_df.join(
            locations_df, col == locations_df.location_normalized, how='left_outer'
        )

        for location_col in locations_df.columns:
            col_name = col._jc.toString()  # type: ignore

            if col_name == 'departure_normalized':
                travel_df = travel_df.withColumnRenamed(location_col, f'departure_{location_col}')
            elif col_name == 'destination_normalized':
                travel_df = travel_df.withColumnRenamed(location_col, f'destination_{location_col}')
            else:
                raise ValueError(f'Unknown column name: {col.name()}')
    return travel_df


def map_locations_to_flights(locations_df: DataFrame, travel_df: DataFrame) -> DataFrame:
    travel_df = join_locations_to_travel_events(locations_df, travel_df)

    # remove flights with no geolocation data
    travel_df = travel_df.filter(
        travel_df.destination_nearest_airport_icao.isNotNull()
        & travel_df.departure_nearest_airport_icao.isNotNull()
    )

    return travel_df


def write_carbon_calculation_flights_df_to_csv(df: DataFrame) -> None:
    df = df.drop(
        'USA_points_used',
        'reg_points_used',
        'special_points_used',
        'total_travel_events_in_claim',
        'transport_cost',
        'destination_location',
        'departure_location',
        'destination_location_normalized',
        'departure_location',
        'departure_location_normalized',
    )
    df.repartition(1).write.option("delimiter", ",").option("header", "true").csv(
        "analysis/flights.csv"
    )


if __name__ == '__main__':
    locations_df = initialize_locations_dataframe()
    travel_df = initialize_travel_dataframe()

    travel_with_locations_df = map_locations_to_flights(locations_df, travel_df)
    df = identify_flight_travel_events(travel_with_locations_df)
    df = apply_carbon_calculations_to_travel_df(df)
    write_carbon_calculation_flights_df_to_csv(df)
