from pyspark.sql import DataFrame

from analysis.dataframe_loader import (
    initialize_locations_dataframe,
    initialize_travel_dataframe,
)
from analysis.flight_identification import identify_flight_travel_events


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
        travel_df.destination_nearest_airport.isNotNull()
        & travel_df.departure_nearest_airport.isNotNull()
    )

    return travel_df


if __name__ == '__main__':
    locations_df = initialize_locations_dataframe()
    travel_df = initialize_travel_dataframe()

    travel_with_locations_df = map_locations_to_flights(locations_df, travel_df)
    df = identify_flight_travel_events(travel_with_locations_df)
    breakpoint()


# with open('locations.csv', 'w') as f:
#     csv_writer = csv.writer(f)
#     for location in locations:

#         for airport in airports:
#             if (
#                 airport.iata == location.nearest_airport
#                 or location.nearest_airport == airport.icao
#             ):
#                 vals = location.model_dump(mode='json') | {'nearest_airport_icao': airport.icao}
#                 csv_writer.writerow(vals.values())
#                 break
#         else:
#             closest_airport, closest_distance = None, None
#             for airport in airports:
#                 distance = calc_distance_between_coordinates(
#                     departure_lat=airport.latitude,
#                     departure_lon=airport.longitude,
#                     destination_lat=location.latitude,
#                     destination_lon=location.longitude,
#                 )
#                 if not closest_airport:
#                     closest_airport, closest_distance = airport, distance

#                 elif distance < closest_distance:
#                     closest_airport, closest_distance = airport, distance

#             vals = location.model_dump(mode='json') | {
#                 'nearest_airport_icao': closest_airport.icao
#             }
#             csv_writer.writerow(vals.values())
# TODO map travel events to location ids
