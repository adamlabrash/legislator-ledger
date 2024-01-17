import csv
import json
from pyspark.sql import DataFrame
from transform.carbon_analysis import apply_carbon_calculations_to_flight_data
from transform.dataframe_loader import initialize_travel_dataframe, initialize_locations_dataframe
from transform.flight_identification import identify_flight_travel_events
from transform.location_mapping import update_locations_dataset


def join_locations_to_travel_events(locations_df: DataFrame, travel_df: DataFrame) -> DataFrame:
    for col in [travel_df.destination_normalized, travel_df.departure_normalized]:
        travel_df = travel_df.join(locations_df, col == locations_df.location_normalized, how='left_outer')

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

    print(
        'Num of missing geo locations: ',
        travel_df.filter(
            travel_df.destination_nearest_airport.isNull() | travel_df.departure_nearest_airport.isNull()
        ).count(),
    )
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
    update_csv_file('locations_df', locations_df)
    exit(0)
    travel_df = map_locations_to_flights(locations_df, travel_df)
    travel_df = identify_flight_travel_events(travel_df)

    # truncate travel_df and only keep 100
    travel_df = travel_df.limit(100)

    travel_df = apply_carbon_calculations_to_flight_data(travel_df)

    travel_df = travel_df.drop(
        'departure_location_normalized',
        'destination_location_normalized',
        'destination_normalized',
        'transport_cost',
        'total_travel_events_in_claim',
        'special_points_used',
        'reg_points_used',
        'USA_points_used',
    )
    # write to json file
    with open('flights.json', 'a') as file:
        file.write('[\n')
        for row in travel_df.collect():
            # TODO use Flight Object to validate
            file.write(json.dumps(row.asDict(), ensure_ascii=False, indent=4) + '\n')
        file.write(']')

    # TODO missing geolocation data
    # TODO map travel events to location ids

    # TODO airplane csv file
    # TODO locations csv file
    # TODO flight csv file
    # TODO schema flights
