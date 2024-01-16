from pyspark.sql import DataFrame
from haversine import haversine
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


def identify_flight_travel_events(travel_df: DataFrame) -> DataFrame:
    '''
    This is the algorithm for identifying flights from the travel dataset.

    In order to be identified as a flight, a travel event must meet all of the following criteria:
    - Departure and destination locations are not the same
    - The distance between locations is greater than 125km
    - The distance between airports is greater than 125km
    - The total distance from the departure/destination locations to their nearest airports must be less than half of The total distance travelled.
    - The average cost per km travelled must be over a certain threshold


    Future improvements:
    -Compare costs of identical travel events across the full dataset and identify drop off price for flights & drives (account for inflation)
    -look at flight time vs driving time estimations
    -all travel within Newfoundland is considered drives, with one or two exceptions
    -better differentiate costs between flights and drives in the same travel claim
    -Compare driving cost estimations to flight cost estimations of same travel event.
    '''
    travel_df = filter_travel_events_with_no_locations(travel_df)
    travel_df = filter_departures_and_destinations_with_same_airport(travel_df)
    travel_df = filter_travel_distances_under_minimum_km(travel_df)
    travel_df = filter_events_with_proportionately_high_distance_to_airports(travel_df)
    # travel_df = filter_cost_per_km_under_threshold(travel_df)

    travel_df = travel_df.cache()
    travel_df.show(vertical=True, n=10)
    # T0214944 -> test case
    # T0215029 -> test case

    return travel_df


def filter_travel_events_with_no_locations(travel_df: DataFrame) -> DataFrame:
    return travel_df.filter((travel_df.departure != '') | (travel_df.destination != ''))


def filter_departures_and_destinations_with_same_airport(travel_df: DataFrame) -> DataFrame:
    travel_df = travel_df.filter(travel_df.departure != travel_df.destination)
    return travel_df.filter(travel_df.departure_nearest_airport != travel_df.destination_nearest_airport)


@udf(returnType=FloatType())
def calc_distance_between_coordinates(
    departure_lat: float, departure_lon: float, destination_lat: float, destination_lon: float
) -> float:
    return haversine((departure_lat, departure_lon), (destination_lat, destination_lon))


def filter_travel_distances_under_minimum_km(travel_df: DataFrame) -> DataFrame:
    travel_df = travel_df.withColumn(
        'distance_between_locations',
        calc_distance_between_coordinates(
            travel_df.departure_latitude,
            travel_df.departure_longitude,
            travel_df.destination_latitude,
            travel_df.destination_longitude,
        ),
    )
    # exclude travel events where distance travelled between locations is less than 125km
    travel_df = travel_df.filter(travel_df.distance_between_locations > 125)

    travel_df = travel_df.withColumn(
        'distance_between_airports',
        calc_distance_between_coordinates(
            travel_df.departure_nearest_airport_latitude,
            travel_df.departure_nearest_airport_longitude,
            travel_df.destination_nearest_airport_latitude,
            travel_df.destination_nearest_airport_longitude,
        ),
    )

    # exclude travel events where distance travelled between airports is less than 125km
    return travel_df.filter(travel_df.distance_between_airports > 125)


def filter_cost_per_km_under_threshold(travel_df: DataFrame) -> DataFrame:
    return travel_df
    # TODO
    # exclude travel events where average cost per km is less than $0.5
    # thresholds will be different for km travelled
    # assume eliminated travel events are drives -> look at difference

    # travel_df = travel_df.withColumn(
    #     'avg_cost_per_event', travel_df.transport_cost / travel_df.total_travel_events_in_claim
    # )
    # return travel_df.filter(travel_df.avg_cost_per_event > 100)


def filter_events_with_proportionately_high_distance_to_airports(travel_df: DataFrame) -> DataFrame:
    # exclude where distance travelling to airports is greater than half of total distance travelled
    return travel_df.filter(
        travel_df.destination_distance_to_airport + travel_df.departure_distance_to_airport
        < travel_df.distance_between_locations / 2
    )
