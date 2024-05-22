from pyspark.sql import DataFrame

from analysis.carbon_calculations import calc_distance_between_coordinates

MIN_TRAVEL_DISTANCE_KM = 125
AVERAGE_COST_PER_KM_THRESHOLD = 0.5
TOTAL_COST_THRESHOLD = 200


def identify_flight_travel_events(travel_df: DataFrame) -> DataFrame:
    '''
    This is the algorithm for identifying which travel expenditures are flights (opposed to driving expenditures) from the travel dataset.

    This algorithm is not perfect, specifically for outlier events (ie someone drives 1000km instead of flying), but leans conservatively when predicting flights.

    In order to be identified as a flight, a travel event must meet ALL of the following criteria:
    - Departure and destination locations are not the same
    - The distance between locations is greater than 125km
    - The distance between airports is greater than 125km
    - Where the distance driving to airports is greater than half of total distance travelled.
    - The total distance from the departure/destination locations to their nearest airports must be less than half of the total distance travelled.

    Airports are determined by matching the geo-cordinates of the expenditure location to the nearest non-military/research, public airport.

    Future improvements:
    -Look at cost per km driving vs flight
    -Compare costs of identical travel events across the full dataset and identify drop off price for flights & drives (account for inflation)
    -look at flight time vs driving time estimations
    -all travel within Newfoundland is considered drives, with one or two exceptions
    -better differentiate costs between flights and drives in the same travel claim
    -Compare driving cost estimations to flight cost estimations of same travel event.
    -Look at available departure/destinations of the airports/airlines
    '''

    travel_df = filter_travel_events_with_no_locations(travel_df)
    travel_df = filter_departures_and_destinations_with_same_airport(travel_df)
    travel_df = filter_travel_distances_under_minimum_km(travel_df)
    travel_df = filter_events_with_proportionately_high_distance_to_airports(travel_df)

    travel_df = travel_df.cache()
    travel_df.show(vertical=True, n=10)

    return travel_df


def filter_travel_events_with_no_locations(travel_df: DataFrame) -> DataFrame:
    return travel_df.filter((travel_df.departure != '') | (travel_df.destination != ''))


def filter_departures_and_destinations_with_same_airport(travel_df: DataFrame) -> DataFrame:
    travel_df = travel_df.filter(travel_df.departure != travel_df.destination)
    return travel_df.filter(
        travel_df.departure_nearest_airport_icao != travel_df.destination_nearest_airport_icao
    )


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
    travel_df = travel_df.filter(travel_df.distance_between_locations > MIN_TRAVEL_DISTANCE_KM)
    return travel_df


def filter_total_cost_below_threshold(travel_df: DataFrame) -> DataFrame:
    return travel_df.filter(travel_df.transport_cost > TOTAL_COST_THRESHOLD)


def filter_events_with_proportionately_high_distance_to_airports(travel_df: DataFrame) -> DataFrame:
    # exclude where distance travelling to airports is greater than half of total distance travelled
    return travel_df.filter(
        travel_df.destination_distance_to_airport + travel_df.departure_distance_to_airport
        < travel_df.distance_between_locations / 2
    )
