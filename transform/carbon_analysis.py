from transform.carbon_calculator.carbon_flight import CarbonFlight
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame


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


calculator = CarbonFlight()


@udf(returnType=IntegerType())
def calc_carbon_emissions(departure_airport: str, destination_airport: str, passenger_class: str) -> int:
    return calculator.calculate_co2_from_airports(
        departure_airport, destination_airport, passenger_class, trip_type='one-way'
    )
