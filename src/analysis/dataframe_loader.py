import csv
import json
from typing import Any, Iterator

from analysis.csv_loader import load_locations_csv, load_member_travel_claims_from_csv
from analysis.items import Location
from extraction.items import ExpenditureItem, MemberTravelClaim
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()


@udf(returnType=StringType())
def normalize_location_str(location: str) -> str:
    return (
        location.replace('-', ' ')
        .replace(' (Ville)', '')
        .replace(' (City / Ville)', '')
        .replace(' (Village)', '')
        .replace(' (District Municipality / Municipalité De District)', '')
        .replace('é', 'e')
        .replace('Sainte', 'Saint')
        .upper()
        .strip()
    )


def add_normalized_location_cols(travel_df: DataFrame) -> DataFrame:

    # add normalized location columns
    travel_df = travel_df.withColumn(
        'departure_normalized', normalize_location_str(travel_df.departure)
    )
    travel_df = travel_df.withColumn(
        'destination_normalized', normalize_location_str(travel_df.destination)
    )
    return travel_df


def initialize_travel_dataframe() -> DataFrame:
    '''
    Loads, flattens and validates data from raw expenditures json file
    '''
    travel_expenditures = load_member_travel_claims_from_csv()
    travel_df = spark.createDataFrame(data=travel_expenditures)
    travel_df = travel_df.drop(
        'claim',
        'category',
        'travel_events',
        'accommodation_cost',
        'meals_and_incidentals_cost',
        'year',
        'quarter',
    )
    travel_df = add_normalized_location_cols(travel_df)
    return travel_df


def initialize_locations_dataframe() -> DataFrame:
    '''
    Load locations.csv dataset into a spark dataframe
    '''
    locations_list = list(load_locations_csv())
    locations_df = spark.createDataFrame(data=locations_list)

    # add normalized location column
    locations_df = locations_df.withColumn(
        'location_normalized', normalize_location_str(locations_df.location)
    )
    return locations_df
