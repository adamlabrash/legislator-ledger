from analysis.base_data_loader import load_locations_csv, load_member_travel_claims_from_csv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()


def initialize_locations_dataframe() -> DataFrame:
    '''
    Load locations.csv dataset into a spark dataframe
    '''
    locations_list = list(load_locations_csv())
    locations_df = spark.createDataFrame(data=locations_list)

    # add normalized location column
    locations_df = locations_df.withColumn(
        'location_normalized', _normalize_location_str(locations_df.location)
    )
    return locations_df


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
    travel_df = _add_normalized_location_cols(travel_df)
    return travel_df


def _add_normalized_location_cols(travel_df: DataFrame) -> DataFrame:
    travel_df = travel_df.withColumn(
        'departure_normalized', _normalize_location_str(travel_df.departure)
    )
    travel_df = travel_df.withColumn(
        'destination_normalized', _normalize_location_str(travel_df.destination)
    )
    return travel_df


@udf(returnType=StringType())
def _normalize_location_str(location: str) -> str:
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
