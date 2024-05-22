import csv
import json
from typing import Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from analysis.items import Location
from extraction.items import ExpenditureItem, MemberTravelClaim

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


def initialize_travel_dataframe() -> DataFrame:
    '''
    Loads, flattens and validates data from raw expenditures json file
    '''

    with open('expenditures_test.json', encoding='utf-8-sig') as f:
        expenditure_data = json.load(f)

    travel_expenditures: list[dict] = []
    for expenditure in expenditure_data:
        expenditure = ExpenditureItem.model_validate(expenditure)
        if isinstance(expenditure.claim, MemberTravelClaim):
            for travel_event_data in expenditure.claim.as_dicts():
                flattened_data = travel_event_data | expenditure.as_dict()
                travel_expenditures.append(flattened_data)

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

    # add normalized location columns
    travel_df = travel_df.withColumn(
        'departure_normalized', normalize_location_str(travel_df.departure)
    )
    travel_df = travel_df.withColumn(
        'destination_normalized', normalize_location_str(travel_df.destination)
    )

    return travel_df


def load_locations_csv() -> Iterator[Location]:
    reader = csv.reader(open('analysis/data/locations.csv', 'r'))
    next(reader)  # skip header
    for row in reader:
        yield Location.from_csv_row(row)


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
