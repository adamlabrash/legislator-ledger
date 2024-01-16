from extraction.expenditures.items import ExpenditureItem, MemberTravelClaim
from transform.items import Location
import csv
import json
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()


@udf(returnType=StringType())
def normalize_location_str(
    location: str,
) -> str:  # for matching expenditure locations with existing locations in dataset
    return (
        location.replace('-', ' ')
        .replace(' (Ville)', '')
        .replace(' (City / Ville)', '')
        .replace(' (Village)', '')
        .replace(' (District Municipality / Municipalité De District)', '')
        .replace('é', 'e')
        .replace('Sainte', 'Saint')  # Replace ste with st? Saint with st?
        .upper()
        .strip()
    )


def initialize_travel_dataframe() -> DataFrame:
    '''
    Loads, flattens and validates data from raw expenditures json file
    '''

    with open('expenditures.json', encoding='utf-8-sig') as f:
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
        'claim', 'category', 'travel_events', 'accommodation_cost', 'meals_and_incidentals_cost', 'year', 'quarter'
    )

    # add normalized location columns
    travel_df = travel_df.withColumn('departure_normalized', normalize_location_str(travel_df.departure))
    travel_df = travel_df.withColumn('destination_normalized', normalize_location_str(travel_df.destination))

    return travel_df


def initialize_locations_dataframe() -> DataFrame:
    locations = []
    reader = csv.reader(open('transform/data/locations.csv', 'r'))
    next(reader)  # skip header
    for row in reader:
        location = Location.from_csv_row(row)
        locations.append(location.model_dump())

    locations_df = spark.createDataFrame(data=locations)

    # add normalized location column
    locations_df = locations_df.withColumn('location_normalized', normalize_location_str(locations_df.location))
    return locations_df


def initialize_airport_dataframe() -> DataFrame:
    airports = []
    with open('transform/carbon_calculator/sources/airports.json') as file:
        json_dict = json.loads(file.read())
        for value in json_dict.values():
            value['airport_longitude'] = float(value['lonlat'][0])
            value['airport_latitude'] = float(value['lonlat'][1])
            del value['lonlat']
            airports.append(value)

    airport_df = spark.createDataFrame(data=airports)

    # NOTE change this when there are international flights
    airport_df.filter(airport_df.icao_region_code != 'NARNAS')

    return airport_df
