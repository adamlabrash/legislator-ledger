import csv
from typing import Any
from urllib.parse import unquote

from models import TravelClaim, TravelEvent


'''
transform the gov_travel_expenditures.csv file into travel_claims and travel_events csv files.
travel_claims.csv and travel_events.csv break down the data from gov_travel_expenditures.csv into two tables that can be joined on claim_id.
These two tables are how the data relationship is represented in the supabase database for the web application.
'''


# TODO clean this up -> use data models
# TODO -> there should be no negative numbers in the data -> abs()


def transform_gov_travel_expenditures_csv_to_supabase_csv_format() -> None:
    claim_writer = csv.writer(open('etl/data/travel_claims.csv', 'w'))
    event_writer = csv.writer(open('etl/data/travel_events.csv', 'w'))
    with open('etl/data/gov_travel_expenditures.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')

        # initialize csv file headers
        claim_writer.writerow([key for key in TravelClaim.model_fields.keys()])
        event_writer.writerow([key for key in TravelEvent.model_fields.keys()])

        next(csvreader)  # skip header
        for row in csvreader:
            row = [unquote(cell) for cell in row]
            if any(row[9:16]):
                del row[3:9]
                claim_writer.writerow(row)
            elif any(row[3:9]):
                del row[9:16]
                event_writer.writerow(row)


def create_members_csv() -> set[Any]:
    with open('members.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Member'])
        members = set()
        with open('etl/data/travel_events.csv', 'r') as csvfile_2:
            csvreader = csv.reader(csvfile_2, delimiter=',')
            next(csvreader)
            for row in csvreader:
                row = [unquote(cell) for cell in row]
                if row[4] == 'Member':
                    members.add(row[3])

        for member in members:
            csvwriter.writerow([member])
    return members
