import csv
from urllib.parse import unquote
from models import TravelClaim, TravelEvent


def transform_gov_travel_expenditures_csv_to_supabase_csvs() -> None:
    claim_writer = csv.writer(open('etl/data/travel_claims.csv', 'w'))
    event_writer = csv.writer(open('etl/data/travel_events.csv', 'w'))
    with open('etl/data/gov_travel_expenditures.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')

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


def get_members():
    with open('members.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)
        return [row[0] for row in csvreader]






#         for location in reader:
#             if location == new_location:
#                 break

#             import pdb

#             pdb.set_trace()

#     csvwriter = csv.writer(csvfile_2)
#     reader = csv.reader(csvfile)
#     csvwriter.writerow(next(reader))

#     for row in reader:
#         del row[1]
#         del row[1]
#         csvwriter.writerow(row)

# with open('etl/data/travel_claims.csv', 'r') as csvfile:
#     vals = list(csv.reader(csvfile))

#     # get duplicates of first index, sort them first

#     vals.sort(key=lambda x: x[0])
#     duplicates = []
#     for i in range(len(vals) - 1):
#         if vals[i][0] == vals[i + 1][0]:
#             if vals[i][-2] > vals[i + 1][-2]:
#                 duplicates.append(vals[i + 1])
#             else:
#                 duplicates.append(vals[i])

#     with open('etl/data/travel_claims_2.csv', 'w') as csvfile_2:
#         csvwriter = csv.writer(csvfile_2)
#         with open('etl/data/travel_claims.csv', 'r') as csvfile_3:
#             for row in csv.reader(csvfile_3):
#                 if row not in duplicates:
#                     csvwriter.writerow(row)
#                 else:
#                     duplicates.remove(row)

# with open('etl/data/gov_travel_expenditures.csv', 'r') as csvfile:
#     csvreader = csv.reader(csvfile)

#     with open('gov_travel_expenditures_filtered.csv', 'w') as csvfile_2:
#         csvwriter = csv.writer(csvfile_2)
#         next(csvreader)
#         members = get_members()
#         for row in csvreader:
#             row = [unquote(cell) for cell in row]
#             for member in members:
#                 if 'Hon.' in member:
#                     last_name = member.split(',')[0]
#                     first_name = member.split('Hon.')[1].strip()

#                     if f'{last_name}, {first_name}' == row[3]:
#                         print(f"changing {row[-1]} to {member}")

#                         row[3] = member
#                     if f'{last_name}, {first_name}' == row[-1]:
#                         print(f"changing {row[-1]} to {member}")

#                         row[-1] = member

#             csvwriter.writerow(row)


def create_members_csv():
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


# create_members_csv()
