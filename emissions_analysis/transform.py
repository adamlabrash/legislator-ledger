import csv

'''
Script that identifies missing location data
'''


def get_extracted_locations():
    locations = set()
    with open('locations.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            locations.add(row[0])
    return locations


def get_missing_locations():
    locations = get_extracted_locations()
    with open('etl/data/travel_events_2.csv', 'r') as eventsfile:
        eventsreader = csv.reader(eventsfile)
        next(eventsreader)
        missing_locations = set()
        for row in eventsreader:
            if row[-2] not in locations:
                missing_locations.add(row[-2])
            if row[-3] not in locations:
                missing_locations.add(row[-3])
        return missing_locations


locations_new = []


for location in get_missing_locations():
    if location_data := get_geo_data_of_location(location):
        closest_airport = get_closest_airport_to_coordinates(airports, location_data.latitude, location_data.longitude)
        locations_new.append(
            [
                location,
                location_data.latitude,
                location_data.longitude,
                closest_airport.iata_code,
                location_data.address,
            ]
        )
try:
    with open('locations.csv', 'a') as locationfile:
        writer = csv.writer(locationfile)
        for location_new in locations_new:
            print(location_new)
            writer.writerow(location_new)
except:
    import pdb

    pdb.set_trace()
