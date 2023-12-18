import csv
import json
from geopy.geocoders import Nominatim
from pydantic import BaseModel, model_validator
from haversine import haversine

'''
Maps location data and nearest airport to MP travel events
'''

def extract_unique_locations_from_travel_events_csv() -> set[str]:
    locations = set()
    with open('data/travel_events.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        next(csvreader)  # skip header
        for row in csvreader:
            # trip cancellation events have no location
            if row[-2] == '' or row[-3] == '':
                continue

            locations.add(row[-2])
            locations.add(row[-3])

    return locations


loc = Nominatim(user_agent="GetLoc")


def get_location_data_of(location: str) -> dict:
    # if location == "Saint George's":
    #     location = "Saint George's newfoundland"
    try:
        return loc.geocode(location, country_codes='CA', exactly_one=True)
    except Exception as e:
        print(f"ERROR GETTING LOCATION DATA: {location}", e)
        # import pdb

        # pdb.set_trace()
        return None


class Airport(BaseModel):
    iata_code: str
    lat: float
    lon: float
    name: str

    @model_validator(mode='before')
    @classmethod
    def model_validate(cls, data: dict) -> dict:
        data['lon'], data['lat'] = data['lonlat'][0], data['lonlat'][1]
        return data

    def distance_from_coordinates(self, lat: float, lon: float) -> float:
        return haversine((self.lat, self.lon), (lat, lon))


def get_closest_airport_to_coordinates(airports: list[Airport], lat: float, lon: float) -> Airport:
    closest_airport = min(airports, key=lambda airport: airport.distance_from_coordinates(lat, lon))
    return closest_airport


def get_airport_list() -> list[Airport]:
    airports = []
    with open('carbon_calculator/sources/airport.json') as file:
        data = json.load(file)
        for airport in data.values():
            if airport['icao_region_code'] != 'NARNAS':
                continue

            airports.append(Airport(**airport))

    return airports


def get_airports_of_travel_event_locations():
    airports = get_airport_list()
    with open('locations.csv', 'a') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(['location', 'latitude', 'longitude', 'nearest_airport', 'full_address'])
        for location in extract_unique_locations_from_travel_events_csv():
            if location_data := get_location_data_of(location):
                closest_airport = get_closest_airport_to_coordinates(
                    airports, location_data.latitude, location_data.longitude
                )
                csvwriter.writerow(
                    [
                        location,
                        location_data.latitude,
                        location_data.longitude,
                        closest_airport.iata_code,
                        location_data.address,
                    ]
                )
            else:
                print(f"UNABLE TO FIND DATA FOR {location}")


airports = get_airport_list()

locations = set()
with open('locations.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        locations.add(row[0])


locations_new = []
with open('etl/data/travel_events_2.csv', 'r') as eventsfile:
    eventsreader = csv.reader(eventsfile)
    next(eventsreader)
    missing_locations = set()
    for row in eventsreader:
        if row[-2] not in locations:
            missing_locations.add(row[-2])
        if row[-3] not in locations:
            missing_locations.add(row[-3])
    import pdb

    pdb.set_trace()

    for location in missing_locations:
        print(f'Processing location: {location}')
        if location:
            if location_data := get_location_data_of(location):
                closest_airport = get_closest_airport_to_coordinates(
                    airports, location_data.latitude, location_data.longitude
                )
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


# TODO -> manually check airports are correct for major locations


# fogo island region
#'Central Waterville'
# 'Seine River Indian Reserve 22A2'
# "Saint George's"
#'Central Waterville'
# Thompson (City / Ville)

# TODO manually add locations of flights that don't have locations
# TODO make OTTAWA lowercase

# St. John's
# North Vancouver (City / Ville)
# Norris Arm North Side
# Sandy Point (Norris Arm)
# Roblin
# saint george
# Montreal
# Old Massett
# Hawke's Bay
# Pidgeon Cove-St Barbe
# Saint-Isidore-de-Laprairie
# Cape Saint Mary's
# Mayne Island Indian Reserve 6
# Charlottetown (Eagle River)
# Lytton Indian Reserve 13A
# Scowlitz Indian Reserve 1
# Big White
# Elsipogtog First Nation
# RAWD
# Bathurst (Parish / Paroisse)
# Saint-Joseph-de-Blandford
# new Bandon Northumb Co
# Paradise Valley
# Two Hills County No. 21
# Constance Lake Indian Reserve 92
# Ways Mills
# 127 Mile House
# Moose Lake (The Pas)
# Paintearth County No. 18
# Saint Jacques-Coomb's Cove
# Kanaka Bar Indian Reserve 1A
# Saint Vincent's-St. Stephen's-Peter's River
