from functools import lru_cache
import json
from haversine import haversine

from pydantic import BaseModel, model_validator


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


@lru_cache(maxsize=1)
def load_airport_data_from_json_file() -> list[Airport]:
    airports = []
    with open('transform/emissions_analysis/carbon_calculator/sources/airport.json') as file:
        data = json.load(file)
        for airport in data.values():
            if airport['icao_region_code'] != 'NARNAS':
                continue

            airports.append(Airport(**airport))

    return airports


def determine_nearest_airport_to_location(latitude: float, longitude: float) -> Airport:
    airport_data = load_airport_data_from_json_file()  # cached data
    return min(
        airport_data,
        key=lambda airport: airport.distance_from_coordinates(latitude, longitude),
    )
