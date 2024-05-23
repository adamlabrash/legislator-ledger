from pydantic import BaseModel
from pydantic_extra_types.coordinate import Latitude, Longitude


class Location(BaseModel):
    location: str
    latitude: Latitude
    longitude: Longitude
    full_address: str
    distance_to_airport: float
    nearest_airport_icao: str

    @classmethod
    def from_csv_row(cls, rows: list) -> 'Location':
        return cls.model_validate(
            {
                'location': rows[1],
                'latitude': rows[2],
                'longitude': rows[3],
                'full_address': rows[4],
                'distance_to_airport': rows[5],
                'nearest_airport_icao': rows[6],
            }
        )


class Airport(BaseModel):
    icao: str
    name: str
    latitude: Latitude
    longitude: Longitude
    city: str
    iata: str
    province: str

    @classmethod
    def from_csv_row(cls, rows: list) -> 'Airport':
        return cls.model_validate(
            {
                'icao': rows[0],
                'name': rows[1],
                'latitude': rows[2],
                'longitude': rows[3],
                'city': rows[4],
                'iata': rows[5],
                'province': rows[6],
            }
        )


# class Flight:
#     caucus: Caucus
#     claim_id: str
#     constituency: str
#     csv_title: str
#     download_url: str
#     extracted_at: datetime
#     institution: Institution
#     mp_id: str
#     name: str

#     departure_full_address: str
#     departure_latitude: float
#     departure_longitude: float

#     destination_nearest_airport: str

#     distance_travelled: float

#     passenger_class: str
#     est_carbon_emissions: int
