from pydantic import BaseModel
from pydantic_extra_types.coordinate import Latitude, Longitude


class Location(BaseModel):
    full_address: str
    distance_to_airport: float

    location: str
    latitude: Latitude
    longitude: Longitude

    nearest_airport: str
    nearest_airport_latitude: Latitude
    nearest_airport_longitude: Longitude

    @classmethod
    def from_csv_row(cls, rows: list) -> 'Location':
        return Location.model_validate(
            {
                'full_address': rows[0],
                'latitude': rows[1],
                'location': rows[2],
                'longitude': rows[3],
                'nearest_airport': rows[4],
                'nearest_airport_latitude': rows[5],
                'nearest_airport_longitude': rows[6],
                'distance_to_airport': rows[7],
            }
        )


# class Flight(TravelEvent):
#     USA_points_used: Decimal
#     caucus: Caucus
#     claim_id: str
#     constituency: str
#     csv_title: str
#     download_url: str
#     extracted_at: datetime
#     institution: Institution
#     mp_id: str
#     name: str
#     reg_points_used: Decimal
#     special_points_used: Decimal
#     transport_cost: Decimal

#     departure_full_address: str
#     departure_latitude: float

#     destination_nearest_airport: str

#     distance_travelled: float

#     passenger_class: str
#     est_carbon_emissions: int
