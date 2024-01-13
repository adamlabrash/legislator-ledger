from extraction.expenditures.items import TravelEvent
from pydantic import BaseModel


class Location(BaseModel):
    location: str
    latitude: float
    longitude: float
    nearest_airport: str
    full_address: str

    @classmethod
    def from_csv_row(cls, rows: list) -> 'Location':
        return Location.model_validate(
            {
                'location': rows[0],
                'latitude': rows[1],
                'longitude': rows[2],
                'nearest_airport': rows[3],
                'full_address': rows[4],
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
