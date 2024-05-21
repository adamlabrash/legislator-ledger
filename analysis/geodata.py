from geopy.geocoders import Nominatim
from geopy.location import Location as GeoLocation

geo_api = Nominatim(user_agent="GetLoc")


def get_geo_api_location_data(location_str: str) -> dict | None:
    geo_location: GeoLocation = geo_api.geocode(location_str, country_codes='CA', exactly_one=True)  # type: ignore
    if geo_location is None:
        print("Unable to find location for:", location_str)
        return None
    return {
        'name': location_str,
        'latitude': geo_location.latitude,
        'longitude': geo_location.longitude,
        'address': geo_location.address,
    }
