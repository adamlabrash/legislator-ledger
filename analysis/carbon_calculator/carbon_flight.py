"carbon-flight.py calculates the emissions of passengers taking a flight"

from math import asin, cos, radians, sin, sqrt

from analysis.carbon_calculator.carbon import Carbon


class CarbonFlight(Carbon):
    """Class to calculate CO2 emmissions in flights."""

    def __init__(self):
        self.domestic_flight_max_km = 400
        self.short_haul_max_km = 3700
        self.detour_constant = 95  # km

    def calculate_co2(self, dist_km, pax_class, trip_type):
        """Calculate the CO2 eq emission of a flight."""

        flight_co2_dict = self.flight_co2_dict_from_json()

        if dist_km < self.domestic_flight_max_km:
            gr_co2 = flight_co2_dict['domestic']['avg']

        elif dist_km < self.short_haul_max_km:
            if pax_class == 'business-class':
                gr_co2 = flight_co2_dict['shortHaul']['businessClass']
            else:
                gr_co2 = flight_co2_dict['shortHaul']['economyClass']

        else:
            if pax_class == 'business-class':
                gr_co2 = flight_co2_dict['longHaul']['businessClass']
            else:
                gr_co2 = flight_co2_dict['longHaul']['economyClass']

        gr_co2_person = gr_co2 * dist_km

        if trip_type == "round-trip":
            gr_co2_person = gr_co2_person * 2

        return int(gr_co2_person)

    def calculate_co2_from_airports(self, iata_orig, iata_dest, pax_class, trip_type):
        """Calculate the CO2 eq emission of a ride by plane, given the airport iata codes as input."""

        dist_km = self.real_distance(iata_orig, iata_dest)
        gr_co2_person = self.calculate_co2(dist_km, pax_class, trip_type)

        return int(gr_co2_person)

    def calculate_co2_from_duration(self, duration_in_minutes, pax_class, trip_type):
        """Calculate the CO2 eq emission of a ride by plane, given a duration as input."""

        dist_km = self.estimate_flight_distance_from_duration(duration_in_minutes)
        gr_co2_person = self.calculate_co2(dist_km, pax_class, trip_type)

        return int(gr_co2_person)

    def flight_co2_dict_from_json(self):
        """Return a dictionary with the data for flights from a given source."""

        relative_path = '/sources/gov_uk.json'
        gov_uk_dict = self.dict_from_json(relative_path)

        co2_dict = gov_uk_dict["gCO2"]["flight"]

        return co2_dict

    def estimate_flight_distance_from_duration(self, duration_in_minutes):
        """Given duration as input, estimate distance covered."""

        dist_km = self.average_speed_from_duration(duration_in_minutes) * duration_in_minutes / 60

        return int(dist_km)

    def average_speed_from_duration(self, duration_in_minutes):
        """
        Taken from NorthApp https://github.com/tmrowco/northapp-contrib/blob/807646968a2878f30820197b2f3c73a7dfc59db2/co2eq/flights/index.js#L137
        which adapted it from https://airasia.listedcompany.com/images/ir-speed-length_7.gif
        """

        hour = duration_in_minutes / 60

        if hour < 3.3:
            avg_speed = (
                14.1
                + 495 * hour
                - 110 * hour * hour
                + 9.85 * hour * hour * hour
                - 0.309 * hour * hour * hour * hour
            )

        else:
            avg_speed = 770

        return int(avg_speed)

    def real_distance(self, iata_orig, iata_dest):
        """Calculate real distance between airports in km, given two airport as iata codes (3-digit code)."""

        relative_path = '/sources/airports.json'
        airports_json = self.dict_from_json(relative_path)

        orig_airport_dict = airports_json[iata_orig]
        dest_airport_dict = airports_json[iata_dest]

        dist_m = self.haversine(
            lat1=orig_airport_dict['lat'],
            lon1=orig_airport_dict['lon'],
            lat2=dest_airport_dict['lat'],
            lon2=dest_airport_dict['lon'],
        )
        dist_km = dist_m // 1000 + self.detour_constant

        return dist_km

    def haversine(self, lat1, lon1, lat2, lon2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        Source: http://stackoverflow.com/a/15737218/5802289
        """

        # convert input to floats
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        km = 6367 * c
        meters = int(km * 1000)

        return meters
