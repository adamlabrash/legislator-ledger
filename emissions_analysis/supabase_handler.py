from supabase import create_client
import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]
supabase = create_client(url, key)


def get_members():
    return [member_dict['member'] for member_dict in supabase.table("MPs").select("*").execute().data]


# TODO make pydantic models
def get_member_flights(member: str):
    flights = []
    claims = supabase.table("TravelClaims").select("*").eq("mp_office", member).execute().data
    for claim in claims:
        if claim['reg_points_used'] > 1 or claim['special_points_used'] > 1 or claim['USA_points_used'] > 1:
            travel_events = supabase.table("TravelEvents").select("*").eq("claim_id", claim['claim_id']).execute().data
            flights.append(travel_events)

    # get most dense travel month of flights

    # sort flights by date
    import pdb

    pdb.set_trace()
    flights.sort(key=lambda x: x['date'])

    # get most dense 2 week period of travel
    for i, flight in enumerate(flights):
        while i < len(flights) and flights[i]['date'] - flights[i + 1]['date'] < 14:
            import pdb

            pdb.set_trace()

    import pdb

    pdb.set_trace()
    return claims


mem = get_members()

for mem in get_members():
    flights = get_member_flights(mem)
    import pdb

    pdb.set_trace()
import pdb

pdb.set_trace()


def query_flights():
    return supabase.table("")
