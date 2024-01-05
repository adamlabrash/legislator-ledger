import { supabase } from './supabaseClient';

export default interface Location {
  location: string;
  nearest_airport: string;
  latitude: number;
  longitude: number;
  full_address: string;
}

interface TravelEvent {
  traveller_name: string;
  traveller_type: string;
  purpose_of_travel: string;
  id: string;
  claim_id: string;
  date: string;
  mp_office: string;
  departure: string;
  destination: string;
}

export default interface TravelClaim {
  claim_id: string;
  from_date: string;
  end_date: string;
  transport_cost: number;
  accommodation_cost: number;
  meals_and_incidentals_cost: number;
  total_cost: number;
  reg_points_used: number;
  special_points_used: number;
  USA_points_used: number;
  mp_office: string;
  travelEvent: TravelEvent;
}

// Define the locationsArray in a scope accessible by getCoords
let locationsArray: Location[] = [];

export const fetchLocations = async (): Promise<Location[]> => {
  const { data, error } = await supabase
    .from('Locations')
    .select('location, nearest_airport, latitude, longitude, full_address');

  if (error) {
    console.error('Error fetching locations:', error);
    return [];
  }

  // Store the fetched data in locationsArray
  locationsArray = data as Location[];
  return locationsArray;
};

// Call fetchLocations to fill locationsArray with data from Supabase
fetchLocations().then(fetchedLocations => {
  // Now locationsArray is filled and can be used by getCoords
});

export function getCoords(location: string): [number, number] | undefined {
  try {
    const filteredLocations = locationsArray.filter(loc => loc.location === location);
    if (filteredLocations.length > 0) {
      const { latitude, longitude } = filteredLocations[0];
      console.log('Location found:', location, latitude, longitude)
      return [latitude, longitude];
    } else {
      throw new Error('Location not found');
    }
  } catch (error) {
    console.log('An error occurred:', error);
    return undefined;
  }
}

export const fetchTravelData = async (): Promise<TravelClaim[]> => {
  let travelClaims: TravelClaim[] = [];

  try {
    // Fetch travel claims
    let { data: claimsData, error: claimsError } = await supabase
      .from('TravelClaims')
      .select('*');

    if (claimsError) {
      throw claimsError;
    }

    // Fetch travel events
    let { data: eventsData, error: eventsError } = await supabase
      .from('TravelEvents')
      .select('*');

    if (eventsError) {
      throw eventsError;
    }

    // Assuming that both tables have been fetched successfully,
    // merge the data based on claim_id
    if (claimsData && eventsData) {
      travelClaims = claimsData.map((claim) => {
        // Find the corresponding event for each claim
        const event = eventsData.find((e) => e.claim_id === claim.claim_id);
        return {
          ...claim,
          travelEvent: event || {} // If no event is found, provide an empty object
        };
      });
    }
  } catch (error) {
    console.error('Error fetching travel data:', error);
  }

  return travelClaims;
};


export const fetchData = async (table: string, columns: string[]) => {
  // Fetch data from the first table
  const { data: tableData, error: tableError } = await supabase
    .from(table)
    .select(columns.join(', '));

  if (tableError) {
    throw tableError;
  }

  // Fetch data from TravelEvents
  const { data: travelEventsData, error: travelEventsError } = await supabase
    .from('TravelEvents')
    .select('*'); // You may want to specify the columns you need

  if (travelEventsError) {
    throw travelEventsError;
  }

  // Join the data based on 'claim_id'
  const joinedData = tableData.map(t => ({
    ...t,
    travelEvent: travelEventsData.find(te => te.claim_id === t.claim_id)
  }));

  return joinedData;
};

