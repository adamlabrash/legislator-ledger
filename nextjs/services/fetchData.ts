import { supabase } from './supabaseClient';

export default interface Markers{
  latLng:[number,number];
  name:string;
}

interface Location {
  location: string;
  nearest_airport: string;
  latitude: number;
  longitude: number;
  full_address: string;
}

interface TravelData {
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
  // Include all other fields from the TravelClaims and TravelEvents tables
  // that are selected in the travel_data view.
  // Add fields for TravelEvents as well.
  traveller_name: string;
  traveller_type: string;
  purpose_of_travel: string;
  event_id: string; // assuming you have renamed the id from TravelEvents to event_id in the view
  event_date: string;
  departure: string;
  destination: string;
  // Add any additional fields from the view here

  departure_latitude: number | null; // Use number | null if the data can be null
  departure_longitude: number | null;
  destination_latitude: number | null;
  destination_longitude: number | null;
}


export default interface TotalTravelCost{
  mp_office: string;
  total_cost: number;
}


export const fetchTravelDataFromView = async (column: string, id:string): Promise<TravelData[]> => {
  let travelDataArray: TravelData[] = [];
  const chunkSize = 100;  // Define how many records to fetch per request
  let offset = 0;         // Start at the beginning of the view
  let hasMore = true;     // Flag to determine if there are more records to fetch

  try {
    while (hasMore) {
      const { data, error } = await supabase
        .from('travel_data_with_coords') // Specify the view name correctly
        .select('*')
        .eq(column, id)         // Select all columns from the view
        .range(offset, offset + chunkSize - 1); // Get a range of records

      if (error) {
        throw error;
      }

      // If data is fetched, add it to the travelDataArray
      if (data && data.length > 0) {
        travelDataArray = [...travelDataArray, ...data];
        offset += data.length; // Increment the offset
        hasMore = data.length === chunkSize; // Check if there are more records to fetch
      } else {
        hasMore = false; // No more records to fetch
      }
    }
  } catch (error) {
    console.error('Error fetching travel data from the view:', error);
  }

  return travelDataArray;
};

let locationsArray: Location[] = [];

export const fetchLocations = async (): Promise<Location[]> => {
  let index = 0; // Start at the beginning of the dataset
  const chunkSize = 100; // Define the size of each chunk
  let isLastChunk = false;

  while (!isLastChunk) {
    const { data, error } = await supabase
      .from('Locations')
      .select('location, nearest_airport, latitude, longitude, full_address')
      .range(index, index + chunkSize - 1); // Get a range of records

    if (error) {
      console.error('Error fetching locations:', error);
      break;
    }

    // Add the data to the locationsArray
    locationsArray = locationsArray.concat(data as Location[]);

    // Check if the last chunk has been reached
    if (!data || data.length < chunkSize) {
      isLastChunk = true; // This was the last chunk
    } else {
      index += chunkSize; // Move to the next chunk
    }
  }

  return locationsArray;
};

// fetchLocations()
//   .then(locations => console.log("Done fetching locations"))
//   .catch(error => console.error(error));

export const convertToMarkers = (): Markers[] => {
  return locationsArray.map((location): Markers => {
    return {
      latLng: [location.latitude, location.longitude],
      name: location.location // Assuming you want to use the 'location' property as the marker's name
    };
  });
};