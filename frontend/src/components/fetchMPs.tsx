import { createClientComponentClient } from '@supabase/auth-helpers-nextjs';

const fetchMPs = async (searchTerm = '') => {
    const supabase = createClientComponentClient();
    
  try {
    // Query the database for MPs
    let query = supabase
      .from('MPData') // Replace with your table name
      .select('mp_id, name, constituency, caucus, year, quarter');

    // Add search filters if a search term is provided
    if (searchTerm) {
      query = query.or(`name.ilike.%${searchTerm}%,constituency.ilike.%${searchTerm}%,caucus.ilike.%${searchTerm}%`);
    }

    const { data, error } = await query;
    if (error) throw error;

    // Return the fetched data
    return data || [];
  } catch (error) {
    console.error('Error fetching MPs:', error);
    return [];
  }
};
