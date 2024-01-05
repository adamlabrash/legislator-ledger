import ECommerce from "@/components/Dashboard/E-commerce";
import Header from '@/components/Header'
import { cookies } from 'next/headers'
import { supabase } from '../services/supabaseClient'
import {fetchTravelData} from '../services/fetchData'
import {getCoords} from '../services/fetchData'
import {fetchLocations} from '../services/fetchData'
import TravelClaim from '../services/fetchData'

import { get } from "http";

export default async function Index() {
  //console.log(supabase)
  try {
    const data = await fetchTravelData();
    let travelData: TravelClaim[] = data;
    console.log(travelData[0]);
} catch (error) {
    console.error('Error fetching data:', error);
}

  return (
    <>
      <ECommerce />
    </>
  )
}
 
