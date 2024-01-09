
import ECommerce from "@/components/Dashboard/E-commerce";
import Header from '@/components/Header'
import { cookies } from 'next/headers'
import { supabase } from '../services/supabaseClient'
import {fetchTravelDataFromView} from '../services/fetchData'
import MapOne from "@/components/Maps/MapOne";
import { get } from "http";
import { map } from "jquery";
import ChartFive from "@/components/Charts/ChartFive";



export default async function Index() {
  try {

    const data = await fetchTravelDataFromView("claim_id", "T0213122");
} catch (error) {
    console.error('Error fetching data:', error);
}

  return (
    <>
      <ECommerce />
    </>
  )
}

 
