import ECommerce from "@/components/Dashboard/E-commerce";
import { createClient } from '@supabase/supabase-js'
import Header from '@/components/Header'
import { cookies } from 'next/headers'

import * as dotenv from 'dotenv';
dotenv.config();

export default async function Index() {
  const supabaseUrl: string = process.env.NEXT_PUBLIC_SUPABASE_URL || '';
  const supabaseAnonKey: string = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';
  
  if (!supabaseUrl || !supabaseAnonKey) {
    throw new Error('Supabase URL and Anon Key must be provided!');
  }
  
  const supabase = createClient(supabaseUrl, supabaseAnonKey);

  //console.log(supabase)

  async function fetchLocations() {
    let { data: locations, error,count } = await supabase
        .from('Locations')
        .select('*', { count: 'exact' })
    if (error) {
        console.error('Error fetching locations:', error);
        return;
    }

    console.log('Locations:', count );
  }

  fetchLocations();

  return (
    <>
      <ECommerce />
    </>
  )
}
