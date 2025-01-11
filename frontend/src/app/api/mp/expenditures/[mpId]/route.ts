import { createRouteHandlerClient } from '@supabase/auth-helpers-nextjs';
import { cookies } from 'next/headers';
import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

function convertPythonToJSON(str: string) {
  if (typeof str !== 'string') {
    return str;
  }

  str = str.replace(/^`|`$/g, '');

  let inString = false;
  let currentQuote = '';
  let escaped = false;
  let result = '';

  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    
    if (!escaped && char === '\\') {
      escaped = true;
      result += char;
      continue;
    }

    if (!escaped && (char === "'" || char === '"')) {
      if (!inString) {
        inString = true;
        currentQuote = char;
        result += '"'; 
      } else if (char === currentQuote) {
        inString = false;
        currentQuote = '';
        result += '"';
      } else {
        result += char;
      }
    } else if (inString) {
      if (char === '"') {
        result += '\\"'; 
      } else if (char === '\\') {
        result += '\\\\'; 
      } else {
        result += char;
      }
    } else {
      if (char === "'") {
        result += '"'; 
      } else if (char === 'T' && str.slice(i, i + 4) === 'True') {
        result += 'true';
        i += 3;
      } else if (char === 'F' && str.slice(i, i + 5) === 'False') {
        result += 'false';
        i += 4;
      } else if (char === 'N' && str.slice(i, i + 4) === 'None') {
        result += 'null';
        i += 3;
      } else {
        result += char;
      }
    }
    
    escaped = false;
  }

  result = result.replace(/\\([^"\\\/bfnrt])/g, '$1');
  
  return result;
}

export async function GET(
  request: NextRequest,
  { params }: { params: { mpId: string } }
) {
  try {
    const supabase = createRouteHandlerClient({ cookies });
    let allMpData = [];
    let hasMore = true;
    let from = 0;
    const PAGE_SIZE = 1000; // Supabase's maximum page size

    // Fetch all data using pagination
    while (hasMore) {
      const { data: mpData, error } = await supabase
        .from('MPData')
        .select('year, quarter, claim')
        .eq('mp_id', params.mpId)
        .order('year', { ascending: true })
        .range(from, from + PAGE_SIZE - 1);

      if (error) {
        console.error('Supabase error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
      }

      if (!mpData || mpData.length === 0) {
        hasMore = false;
      } else {
        allMpData = [...allMpData, ...mpData];
        from += PAGE_SIZE;
        
        // Check if we got less than PAGE_SIZE results, meaning we've reached the end
        if (mpData.length < PAGE_SIZE) {
          hasMore = false;
        }
      }
    }

    if (!allMpData.length) {
      return NextResponse.json({ error: 'No expenditure data found' }, { status: 404 });
    }

    const processedData = allMpData.map(exp => {
      try {
        let claimData;
        if (typeof exp.claim === 'string') {
          const jsonString = convertPythonToJSON(exp.claim);
          try {
            claimData = JSON.parse(jsonString);
          } catch (parseError) {
            console.error('JSON Parse error for:', jsonString);
            console.error('Original claim:', exp.claim);
            throw parseError;
          }
        } else {
          claimData = exp.claim;
        }

        let amount = 0;
        let type: 'contract' | 'hospitality' | 'travel';

        if ('transport_cost' in claimData) {
          type = 'travel';
          amount = 
            Number(claimData.transport_cost || 0) +
            Number(claimData.accommodation_cost || 0) +
            Number(claimData.meals_and_incidentals_cost || 0);
        } else if ('num_attendees' in claimData) {
          type = 'hospitality';
          amount = Number(claimData.total_cost || 0);
        } else {
          type = 'contract';
          amount = Number(claimData.total_cost || 0);
        }

        const date = claimData.date || `${exp.year}-${((exp.quarter - 1) * 3 + 2).toString().padStart(2, '0')}-15`;

        return {
          date,
          amount,
          type
        };
      } catch (itemError) {
        console.error('Error processing item:', exp);
        console.error('Error:', itemError);
        return null;
      }
    }).filter(Boolean);

    // Sort and calculate cumulative amounts
    processedData.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
    
    let cumulative = 0;
    const finalData = processedData.map(item => ({
      ...item,
      cumulative: (cumulative += item.amount)
    }));

    // Add metadata about the data fetch
    return NextResponse.json({
      mp_id: params.mpId,
      expenditures: finalData,
      metadata: {
        total_records: finalData.length,
        total_pages_fetched: Math.ceil(from / PAGE_SIZE)
      }
    });
    
  } catch (error) {
    console.error('Error processing expenditures:', error);
    return NextResponse.json(
      { error: 'Failed to process expenditures' }, 
      { status: 500 }
    );
  }
}