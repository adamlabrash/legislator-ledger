export type ContractClaim = {
    supplier: string;
    description: string;
    date: string;
    total_cost: number;
  }
  
  export type HospitalityClaim = {
    claim_id: string;
    date: string;
    location: string;
    num_attendees: number;
    purpose_of_hospitality: string;
    event_type: string;
    supplier: string;
    total_cost: number;
  }
  
  export type TravelClaim = {
    claim_id: string;
    transport_cost: number;
    accommodation_cost: number;
    meals_and_incidentals_cost: number;
    reg_points_used: number;
    special_points_used: number;
    USA_points_used: number;
    travel_events: Array<{
      traveller_name?: string;
      traveller_type: string;
      purpose_of_travel: string;
      date: string;
      departure: string;
      destination: string;
    }>;
  }
  
  export type Claim = ContractClaim | HospitalityClaim | TravelClaim;
  
  export interface ExpenditureData {
    date: string;
    amount: number;
    cumulative: number;
    type: 'contract' | 'hospitality' | 'travel';
  }