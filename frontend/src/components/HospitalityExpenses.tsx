import React, { useMemo } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import _ from 'lodash';

interface Props {
  data: {
    expenditures: Array<{
      date: string;
      amount: number;
      type: 'travel' | 'hospitality' | 'contract';
      cumulative: number;
    }>;
  };
  mpName: string;
}

const HospitalityExpensesGraph: React.FC<Props> = ({ data, mpName }) => {
  const { processedData, stats } = useMemo(() => {
    // Filter only hospitality expenses
    const hospitalityExpenses = data.expenditures.filter(exp => exp.type === 'hospitality');
    
    // Get unique dates with hospitality events
    const uniqueDates = new Set(hospitalityExpenses.map(exp => exp.date.split('T')[0]));
    
    // Calculate statistics
    const totalSpent = _.sumBy(hospitalityExpenses, 'amount');
    const totalEvents = hospitalityExpenses.length;
    const averageEventCost = totalEvents > 0 ? totalSpent / totalEvents : 0;
    const mostExpensiveEvent = Math.max(...hospitalityExpenses.map(exp => exp.amount));
    const monthlyAverage = totalSpent / (uniqueDates.size / 30);

    return {
      processedData: hospitalityExpenses,
      stats: {
        totalSpent,
        totalEvents,
        averageEventCost,
        mostExpensiveEvent,
        monthlyAverage
      }
    };
  }, [data.expenditures]);

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-CA', {
      style: 'currency',
      currency: 'CAD',
      maximumFractionDigits: 0
    }).format(amount);
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-CA', { 
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (!active || !payload || !payload[0]) return null;
    
    return (
      <div className="bg-gray-800 border border-gray-700 rounded p-2 shadow-lg">
        <p className="text-gray-300 mb-1">{formatDate(label)}</p>
        <p style={{ color: payload[0].color }} className="flex justify-between gap-4">
          <span>Event Cost:</span>
          <span className="font-mono">{formatCurrency(payload[0].value)}</span>
        </p>
      </div>
    );
  };

  if (!processedData.length) {
    return (
      <Card className="w-full bg-white/5 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-blue-100">No Hospitality Expenses Found</CardTitle>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card className="w-full bg-white/5 backdrop-blur">
      <CardHeader>
        <CardTitle className="text-blue-100">Hospitality Expenses for {mpName}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-2 mb-4">
          <div className="text-amber-100">
            <p className="text-xs font-medium">Total Expenses</p>
            <p className="text-lg font-bold">{formatCurrency(stats.totalSpent)}</p>
          </div>
          <div className="text-orange-100">
            <p className="text-xs font-medium">Total Events</p>
            <p className="text-lg font-bold">{stats.totalEvents}</p>
          </div>
          <div className="text-yellow-100">
            <p className="text-xs font-medium">Avg Event Cost</p>
            <p className="text-lg font-bold">{formatCurrency(stats.averageEventCost)}</p>
          </div>
          <div className="text-lime-100">
            <p className="text-xs font-medium">Monthly Avg</p>
            <p className="text-lg font-bold">{formatCurrency(stats.monthlyAverage)}</p>
          </div>
        </div>

        <div className="h-[400px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={processedData}
              margin={{
                top: 5,
                right: 30,
                left: 20,
                bottom: 5,
              }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis 
                dataKey="date"
                tickFormatter={formatDate}
                stroke="#9CA3AF"
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis 
                tickFormatter={formatCurrency}
                stroke="#9CA3AF"
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              
              <Line
                type="monotone"
                dataKey="amount"
                name="Event Cost"
                stroke="#F59E0B"
                strokeWidth={2}
                dot={true}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
};

export default HospitalityExpensesGraph;