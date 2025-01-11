import React, { useState, useMemo } from 'react';
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
  Brush
} from 'recharts';
import { ExpenditureData } from '@/types/expenditures';
import _ from 'lodash';

const formatDate = (dateStr: string) => {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-CA', { 
    year: 'numeric',
    month: 'short'
  });
};

const formatCurrency = (amount: number) => {
  return new Intl.NumberFormat('en-CA', {
    style: 'currency',
    currency: 'CAD'
  }).format(amount);
};

interface Props {
  data: {
    mp_id: string;
    expenditures: ExpenditureData[];
  };
  mpName: string;
}

const ExpenditureGraph: React.FC<Props> = ({ data, mpName }) => {
  const [showTypes] = useState(true);
  const [timeRange, setTimeRange] = useState<[number, number]>([0, 100]);

  // Memoize the processed data and stats to prevent unnecessary recalculations
  const { processedData, stats } = useMemo(() => {
    if (!data?.expenditures?.length) return { processedData: [], stats: null };
    
    // Sort data chronologically
    const sortedData = [...data.expenditures].sort(
      (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
    );
    
    // Calculate statistics
    const totalExpenses = _.sumBy(sortedData, 'amount');
    const travelExpenses = _.sumBy(sortedData.filter(d => d.type === 'travel'), 'amount');
    const hospitalityExpenses = _.sumBy(sortedData.filter(d => d.type === 'hospitality'), 'amount');
    const contractExpenses = _.sumBy(sortedData.filter(d => d.type === 'contract'), 'amount');

    // Calculate date range and monthly average
    const startDate = new Date(sortedData[0].date);
    const endDate = new Date(sortedData[sortedData.length - 1].date);
    const monthsDiff = (endDate.getFullYear() - startDate.getFullYear()) * 12 + 
                      (endDate.getMonth() - startDate.getMonth());
    const monthlyAverage = totalExpenses / (monthsDiff || 1);

    const stats = {
      totalExpenses,
      travelExpenses,
      hospitalityExpenses,
      contractExpenses,
      monthlyAverage,
      totalTransactions: sortedData.length
    };
    
    // For large datasets, downsample by averaging values within time windows
    const maxDataPoints = 200;
    let processedData = sortedData;
    if (sortedData.length > maxDataPoints) {
      const windowSize = Math.ceil(sortedData.length / maxDataPoints);
      const downsampledData = [];
      
      for (let i = 0; i < sortedData.length; i += windowSize) {
        const window = sortedData.slice(i, i + windowSize);
        const avgAmount = window.reduce((sum, item) => sum + item.amount, 0) / window.length;
        const lastItem = window[window.length - 1];
        downsampledData.push({
          ...window[0],
          amount: avgAmount,
          date: window[0].date,
          cumulative: lastItem.cumulative
        });
      }
      processedData = downsampledData;
    }
    
    return { processedData, stats };
  }, [data?.expenditures]);

  const CustomTooltip = ({ active, payload, label }) => {
    if (!active || !payload) return null;
    
    return (
      <div className="bg-gray-800 border border-gray-700 rounded p-2 shadow-lg">
        <p className="text-gray-300">{formatDate(label)}</p>
        {payload.map((entry, index) => (
          <p key={index} style={{ color: entry.color }} className="flex justify-between gap-4">
            <span>{entry.name}:</span>
            <span className="font-mono">{formatCurrency(entry.value)}</span>
          </p>
        ))}
      </div>
    );
  };

  if (!processedData.length) {
    return null;
  }

  return (
    <Card className="w-full bg-white/5 backdrop-blur">
      <CardHeader>
        <CardTitle className="text-blue-100">
          Overall Expenditures for {mpName}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4 mb-6">
          <div className="text-blue-100">
            <p className="text-xs font-medium">Total Expenses</p>
            <p className="text-lg font-bold">{formatCurrency(stats.totalExpenses)}</p>
          </div>
          <div className="text-green-100">
            <p className="text-xs font-medium">Travel</p>
            <p className="text-lg font-bold">{formatCurrency(stats.travelExpenses)}</p>
          </div>
          <div className="text-yellow-100">
            <p className="text-xs font-medium">Hospitality</p>
            <p className="text-lg font-bold">{formatCurrency(stats.hospitalityExpenses)}</p>
          </div>
          <div className="text-pink-100">
            <p className="text-xs font-medium">Contracts</p>
            <p className="text-lg font-bold">{formatCurrency(stats.contractExpenses)}</p>
          </div>
          <div className="text-purple-100">
            <p className="text-xs font-medium">Monthly Average</p>
            <p className="text-lg font-bold">{formatCurrency(stats.monthlyAverage)}</p>
          </div>
          <div className="text-indigo-100">
            <p className="text-xs font-medium">Total Transactions</p>
            <p className="text-lg font-bold">{stats.totalTransactions.toLocaleString()}</p>
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
                type="category"
              />
              <YAxis 
                tickFormatter={formatCurrency}
                stroke="#9CA3AF"
                type="number"
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              
              <Line
                type="monotone"
                dataKey="cumulative"
                name="Total Cumulative"
                stroke="#60A5FA"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
              {showTypes && (
                <Line
                  type="monotone"
                  dataKey="amount"
                  name="Individual Expense"
                  stroke="#34D399"
                  strokeWidth={1}
                  isAnimationActive={false}
                  dot={(props) => {
                    if (!props.payload) return null;
                    const colors = {
                      contract: '#EC4899',
                      hospitality: '#FBBF24',
                      travel: '#34D399'
                    };
                    return (
                      <circle
                        {...props}
                        r={3}
                        fill={colors[props.payload.type]}
                        stroke="none"
                      />
                    );
                  }}
                />
              )}

              <Brush
                dataKey="date"
                height={30}
                stroke="#60A5FA"
                tickFormatter={formatDate}
                fill="#1F2937"
                onChange={(range) => setTimeRange(range)}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
};

export default ExpenditureGraph;