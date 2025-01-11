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

  // Memoize the processed data to prevent unnecessary recalculations
  const processedData = useMemo(() => {
    if (!data?.expenditures?.length) return [];
    
    // Sort data chronologically
    const sortedData = [...data.expenditures].sort(
      (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
    );
    
    // For large datasets, downsample by averaging values within time windows
    const maxDataPoints = 200; // Maximum number of points to display
    if (sortedData.length > maxDataPoints) {
      const windowSize = Math.ceil(sortedData.length / maxDataPoints);
      const downsampledData = [];
      
      for (let i = 0; i < sortedData.length; i += windowSize) {
        const window = sortedData.slice(i, i + windowSize);
        const avgAmount = window.reduce((sum, item) => sum + item.amount, 0) / window.length;
        downsampledData.push({
          ...window[0],
          amount: avgAmount,
          date: window[0].date
        });
      }
      
      // Recalculate cumulative for downsampled data
      let cumulative = 0;
      return downsampledData.map(item => ({
        ...item,
        cumulative: (cumulative += item.amount)
      }));
    }
    
    return sortedData;
  }, [data?.expenditures]);

  const CustomTooltip = ({ active, payload, label }) => {
    if (!active || !payload) return null;
    
    return (
      <div className="bg-gray-800 border border-gray-700 rounded p-2 shadow-lg">
        <p className="text-gray-300">{formatDate(label)}</p>
        {payload.map((entry, index) => (
          <p key={index} style={{ color: entry.color }}>
            {entry.name}: {formatCurrency(entry.value)}
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
          Expenditures for {mpName}
        </CardTitle>
      </CardHeader>
      <CardContent>
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
                  name="Individual Expenses"
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