import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
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
  const [showTypes, setShowTypes] = useState(true);

  if (!data?.expenditures?.length) {
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
              data={data.expenditures}
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
              />
              <YAxis 
                tickFormatter={formatCurrency}
                stroke="#9CA3AF"
              />
              <Tooltip 
                formatter={(value: number) => formatCurrency(value)}
                labelFormatter={formatDate}
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: '1px solid #374151'
                }}
                itemStyle={{ color: '#9CA3AF' }}
                labelStyle={{ color: '#9CA3AF' }}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="cumulative"
                name="Total Cumulative"
                stroke="#60A5FA"
                strokeWidth={2}
                dot={false}
              />
              {showTypes && (
                <>
                  <Line
                    type="monotone"
                    dataKey="amount"
                    name="Individual Expenses"
                    stroke="#34D399"
                    strokeWidth={1}
                    dot={(props) => {
                      const { payload } = props;
                      const colors = {
                        contract: '#EC4899',
                        hospitality: '#FBBF24',
                        travel: '#34D399'
                      };
                      return (
                        <circle
                          {...props}
                          r={4}
                          fill={colors[payload.type]}
                          stroke="none"
                        />
                      );
                    }}
                  />
                </>
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
};

export default ExpenditureGraph;