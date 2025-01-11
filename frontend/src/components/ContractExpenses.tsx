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

const ContractExpensesGraph: React.FC<Props> = ({ data, mpName }) => {
  const { processedData, stats } = useMemo(() => {
    // Filter only contract expenses
    const contractExpenses = data.expenditures.filter(exp => exp.type === 'contract');
    
    // Get unique dates with contract expenses
    const uniqueDates = new Set(contractExpenses.map(exp => exp.date.split('T')[0]));
    
    // Calculate statistics
    const totalSpent = _.sumBy(contractExpenses, 'amount');
    const totalContracts = contractExpenses.length;
    const averageContractValue = totalContracts > 0 ? totalSpent / totalContracts : 0;
    const largestContract = Math.max(...contractExpenses.map(exp => exp.amount));

    return {
      processedData: contractExpenses,
      stats: {
        totalSpent,
        totalContracts,
        averageContractValue,
        largestContract
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
          <span>Contract Amount:</span>
          <span className="font-mono">{formatCurrency(payload[0].value)}</span>
        </p>
      </div>
    );
  };

  if (!processedData.length) {
    return (
      <Card className="w-full bg-white/5 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-blue-100">No Contract Expenses Found</CardTitle>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card className="w-full bg-white/5 backdrop-blur">
      <CardHeader>
        <CardTitle className="text-blue-100">Contract Expenses for {mpName}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-6">
          <Card className="bg-indigo-500/10 border-indigo-400/20">
            <CardContent className="pt-6">
              <div className="text-indigo-100">
                <p className="text-sm font-medium">Total Contract Expenses</p>
                <p className="text-2xl font-bold">{formatCurrency(stats.totalSpent)}</p>
              </div>
            </CardContent>
          </Card>
          <Card className="bg-violet-500/10 border-violet-400/20">
            <CardContent className="pt-6">
              <div className="text-violet-100">
                <p className="text-sm font-medium">Total Contracts</p>
                <p className="text-2xl font-bold">{stats.totalContracts}</p>
              </div>
            </CardContent>
          </Card>
          <Card className="bg-fuchsia-500/10 border-fuchsia-400/20">
            <CardContent className="pt-6">
              <div className="text-fuchsia-100">
                <p className="text-sm font-medium">Average Contract Value</p>
                <p className="text-2xl font-bold">{formatCurrency(stats.averageContractValue)}</p>
              </div>
            </CardContent>
          </Card>
          <Card className="bg-pink-500/10 border-pink-400/20">
            <CardContent className="pt-6">
              <div className="text-pink-100">
                <p className="text-sm font-medium">Largest Contract</p>
                <p className="text-2xl font-bold">{formatCurrency(stats.largestContract)}</p>
              </div>
            </CardContent>
          </Card>
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
                name="Contract Amount"
                stroke="#818CF8"
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

export default ContractExpensesGraph;