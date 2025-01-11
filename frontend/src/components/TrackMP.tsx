'use client';

import { motion } from 'framer-motion';
import Link from 'next/link';
import { ArrowLeft } from 'lucide-react';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';
import MPSearch from '@/components/MPSearch';

export default function TrackMP() {
  return (
    <div className="min-h-screen bg-[#1e2a4a]">
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="p-4"
      >
        <Link 
          href="/"
          className="inline-flex items-center text-blue-200 hover:text-blue-100 transition-colors"
        >
          <ArrowLeft className="w-5 h-5 mr-2" />
          Back to Home
        </Link>
      </motion.div>

      <div className="max-w-7xl mx-auto px-4 py-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h1 className="text-5xl font-bold text-blue-100 mb-4">
            Track Your MP
          </h1>
          <p className="text-blue-200 text-xl max-w-2xl mx-auto mb-12">
            Explore detailed expenditure reports, travel patterns, and parliamentary activities of your Member of Parliament.
          </p>

          <MPSearch />
        </motion.div>

        <div className="grid md:grid-cols-2 gap-6 mt-16">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.2 }}
          >
            <Card className="bg-white/5 backdrop-blur border-blue-400/20">
              <CardHeader>
                <CardTitle className="text-blue-100">Expenditure Analysis</CardTitle>
                <div className="text-blue-200">
                  View detailed breakdowns of:
                  <ul className="list-disc ml-4 space-y-1 mt-2">
                    <li>Travel expenses and patterns</li>
                    <li>Hospitality events</li>
                    <li>Contract spending</li>
                    <li>Quarterly summaries</li>
                  </ul>
                </div>
              </CardHeader>
            </Card>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.3 }}
          >
            <Card className="bg-white/5 backdrop-blur border-blue-400/20">
              <CardHeader>
                <CardTitle className="text-blue-100">Comparative Insights</CardTitle>
                <div className="text-blue-200">
                  Compare your MP's spending with:
                  <ul className="list-disc ml-4 space-y-1 mt-2">
                    <li>Party averages</li>
                    <li>Regional benchmarks</li>
                    <li>Historical trends</li>
                    <li>Similar constituencies</li>
                  </ul>
                </div>
              </CardHeader>
            </Card>
          </motion.div>
        </div>
      </div>
    </div>
  );
}
