"use client";
import React, { useState, useEffect } from 'react';
import { Search } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { createClientComponentClient } from '@supabase/auth-helpers-nextjs';
import ExpenditureGraph from '@/components/ExpenditureGraph';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';

const MPSearch = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [mps, setMps] = useState([]);
  const [filteredMps, setFilteredMps] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isLoadingExpenditures, setIsLoadingExpenditures] = useState(false);
  const supabase = createClientComponentClient();
  const [selectedMP, setSelectedMP] = useState(null);
  const [expenditureData, setExpenditureData] = useState(null);
  
  const handleMPSelect = async (mp) => {
    setSelectedMP(mp);
    setSearchTerm('');
    setFilteredMps([]);
    setIsLoadingExpenditures(true);
    try {
      const response = await fetch(`/api/mp/expenditures/${mp.mp_id}`);
      const data = await response.json();
      setExpenditureData(data);
    } catch (error) {
      console.error('Failed to fetch expenditure data:', error);
    } finally {
      setIsLoadingExpenditures(false);
    }
  };

  useEffect(() => {
    const fetchMPs = async () => {
      try {
        const { data, error } = await supabase
          .from('uniquemps')
          .select('mp_id, name, constituency, caucus')
          .order('name');

        if (error) {
          throw error;
        }

        if (data) {
          setMps(data);
        }
      } catch (error) {
      } finally {
        setIsLoading(false);
      }
    };

    fetchMPs();
  }, []);

  useEffect(() => {
    if (!searchTerm.trim()) {
      setFilteredMps([]);
      return;
    }

    const searchTermLower = searchTerm.toLowerCase();
    const filtered = mps
      .filter((mp) => {
        if (!mp) return false;

        return (
          mp.name.toLowerCase().includes(searchTermLower) ||
          mp.constituency.toLowerCase().includes(searchTermLower) ||
          mp.caucus.toLowerCase().includes(searchTermLower)
        );
      })
      .slice(0, 10);

    setFilteredMps(filtered);
  }, [searchTerm, mps]);

  const handleSearchChange = (e) => {
    const value = e.target.value;
    setSearchTerm(value);
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.3 }}
      className="w-full max-w-3xl mx-auto relative"
    >
      <div className="relative">
        <Search className="absolute left-6 top-1/2 transform -translate-y-1/2 text-blue-200 w-6 h-6" />
        <input
          type="text"
          placeholder={isLoading ? "Loading MPs..." : "Search by MP name, constituency, or party..."}
          value={searchTerm}
          onChange={handleSearchChange}
          disabled={isLoading}
          className="w-full px-16 py-6 bg-transparent border-2 border-blue-400/20 
                   rounded-2xl text-xl text-blue-100 placeholder-blue-300/50
                   focus:outline-none focus:border-blue-400/40 transition-colors
                   disabled:opacity-50 disabled:cursor-not-allowed"
        />
        {isLoading && (
          <div className="absolute right-6 top-1/2 transform -translate-y-1/2">
            <div className="w-5 h-5 border-2 border-blue-200 border-t-transparent rounded-full animate-spin" />
          </div>
        )}
      </div>

      <AnimatePresence>
        {filteredMps.length > 0 && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="absolute w-full mt-2 bg-white/10 backdrop-blur-lg rounded-xl 
                     border border-blue-400/20 shadow-xl overflow-hidden z-50"
          >
            <div>
              <ul className="list-none m-0 p-0">
                {filteredMps.map((mp, index) => (
                  <motion.li
                    key={mp.mp_id}
                    initial={{ opacity: 0, x: -20 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: index * 0.1 }}
                    className="w-full text-left p-4 hover:bg-white/10 cursor-pointer transition-colors"
                    onClick={() => handleMPSelect(mp)}
                  >
                    <div className="flex justify-between items-start">
                      <div>
                        <h3 className="text-blue-100 font-medium">{mp.name}</h3>
                        <p className="text-blue-200 text-sm">{mp.constituency}</p>
                      </div>
                      <span className={`text-sm font-medium ${getCaucusColor(mp.caucus)}`}>
                        {mp.caucus}
                      </span>
                    </div>
                  </motion.li>
                ))}
              </ul>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {selectedMP && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="mt-8"
        >
          {isLoadingExpenditures ? (
            <Card className="w-full bg-white/5 backdrop-blur">
              <CardHeader className="flex flex-row items-center justify-center space-y-0">
                <div className="flex flex-col items-center space-y-4">
                  <CardTitle className="text-blue-100">
                    Loading expenditure data for {selectedMP.name}...
                  </CardTitle>
                  <div className="w-8 h-8 border-4 border-blue-200 border-t-transparent rounded-full animate-spin" />
                </div>
              </CardHeader>
            </Card>
          ) : expenditureData && (
            <ExpenditureGraph 
              data={expenditureData}
              mpName={selectedMP.name}
            />
          )}
        </motion.div>
      )}
    </motion.div>
  );
};

const getCaucusColor = (caucus) => {
  const colors = {
    'Conservative': 'text-blue-300',
    'Liberal': 'text-red-300',
    'New Democratic Party': 'text-orange-300',
    'Bloc Québécois': 'text-blue-300',
    'Green Party': 'text-green-300',
    Independent: 'text-gray-300',
  };
  return colors[caucus] || 'text-gray-300';
};

export default MPSearch;