'use client';
import { motion } from 'framer-motion';
import Link from 'next/link';

const NavButton = ({ 
  text, 
  delay = 0,
  gradientFrom,
  gradientTo,
  isScroll = false
}: { 
  text: string; 
  delay?: number;
  gradientFrom: string;
  gradientTo: string;
  isScroll?: boolean;
}) => {
  const handleClick = () => {
    if (isScroll) {
      const element = document.getElementById('about-us');
      element?.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay }}
    >
      {isScroll ? (
        <button 
          onClick={handleClick}
          className="inline-block"
        >
          <div className={`px-8 py-3 rounded-full text-base font-medium
                      bg-gradient-to-r ${gradientFrom} ${gradientTo}
                      text-white shadow-lg hover:shadow-xl
                      transition-all duration-200 transform hover:scale-105`}>
            {text}
          </div>
        </button>
      ) : (
        <Link 
          href={`/${text.toLowerCase().replace(' ', '-')}`}
          className="inline-block"
        >
          <div className={`px-8 py-3 rounded-full text-base font-medium
                      bg-gradient-to-r ${gradientFrom} ${gradientTo}
                      text-white shadow-lg hover:shadow-xl
                      transition-all duration-200 transform hover:scale-105`}>
            {text}
          </div>
        </Link>
      )}
    </motion.div>
  );
};

export default function LegislatorLanding() {
  return (
    <div className="h-[60vh] bg-[#1e2a4a] relative overflow-hidden">
      {/* Left Decorations */}
      <div className="absolute left-10 top-20">
        <motion.div 
          initial={{ scale: 0, rotate: -180 }}
          animate={{ scale: 1, rotate: 0 }}
          transition={{ duration: 0.6 }}
          className="w-16 h-16 bg-blue-400 rounded-xl mb-4"
        />
        <motion.div 
          initial={{ scale: 0, rotate: -180 }}
          animate={{ scale: 1, rotate: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="w-12 h-12 bg-blue-500 rounded-lg mb-4"
        />
        <motion.div 
          initial={{ scale: 0, rotate: -180 }}
          animate={{ scale: 1, rotate: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="w-8 h-8 bg-blue-600 rounded-md"
        />
      </div>

      {/* Right Decorations */}
      <div className="absolute right-10 top-20">
        <motion.div 
          initial={{ scale: 0, rotate: 180 }}
          animate={{ scale: 1, rotate: 0 }}
          transition={{ duration: 0.6 }}
          className="w-16 h-16 bg-orange-400 rounded-tr-3xl mb-4"
        />
        <motion.div 
          initial={{ scale: 0, rotate: 180 }}
          animate={{ scale: 1, rotate: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="w-12 h-12 bg-yellow-400 rounded-tr-2xl mb-4"
        />
        <motion.div 
          initial={{ scale: 0, rotate: 180 }}
          animate={{ scale: 1, rotate: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="w-8 h-8 bg-red-400 rounded-tr-xl"
        />
      </div>

      {/* Main Content */}
      <div className="relative z-10 max-w-7xl mx-auto px-4 py-20">
        <motion.div
          initial={{ opacity: 0, y: 100 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: "easeOut" }}
          className="text-center mb-16"
        >
          <h1 className="text-6xl md:text-7xl font-bold text-blue-100 mb-8">
            The Legislator Ledger
          </h1>
          <p className="text-blue-200 text-xl max-w-2xl mx-auto">
            Track and analyze parliamentary expenditures with unprecedented transparency
          </p>
        </motion.div>

        {/* Navigation Buttons */}
        <div className="flex flex-col sm:flex-row gap-6 justify-center items-center mt-8">
          <NavButton 
            text="About Us" 
            delay={0.3} 
            gradientFrom="from-blue-500"
            gradientTo="to-blue-700"
            isScroll={true}
          />
          <NavButton 
            text="Track MP" 
            delay={0.4} 
            gradientFrom="from-purple-500"
            gradientTo="to-pink-600"
          />
          <NavButton 
            text="Trends" 
            delay={0.5} 
            gradientFrom="from-green-500"
            gradientTo="to-emerald-700"
          />
        </div>

        {/* Search Bar Decoration */}
        <motion.div 
          initial={{ opacity: 0, x: -100 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.5, delay: 0.6 }}
          className="absolute left-12 bottom-20 flex items-center"
        >
          <div className="w-12 h-12 bg-white rounded-full flex items-center justify-center shadow-lg">
            <span className="text-2xl">🔍</span>
          </div>
          <div className="h-2 w-24 bg-gradient-to-r from-white to-transparent rounded-full ml-2" />
        </motion.div>
      </div>

      {/* Bottom Right Decoration */}
      <motion.div
        initial={{ opacity: 0, scale: 0 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.5, delay: 0.7 }}
        className="absolute bottom-10 right-10 w-24 h-24 bg-gradient-to-br from-blue-400/20 to-transparent rounded-tl-3xl"
      />
    </div>
  );
}