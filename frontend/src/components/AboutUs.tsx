'use client';

import { motion } from 'framer-motion';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

const AboutUs = () => {
  return (
    <div id="about-us" className="min-h-screen bg-gradient-to-b from-[#1e2a4a] to-[#2a3b66] py-20">
      <div className="max-w-7xl mx-auto px-4">
        {/* Introduction Section */}
        <motion.div
          initial={{ opacity: 0, y: 50 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <h2 className="text-5xl font-bold text-blue-100 mb-6">About Us</h2>
          <p className="text-blue-200 text-xl max-w-3xl mx-auto">
            Empowering citizens with transparency in parliamentary expenditures through
            data-driven insights and comprehensive analysis.
          </p>
        </motion.div>

        {/* Main Content Cards */}
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8 mt-12">
          {/* Mission Card */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            whileInView={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
            viewport={{ once: true }}
          >
            <Card className="bg-white/10 backdrop-blur border-blue-400/20 h-full">
              <CardHeader>
                <CardTitle className="text-blue-100">Our Mission</CardTitle>
              </CardHeader>
              <CardContent className="text-blue-200">
                To provide unprecedented transparency in parliamentary expenditures,
                making complex financial data accessible and understandable to every citizen.
              </CardContent>
            </Card>
          </motion.div>

          {/* Data Sources Card */}
          <motion.div
            initial={{ opacity: 0, y: 50 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.4 }}
            viewport={{ once: true }}
          >
            <Card className="bg-white/10 backdrop-blur border-blue-400/20 h-full">
              <CardHeader>
                <CardTitle className="text-blue-100">Data Sources</CardTitle>
              </CardHeader>
              <CardContent className="text-blue-200">
                We aggregate and analyze thousands of expenditure reports from individual
                Members of Parliament, processing over 1,000,000 expenditures from 12,000+
                separate reports.
              </CardContent>
            </Card>
          </motion.div>

          {/* Impact Card */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            whileInView={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.6, delay: 0.6 }}
            viewport={{ once: true }}
          >
            <Card className="bg-white/10 backdrop-blur border-blue-400/20 h-full">
              <CardHeader>
                <CardTitle className="text-blue-100">Our Impact</CardTitle>
              </CardHeader>
              <CardContent className="text-blue-200">
                By making parliamentary expenditure data easily accessible, we enable
                citizens to make informed decisions and hold their representatives
                accountable.
              </CardContent>
            </Card>
          </motion.div>
        </div>

        {/* Timeline Section */}
        <motion.div
          initial={{ opacity: 0, y: 50 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="mt-20"
        >
          <Card className="bg-white/10 backdrop-blur border-blue-400/20">
            <CardHeader>
              <CardTitle className="text-blue-100 text-center">Our Coverage</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col md:flex-row justify-between items-center gap-8 text-blue-200">
                <div className="text-center">
                  <div className="text-4xl font-bold text-blue-100 mb-2">1M+</div>
                  <div>Expenditures Tracked</div>
                </div>
                <div className="text-center">
                  <div className="text-4xl font-bold text-blue-100 mb-2">12K+</div>
                  <div>Reports Analyzed</div>
                </div>
                <div className="text-center">
                  <div className="text-4xl font-bold text-blue-100 mb-2">4</div>
                  <div>Years of Data</div>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      </div>
    </div>
  );
};

export default AboutUs;