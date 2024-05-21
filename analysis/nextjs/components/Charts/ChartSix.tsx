"use client";
import React from 'react';
import dynamic from "next/dynamic";
import { ApexOptions } from "apexcharts";
import { useState, useEffect } from "react";
import TotalTravelCost from '../../services/fetchData'
import { supabase } from '@/services/supabaseClient';

// Top 10 MP Offices with the highest total costs
// Bar Chart
// Bar broken into segments by travel type


const ReactApexChart = dynamic(() => import("react-apexcharts"), {
  ssr: false,
});



interface MyComponentProps {}

const ChartSix = () => {
  const [chartData, setChartData] = useState<{ series: any[], categories: string[]}>({
    series: [],
    categories: [],
  });
  useEffect(() => {
    const fetchData = async () => {
      // Call your async function here (e.g., fetching data from an API)
      // For demonstration, I'll use a setTimeout to simulate async data fetching
        let mp_office: string[] = [];
        let sum_transpost: number[] = [];
        let sum_accomodation: number[] = [];
        let sum_meals: number[] = [];


      const { data, error } = await supabase
      .from('top_ten_costs') // Specify the view name correctly
      .select('*')
    
      let a = {};

      for(let i =0; i<data.length; i++){
        mp_office.push(data[i].mp_office);
        sum_transpost.push(data[i].sum_transport_cost);
        sum_accomodation.push(data[i].sum_accommodation_cost);
        sum_meals.push(data[i].sum_meals_and_incidentals_cost);

      }


      // Update your chart data state with the fetched data
      setChartData({ series: [{ name: 'Transportation', data: sum_transpost }, { name: 'Accomodation', data: sum_accomodation }, { name: 'Meals and Incidentals', data: sum_meals }], categories: mp_office });
    };

    // Call the async function
    fetchData();

  }, []);

  const chartOptions: ApexOptions = {
    chart: {
        type: 'bar',
        height: 350,
        stacked: true,
        toolbar: {
          show: true
        },
        zoom: {
          enabled: true
        }
      },
      responsive: [{
        breakpoint: 480,
        options: {
          legend: {
            position: 'bottom',
            offsetX: -10,
            offsetY: 0
          }
        }
      }],
      plotOptions: {
        bar: {
          horizontal: false,
          borderRadius: 10,
          dataLabels: {
            hideOverflowingLabels: true,
            
          },
        },
      },legend: {
        position: 'right',
        offsetY: 40
      },
      fill: {
        opacity: 1
      },
    xaxis: {
      categories: chartData.categories,
      title:{
        text: 'MP Office'
      },
      axisBorder: {
        show: false,
      },
      axisTicks: {
        show: false,
      },
    },
    yaxis: {
      title: {
        style: {
          fontSize: "0px",
        },
        text: "Total Travel Cost",
      },
    },
    series: chartData.series,
  };

  return (
<div className="col-span-12 rounded-sm border border-stroke bg-white px-5 pt-7.5 pb-5 shadow-default dark:border-strokedark dark:bg-boxdark sm:px-7.5 xl:col-span-8">
      <div>
        <div id="chartOne" className="-ml-5 h-[355px] w-[105%]">
          <ReactApexChart
            options={chartOptions}
            series={chartData.series}
            type="bar"
            width="100%"
            height="100%"
          />
        </div>
      </div>
    </div>
  );
};

export default ChartSix;