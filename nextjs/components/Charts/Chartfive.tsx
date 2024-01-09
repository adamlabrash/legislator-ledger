"use client";
import React from 'react';
import dynamic from "next/dynamic";
import { ApexOptions } from "apexcharts";
import { useState, useEffect } from "react";
import TotalTravelCost from '../../services/fetchData'
import { supabase } from '@/services/supabaseClient';

const ReactApexChart = dynamic(() => import("react-apexcharts"), {
  ssr: false,
});




interface MyComponentProps {}

const ChartFive = () => {
  const [chartData, setChartData] = useState<{ series: any[]; categories: string[] }>({
    series: [],
    categories: [],
  });
  useEffect(() => {
    const fetchData = async () => {
      // Call your async function here (e.g., fetching data from an API)
      // For demonstration, I'll use a setTimeout to simulate async data fetching
      let newData: number[] = [];
      let newCategories: string[] = [];
      const { data, error } = await supabase
      .from('total_transportation_cost') // Specify the view name correctly
      .select('* ')

      for(let i =0; i<data.length; i++){
        newData.push(data[i].total_transport_cost);
        newCategories.push(data[i].mp_office)
      }


      // Update your chart data state with the fetched data
      setChartData({ series: [{ name: 'Series 1', data: newData }], categories: newCategories });
    };

    // Call the async function
    fetchData();

  }, []);

  const chartOptions: ApexOptions = {
    legend: {
      show: false,
      position: "top",
      horizontalAlign: "left",
    },
    colors: ["#3C50E0", "#80CAEE"],
    chart: {
      // events: {
      //   beforeMount: (chart) => {
      //     chart.windowResizeHandler();
      //   },
      // },
      fontFamily: "Satoshi, sans-serif",
      height: 335,
      type: "area",
      dropShadow: {
        enabled: true,
        color: "#623CEA14",
        top: 10,
        blur: 4,
        left: 0,
        opacity: 0.1,
      },
  
      toolbar: {
        show: false,
      },
    },
    responsive: [
      {
        breakpoint: 1024,
        options: {
          chart: {
            height: 300,
          },
        },
      },
      {
        breakpoint: 1366,
        options: {
          chart: {
            height: 350,
          },
        },
      },
    ],
    stroke: {
      width: [2, 2],
      curve: "straight",
    },
    // labels: {
    //   show: false,
    //   position: "top",
    // },
    grid: {
      xaxis: {
        lines: {
          show: true,
        },
      },
      yaxis: {
        lines: {
          show: true,
        },
      },
    },
    dataLabels: {
      enabled: false,
    },
    markers: {
      size: 4,
      colors: "#fff",
      strokeColors: ["#3056D3", "#80CAEE"],
      strokeWidth: 3,
      strokeOpacity: 0.9,
      strokeDashArray: 0,
      fillOpacity: 1,
      discrete: [],
      hover: {
        size: undefined,
        sizeOffset: 5,
      },
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
            type="area"
            width="100%"
            height="100%"
          />
        </div>
      </div>
    </div>
  );
};

export default ChartFive;