"use client";
import React from 'react';
import dynamic from "next/dynamic";
import { ApexOptions } from "apexcharts";
import { useState, useEffect } from "react";
import { supabase } from '@/services/supabaseClient';

// Top 10 MP Offices with the highest total costs
// Bar Chart
// Bar broken into segments by travel type


const ReactApexChart = dynamic(() => import("react-apexcharts"), {
    ssr: false,
});



interface MyComponentProps { }

const ChartSeven = () => {
    const [chartData, setChartData] = useState<{ series: any[], categories: string[] }>({
        series: [],
        categories: [],
    });
    const [scrollIndex, setScrollIndex] = useState(0);
    const fetchData = async (startIndex: number) => {
        // Call your async function here (e.g., fetching data from an API)
        // For demonstration, I'll use a setTimeout to simulate async data fetching
        let mp_office: string[] = [];
        let sum_transpost: number[] = [];
        let sum_accomodation: number[] = [];
        let sum_meals: number[] = [];


        const { data, error } = await supabase
            .from('costs_broken_down') // Specify the view name correctly
            .select('*')
            .range(startIndex, startIndex + 10)



        let a = {};

        for (let i = 0; i < data.length; i++) {
            mp_office.push(data[i].mp_office);
            sum_transpost.push(data[i].sum_transport_cost);
            sum_accomodation.push(data[i].sum_accommodation_cost);
            sum_meals.push(data[i].sum_meals_and_incidentals_cost);

        }



        // Calculate the end index based on the start index and the desired number of items to display
        const endIndex = startIndex + 10;

        // Slice the data arrays to only include the range from startIndex to endIndex
        const visibleMpOffice = mp_office.slice(startIndex, endIndex);
        const visibleSumTransport = sum_transpost.slice(startIndex, endIndex);
        const visibleSumAccomodation = sum_accomodation.slice(startIndex, endIndex);
        const visibleSumMeals = sum_meals.slice(startIndex, endIndex);

        // Update your chart data state with the sliced data
        setChartData({
            series: [
                { name: 'Transportation', data: visibleSumTransport },
                { name: 'Accomodation', data: visibleSumAccomodation },
                { name: 'Meals and Incidentals', data: visibleSumMeals }
            ],
            categories: visibleMpOffice
        });
    };

    useEffect(() => {


        // Call the async function
        fetchData(scrollIndex);

    }, [scrollIndex]);
    const handleScrollbarChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setScrollIndex(parseInt(event.target.value, 10));
    };

    const chartOptions: ApexOptions = {
        chart: {
            type: 'bar',
            height: 350,
            zoom: {
                enabled: true,
                type: 'x',
                autoScaleYaxis: true
            },
            toolbar: {
                autoSelected: 'zoom'
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
        }, legend: {
            position: 'right',
            offsetY: 40
        },
        fill: {
            opacity: 1
        },
        xaxis: {
            categories: chartData.categories,
            title: {
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
                <div id="chartSeven" className="-ml-5 h-[355px] w-[105%]">
                    <ReactApexChart
                        options={chartOptions}
                        series={chartData.series}
                        type="bar"
                        width="100%"
                        height="100%"
                    />

                </div>
                <input
                    type="range"
                    min="0"
                    max={399} // Set the max value to the length of the data minus the number of items you want to display
                    value={scrollIndex}
                    onChange={handleScrollbarChange}
                    className="custom-scrollbar" // Add your custom scrollbar styling here
                />
            </div>
        </div>
    );
};

export default ChartSeven;