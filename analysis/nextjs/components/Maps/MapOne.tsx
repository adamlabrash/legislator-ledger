"use client";
import React, { useState, useEffect } from "react";
import { VectorMap } from "@react-jvectormap/core";
import { caMill  } from "@react-jvectormap/canada"; // This import depends on the actual name of the Canada map in the library
import Markers from "../../services/fetchData";

const MapOne= () => {

  const [marks_loc, setState] = useState<>();

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
      setState({ series: [{ name: 'Series 1', data: newData }], categories: newCategories });
    };

    // Call the async function
    fetchData();

  }, []);

  return (
    <div className="col-span-12 rounded-sm border border-stroke bg-white py-6 px-7.5 shadow-default dark:border-strokedark dark:bg-boxdark xl:col-span-7">
      <h4 className="mb-2 text-xl font-semibold text-black dark:text-white">
        Region labels
      </h4>
      <div id="mapOne" className="mapOne map-btn h-90">
        <VectorMap
          map={caMill} // Use the imported Canada map here
          markers={marks_loc}
          backgroundColor="white"
          regionStyle={{
            initial: {
              fill: "#D1D5DB",
            },
            hover: {
              fillOpacity: 1,
              fill: "blue",
            },
            selected: {
              fill: "#FFFB00",
            },
          }}
          onRegionTipShow={function reginalTip(event, label, code) {
            //@ts-ignore
            return label.html(`
                  <div style="background-color: #F8FAFC; color: black; padding: 2px 8px"; >
                    ${
                      //@ts-ignore
                      label.html()
                    }
                  </div>`);
          }}
        />
      </div>
    </div>
  );
};

export default MapOne;
