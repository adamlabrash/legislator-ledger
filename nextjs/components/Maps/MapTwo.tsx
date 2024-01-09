import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet';

interface Props {
  jsonFilePath: string; // Path to the JSON file
}

const CanadaMap: React.FC<Props> = ({ jsonFilePath }) => {
  const [jsonData, setJsonData] = useState<GeoJSON.GeoJsonObject | null>(null);

  useEffect(() => {
    fetch(jsonFilePath)
      .then((response) => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.json();
      })
      .then((data) => {
        setJsonData(data);
      })
      .catch((error) => {
        console.error('Error fetching JSON data: ', error);
      });
  }, [jsonFilePath]);

  return (
    <MapContainer center={[45.4215, -75.6972]} zoom={10} style={{ height: '100%', width: '100%' }}>
      <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
      {jsonData && <GeoJSON data={jsonData} />}
    </MapContainer>
  );
};

export default CanadaMap;