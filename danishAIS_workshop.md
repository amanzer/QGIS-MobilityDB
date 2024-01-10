Danish AIS data is used to work on this project, the following code comes directly from the mobilityDB workshop example found here : https://mobilitydb.github.io/MobilityDB-workshop/develop/html/ch01.html

```bash
sudo -i -u postgres
wget https://web.ais.dk/aisdata/aisdk-2023-06-01.zip
unzip aisdk-2023-06-01.zip
createdb DanishAIS
psql -d DanishAIS

```

```SQL


CREATE EXTENSION MobilityDB CASCADE;

CREATE TABLE AISInput(
  T timestamp,
  TypeOfMobile varchar(100),
  MMSI integer,
  Latitude float,
  Longitude float,
  navigationalStatus varchar(100),
  ROT float,
  SOG float,
  COG float,
  Heading integer,
  IMO varchar(100),
  Callsign varchar(100),
  Name varchar(100),
  ShipType varchar(100),
  CargoType varchar(100),
  Width float,
  Length float,
  TypeOfPositionFixingDevice varchar(100),
  Draught float,
  Destination varchar(100),
  ETA varchar(100),
  DataSourceType varchar(100),
  SizeA float,
  SizeB float,
  SizeC float,
  SizeD float,
  Geom geometry(Point, 4326)
);


```


```bash
psql -d DanishAIS -c "\copy AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus, ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length, TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType, SizeA, SizeB, SizeC, SizeD) FROM 'aisdk-2023-06-01.csv' DELIMITER  ',' CSV HEADER;"
```

We clean up some of the fields in the table and create spatial points with the following command.

```SQL
UPDATE AISInput SET
  NavigationalStatus = CASE NavigationalStatus WHEN 'Unknown value' THEN NULL END,
  IMO = CASE IMO WHEN 'Unknown' THEN NULL END,
  ShipType = CASE ShipType WHEN 'Undefined' THEN NULL END,
  TypeOfPositionFixingDevice = CASE TypeOfPositionFixingDevice
  WHEN 'Undefined' THEN NULL END,
  Geom = ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326);


CREATE TABLE AISInputFiltered AS
SELECT DISTINCT ON(MMSI, T) *
FROM AISInput
WHERE Longitude BETWEEN -16.1 AND 32.88 AND Latitude BETWEEN 40.18 AND 84.17;
-- Query returned successfully: 11545496 rows affected, 00:45 minutes execution time.
SELECT COUNT(*) FROM AISInputFiltered;
--11545496

```

Now we are ready to construct ship trajectories out of their individual observations:
```SQL
CREATE TABLE Ships(MMSI, Trip, SOG, COG) AS 
SELECT MMSI, 
  tgeompoint_seq(array_agg(tgeompoint_inst(ST_Transform(Geom, 25832), T) ORDER BY T)), 
  tfloat_seq(array_agg(tfloat_inst(SOG, T) ORDER BY T) FILTER (WHERE SOG IS NOT NULL)), 
  tfloat_seq(array_agg(tfloat_inst(COG, T) ORDER BY T) FILTER (WHERE COG IS NOT NULL)) 
FROM AISInputFiltered 
GROUP BY MMSI;
-- Query returned successfully: 6264 rows affected, 00:52 minutes execution time.


ALTER TABLE Ships ADD COLUMN Traj geometry;
UPDATE Ships SET Traj = trajectory(Trip);
-- Query returned successfully: 6264 rows affected, 3.8 secs execution time.

```
