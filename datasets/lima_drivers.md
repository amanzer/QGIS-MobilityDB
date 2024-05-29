This dataset contains positions of drivers in Lima Peru.


```bash
createdb lima_demo
psql -d lima_demo -c "CREATE EXTENSION mobilitydb CASCADE;"
psql -d lima_demo

```



```sql


CREATE TABLE driver_trajectories (
    driver_id TEXT,
    timestamp TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

\copy driver_trajectories(driver_id, timestamp, latitude, longitude) FROM '/var/lib/postgresql/drivers.csv' DELIMITER ',' CSV HEADER;

ALTER TABLE driver_trajectories ADD COLUMN tpoint tgeompoint;


UPDATE driver_trajectories
SET tpoint = tgeompoint(
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
    timestamp
);


CREATE TABLE driver_paths (
    driver_id TEXT PRIMARY KEY,
    trajectory tgeompoint
);



WITH ordered_driver_trajectories AS (
    SELECT DISTINCT ON (driver_id, timestamp)
           driver_id,
           timestamp,
           tpoint
    FROM driver_trajectories
    ORDER BY driver_id, timestamp
)
INSERT INTO driver_paths (driver_id, trajectory)
SELECT driver_id, tgeompointseq(array_agg(tpoint ORDER BY timestamp))
FROM ordered_driver_trajectories
GROUP BY driver_id;


```