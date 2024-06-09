CREATE TABLE IF NOT EXISTS processed_data (
    MMSI INT,
    traj tgeompoint
);


INSERT INTO processed_data (MMSI, traj)
WITH trajectories AS (
    SELECT 
        MMSI,
        atStbox(
            a.trajectory::tgeompoint,
            stbox(
                ST_MakeEnvelope(-180, -90, 180, 90, 4326),
                tstzspan('[2023-06-01 00:00:00, 2023-06-01 23:59:59]')
            )
        ) as traj
    FROM public.PyMEOS_demo as a 
    
),
processed_trajectory AS (
    SELECT 
        MMSI, tprecision(traj, INTERVAL '1 minute', TIMESTAMP '2023-06-01 00:00:00') AS precise_trajectory
    FROM 
        trajectories 
),
resampled AS (
    SELECT 
        MMSI, tsample(precise_trajectory, INTERVAL '1 minute', TIMESTAMP '2023-06-01 00:00:00')  AS resampled_trajectory
    FROM 
        processed_trajectory
)
    SELECT
        MMSI,
        resampled_trajectory 
    FROM 
        resampled;
		
		
select * from processed_data LIMIT 10;
