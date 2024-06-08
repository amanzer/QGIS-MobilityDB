

-- Resampling trajectories to 1 minute intervals
WITH trajectories AS (
    SELECT 
        MMSI,
        atStbox(
            a.trajectory::tgeompoint,
            stbox(
                ST_MakeEnvelope(-180, -90, 180, 90, 4326),
                tstzspan('[2023-06-01 00:00:00, 2023-06-01 59:59:59]')
            )
        ) as traj
    FROM public.PyMEOS_demo as a 
    WHERE a.MMSI in ('5322', '2190045', '2444044')), -- the list of all objetc ids 

processed_trajectory AS (
    SELECT 
        tprecision(traj, INTERVAL '1 minute', TIMESTAMP '2023-06-01 00:00:00') AS precise_trajectory
    FROM 
        trajectories 
),
	
resampled AS 
		(SELECT 
			tsample(precise_trajectory, INTERVAL '1 minute', TIMESTAMP '2023-06-01 00:00:00')  AS resampled_trajectory
			FROM 
			processed_trajectory
			),
	

	final_values AS (
SELECT
	startTimestamp(resampled_trajectory) as start_timestamp, endTimestamp(resampled_trajectory) as end_timestamp,
    resampled_trajectory 
FROM 
    resampled
),
-- The following is to get the frame index of the start and end timestamps
minute_intervals AS (
    SELECT 
        generate_series(
            timestamp '2023-06-01 00:00:00', 
            timestamp '2023-06-01 23:59:59', 
            interval '1 minute'
        ) AS ts
),

timestamps_with_index AS (
    SELECT 
        ts,
        row_number() OVER (ORDER BY ts) - 1 AS idx 
    FROM 
        minute_intervals
),
	
start_end_indices AS (
    SELECT 
        MIN(t.idx) AS start_index,
        MAX(g.idx) AS end_index,
		p.resampled_trajectory as traj
    FROM 
        final_values p
    JOIN 
        timestamps_with_index t ON t.ts = p.start_timestamp
    JOIN 
        timestamps_with_index g ON g.ts = p.end_timestamp
	GROUP BY
	p.resampled_trajectory

)

SELECT
    start_index,
    end_index,
	traj
	
	
FROM
    start_end_indices;
