-- Resampling and indexing trajectories  through the SQL query

WITH trajectories as (
		SELECT 
			atStbox(
				a.trajectory::tgeompoint,
				stbox(
					ST_MakeEnvelope(
						-180,-90,180,90,
						4326 -- SRID
					),
					tstzspan('[2023-06-01 00:00:00, 2023-06-01 03:00:00]')
				)
			) as trajectory
		FROM public.pymeos_demo as a),
		
		resampled as (
		SELECT tsample(traj.trajectory, INTERVAL '1 minute', TIMESTAMP '2023-06-01 00:00:00')  AS resampled_trajectory
			FROM 
				trajectories as traj)
				
		SELECT
				EXTRACT(EPOCH FROM (startTimestamp(rs.resampled_trajectory) - '2023-06-01 00:00:00'::timestamp))::integer / 60 AS start_index ,
				EXTRACT(EPOCH FROM (endTimestamp(rs.resampled_trajectory) - '2023-06-01 00:00:00'::timestamp))::integer / 60 AS end_index,
				rs.resampled_trajectory
		FROM resampled as rs ;
		
	

-- Without indexes

WITH trajectories as (
		SELECT 
			atStbox(
				a.trajectory::tgeompoint,
				stbox(
					ST_MakeEnvelope(
						-180,-90,180,90,
						4326 -- SRID
					),
					tstzspan('[2023-06-01 00:00:00, 2023-06-01 03:00:00]')
				)
			) as trajectory
		FROM public.pymeos_demo as a)
		SELECT tsample(traj.trajectory, INTERVAL '1 minute', TIMESTAMP '2023-06-01 00:00:00')  AS resampled_trajectory
			FROM 
				trajectories as traj ;
		