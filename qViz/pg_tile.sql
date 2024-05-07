

CREATE OR REPLACE
FUNCTION public.VECTOR_TILE(
            z integer, x integer, y  integer,mmsi_ integer,p_start text, p_end text)
RETURNS bytea
AS $$
    WITH bounds AS (
        SELECT ST_TileEnvelope(z,x,y) as geom
    ),
    trips_ AS (
        SELECT * from pymeos_demo as a where a.mmsi = mmsi_ or mmsi_ = 2190045
    )
    ,
    vals AS (
        SELECT mmsi,numInstants(trip) as size,asMVTGeom(transform(attime(trip,span(p_start::timestamptz, p_end::timestamptz, true, true)),3857), transform((bounds.geom)::stbox,3857))
            as geom_times
        FROM (
            SELECT
            mmsi,
            trajectory::tgeompoint AS trip
            FROM
            trips_

        ) as ego, bounds
    ),
    mvtgeom AS (
        SELECT (geom_times).geom, (geom_times).times,size,mmsi
        FROM vals
    )
SELECT ST_AsMVT(mvtgeom) FROM mvtgeom
                                  $$
    LANGUAGE 'sql'
STABLE
PARALLEL SAFE;


---

SELECT public.VECTOR_TILE(0, 0, 0, 2190045, '2023-06-01 00:00:00', '2023-06-02 00:00:00');
