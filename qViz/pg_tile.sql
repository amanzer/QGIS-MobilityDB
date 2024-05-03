CREATE OR REPLACE
FUNCTION public.simplAnim(
            z integer, x integer, y  integer,mmsi_ integer,p_start text, p_end text)
RETURNS bytea
AS $$
    WITH bounds AS (
        SELECT ST_TileEnvelope(z,x,y) as geom
    ),
    vals AS (
        SELECT mmsi,numInstants(trip) as size,asMVTGeom(transform(attime(trip,span(p_start::timestamptz, p_end::timestamptz, true, true)),3857), transform((bounds.geom)::stbox,3857))
            as geom_times
        FROM (
            SELECT
            mmsi,
            trajectory AS trip
            FROM

            pymeos_demo
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

%

CREATE OR REPLACE
FUNCTION public.simplAnim2(
            z integer, x integer, y  integer,mmsi_ integer)
RETURNS bytea
AS $$
    WITH bounds AS (
        SELECT ST_TileEnvelope(z,x,y) as geom
    ),
    trips_ AS (
        SELECT *,1 AS index from pymeos_demo as a where a.mmsi = mmsi_ or mmsi_ = -1
    )
    ,
    vals AS (
        SELECT mmsi,index,numInstants(trip) as size,asMVTGeom(transform(trip::tgeompoint), transform((bounds.geom)::stbox,3857))
            as geom_times
        FROM (
            SELECT
            mmsi,index,
            trajectory::tgeompoint AS trip
            FROM
            trips_

        ) as ego, bounds
    ),
    mvtgeom AS (
        SELECT (geom_times).geom, (geom_times).times,index,size,mmsi
        FROM vals
    )
SELECT ST_AsMVT(mvtgeom) FROM mvtgeom
                                  $$
    LANGUAGE 'sql'
STABLE
PARALLEL SAFE;