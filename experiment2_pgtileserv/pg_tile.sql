

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


-------



CREATE OR REPLACE
FUNCTION public.qviz(
            z integer, x integer, y  integer, p_start text, p_end text)
RETURNS bytea
AS $$
    WITH sub_mmsi AS (
        SELECT * from pymeos_demo 
        LIMIT (SELECT CEIL(0.1 * COUNT(*)) FROM pymeos_demo); 
    ),
    bounds AS (
        SELECT ST_TileEnvelope(z,x,y) as geom
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
            sub_mmsi

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

CREATE OR REPLACE
FUNCTION public.countries_name(
            z integer, x integer, y integer,
            mmsi_ integer default 2190045)
RETURNS bytea
AS $$
DECLARE
    result bytea;
BEGIN
    WITH
    bounds AS (
      SELECT ST_TileEnvelope(z, x, y) AS geom
    ),
    mvtgeom AS (
      SELECT ST_AsMVTGeom(ST_Transform(t.geom, 3857), bounds.geom) AS geom,
        t.mmsi
      FROM test t, bounds
      WHERE ST_Intersects(t.geom, ST_Transform(bounds.geom, 4326))
      AND t.mmsi = mmsi_
    )
    SELECT ST_AsMVT(mvtgeom, 'default')
    INTO result
    FROM mvtgeom;

    RETURN result;
END;
$$
LANGUAGE 'plpgsql'
STABLE
PARALLEL SAFE;

COMMENT ON FUNCTION public.countries_name IS 'Filters the countries table by the initial letters of the name using the "name_prefix" parameter.';



----
--DOES NOT WORK

CREATE OR REPLACE
FUNCTION public.newfunction(
            z integer, x integer, y integer)
RETURNS bytea
AS $$
DECLARE
    result bytea;
BEGIN
    WITH
    bounds AS (
      SELECT ST_TileEnvelope(z, x, y) AS geom
    ),
    mvtgeom AS (
      SELECT ST_AsMVTGeom2(ST_transform(t.traj::geometry,3857), bounds.geom) AS geom
      FROM test t, bounds
      WHERE ST_Transform(t.traj::geometry, 3857) && bounds.geom
    )
    SELECT ST_AsMVT(mvtgeom.*)
    INTO result
    FROM mvtgeom;

    RETURN result;
END;
$$
LANGUAGE 'plpgsql'
STABLE
PARALLEL SAFE;

COMMENT ON FUNCTION public.countries_name IS 'Filters the countries table by the initial letters of the name using the "name_prefix" parameter.';


----


CREATE OR REPLACE
FUNCTION public.NEW_METHOD(
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
        SELECT mmsi,numInstants(trip) as size,asMVTGeom(attime(trip,span(p_start::timestamptz, p_end::timestamptz, true, true)), transform((bounds.geom)::stbox,3857))
            as tgeom
        FROM (
            SELECT
            mmsi,
            trajectory::tgeompoint AS trip
            FROM
            trips_

        ) as ego, bounds
    ),
    mvtgeom AS (
        SELECT (tgeom).geom,size,mmsi
        FROM vals
    )
SELECT ST_AsMVT(mvtgeom) FROM mvtgeom
                                  $$
    LANGUAGE 'sql'
STABLE
PARALLEL SAFE;


---

CREATE OR REPLACE
FUNCTION public.omega(
            z integer, x integer, y  integer,p_start text, p_end text)
RETURNS bytea
AS $$
    WITH bounds AS (
        SELECT ST_TileEnvelope(z,x,y) as geom
    ),
    trips_ AS (
        SELECT * from pymeos_demo ORDER BY mmsi LIMIT 500
    )
    ,
    vals AS (
        SELECT mmsi,numInstants(trip) as size,asMVTGeom(attime(trip,span(p_start::timestamptz, p_end::timestamptz, true, true)), transform((bounds.geom)::stbox,3857))
            as tgeom
        FROM (
            SELECT
            mmsi,
            trajectory::tgeompoint AS trip
            FROM
            trips_

        ) as ego, bounds
    ),
    mvtgeom AS (
        SELECT (tgeom).geom,size,mmsi
        FROM vals
    )
SELECT ST_AsMVT(mvtgeom) FROM mvtgeom
                                  $$
    LANGUAGE 'sql'
STABLE
PARALLEL SAFE;

----

--- Current method


CREATE OR REPLACE
FUNCTION public.tpoint_mvt(
            z integer, x integer, y  integer,p_start text, p_end text, number_of_points integer default 500)
RETURNS bytea 
AS $$
    WITH bounds AS (
        SELECT ST_TileEnvelope(z,x,y) as geom
    ),
    trips_ AS (
        SELECT * from pymeos_demo ORDER BY mmsi LIMIT number_of_points
    )
    ,
    vals AS (
        SELECT mmsi,numInstants(trip) as size,asMVTGeom(attime(trip,span(p_start::timestamptz, p_end::timestamptz, true, true)), transform((bounds.geom)::stbox,3857))
            as tgeom
        FROM (
            SELECT
            mmsi,
            trajectory::tgeompoint AS trip
            FROM
            trips_

        ) as ego, bounds
    ),
    mvtgeom AS (
        SELECT (tgeom).geom,size,mmsi
        FROM vals
    )
SELECT ST_AsMVT(mvtgeom) FROM mvtgeom
                                  $$
    LANGUAGE 'sql'
STABLE
PARALLEL SAFE;