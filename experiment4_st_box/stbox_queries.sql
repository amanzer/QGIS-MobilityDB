SELECT asText(atStbox(
  tgeompoint('[Point(55 10)@2023-06-01 00:00:00+00, Point(55 11)@2023-06-01 23:59:59+00]'),
  stbox('STBOX T((1.866156842105263, 53.52940074266616932, 2023-01-01 00:00:00+00), (12.32286644444444512, 58.04979999999999762, 2023-12-31 23:59:59+00))')
));

-- Combine the geometry and temporal span into an stbox
SELECT stbox(
  ST_MakeLine(
    ST_MakePoint(0, 0),
    ST_MakePoint(2, 2)
  ),
  tstzspan('[2012-01-02 00:00:00, 2012-01-04 23:59:59]')
);


SELECT asText(atStbox(tgeompoint '[Point(0 0)@2012-01-01, Point(3 3)@2012-01-04)',
stbox(
  ST_MakeLine(
    ST_MakePoint(0, 0),
    ST_MakePoint(2, 2)
  ),
  tstzspan('[2012-01-02 00:00:00, 2012-01-04 23:59:59]')
)));
-- "{[POINT(1 1)@2012-01-02, POINT(2 2)@2012-01-03]}"

SELECT asText(atStbox(
  tgeompoint '[Point(55 10)@2023-06-01, Point(55 11)@2023-06-01 23:59:59]',
  stbox 'STBOX T((1.22668578666666694, 53.33359999999999701, 2023-01-01 00:00:00), (16.85087575150300765, 58.54095589941972833, 2023-12-31 23:59:59))'
));



SELECT asText(atStbox(
  tgeompoint '[Point(55 10)@2023-06-01 00:00:00+00, Point(55 11)@2023-06-01 23:59:59+00]',
  stbox(
    ST_MakeLine(
      ST_MakePoint(0, 0),
      ST_MakePoint(60, 60)
    ),
    tstzspan('[2023-01-01 00:00:00+00, 2023-12-31 23:59:59+00]')
  )
));

SELECT 
	astext(atStbox(
  a.trajectory::tgeompoint,
  stbox(
    ST_MakeEnvelope(
      1.22668578666666694, 53.33359999999999701, -- xmin, ymin
      16.85087575150300765, 58.54095589941972833, -- xmax, ymax
      4326 -- SRID
    ),
    tstzspan('[2023-06-01 00:00:00+00, 2023-06-01 23:59:59+00]')
  )
))
	FROM public.PyMEOS_demo as a 
WHERE a.MMSI = 9112856 ;


SELECT srid(a.trajectory) FROM public.Pymeos_demo as a WHERE a.MMSI = 9112856;
