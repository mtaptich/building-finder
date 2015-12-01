CREATE OR REPLACE FUNCTION public.makegrid_2d (
  bound_polygon public.geometry,
  slices integer,
  metric_srid integer  
)
RETURNS public.geometry AS
$body$
DECLARE
  BoundM public.geometry; --Bound polygon transformed to the metric projection (with metric_srid SRID)
  Xmin DOUBLE PRECISION;
  Xmax DOUBLE PRECISION;
  Ymax DOUBLE PRECISION;
  slice_here DOUBLE PRECISION;
  X DOUBLE PRECISION;
  Y DOUBLE PRECISION;
  sectors public.geometry[];
  i INTEGER;
BEGIN
  BoundM := ST_Transform($1, $3); --From WGS84 (SRID 4326) to the metric projection, to operate with step in meters
  Xmin := ST_XMin(BoundM);
  Xmax := ST_XMax(BoundM);
  Ymax := ST_YMax(BoundM);
  slice_here:= (Xmax - Xmin) / slices;

  Y := ST_YMin(BoundM); --current sector's corner coordinate
  i := -1;
  <<yloop>>
  LOOP
    IF (Y >= Ymax) THEN  --Better if generating polygons exceeds the bound for one step. You always can crop the result. But if not you may get not quite correct data for outbound polygons (e.g. if you calculate frequency per sector)
        EXIT;
    END IF;

    X := Xmin;
    <<xloop>>
    LOOP
      IF (X >= Xmax) THEN
          EXIT;
      END IF;

      i := i + 1;
      sectors[i] := ST_GeomFromText('POLYGON(('||X||' '||Y||', '||(X+slice_here)||' '||Y||', '||(X+slice_here)||' '||(Y+slice_here)||', '||X||' '||(Y+slice_here)||', '||X||' '||Y||'))', $3);

      X := X + slice_here;
    END LOOP xloop;
    Y := Y + slice_here;
  END LOOP yloop;

  RETURN ST_Transform(ST_Collect(sectors), ST_SRID($1));
END;
$body$
LANGUAGE 'plpgsql';

-- Example for San Francisco
/*
-- Get the text bounding box (1) and plug the result into (2)
-- (1)
SELECT ST_AsText(ST_Envelope(ST_Collect(the_geom))) FROM lidar_sf;

--(2)
SELECT cell FROM 
(SELECT (
ST_Dump(makegrid_2d(ST_GeomFromText('POLYGON((550000 4181000,550000 4182500,551500 4182500,551500 4181000,550000 4181000))',
 32610), -- WGS84 SRID
 4) -- cell step in meters
)).geom AS cell) AS q_grid;
*/