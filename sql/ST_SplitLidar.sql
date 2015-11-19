-- Get the freightshed for a given county
DROP FUNCTION IF EXISTS ST_SplitLidar(
  parent text,
  bound_polygon_text text,
  slices integer,
  metric_srid integer
);

CREATE OR REPLACE FUNCTION ST_SplitLidar(_parent text, bound_polygon_text text, slices integer, metric_srid integer = 32610)
  RETURNS TABLE(id text,  the_geom geometry) AS
$$
BEGIN
   RETURN QUERY EXECUTE 
   'SELECT CONCAT('''||_parent||''',''__'' ,CAST(ROW_NUMBER() OVER (ORDER BY cell) AS text)  ) as id, cell FROM (SELECT ( ST_Dump(makegrid_2d(ST_GeomFromText('''|| bound_polygon_text ||''', '|| metric_srid ||'), '|| slices ||'))).geom AS cell) AS q_grid;';

END;
$$ LANGUAGE plpgsql;


/*
DROP TABLE IF EXISTS temp;
CREATE TABLE temp AS SELECT * FROM ST_SplitLidar(22, 'POLYGON((550000 4181000,550000 4182500,551500 4182500,551500 4181000,550000 4181000))', 4, 32610);
*/


