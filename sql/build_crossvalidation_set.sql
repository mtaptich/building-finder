DROP TABLE IF EXISTS crossvalidationdata;
CREATE TABLE crossvalidationdata AS
WITH portion AS (
	SELECT a.x, a.y, a.z, a.id, a.the_geom
	FROM z_20130805_usgsnyc14_18TWL835075 a
	JOIN z_20130805_usgsnyc14_18TWL835075_bbox b
	ON b.id = 'z_20130805_usgsnyc14_18TWL835075__1' AND ST_intersects(a.the_geom, b.the_geom)
), in_building AS (
	SELECT a.id, CAST(1 AS int) AS is_building
	FROM portion a, bld_footprint_sf b 
	WHERE ST_Intersects(a.the_geom, b.geom)
) SELECT b.x, b.y, b.z, b.id, coalesce(a.is_building,0) AS is_building --, b.the_geom -- Add to visualize
FROM portion b
LEFT JOIN in_building a
ON a.id=b.id; 

\COPY crossvalidationdata to '/Users/Mike/Google Drive/water_Mike_Olga/Group Project/model/tests/data/CrossValidationData.csv' CSV HEADER;


-- How many buildings ?
WITH portion AS (
	SELECT a.x, a.y, a.z, a.id, a.the_geom
	FROM lidar_sf a
	JOIN lidar_sf_bbox b
	ON b.id = 'lidar_sf__4' AND ST_intersects(a.the_geom, b.the_geom)
), ids AS (
	SELECT DISTINCT ON (gid) b.gid
	FROM portion a, bld_footprint_sf b 
	WHERE ST_Intersects(a.the_geom, b.geom)
) SELECT COUNT(*)
FROM ids;