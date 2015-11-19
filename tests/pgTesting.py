import os, sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, '../util')
from pgMethods import Methods 


class pgDataRetriever:
	def __init__(self, database='building', User='Mike', host='localhost', projection=4326):
		# Start server 
		self.start_server(database, User, host)

		# Set projection
		self.projection = projection

	def start_server(self, database, User, host):
		# save database
		self.database = database

		# save cursor
		self.cur  = create_engine('postgresql://%s@%s:5432/%s' % (User, host,database))

	def getCrossValidationData(self, lidar_name, shp_name, path_to_data_directory='tests/data/'):
		# Select the center of the lidar data, create a box, and designate the points that intersect buidings.
		SQLcommand = """WITH bbox AS ( SELECT ST_Envelope(ST_Collect(the_geom)) as the_geom FROM %s), \ 
		onebigbuilding AS (SELECT ST_Expand(the_geom, 150) as the_geom FROM (SELECT ST_Centroid(the_geom) as the_geom FROM bbox) a ), \
		portion AS ( SELECT a.*, ROW_NUMBER() OVER (ORDER BY a.x) as r FROM %s a, onebigbuilding b WHERE ST_Intersects(a.the_geom, b.the_geom) ), \
		in_building AS ( SELECT a.r, CAST(1 AS int) AS is_building, b.gid as mock_label FROM portion a, %s b  WHERE ST_Intersects(a.the_geom, b.geom)) \
		SELECT b.x, b.y, b.z, b.id, COALESCE(a.is_building,0) as is_building, COALESCE(a.mock_label,-1) as mock_label FROM portion b LEFT JOIN in_building a ON a.r=b.r;""" % (lidar_name, lidar_name, shp_name)
		
		# Read the results to the quiry to a dataframe
		df = pd.read_sql_query(SQLcommand, self.cur)

		# Export the data for calibrating model
		df.to_csv(path_to_data_directory+'CrossValidationData.csv', index=False, header=0)

	def MockCallBacks(self):
		df = pd.read_csv('data/CrossValidationData.csv');
		df.columns = ['x','y','z','id', 'is_building', 'label']
		
		deleteIndex = df[df['is_building']==0]['id'];
		labels = df[df['is_building']==1][['id','label']];

		# The prune model
		sd = Methods()
		df = sd.splitCallBack(labels, 'lidar_sf__1')
		sd.hullCallBack(df, 'lidar_sf__1');
		sd.deleteCallBack(df, 'lidar_sf__1');
		sd.deleteCallBack(deleteIndex, 'lidar_sf__1')

	def MockPartition(self, split=6):
		sd = Methods(projection=32610)
		sd.partionSpace('lidar_sf', split=split)


if __name__ == "__main__":
	p = pgDataRetriever()
	#p.getCrossValidationData('lidar_sf', 'bld_footprint_sf', path_to_data_directory='../tests/data/')
	#p.MockCallBacks()
	#p.MockPartition()
