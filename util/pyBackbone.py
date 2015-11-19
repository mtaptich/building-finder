import pandas as pd, numpy as np
from ntpath import basename

from pgDataLoader import pgDataLoader
from pgMethods import Methods
from pyCluster import dbscan

class Building:
	def __init__(self, database='building', User='Mike', host='localhost', projection=4326):
		# file uploads 
		self.upload = pgDataLoader(database=database, User=User, host=host, projection=projection)

		# methods
		self.methods = Methods(database=database, User=User, host=host, projection=projection);

	def screen(self, file_path):
		# Create table name from file path
		lidar_name = "z_"+basename(file_path).split('.')[0];
		lidar_name = 'lidar_sf'
		
		# Load the data to server
		#self.upload.lidar_to_server(file_path, lidar_name)

		# Split the bounding box into equal children
		self.methods.partionSpace(lidar_name)

		# Get a dataframe of bounding box id's
		bbox_ids = self.methods.pg_df('SELECT id FROM %s_bbox' % lidar_name);
		
		for index, row in bbox_ids.T.iteritems():
			# Get the id of the child bounding box
			child_id = row.id;
			print child_id

			# Get the points intersecting this bounding box
			pts = self.methods.partionPoints(child_id)

			# Get the points of buildings and not buildings
			blds, not_blds = dbscan(pts)
			pts = None; # Save some memory

			# Split points into interior points and boundary points. 
			# Save the boundary points to an overflow table. 
			inside_points = self.methods.splitCallBack(blds, child_id)
			blds = None; # Save some memory

			# Calculate the outline of the interior buildings and save geometries.
			self.methods.hullCallBack(inside_points, child_id)
			inside_points = None


if __name__ == "__main__":
	b = Building(projection=32610)
	b.screen('../../Data/Lidar/lidar_SF.txt')