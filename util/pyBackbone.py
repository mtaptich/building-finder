import pandas as pd, numpy as np
from ntpath import basename

from pgDataLoader import pgDataLoader
from pgMethods import Methods
from pyCluster import dbscan

class Building:
	def __init__(self, database='building', User='Mike', host='localhost', projection=4326, transform_to=None):
		# If the dataset is not in metric units, transform projection
		self.transform_to = transform_to;

		# file uploads 
		self.upload = pgDataLoader(database=database, User=User, host=host, projection=projection)

		# methods to cal to server
		self.methods = Methods(database=database, User=User, host=host, projection=projection);

		# Reset storage tables
		self.methods.tableSetUp(transform_to=transform_to);

	def screen(self, file_path, hull=True, load_lidar=False, split=10):
		# Create table name from file path
		lidar_name = "z_"+basename(file_path).split('.')[0];
		
		if load_lidar:
			# Load the data to server
			self.upload.lidar_to_server(file_path, lidar_name, transform_to=self.transform_to)

		# Split the bounding box into equal children
		self.methods.partionSpace(lidar_name, split=split, transform_to=self.transform_to)

		# Get a dataframe of bounding box id's
		bbox_ids = self.methods.pg_df('SELECT id FROM %s_bbox' % lidar_name);
		
		for index, row in bbox_ids.T.iteritems():
			# Get the id of the child bounding box
			child_id = row.id;
			print child_id

			# Get the points intersecting this bounding box
			pts = self.methods.partionPoints(child_id)
	
			# Get the points of buildings and not buildings
			blds, not_blds = dbscan(pts, zboost=1.4)
			pts = None; # Save some memory

			if hull:

				# Split points into interior points and boundary points. 
				# Save the boundary points to an overflow table. 
				inside_points = self.methods.splitCallBack(blds, child_id)
				blds = None; # Save some memory

				# Calculate the outline of the interior buildings and save geometries.
				self.methods.hullCallBack(inside_points, child_id)
				inside_points = None

			else:

				self.methods.loadpointsCallBack(blds, child_id);
				blds = None; # Save some memory


if __name__ == "__main__":
	b = Building(transform_to=5070)
	b.screen('/Volumes/Mike_external/Data/LIDAR/NYC/20130805_usgsnyc14_18TWL835075.txt', hull=False);

	