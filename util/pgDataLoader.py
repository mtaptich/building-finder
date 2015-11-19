import os
import pandas as pd
from sqlalchemy import create_engine
import string, random


class pgDataLoader:
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


	def shp_to_server(self, shp_path, shp_name, base_projection=4326):
		# Create connection
		connection = self.cur.connect()

		# Drop table from server 
		SQLcommand = "DROP TABLE IF EXISTS %s;"  %  shp_name;
		connection.execute(SQLcommand)

		# Load shapefile to server
		c = "shp2pgsql -I -s %i '%s' public.%s | psql -d %s" % (base_projection, shp_path, shp_name, self.database)
		os.system(c)

		# Update projection if needed
		if base_projection != self.projection:
			SQLcommand = "ALTER TABLE %s ALTER COLUMN geom TYPE geometry USING ST_Transform(geom, %i)"  % (shp_name, self.projection)
			connection.execute(SQLcommand)

		# Close server connection
		connection.close()

	def lidar_to_server(self, lidar_path_to_dilimited,lidar_name):

		# Create connection
		connection = self.cur.connect()

		# force table to lowercase
		lidar_name = lidar_name.lower();

		#Drop table from server
		SQLcommand = "DROP TABLE IF EXISTS %s" % lidar_name
		connection.execute(SQLcommand)

		# Create empty table
		SQLcommand = "CREATE TABLE %s (x double precision, y double precision, z double precision)"  % lidar_name
		connection.execute(SQLcommand)

		# Insert lidar data
		SQLcommand = "COPY %s FROM '%s' CSV;" % ( lidar_name, lidar_path_to_dilimited)
		connection.execute(SQLcommand)

		# Add a geometry column
		SQLcommand = "SELECT AddGeometryColumn('%s','the_geom',%i,'POINT',2)"  % (lidar_name, self.projection)
		connection.execute(SQLcommand)

		# Use point data to create geometry
		SQLcommand = "UPDATE %s SET the_geom = ST_GeomFromText('POINT(' || x || ' ' || y || ')',%i)"  % (lidar_name, self.projection)
		connection.execute(SQLcommand)

		# Drop previous spatial index is needed
		SQLcommand = "DROP INDEX if EXISTS %s_gis" % lidar_name
		connection.execute(SQLcommand)

		# Add spatial index to improve spatial operations
		SQLcommand = "CREATE INDEX %s_gis ON %s USING GIST (the_geom) "  % (lidar_name, lidar_name)
		connection.execute(SQLcommand)

		_random = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(2))
		SQLcommand = "DROP SEQUENCE IF EXISTS ids_%s; CREATE SEQUENCE ids_%s; ALTER TABLE %s ADD id INT UNIQUE; UPDATE %s SET id = NEXTVAL('ids_%s');"  %(_random, _random, lidar_name, lidar_name, _random)
		connection.execute(SQLcommand)

		SQLcommand = "CREATE INDEX idx_%s ON %s USING btree (id);" %(_random, lidar_name)
		connection.execute(SQLcommand)

		# Close connection with server
		connection.close()




if __name__ == "__main__":
	#dl = pgDataLoader(projection=32610)
	#dl.lidar_to_server('/Users/Mike/Google Drive/water_Mike_Olga/Group Project/Data/lidar_SF.txt', 'lidar_sf')
	pass

