import os
import pandas as pd
from sqlalchemy import create_engine
import string, random

class Methods:
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

	def pg_post(self, SQLcommand):
		connection = self.cur.connect()
		connection.execute(SQLcommand);
		connection.close()

	def pg_df(self, SQLcommand):
		
		return pd.read_sql(SQLcommand, self.cur)


	def df_pg(self, dataframe, table_name, if_exists='replace'):
		
		return dataframe.to_sql(table_name, self.cur, if_exists=if_exists)


	def tableSetUp(self, transform_to=None):
		if transform_to:
			projection = transform_to
		else:
			projection = self.projection 
		# Create connection
		connection = self.cur.connect()
		connection.execute("""DROP TABLE IF EXISTS spill_over_master; DROP TABLE IF EXISTS geometries_master; DROP TABLE IF EXISTS points_master;""")
		connection.execute("""CREATE TABLE  spill_over_master (x double precision, y double precision, z double precision, the_geom geometry(Point,%s));""" % projection);
		connection.execute("""CREATE TABLE  geometries_master (label bigint, height double precision, the_geom geometry)""");
		connection.execute("""CREATE TABLE  points_master (x double precision, y double precision, z double precision, the_geom geometry(Point,%s));""" % projection);
		connection.close()



	def partionSpace(self, parent_table, split=3, transform_to=None):
		# Create connection
		connection = self.cur.connect()

		# Get the bounding box of the parent lidar table
		if transform_to:
			boundary = connection.execute('SELECT ST_AsText(ST_Envelope(ST_Collect(ST_Transform(the_geom, %i)))) as bbox FROM %s;' % (self.projection, parent_table)).first().bbox
			projection = transform_to;

			# partion the bounding box into squares with equal area.
			connection.execute("""DROP TABLE IF EXISTS %(parent_table)s_bbox;  
				CREATE TABLE %(parent_table)s_bbox AS \
				SELECT id, ST_Transform(the_geom, %(transform_to)i) AS the_geom\
				FROM ST_SplitLidar('%(parent_table)s', '%(boundary)s', %(split)i, %(projection)i)""" % {'parent_table': parent_table, "split": split, 'boundary': boundary, 'projection':self.projection,'transform_to':transform_to});
		

		else:
			boundary = connection.execute('SELECT ST_AsText(ST_Envelope(ST_Collect(the_geom))) as bbox FROM %s;' % parent_table).first().bbox
			projection = self.projection;
		
			# partion the bounding box into squares with equal area.
			connection.execute("""DROP TABLE IF EXISTS %(parent_table)s_bbox;  
				CREATE TABLE %(parent_table)s_bbox AS \
				SELECT * \
				FROM ST_SplitLidar('%(parent_table)s', '%(boundary)s', %(split)i, %(projection)i)""" % {'parent_table': parent_table, "split": split, 'boundary': boundary, 'projection':projection});
		
		connection.close()
		

	def partionPoints(self, child_bbox_id):
		# Get table names
		parent_table, child_id = child_bbox_id.split('__');

		SQLcommand = """SELECT a.x, a.y, a.z, a.id FROM %(parent_table)s a JOIN %(parent_table)s_bbox b ON ST_Intersects(a.the_geom, b.the_geom) WHERE b.id='%(child_bbox_id)s'""" % {'parent_table':parent_table, 'child_bbox_id':child_bbox_id}

		return pd.read_sql(SQLcommand, self.cur)


	def deleteCallBack(self, dataframe, child_bbox_id):
		# Get table names
		parent_table, child_id = child_bbox_id.split('__');

		# Create a randomly-generated and temporary table
		table_random = "temp_"+''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))
		
		# Send the delete id's to the server 
		dataframe.to_sql(table_random, self.cur, );

		# Delete from the parent table the rows that match the delete id's
		connection = self.cur.connect()
		connection.execute("""DELETE FROM %s z USING %s a WHERE z.id=a.id;""" % (parent_table, table_random));
		
		# Drop the temporary table
		connection.execute('DROP TABLE %s;' % table_random);

		# Close connection
		connection.close();


	def splitCallBack(self, dataframe, child_bbox_id, splill_over_table='spill_over_master'):
		"""
		DataFrame Columns: id, label
		child_bbox_id: {parent table}_{child table id}

		Sends the points of buildings touching the boundary of the group to the server
		and returns a dataframe with the id's of points of buildings in the interior
		"""
		# Create connection
		connection = self.cur.connect()

		# Get table names
		parent_table, child_id = child_bbox_id.split('__');

		# Create a randomly-generated and temporary table
		table_random_index_labels = "temp_"+''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))
		table_random_hold = "temp_"+''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))
		
		# Send the delete id's to the server 
		dataframe.to_sql(table_random_index_labels, self.cur, if_exists='replace');

		# Create temporary table to store spill over data
		SQLcommand = """DROP TABLE IF EXISTS %(table_random_hold)s; \
			CREATE TABLE %(table_random_hold)s AS \
			WITH children AS ( \
			SELECT a.*, b.label \
			FROM %(parent_table)s a \
			RIGHT JOIN %(table_random_index_labels)s b  \
			ON a.id = b.id \
			), blds_touching AS ( \
			SELECT DISTINCT ON (a.label) a.label \
			FROM children a \
			JOIN (SELECT * FROM %(temp_table_bounding_box)s WHERE id='%(temp_table_bounding_box_id)s') b \
			ON ST_DWithin(ST_boundary(b.the_geom), a.the_geom, 1) \
			) SELECT x, y, z, the_geom, id \
			FROM children \
			WHERE label IN (SELECT label FROM blds_touching);""" %{'table_random_hold': table_random_hold,'parent_table': parent_table, 'table_random_index_labels': table_random_index_labels, 'temp_table_bounding_box': parent_table+'_bbox', 'temp_table_bounding_box_id': child_bbox_id}
	
		connection.execute(SQLcommand)

		# Insert spillovers into master table
		SQLcommand = "INSERT INTO %s SELECT x, y, z, the_geom FROM %s;" % (splill_over_table,table_random_hold)
		connection.execute(SQLcommand)

		# pass back id's and labels for buildings not touching the edges
		SQLcommand = "SELECT * FROM %s WHERE id NOT IN (SELECT id FROM %s);" %(table_random_index_labels, table_random_hold)
		inside_points = pd.read_sql_query(SQLcommand, self.cur)

		# Drop the temporary table
		connection.execute('DROP TABLE IF EXISTS %s;' % table_random_index_labels);
		connection.execute('DROP TABLE IF EXISTS %s;' % table_random_hold);

		# Close connection
		connection.close();

		return inside_points[['id', 'label']]


	def hullCallBack(self, dataframe, child_bbox_id, target_percent=1, geometry_master_table='geometries_master'):
		"""
		target_percent: The target_percent is the target percent of area of convex hull the PostGIS solution will try 
		to approach before giving up or exiting. One can think of the concave hull as the geometry you get by vacuum 
		sealing a set of geometries. The target_percent of 1 will give you the same answer as the convex hull. A 
		target_percent between 0 and 0.99 will give you something that should have a smaller area than the convex hull. 
		This is different from a convex hull which is more like wrapping a rubber band around the set of geometries.
		"""

		# Create connection
		connection = self.cur.connect()

		# Get table names
		parent_table, child_id = child_bbox_id.split('__');

		# Create a randomly-generated and temporary table
		table_random_index_labels = "temp_"+''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))
		
		# Send the delete id's to the server 
		dataframe.to_sql(table_random_index_labels, self.cur, if_exists='replace');

		SQLcommand = """INSERT INTO %(geometry_master_table)s\
			WITH children AS ( \
			SELECT a.*, b.label \
			FROM %(parent_table)s a \
			RIGHT JOIN %(table_random_index_labels)s b  \
			ON a.id = b.id \
			) SELECT label, AVG(z) AS height, ST_ConcaveHull(ST_Collect(the_geom), %(target_percent)f) as the_geom
			FROM children GROUP BY label""" %{'geometry_master_table': geometry_master_table, 'parent_table': parent_table, 'table_random_index_labels': table_random_index_labels, 'target_percent': target_percent}

		connection.execute(SQLcommand)

		# Drop the temporary table
		connection.execute('DROP TABLE IF EXISTS %s;' % table_random_index_labels);

		# Close connection
		connection.close();


	def loadpointsCallBack(self, dataframe, child_bbox_id, points_master_table='points_master'):
		# Create connection
		connection = self.cur.connect()

		# Get table names
		parent_table, child_id = child_bbox_id.split('__');

		# Create a randomly-generated and temporary table
		table_random_index_labels = "temp_"+''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))
		
		# Send the delete id's to the server 
		dataframe.to_sql(table_random_index_labels, self.cur, if_exists='replace');

		# Insert spillovers into master table
		SQLcommand = "INSERT INTO %s SELECT a.x, a.y, a.z, a.the_geom FROM %s a RIGHT JOIN %s b on a.id=b.id;" % (points_master_table, parent_table, table_random_index_labels)
		connection.execute(SQLcommand)

		# Drop the temporary table
		connection.execute('DROP TABLE IF EXISTS %s;' % table_random_index_labels);

		# Close connection
		connection.close();

		
