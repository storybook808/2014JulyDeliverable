#!/usr/bin/env python
'''
This script is a server utility for the ADASEED software.
'''

import datetime
import time
import os
import psycopg2
import logging
import csv

__author__ = "Christian A. Damo"
__copyright__ = "Copyright 2014 School of Architecture, University of Hawaii at Manoa"
__credits__ = ["Christian A. Damo", "Reed Shinsato"]
__version__ = "0.01"
__maintainer__ = "Eileen Peppard"
__email__ = "epeppard@hawaii.edu"
__status__ = "Prototype"

logging.basicConfig(filename='error_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

class ServerUtility:
	def __init__(self,dbname='postgres',user='postgres',password='postgres'):
		serverInfo = 'dbname='+dbname+' user='+user+' password='+password
		self.conn = psycopg2.connect(serverInfo)
		self.cur = self.conn.cursor()

	def insert_data_into_database(self, insertFilename, tableName):
		'''
		given: a file name of a csv file and a table name in string form
		return: nothing, but inputs the dta into a psql database on the local comuter
		        into the specified table name
		'''
		
	 
		insertFile = open(insertFilename,'r')
		reader = csv.reader(insertFile)
		error = 0
		print "Inserting data into the "+tableName+" now ..."
		for row in reader:
			try:
				self.cur.execute("BEGIN;")
				self.cur.execute("SAVEPOINT my_savepoint;")
				#create the command name here
				sql = "INSERT INTO "+tableName+" (timestamp,sensor_id,value) VALUES ('"+row[0]+"','"+row[1]+"',"+ row[2]+");" 
				self.cur.execute(sql)
				#self.cur.execute("INSERT INTO hobo (datetime,sensor_id,value) VALUES (%s,%s,%s);",(row[0],row[1],row[2]))
				self.conn.commit()
			except Exception, e:
				self.cur.execute("ROLLBACK TO SAVEPOINT my_savepoint;")
				logging.warning(str(row[0])+","+row[1]+","+str(row[2])+str(e)+"\n")
				error = error + 1
		print "At "+str(datetime.datetime.now()) + " there were "+str(error)+" error(s)"
		print "please refer to the 'error_log' file for more information.\n"
		insertFile.close()
		#os.remove(insertFilename)

	def create_table(self,tableName):
		'''
		given: a string
		return: nothing but creates a table in the psql database with the string as the name
		'''
		if self.check_table_exists(tableName) == True:
			return 1
		try:
			sql = 'CREATE TABLE '+tableName+'( "timestamp" timestamp without time zone NOT NULL, sensor_id character varying NOT NULL, value double precision, CONSTRAINT '+tableName+'_prim_key PRIMARY KEY ("timestamp", sensor_id)) WITH( OIDS=FALSE);'			
			self.cur.execute(sql)
			self.conn.commit()
			sql = 'ALTER TABLE '+tableName+' OWNER TO postgres;'
			self.cur.execute(sql)
			self.conn.commit()
		except Exception, e:
			print "did not create table "+tableName


	def close_conn(self):
		self.conn.close()

	def check_table_exists(self,tableName):
		self.cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName,))
		return self.cur.fetchone()[0]



