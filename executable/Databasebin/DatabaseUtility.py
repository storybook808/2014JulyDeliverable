#!/usr/bin/env python
'''
This script is a database utility for the ADASEED software.
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

logging.basicConfig(filename='ADASEED_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

class DatabaseUtility:
	'''
	This is a package for interfacing with the postgres database
	through the python. This is basically an API that controls actions
	done to the database utilizing the psycopg2 package.
	'''
	def __init__(self,dbname='postgres',user='postgres',password='postgres'):
		DatabaseInfo = 'dbname='+dbname+' user='+user+' password='+password
		self.conn = psycopg2.connect(DatabaseInfo)
		self.cur = self.conn.cursor()

	def insert_data_into_database(self, insertFilename, tableName):
		'''
		given: a file name of a csv file and a table name in string form
		return: nothing, but inputs the dta into a psql database on the
		        local comuter into the specified table name
		'''
		#setup the csv reader object
		insertFile = open(insertFilename,'r')
		reader = csv.reader(insertFile)
		#start a counter to keep track of how many errors occurred
		error = 0
		print "Inserting data into the "+tableName+" now ..."
		
		#this block of code feeds into the database one entry at a time
		#if there is an error during insertion eg, violates primary key
		#constraints, then it counts the error and outputs it to the 
		#user.
		for row in reader:
			try:
				#keeps a savepoint so that in the event there
				#is an error, then it will roll back in the 
				#exception block of code
				self.cur.execute("BEGIN;")
				self.cur.execute("SAVEPOINT my_savepoint;")
				#create the command name here
				sql = "INSERT INTO "+tableName+" (timestamp,sensor_id,value) VALUES ('"+row[0]+"','"+row[1]+"',"+ row[2]+");" 
				#this executes the command stored in sql
				#it acts like you just typed the command line
				#at the psql prompt
				self.cur.execute(sql)
				#this commits the command in the psql
				#similar to hitting enter
				self.conn.commit()
			except Exception, e:
				self.cur.execute("ROLLBACK TO SAVEPOINT my_savepoint;")
				logging.warning(str(row[0])+","+row[1]+","+str(row[2])+str(e)+"\n")
				#keeps record of how many errors occured
				error = error + 1
		#output to the user stating at what time the insertion
		#finished and how many errors there were
		print "At "+str(datetime.datetime.now()) + " there were "+str(error)+" error(s)"
		print "please refer to the 'ADASEED_log' file for more information.\n"
		insertFile.close()

	def create_table(self,tableName):
		'''
		given: a string
		return: nothing but creates a table in the psql database with
		       the string as the name
		'''
		#this checks to see if the table already exists in the database
		#if the specified table name exists then it ends the method
		#with a return of 1
		if self.check_table_exists(tableName) == True:
			return 1
			
		try:
			#otherwise the program creates the command string
			sql = 'CREATE TABLE '+tableName+'( "timestamp" timestamp without time zone NOT NULL, sensor_id character varying NOT NULL, value double precision, CONSTRAINT '+tableName+'_prim_key PRIMARY KEY ("timestamp", sensor_id)) WITH( OIDS=FALSE);'			
			#types it out at the psql prompt
			self.cur.execute(sql)
			#then hits enter to commit the command
			self.conn.commit()
			#We then change the owner of the table to postgres
			#for security purposes, most likely it defaults to
			#postgres, but this is just to make sure
			sql = 'ALTER TABLE '+tableName+' OWNER TO postgres;'
			#again execute is as if you're typing thte command at
			#the psql prompt
			self.cur.execute(sql)
			#commits it as if you're hitting enter at the end of
			#the command
			self.conn.commit()
		except Exception, e:
			#in the event there's some error with the above code
			#the software will notify the user that it failed
			print "did not create table "+tableName


	def close_conn(self):
		'''
		given: nothing
		return: nothing but closes the connection, this is just for good
		house keeping practice
		'''
		self.conn.close()

	def check_table_exists(self,tableName):
		'''
		given: a string of the name of the table desired
		return: a True or False to say if the specified table name
		       does indeed exist
		'''
		#queries the database to see if the table is within the list
		#of tables that's in the database's schema
		self.cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName,))
		#return True or False if the specified table name exists or not
		return self.cur.fetchone()[0]



