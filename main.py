#--------------------------
# Author: Christian A. Damo
# date: 2014-06-26
#--------------------------

import os
import subprocess
import csv
import shutil
import sys
import xmlrpclib
import psycopg2
import datetime
import shutil
import logging
from hoboReshape import process_hobo_file

#setup logging
logging.basicConfig(filename='error_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

#setup the server connection for data transferring
conn = psycopg2.connect("dbname=postgres user=postgres password=energyaudit1!")
cur = conn.cursor()

def insert_data(insertFile,conn,table):
	import csv
	reader = csv.reader(insertFile)
	error = 0
	for row in reader:
		try:
			cur.execute("BEGIN;")
			cur.execute("SAVEPOINT my_savepoint;")
			if table == 'hobo':
				cur.execute("INSERT INTO hobo (datetime,sensor_id,value) VALUES (%s,%s,%s);",(row[0],row[1],row[2]))
			elif table == 'egauge':
				cur.execute("INSERT INTO egauge (datetime,sensor_id,value) VALUES (%s,%s,%s);",(row[0],row[1],row[2]))
			else:
				print "Warning: table not specified"
				logging.warning('table not specified')
				quit()
			conn.commit()

		except Exception, e:
			cur.execute("ROLLBACK TO SAVEPOINT my_savepoint;")
			logging.warning(str(row[0])+","+row[1]+","+str(row[2])+str(e)+"\n")
			error = error + 1
#		else:
#			cur.execute("RELEASE SAVEPOINT my_savepoint;")
	print "There are "+str(error)+" error(s)\n"
	insertFile.close()

def inser_data_egauge(insertFile,conn):
	import csv
	reader = csv.reader(insertFile)
	cur.execute("BEGIN;")
	error = 0
	for row in reader:
		try:
			cur.execute("SAVEPOINT my_savepoint;")
			cur.execute("INSERT INTO egauge (datetime,sensor_id,value) VALUES (%s,%s,%s);",(row[0],row[1],row[2]))
		except Exception, e:
			cur.execute("ROLLBACK TO SAVEPOINT my_savepoint;")
			loginput = open("log.txt","a")
			loginput.write(str(row[0])+","+row[1]+","+str(row[2])+str(e)+"\n")
			loginput.close()
			error = error + 1
		else:
			cur.execute("RELEASE SAVEPOINT my_savepoint;")
	print "There are "+str(error)+" error(s)\n"
	conn.commit()
	

#gather the names of files from the hobo network
targetFilenames = os.listdir('hobo')

#for each target file
for targetFilename in targetFilenames:
	#run the script for that target file
	targetPath = os.path.join('hobo',targetFilename)
	process_hobo_file(targetPath)

	#insert the data into the database
	insertFile = open("filtered_conversion.csv","r")
	insert_data(insertFile,conn,'hobo')
	quit()


#do the same thing with the egauge
targetFilenames = os.listdir('egauge')
for targetFilename in targetFilenames:
	targetPath = os.path.join('egauge',targetFilename)
	insertFile = open(targetPath,"r")
	insert_data(insertFile,conn,'egauge')
	insertFile.close()
		
#move file to archive and delete all metafiles
os.remove("filtered_conversion.csv")
os.remove("full_conversion.csv")
#get the list of hobo files
targetFilenames = os.listdir('hobo')
for targetFilename in targetFilenames:
	src = os.path.join('hobo',targetFilename)
	dst = os.path.join('archive','hobo',targetFilename)
	shutil.move(src,dst)

#get the list of egauge files
targetFilenames = os.listdir('egauge')
for targetFilename in targetFilenames:
	src = os.path.join('egauge',targetFilename)
	dst = os.path.join('archive','egauge',targetFilename)
	shutil.move(src,dst)
