#!/usr/bin/env python
'''
This script is designed to gather data and input them into a PostgreSQL
database.

'''
import datetime
import time
from Egaugebin import EgaugeUtility
from Serverbin import ServerUtility
from Hobowarebin import HobowareUtility
import os

__author__ = "Christian A. Damo"
__copyright__ = "Copyright 2014 School of Architecture, University of Hawaii at Manoa"
__credits__ = ["Christian A. Damo", "Reed Shinsato"]
__version__ = "0.01"
__maintainer__ = "Eileen Peppard"
__email__ = "epeppard@hawaii.edu"
__status__ = "Prototype"


def process_hobo_files():
	hobo_files = hobo.get_hobo_files()
	if len(hobo_files) == 0:
		print "At "+str(datetime.datetime.now())+" there were no data collected from the Hobo Network\n"
		return 0
	for hobo_file in hobo_files:
		print 'processing '+hobo_file
		output_filename = hobo.extract_data(hobo_file)
		output_filename = hobo.edit_output(output_filename)
		server.insert_data_into_database(output_filename,'hobo')
		hobo.clean_folder()

try:
	#displaoy greeting
	print ''
	print '                  *****    Welcome to the    *****'
	print '"Automatic Data Acquisition System for Energy and Environmental Data"'
	print '                             (ADASEED)\n'

	#instance Class
	egauge = EgaugeUtility.EgaugeUtility()
	server=ServerUtility.ServerUtility()
	hobo = HobowareUtility.HobowareUtility()

	#setup the tables if not setup
	try:
		server.create_table('hobo')
		server.create_table('egauge')
	except:
		print 'PostgreSQL is not properly installed.'
		print 'Please refer to the manual for proper installation,'
		print 'and then relaunch ADASEED again.'
		raw_input('Press Enter to quit "ADASEED"...')
		quit()
		

	#check to see if egauge is properly installed and synced up
	egauge.check_time_sync()
	raw_input('Press Enter to start "ADASEED"...')

	#get time now and store as 'from' time
	old_time = datetime.datetime.now()
	from_str = str(old_time)[:-9]+'00'
	print 'Your session has started at '+from_str
	print 'Hit control-c to end session'
	egauge.store_last_query_time(from_str)
	#start looping
	while 1:
		#wait every 2 hours
		time.sleep(7200)
		#insert hobo data into database
		process_hobo_files()
		#take time now store as "to" time
		new_time = datetime.datetime.now()
		to_str = str(new_time)[:-9]+'00'
		#query egauge data between old time and new time
		data, from_str, to_str = egauge.pull_from_egauge(from_str,to_str)
		#convert the data to csv
		egaugeFilename = egauge.data_to_csv(data,from_str,to_str)
		#convert csv to eileen shape
		outputFilename= egauge.egauge_to_eshape(egaugeFilename)
		#insert egauge data into database
		server.insert_data_into_database(outputFilename,'egauge')
		#set old time to new time
		from_str = to_str
		egauge.store_last_query_time(from_str)
		#move files to the archive folder
		egauge.archive_egauge_data()
		hobo.archive_hobo_data()
except KeyboardInterrupt:
	print 'Ending the "Automatic Data Acquisition System for Energy and Environmental Data"\n'
	#process any hobo files
	process_hobo_files()
	#gather the last set of data
	egauge.retrieve_last_query_time()
	os.remove('lastQueryTime.csv')
	to_str = str(datetime.datetime.now())[:-9]+'00'
	#query egauge data between old time and new time
	data, from_str, to_str = egauge.pull_from_egauge(from_str,to_str)
	#convert the data to csv
	egaugeFilename = egauge.data_to_csv(data,from_str,to_str)
	#convert csv to eileen shape
	outputFilename= egauge.egauge_to_eshape(egaugeFilename)
	#insert egauge data into database
	server.insert_data_into_database(outputFilename,'egauge')
	egauge.archive_egauge_data()
	hobo.archive_hobo_data()
	egauge.clean_egauge_meta_files()
	raw_input('Press Enter to quit "ADASEED"...')
		
	
