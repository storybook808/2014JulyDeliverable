#!/usr/bin/env python
'''
This script is designed to gather data and input them into a PostgreSQL
database.

'''
import datetime
import time
from Egaugebin import EgaugeUtility
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
		print "There were no data collected from the Hobo Network\n"
		return 0
	for hobo_file in hobo_files:
		output_filename = hobo.extract_data(hobo_file)
		output_filename = hobo.edit_output(output_filename)
		hobo.insert_hobo_data_into_database(output_filename)
		hobo.clean_folder()

try:
	print '\n                *****    Welcome to the    *****'
	print '"Autonomous Acquisition of Energy and Environmental Data System"'
	print '                           (A2-E2 DS)\n'
	egauge = EgaugeUtility.EgaugeUtility()
	try:
		old_time = datetime.datetime.now()
		from_str = str(old_time)[:-9]+'00'	
		to_str = from_str
		data, from_str, to_str = egauge.pull_from_egauge(from_str,to_str)
	except:
		print 'Your eGauge device is not properly connected.'
		print 'Please check your settings and that the ethernet'
		print 'cable is properly connected, then run ADASEED again.'
		quit()
	raw_input('Press Enter to start the "A2-E2 DS"...')
	#instance EgaugeUtility Class
	hobo = HobowareUtility.HobowareUtility()
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
		egauge.insert_egauge_data_into_database(outputFilename)
		#set old time to new time
		from_str = to_str
		egauge.store_last_query_time(from_str)
		#move files to the archive folder
		egauge.archive_egauge_data()
		hobo.archive_hobo_data()
except KeyboardInterrupt:
	print 'Ending the "Autonomous Acquisition of Energy and Environmental Data System (A2-E2 DS)"\n'
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
	egauge.insert_egauge_data_into_database(outputFilename)
	egauge.archive_egauge_data()
	hobo.archive_hobo_data()
		
	
