#!/usr/bin/env python
'''
This script is designed to gather data and input them into a PostgreSQL
database.

'''
import datetime
import time
from Egaugebin import EgaugeUtility
from Databasebin import DatabaseUtility
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
	'''
	given: nothing
	return: nothing, but carries out the entire processing of the hobo files
	        when they are saved to the ADASEED>hobo folder. Reshapes the raw file to a csv,
		then edits the output by reshaping to eshape and finally inserting it
		into the database.
	'''
	#get the files in the ADASEED>hobo directory
	hobo_files = hobo.get_hobo_files()
	#if there's no files to process, tell the user there isn't any and end the function
	if len(hobo_files) == 0:
		print "At "+str(datetime.datetime.now())+" there were no data collected from the Hobo Network\n"
		return 0
	#if there ARE files to process, then for each file do the following
	for hobo_file in hobo_files:
		#inform the user you're processing a particular file
		print 'processing '+hobo_file
		#extract the data from the raw hobo shaped file
		output_filename = hobo.extract_data(hobo_file)
		#edit the file to do something, look at the HoboUtility comments
		output_filename = hobo.edit_output(output_filename)
		#once the file is in an eshape go ahead and insert it into the database
		database.insert_data_into_database(output_filename,'hobo')
		#delete all the hobo meta files
		hobo.clean_folder()

try:
	#displaoy greeting
	print ''
	print '                  *****    Welcome to the    *****'
	print '"Automatic Data Acquisition System for Energy and Environmental Data"'
	print '                             (ADASEED)\n'

	#instance Classes
	egauge = EgaugeUtility.EgaugeUtility()
	database=DatabaseUtility.DatabaseUtility()
	hobo = HobowareUtility.HobowareUtility()

	#setup the tables in the database if not setup
	try:
		database.create_table('hobo')
		database.create_table('egauge')
	except:
		#if it fails, let the user know that it failed and end the program
		print 'PostgreSQL is not properly installed.'
		print 'Please refer to the manual for proper installation,'
		print 'and then relaunch ADASEED again.'
		raw_input('Press Enter to quit "ADASEED"...')
		quit()
  
     #setup directories for processing
	if not os.path.exists('egauge'):
		os.makedirs('egauge')
  
	if not os.path.exists('hobo'):
		os.makedirs('hobo')

	if not os.path.exists('archive\egauge'):
		os.makedirs('archive\egauge')
      
	if not os.path.exists('archive\hobo'):
		os.makedirs('archive\hobo')
          
		

	#check to see if egauge is properly installed and synced up
	egauge.check_time_sync()
	#wait for the user so that they can make sure everything is in working order
	raw_input('Press Enter to start "ADASEED"...')

	#get time now and store as 'from' time for use in the "except" block.
	old_time = datetime.datetime.now()
	from_str = str(old_time)[:-9]+'00'

	#let the user know that we're starting the session
	print 'Your session has started at '+from_str
	print 'Hit control-c to end session'

	#this is a workaround to hand over a variable to the except block. If someone can
	#do this correctly please do
	egauge.store_last_query_time(from_str)
	#start looping
	while 1:
		#wait every 2 hours
		time.sleep(7200)

		#insert hobo data into database, etc. see documentation above
		process_hobo_files()

		#take time now store as "to" time, the "from" and "to" time is neeaded
		#to query the window of data you want from the egauge.
		#probably should package this into one function to keep the main code clean
		new_time = datetime.datetime.now()
		#thisis a formatting issue
		to_str = str(new_time)[:-9]+'00'
		#query egauge data between old time and new time
		data, from_str, to_str = egauge.pull_from_egauge(from_str,to_str)
		#convert the data to csv
		egaugeFilename = egauge.data_to_csv(data,from_str,to_str)
		#convert csv to eileen shape (aka eshape)
		outputFilename= egauge.egauge_to_eshape(egaugeFilename)
		#insert egauge data into database
		database.insert_data_into_database(outputFilename,'egauge')
		#set old time to new time, for the next iteration 2 hours from now
		from_str = to_str
		egauge.store_last_query_time(from_str)

		#move files to the archive folder
		egauge.archive_egauge_data()
		hobo.archive_hobo_data()

except KeyboardInterrupt:
	#this block of code basically carries out the same sequence of commands
	#in the while loop above
	#let the user know wer're finishing up
	print 'Ending the "Automatic Data Acquisition System for Energy and Environmental Data"\n'
	#process any hobo files
	process_hobo_files()

	#gather the last set of data from the eguage
	egauge.retrieve_last_query_time()
	to_str = str(datetime.datetime.now())[:-9]+'00'
	#query egauge data between old time and new time
	data, from_str, to_str = egauge.pull_from_egauge(from_str,to_str)
	#convert the data to csv
	egaugeFilename = egauge.data_to_csv(data,from_str,to_str)
	#convert csv to eileen shape
	outputFilename= egauge.egauge_to_eshape(egaugeFilename)
	#insert egauge data into database
	database.insert_data_into_database(outputFilename,'egauge')

	#move files to the archive folder
	egauge.archive_egauge_data()
	hobo.archive_hobo_data()

	#basic cleanup of any outstanding meta files
	egauge.clean_egauge_meta_files()
	os.remove('lastQueryTime.csv')

	#let the user manually end the program so that they can read
	#any outputs that were created.
	raw_input('Press Enter to quit "ADASEED"...')
		
	
