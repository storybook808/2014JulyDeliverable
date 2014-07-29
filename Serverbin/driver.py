#!/usr/bin/env python
'''
this is a driver to test the different methods of the ServerUtility class
'''

import ServerUtility

server = ServerUtility.ServerUtility()

#create hobo table
#print 'going to create table'

#insert data into table
#server.insert_data_into_database('a.csv','hobo')

print "this should be true"
print server.check_table_exists('hobo')
print "this should be false"
print server.check_table_exists('reed')
server.create_table('hobo')
server.create_table('reed')
print "this should be true"
print server.check_table_exists('hobo')
print "this should be true"
print server.check_table_exists('reed')

