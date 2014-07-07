#-------------
# Christian A. Damo
# 2014-06-26
#-------------
# Patch Notes
#-------------

#declare imports
import psycopg2
import sys

#declare globals
conn = psycopg2.connect("dbname=postgres user=postgres password=energyaudit1!")
cur = conn.cursor()
insertFile = open(sys.argv[1],"r")
table = sys.argv[2]



#change into eileen shape

#find nulls
#do not add to eileen shape
#add to log file

#insert into database
cur.copy_from(insertFile, table, sep=',')
