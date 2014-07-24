import logging
import shutil
import httplib2
import httplib
from datetime import datetime
from datetime import timedelta
import calendar
import xml.etree.ElementTree as ET
import csv
import os

logging.basicConfig(filename='error_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

class EgaugeUtility:
	
	def __init__(self,\
		file_location = os.path.dirname(os.path.abspath(__file__)),\
		output_file_location = os.path.dirname(os.path.abspath(__file__))):
		self._file_location = file_location
		self._output_file_location = output_file_location

	def egauge_to_eshape(self,src):
		'''
		takes an egauge.csv file and reshapes it into the eileen
		shape which is a timestamp,sensor_id,value
		'''
		inputFilePath = os.path.join('egauge',src)
		inputFile = open(inputFilePath,"r")
		reader = csv.reader(inputFile)
		outputFilename = 'temp.csv'
		outputFile= open(outputFilename,'wb')
		writer = csv.writer(outputFile)
		header=reader.next()
		header=header[1:]
		#disregard the first line which has cumulative value
		try:
			reader.next()
		except:
			pass


		for row in reader:
			timestamp = row[0]
			row = row[1:]
			newRow = [timestamp]
			for index in range(len(header)):
				sensor_id = header[index].replace(' ','_')
				newRow.append(sensor_id)
				value = round(-(float(row[index])/3600000),5)
				newRow.append(value)
				writer.writerow(newRow)
				newRow = [timestamp]

		inputFile.close()
		outputFile.close()
		return outputFilename

	def parse_datetime(self,str) :
	  '''
	  takes a string of various formats and returns a
	  datetime object representing the string
	  '''
	  if str == None :
	    return None
	  try :
	    return datetime.strptime(str, "%Y-%m-%d %H:%M:%S")
	  except ValueError :
	    try :
	      return datetime.strptime(str, "%Y-%m-%dT%H:%M:%S")
	    except ValueError :
	      return datetime.strptime(str, "%Y-%m-%d")
	      
	def get_ts(self,dt):
	    return calendar.timegm(dt.utctimetuple())

	def egauge_fetch_data(self,egauge_url, from_time, to_time, username=None, password=None, seconds=False):
	  """
	  curl -v --digest  -uuser
	  'http://egauge2592.egaug.es/cgi-bin/egauge-show?f=1337716679&m&C'
	
	  params
	  f: from unix timestamp
	  t: to unix tsimestmp
	  seconds: fetch 1second granularity data, default is 1 minute
	  egauge needs digest authentication
	  """
	  # f - The timestamp of the first row to be returned
	  # t - The timestamp of the last row to be returned
	  # The data is in decending order
	
	  gw_url = "http://"+egauge_url+"/cgi-bin/egauge-show"
	
	  if from_time and to_time:
	    params = "?f=" + str(to_time) + "&t=" + str(from_time) + "&C"
	    if seconds:
	      params += "&S"
	    else:
	      params += "&m"
	  else:
	    logging.info("-f and -t both are mandatory.")
	    return None, None
	
	  logging.info( "Fetching :%s%s "% (gw_url, params))
	
	  # If the DB has value 'NULL' then that maps to 'None' in python object.
	  # It gives exception for httplib API's, so set it to "" empty string if they are 'None'
	  if username == None:
	    username = "owner"
	  if password == None:
	    password = "default"
	
	  try:
	    req = httplib2.Http(timeout=15)
	    req.add_credentials(username, password)   # Digest Authentication
	    response, content = req.request (gw_url + params, headers={'Connection': 'Keep-Alive', 'accept-encoding': 'gzip'})
	    if response['status'] == '401':
	      logger.info("Unauthorized request!")
	    elif response['status'] == '400':
	      logger.info( "Bad Request!")
	    elif response['status'] == '500':
	      logger.info("Internal Error!")
	    elif response['status'] == '408':
	      logger.info("Request timeout!")
	    elif response['status'] == '404':
	      logger.info("device not found. Probably it is not up")
	
	    if response['status'] != '200':
	      return None, None
	  except httplib.IncompleteRead as e:
	    raise(e)
	  except Exception as e:
	    print e
	    return None, None
	  return response, content
	    
	def pull_from_egauge(self,from_str,to_str):
	    from optparse import OptionParser
	    parser = OptionParser(usage="usage: %prog device_url from_date to_date [options]")
	    parser.add_option("--seconds", default=False, action="store_true",
	                    help="will try to fetch seconds data if specified")
	    parser.add_option( "--username")
	    parser.add_option( "--password")
	    (options, args) = parser.parse_args()
	
	    #if len(args)<3:
	    #    parser.print_help()
	    #    exit(2)
	
	    #device_url = args[0]
	    #from_str = args[1]
	    #to_str = args[2]
	
	    from_ts = self.get_ts(self.parse_datetime(from_str))
	    to_ts = self.get_ts(self.parse_datetime(to_str))
	    #add timezone offset here
	    offset = self.get_local_time_offset()
	    gmt_from_ts = from_ts - offset
	    gmt_to_ts = to_ts - offset

	    #status, data = egauge_fetch_data('egauge4077.egaug.es',
	    #            1356512726,1356612726)
	
	
	    status, data = self.egauge_fetch_data('192.168.1.88', gmt_from_ts, gmt_to_ts,
	            username=options.username, password=options.password)

	    root = ET.fromstring(data)
	    return root, from_str, to_str
	 
	def data_to_csv(self, root, from_str, to_str):
	    from_str = from_str.replace(' ','_').replace(':','.')
	    to_str = to_str.replace(' ','_').replace(':','.')
	    outputFilename = 'egauge_'+from_str+'_'+to_str+'.csv'
	    outputFilePath = os.path.join('egauge', 'egauge_'+from_str+'_'+to_str+'.csv')
	    outputFile = open(outputFilePath,"wb")
	    writer = csv.writer(outputFile)
	
	    newRow = ["datetime"]
	    #get the number of columns
	    columns = int(root[0].attrib['columns'])
	    for index in range(columns):
		    newRow.append(root[0][index].text)
	    writer.writerow(newRow)
	
	    #setup timestamp
	    #this is where you need to fix it so that the timestamp is local time
	    timestamp = int(root[0].attrib['time_stamp'],0)
	    offset = self.get_local_time_offset()
	    timestamp = timestamp + offset
	    timestamp = datetime.utcfromtimestamp(timestamp)
	    
	    deltat = int(root[0].attrib['time_delta'],0)
	    deltat = timedelta(0,deltat,0)
	
	    newRow = [timestamp]
	
	    for index in range(columns,len(root[0])):
		    for x in range(columns):
		        newRow.append(root[0][index][x].text)
		    writer.writerow(newRow)
		    #setup time for next iteration
		    timestamp = timestamp-deltat
	            newRow = [timestamp]
	
	    outputFile.close()
	    return outputFilename

	def insert_egauge_data_into_database(self,insertFilename):
		import csv
		import psycopg2

		conn = psycopg2.connect("dbname=postgres user=postgres password=energyaudit1!")
		cur = conn.cursor()

		insertFile = open(insertFilename,'r')
		reader = csv.reader(insertFile)
		error = 0
		print "Inserting Egauge Data into database now ..."
		for row in reader:
			try:
				cur.execute("BEGIN;")
				cur.execute("SAVEPOINT my_savepoint;")
				cur.execute("INSERT INTO egauge (datetime,sensor_id,value) VALUES (%s,%s,%s);",(row[0],row[1],row[2]))
				conn.commit()
			except Exception, e:
				cur.execute("ROLLBACK TO SAVEPOINT my_savepoint;")
				logging.warning(str(row[0])+","+row[1]+","+str(row[2])+str(e)+"\n")
				error = error + 1
		print "At "+ str(datetime.now()) + " there were "+str(error)+" error(s)"
		print "please refer to the 'error_log' file for more information.\n"
		insertFile.close()
		os.remove(insertFilename)
		conn.close()

	def archive_egauge_data(self):
		files = os.listdir('egauge')
		if len(files) !=0:
			for file in files:
				src = os.path.join('egauge',file)
				dst = os.path.join('archive','egauge',file)
				shutil.move(src,dst)

	def store_last_query_time(self,from_str):
		lastQueryTime = open('lastQueryTime.csv','wb')
		lastQueryTime.write(from_str)
		lastQueryTime.close()



	def retrieve_last_query_time(self):
		lastQueryTime = open('lastQueryTime.csv','r')
		from_str = lastQueryTime.read()
		lastQueryTime.close()

	def get_local_time_offset(self):
		import dateutil.tz
		localtz = dateutil.tz.tzlocal()
		localoffset = localtz.utcoffset(datetime.now())	
		return localoffset.days*86400 + localoffset.seconds
