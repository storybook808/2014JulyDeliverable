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

logging.basicConfig(filename='ADASEED_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

class EgaugeUtility:
	'''
	EguageUtility class is a package of methods used to interact with the an eGauge2.
	As of 2014-07-31 it is has  been discontinued, but we have a bunch in the office
	for us to use.
	'''
	
	def __init__(self,egauge_url='192.168.1.88',\
		file_location = os.path.dirname(os.path.abspath(__file__)),\
		output_file_location = os.path.dirname(os.path.abspath(__file__))):
		'''
		the init method creates the class.
		given: the url of the egauge device (which is defaulted to an ip used
		       when you are directly connected)
		return: an egaugeUtility object containing methods to interact with
		       the egauge device that has the given ip address
		'''
		self._file_location = file_location
		self._output_file_location = output_file_location
		self._egauge_url = egauge_url

	def egauge_to_eshape(self,src):
		'''
		given: the source file in a normal csv shape where the columns are
		       the "timestamp, device 1, device 2,..." and has the values
		       running down it
		return: the data in an eshape where the colummns are "timestmap,
		       sensor_id, value"
		'''
		#create the pathname to the file given
		inputFilePath = os.path.join('egauge',src)
		#creates a csv object named "reader" containing the file in question
		inputFile = open(inputFilePath,"r")
		reader = csv.reader(inputFile)

		#sets up the outputfile named temp.csv that we will write the new
		#data shape to
		outputFilename = 'temp.csv'
		outputFile= open(outputFilename,'wb')
		writer = csv.writer(outputFile)

		#pick up the header line and slice out the timestamp so that
		#we have a list of just the names of the fields
		header=reader.next()
		header=header[1:]

		#disregard the first line which has cumulative value
		#had to put a try/catch here in the event that there's no
		#data in the file and just a header
		try:
			reader.next()
		except:
			pass

		#so if there's data to be processed, we're going to process
		#them one row at a time
		for row in reader:
			#get the timestamp that we're going to use to create
			#the new lines with
			timestamp = row[0]
			#slice out the timestamp from the row of values
			row = row[1:]
			#create the new Row that we're going to write to the
			#outputfile
			newRow = [timestamp]

			#we're going to write as many lines as there are fields
			for index in range(len(header)):
				#create the sensor_id and take out the whitespaces
				#because postgres doesn't like them. it's like nato
				sensor_id = header[index].replace(' ','_')
				#tack on the sensor_id to the right hand side of the row
				newRow.append(sensor_id)
				#the raw data is given in kWs, so to get kWh we divide
				#by 3600000, and then round it to 5 decimal points
				value = round(-(float(row[index])/3600000),5)
				#tack the value data onto the right hand side of our
				#new row, so now you have the timestamp,sensor_id,value
				#aka eshape
				newRow.append(value)
				#go ahead and write it to the outputfile
				writer.writerow(newRow)
				#restart a new row for the next iteration of the for loop
				newRow = [timestamp]

		#housecleaning, always try to remember to close files, otherwise other
		#programs will not be allowed to open them
		inputFile.close()
		outputFile.close()
		#by the way, don't forget to return the name of the new file you just made
		return outputFilename

	def parse_datetime(self,str) :
	  '''
	  takes a string of various formats and returns a
	  datetime object representing the string
	  '''
	  #if string is empty just return none
	  if str == None :
	    return None

    	  #this block of code basically assumes that the string may be in different formats
	  #so it continually tries to strip the datetime off the string
	  #until if finds the correct strptime format, pretty ingenious
	  try :
	    return datetime.strptime(str, "%Y-%m-%d %H:%M:%S")
	  except ValueError :
	    try :
	      return datetime.strptime(str, "%Y-%m-%dT%H:%M:%S")
	    except ValueError :
	      return datetime.strptime(str, "%Y-%m-%d")
	      
	def get_ts(self,dt):
	    '''
	    this is used internally in the EguageUtility class
	    given: a datetime object
	    returns: the gmt time of that object
	    '''
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

	  #this starts the string that you're going to use to ping the egauge with	
	  gw_url = "http://"+egauge_url+"/cgi-bin/egauge-show"
	
	  #this keeps building the query http string
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
	
	  #this is the meat of the method. Here it creates an Http object and sets it so that
	  #it waits 15 seconds, if no reply back then it times out the query
	  try:
	    req = httplib2.Http(timeout=15)
	    req.add_credentials(username, password)   # Digest Authentication
	    response, content = req.request (gw_url + params, headers={'Connection': 'Keep-Alive', 'accept-encoding': 'gzip'})
	    #iterates through the different statuses that is returned and notifies the user
	    #what happened
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
    	  #returns the response and the xml content that is given by the egauge device
	  #when pinged/queried
	  return response, content

        def check_time_sync(self):
	    '''
	    given: nothing
	    returns: checks to see if the time of the egauge device specified by the
	            ip address given when instancing the object (look at __init__())
	    '''
	    print 'Checking to see if the eGauge is properly installed.\nThis may take up to 15 seconds.'
	    #go ahead and try to check the time
	    try:
		#take a time now from the computer you're on
	    	local_computer_time = datetime.now()
		#take the time from the egauge device
	    	epoch_local_egauge_time = int(self.get_egauge_time())
	    	local_egauge_time = datetime.fromtimestamp(epoch_local_egauge_time)
		#take a difference between the two times
	    	timeDiff = local_computer_time - local_egauge_time

	    except:
		#if that above block of code doesn't work, go and tell the user that they need to
		#sync up the damn things together
		print '\nYour eGauge device is not properly connected.'
		print 'Please check your settings and that the ethernet'
		print 'cable is properly connected, then run ADASEED again.\n'
		raw_input('Press Enter to close ADASEED...')
		quit()
	   
	    #here we do the straight comparison, note I had to get the seconds value from the timedelta
	    #object named "timeDiff". Remember that when you take the difference between two datetime
	    #objects you get a timedelta object.
	    if abs(timeDiff.days*86400+timeDiff.seconds) > 45 :
		    print "\nComputer and eGauge is not synced, please check the settings\nof the eGauge and local computer then run ADASEED again.\n"
		    raw_input('Press Enter to close ADASEED...')
		    quit()
            print 'Your eGauge is properly installed.'


	def gmt_to_local(self,datetime):
	    '''
	    given: a datetime object that represents a gmt time
	    return: gives back a datetime object of the local time
	    '''
	    #this is how to create a timedelta object with just the seconds
	    offset = timedelta(seconds=self.get_local_time_offset())
	    return datetime + offset

	def local_to_gmt(self,datetime):
	    '''
	    given: a datetime object that represents a local time
	    return: a datetime object in gmt time relative to the timzone
	           of the computer that's processing it.
	    '''
	    offset = timedelta(seconds=self.get_local_time_offset())
	    return datetime - offset

	def pull_from_egauge(self,from_str,to_str):
	    '''
	    given: 2 datetime strings
	    return: an elementTree object that contains the data returned from the
	           egauge device in the form of an xml
	    '''
	    #yes you can import this in a code. i got lazy, you should do all this
	    #at the top of the file for good coding practice.
	    #remember! Do as I say, not as I do.
	    from optparse import OptionParser
	    #this code was taken from someone else, google it and you'll find his stuff
	    #on github. the parse thing takes arguments, but i think this is all depricated
	    #since i changed things up earlier
	    parser = OptionParser(usage="usage: %prog device_url from_date to_date [options]")
	    parser.add_option("--seconds", default=False, action="store_true",
	                    help="will try to fetch seconds data if specified")
	    parser.add_option( "--username")
	    parser.add_option( "--password")
	    (options, args) = parser.parse_args()
	
	    #get the datetime objects from the supplied strings
	    from_ts = self.get_ts(self.parse_datetime(from_str))
	    to_ts = self.get_ts(self.parse_datetime(to_str))

	    #add timezone offset here because when you ping the egauge in this manner
	    #you need to give it the gmt time not local time.
	    offset = self.get_local_time_offset()
	    gmt_from_ts = from_ts - offset
	    gmt_to_ts = to_ts - offset

	    #this is the meat of the method where you query/ping the egauge to get the data
	    status, data = self.egauge_fetch_data('192.168.1.88', gmt_from_ts, gmt_to_ts,
	            username=options.username, password=options.password)

	    #you're taking the xml string data received from the egauge and
	    #turning it into an elementTree object for easier retrieval
	    #of the data
	    root = ET.fromstring(data)
	    #by the way, don't forget to return it so that other methds and use it
	    return root, from_str, to_str

        def get_local_time(self,root):
	    '''
	    given: an elementTree object
	    return: return the timestamp as a datetime object
	    '''
	    timestamp = int(root[0].attrib['time_stamp'],0)
	    #remember, the data coming from the egauge in this manner is in gmt
	    #so you need to get the offset and return the local time
	    offset = self.get_local_time_offset()
	    timestamp = timestamp + offset
	    timestamp = datetime.utcfromtimestamp(timestamp)
	    return timestamp
	 
	def data_to_csv(self, root, from_str, to_str):
	    '''
	    given: the elementTree object, and the to and from strings (strings
	          are used to make the name of the output file)

	    return: a csv file that has a normal shape where each entry is the timestamp
	          and values going across.
	    '''
	    from_str = from_str.replace(' ','_').replace(':','.')
	    to_str = to_str.replace(' ','_').replace(':','.')
	    outputFilename = 'egauge_'+from_str+'_'+to_str+'.csv'
	    outputFilePath = os.path.join('egauge', 'egauge_'+from_str+'_'+to_str+'.csv')
	    outputFile = open(outputFilePath,"wb")
	    writer = csv.writer(outputFile)
	
	    #this is where we start constructing the header line
	    newRow = ["datetime"]
	    #get the number of columns
	    columns = int(root[0].attrib['columns'])
	    #lay out the fields and add them to the header line
	    for index in range(columns):
		    newRow.append(root[0][index].text)
	    writer.writerow(newRow)
	
	    #setup timestamp
	    #remember that the time in the root is straight from the egauge is in
	    #gmt so you extract it from the xml and apply the offset to get local time
	    timestamp = self.get_local_time(root)
	    
	    #extract the time between each entry of the xml and create a timedelta object
	    deltat = int(root[0].attrib['time_delta'],0)
	    deltat = timedelta(0,deltat,0)
	
	    #start making the data rows
	    newRow = [timestamp]
	
	    #this is a bit complicated. Just realize that to read the xml as an elementTree
	    #you need to map out exactly how the data is retrieved. I'll draw it out and
	    #and probably place it somewhere on the office wiki.
	    for index in range(columns,len(root[0])):
		    for x in range(columns):
		        newRow.append(root[0][index][x].text)
		    writer.writerow(newRow)
		    #setup time for next iteration
		    timestamp = timestamp-deltat
	            newRow = [timestamp]
	
	    #housecleaning like always
	    outputFile.close()
	    #return the name of the file
	    return outputFilename

	def archive_egauge_data(self):
		'''
		given: nothing
		return: nothing, but moves the files in the ADASEED>egauge folder to
		       the ADASEED>archive>egauge folder for future use.
		'''
		#get the list of files in the ADASEED>egauge folder
		files = os.listdir('egauge')
		#if there are indeed files in the folder
		if len(files) !=0:
			#move each folder into the ADASEED>archive=
			for file in files:
				src = os.path.join('egauge',file)
				dst = os.path.join('archive','egauge',file)
				shutil.move(src,dst)

	def store_last_query_time(self,from_str):
		'''
		given: a timestamp in string form
		return: nothing but saves the string to a csv file in the working
		       directory
		'''
		lastQueryTime = open('lastQueryTime.csv','wb')
		lastQueryTime.write(from_str)
		lastQueryTime.close()



	def retrieve_last_query_time(self):
		'''
		given: nothing
		return: picks up the timestamp in string form and returns the string
		'''
		lastQueryTime = open('lastQueryTime.csv','r')
		from_str = lastQueryTime.read()
		lastQueryTime.close()

	def get_local_time_offset(self):
		'''
		given: nothing
		return: the offset time between the local time of the computer
		       and gmt in seconds
		'''
		import dateutil.tz
		localtz = dateutil.tz.tzlocal()
		localoffset = localtz.utcoffset(datetime.now())	
		return localoffset.days*86400 + localoffset.seconds

	def get_egauge_time(self):
		'''
		given: nothing
		return: the time of egauge in epoch form
		'''
		import httplib2
		req = httplib2.Http(timeout=15)
		req.add_credentials('owner','default')
		response,content = req.request("http://"+self._egauge_url+"/cgi-bin/egauge?inst",headers={'Connect': 'Keep-Alive','accept-encoding': 'gzip'})
		root = ET.fromstring(content)
		return root[0].text

	def clean_egauge_meta_files(self):
		'''
		given: nothing
		return: nothing, but deletes any leftover meta files made by the
		       class
		'''
		os.remove('temp.csv')

