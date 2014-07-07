#-------------------
# Author: Christian A. Damo
# file name: extractData.py
# rev. by: Christian A. Damo
# rev. date: 2014-06-17
#------------------
#
# Patch Notes:
#
#------------------

class GroupChannel:
	def __init__(self):
		timestamp = []
		channelName = []
		dataType = []

	def print_group_and_channel(self):
		#construct group string
		groupString = timestamp + " - " + dataType + " - " + "All Data"
		print groupString
		#print channel name
		print channelName

from nptdms import TdmsFile
import sys
import os
import datetime
import csv
from copy import copy
import xmlrpclib

#print sys.argv[1]
#print "successfully called"

def convert_to_datetime_object(line):
	line = line.split(" ")
	date = line[3]
	date = date.split("/")
	year = int(date[2])
	day = int(date[1])
	month = int(date[0])
	#print year
	#print month
	#print day
	time = line[4]
	time = time[:-1]
	time = time.split(":")
	hour = int(time[0])
	minute = int(time[1])
	second = time[2].split(".")
	second = int(second[0])
	beginTime = datetime.datetime(year,month,day,hour,minute,second)
	return beginTime



#global variables
metaVoltageFilename = os.path.join(sys.argv[1],"Voltage_meta.txt")
metaCurrentFilename = os.path.join(sys.argv[1],"Current_meta.txt")
tdmsVoltageFilename = os.path.join(sys.argv[1], "Voltage.tdms")

tdmsCurrentFilename = os.path.join(sys.argv[1], "Current.tdms")

#print "made file names"

voltageTempNames = []
currentTempNames = []
timestamps = []
voltageGroupChannels = []
currentGroupChannels = []
voltageChannelNames = []
currentChannelNames = []


tdmsVoltageFile = TdmsFile(tdmsVoltageFilename)
tdmsCurrentFile = TdmsFile(tdmsCurrentFilename)

#print "called file names"
###### find group name
# look at meta file

#find "Log signal names:"
#extract group name and channel names

metaVoltageFile = open(metaVoltageFilename)
metaCurrentFile = open(metaCurrentFilename)
#print "successfully opened meta files"
voltageTempNames = []
currentTempNames = []


for line in metaVoltageFile:
	#get the line with the names of the channels
	if line[0] == " ":
		voltageTempNames.append(line)
	#get the start time
	if "Log start time" in line:
		startTime = convert_to_datetime_object(line)
#print "start time is " + str(startTime)

for line in metaCurrentFile:
	if line[0] == " ":
		#get the line with the current channel names
		currentTempNames.append(line)

#check good to here
#print voltageTempNames[0]
#print startTime
#quit()


for line in voltageTempNames:
	#get timestamp to create the group name
	tempLine = line.split("-")
	timestamp = tempLine[0]
	timestamp = timestamp[5:-1]

	#get channel name
	channelName = tempLine[-1]
	channelName = channelName[1:-1]
	voltageChannelNames.append(channelName)

	#get type
	dataType = tempLine[1]
	dataType = dataType[1:-1]

	#build group string
	voltageGroupString = timestamp + " - " + dataType + " - " + "All Data"

for line in currentTempNames:
	#get timestamp to create the group name
	tempLine = line.split("-")
	timestamp = tempLine[0]
	timestamp = timestamp[5:-1]

	#get channel name
	channelName = tempLine[-1]
	channelName = channelName[1:-1]
	currentChannelNames.append(channelName)

	#get type
	dataType = tempLine[1]
	dataType = dataType[1:-1]

	#build group string
	currentGroupString = timestamp + " - " + dataType + " - " + "All Data"



#what i have now is the group and channel name
#now we call the data and time from tdms file
datas = []
times = []

#setup tdms file
#from nptdms import TdmsFile
#just get the data from each channel
for channelName in voltageChannelNames:
	channel = tdmsVoltageFile.object(voltageGroupString,channelName)
	data = channel.data
	datas.append(data)
	times = channel.time_track()

for channelName in currentChannelNames:
	channel = tdmsCurrentFile.object(currentGroupString,channelName)
	data = channel.data
	datas.append(data)



#put all channel names into one list
channelNames = voltageChannelNames + currentChannelNames

#print channelNames

outputFile = open('output1.csv','wb')
writer = csv.writer(outputFile)


newRow = list(channelNames)
newRow.insert(0,"timestamp")
writer.writerow(newRow)

#now to write down the data
#for time in times:
#	delta = datetime.timedelta(seconds = time+1)
#	currTime = startTime + delta
#	newRow = [currTime]
#	for 
#	writer.writerow(newRow)

for x in range(0,len(times)):
	delta = datetime.timedelta(seconds = times[x]+1)
	currTime = startTime + delta
	newRow = [currTime]
	for y in range(0,len(datas)):
		data = datas[y][x]
		newRow.append(data)
	writer.writerow(newRow)

outputFile.close()
metaVoltageFile.close()
metaCurrentFile.close()

#here we calibrate and scale

inputFile = open('output1.csv','r')
outputFile = open('output2.csv','wb')

reader = csv.reader(inputFile)
writer = csv.writer(outputFile)

reader.next()
newRow = ["datetime","position","value"]
writer.writerow(newRow)

newChannelNames = []
for name in channelNames:
	name = name.split("_")
	name = name[1]
	newChannelNames.append(name)
	
	
#print newChannelNames


for row in reader:
	currTime = row[0]
	row = row[1:]
	for x in range(0,len(newChannelNames)):
		#build the new row
		newRow = []
		#get time
		newRow.append(currTime)
		#get sensor name
		newRow.append(newChannelNames[x])
		#get value
		newRow.append(float(row[x]))
		writer.writerow(newRow)
#print "finished writing output3.csv"
	
inputFile.close()
outputFile.close()







#if len(newRow) == 11:
#	for row in reader:
#		newRow = []
#		newRow.append(row[0]) #timestamp
#		value = float(row[1])-0.023323 
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[2])-0.024107 
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[3])-0.029757 
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[4])-0.027621
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[5])-0.000362
#		value = value*1.25
#		newRow.append(value)
#		newRow.append(float(row[6])-0.025223)
#		value = (value*2-10)*0.0040146
#		newRow.append(value)
#		newRow.append(float(row[7])-0.179812)
#		value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[8])-0.203984)
#		value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[9])-0.160342)
#		value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[10]))
#		value = value*.02-.001
#		newRow.append(value)
#		writer.writerow(newRow)
#else:
#	for row in reader:
#		print "look at this one " + sys.argv[1]
#		newRow = []
#		newRow.append(row[0]) #timestamp
#		value = float(row[1])-0.023323 #ai0 
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[3])-0.024107 #ai2
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[4])-0.029757 #ai3
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[5])-0.027621 #ai4
#		value = value*1.25
#		newRow.append(value)
#		value = float(row[6])-0.000362 #ai5
#		value = value*1.25
#		newRow.append(value)
#		newRow.append(float(row[6])-0.025223) #ai6
#		value = (value*2-10)*0.0040146 #p26 gives value in pascals then 1pa = .0040146 inch WC
#		#value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[7])-0.179812) #ai8
#		value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[8])-0.203984) #ai9
#		value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[9])-0.160342) #ai10
#		value = value*.02-.001
#		newRow.append(value)
#		newRow.append(float(row[10]-.000000876)) #ai7
#		value = (value*1000)*0.0062-0.0246
#		newRow.append(value)
#		writer.writerow(newRow)

#print "finished printing output2.csv"

#here we calibrate and scale
inputFile = open('output2.csv','r')
outputFileName = str(sys.argv[1]) + '_output3.csv'
outputFile = open(outputFileName,'wb')

reader = csv.reader(inputFile)
writer = csv.writer(outputFile)


#take care of the header
newRow = reader.next()
writer.writerow(newRow)

for row in reader:
	if row[1] == 'ai0':
		value = float(row[2])-0.023323
		value = value*1.25
	if row[1] == 'ai1':
		value = float(row[2])+10
	if row[1] == 'ai2':
		value = float(row[2])-0.024107
		value = value*1.25
	if row[1] == 'ai3':
		value = float(row[2])-0.029757
		value = value*1.25
	if row[1] == 'ai4':
		value = float(row[2])-0.027621
		value = value*1.25
	if row[1] == 'ai5':
		value = float(row[2])-.000362
		value = value*1.25
	if row[1] == 'ai6':
		value = float(row[2])-0.025223
		value = (value*2.0-10.0)*0.0040146
	if row[1] == 'ai7':
		value = float(row[2])-.000000876
		value = (value*1000)*0.0062-0.0246
	if row[1] == 'ai8':
		value = float(row[2])-0.179812
		value = value*.02-.001
	if row[1] == 'ai9':
		value = float(row[2])-0.203984
		value = value*.02-.001
	if row[1] == 'ai10':
		value = float(row[2])-0.160342
		value = value*.02-.001
	newRow = [row[0],row[1],round(value,5)]
	writer.writerow(newRow)

inputFile.close()
outputFile.close()

#to push to the server

server = xmlrpclib.Server('http://128.171.152.55:9000')

with open(outputFileName,"rb") as handle:
	binary_data = xmlrpclib.Binary(handle.read())
	server.push_csv_to_server(binary_data,outputFileName)
       	
os.remove(outputFileName)
os.remove('output1.csv')
os.remove('output2.csv')
