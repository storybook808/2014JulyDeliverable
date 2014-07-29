#-------------------
# Author: Reed Shinsato
# file name: HobowareUtility.py
# date: 2014-07-11
# rev. by: Christian A. Damo
# rev. date: 2014-07-23
#------------------
#
# Patch Notes: Utilizing Christian Damo's scripts to setup the server pushes.
#
# Patch Notes 2014-07-23: deleted the arguments to initialzie HobowareUtility
# Class. Reworked "extract_data" method to take in filename and hardcoded the
# source folder to "hobo".
#------------------
"""
    This script convertes .tdms files from NISignalExpress into .csv files.
    Then, it pushes the .csv files to a server with a specified ip.
"""

# Import Libraries
import sys
import os
import datetime
import csv 
import logging
import shutil

logging.basicConfig(filename='error_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

class HobowareUtility:
    def __init__(self):
        self._serial_names = {}     
        self._output_filename = "output1.csv"

        
        
    def __add_output_file_location(self, filename):
        """
            This function returns the filename in the output directory.
        """
        # Return the joined path of the output directory and the filename
        return os.path.join(self._output_file_location, filename)

    def __in_delete_serial_name(self, name):
        input_filename = "HobowareDeleteSerial.csv"
        input_file = open(os.path.join('Hobowarebin',input_filename), "r")
        reader = csv.reader(input_file, delimiter = ",")
        
        for input_row in reader:
            if name in input_row:
                return True
            
        input_file.close()
        
        return False
        
    def __skip_null_values(self, row, index):
        if row[index] == "":
            return True
        else:
            return False
            
        
    def extract_data(self,filename):
	'''
	given: a file in the hobo folder that is in the hobo format
	return: extracts the data from the hobo file and saves it as
	        a csv file. Returns a string of the name of the file
		located in the working directory.
	'''
        input_file = open(os.path.join('hobo',filename), "r")
        reader = csv.reader(input_file, delimiter = ",")
        
        output_filename = "output1.csv"
        output_file = open(output_filename, "wb")
        writer = csv.writer(output_file) 
        input_row = reader.next()
        while len(input_row) < 3:
            input_row = reader.next()
        
        sensors_list = []
        
        sensors_list = input_row[2:]
        for sensors_index in range(len(sensors_list)):
            try: 
                sensors_list[sensors_index] = self.\
                    _serial_names[str(sensors_list[sensors_index].\
                    split(",")[2].split(" ")[-1].replace(")", ""))]
            except:
                sensors_list[sensors_index] = str(sensors_list[sensors_index].\
                    split(",")[2].split(" ")[-1].replace(")", ""))
        
        output_row = []
        for input_row in reader:
            for input_row_index in range(2, len(input_row)):
                output_row = []
                timestamp = datetime.datetime.strptime(input_row[1],\
                    "%m/%d/%y %I:%M:%S %p")
                output_row.append(timestamp)
                output_row.append(sensors_list[input_row_index - 2])
                output_row.append(input_row[input_row_index])
                writer.writerow(output_row)
        input_file.close()
        output_file.close()
	return output_filename
        
 
    def _update_edit_serial_names(self):
        input_filename = "HobowareEditSerialNames.csv"
        input_file = open(os.path.join('Hobowarebin',input_filename), "r")
        reader = csv.reader(input_file, delimiter = ",")
        
        for input_row in reader:
            if self._serial_names.get(input_row[0]) == None:
                self._serial_names[input_row[0]] = input_row[1]
        
        input_file.close()
    
    def clean_folder(self):
        """
            This function deletes all the output files created.
        """
        # Remove the 1st output
        # Remove the 2nd output
        # Remove the calibrated output
        try:
            os.remove("output1.csv")
        except:
            pass
        try:            
            os.remove("output2.csv")
        except:
            pass
        try:
            os.remove(self.__add_output_file_location(self._output_filename))
        except:
            pass
        
    def edit_output(self,input_filename):
	'''
	given: name of input file in the working directory
	returns: does something according to Reed then saves the file
	       to output2.csv in the working file as well as returns
	       it as a string.
	'''
        self._update_edit_serial_names()
        input_filename = "output1.csv"
        input_file = open(input_filename, "r")
        reader =  csv.reader(input_file, delimiter = ",")
        
        output_filename = "output2.csv"
        output_file = open(output_filename, "wb")
        writer = csv.writer(output_file, delimiter = ",")
        
        for input_row in reader:
            if self.__in_delete_serial_name(input_row[1]) == False:
                if self.__skip_null_values(input_row, 2) == False:
                    writer.writerow(input_row)  
        self._output_filename = output_filename  
        input_file.close()
        output_file.close()
	return output_filename
    
    def add_delete_serial_names(self, serial_name):
        input_filename = "HobowareDeleteSerial.csv"
        delete_serial_name = False    
        name = str(serial_name)
        with open(input_filename, "r") as input_file:
            for row in csv.reader(input_file):
                if name in row:
                    delete_serial_name = True
        
        if delete_serial_name == True:
            self.remove_delete_serial_names(name)

        with open(input_filename, "a") as input_file:
            row = (str(name) + "\n")
            input_file.write(row)

    def remove_delete_serial_names(self, serial_name):
        input_filename = "DeleteSerial.csv"
        temp_filename = "temp.csv"
        name = str(serial_name)
        with open(temp_filename, "wb") as temp_file:   
            with open(input_filename, "r") as input_file:
                for input_row in csv.reader(input_file):
                    if name not in input_row:
                        csv.writer(temp_file).writerow(input_row)
                        
        os.remove(input_filename)
        os.rename(temp_filename, input_filename)
   
    def add_edit_serial_names(self, serial_name, new_name):
        input_filename = "EditSerialNames.csv"
        delete_serial_name = False    
        name = str(serial_name)
        new = str(new_name)
        with open(input_filename, "r") as input_file:
            for row in csv.reader(input_file):
                if name in row:
                    delete_serial_name = True
        
        if delete_serial_name == True:
            self.remove_edit_serial_names(name)
        
        with open(input_filename, "a") as input_file:
            row = (str(name) + "," + str(new) + "\n")
            input_file.write(row)

    def remove_edit_serial_names(self, serial_name):
        input_filename = "HobowareEditSerialNames.csv"
        temp_filename = "temp.csv"
        name = str(serial_name)
        with open(temp_filename, "wb") as temp_file:   
            with open(input_filename, "r") as input_file:
                for input_row in csv.reader(input_file):
                    if name not in input_row:
                        csv.writer(temp_file).writerow(input_row)
                        
        os.remove(input_filename)
        os.rename(temp_filename, input_filename)
    
    def return_output_path(self):
        """
            This function returns the output path.
        """
        # Return the path of the output file
        return os.path.join(self._output_file_location, self._output_filename)

    def get_hobo_files(self):
       '''
       given: nothing
       return: list of names of hobo files in the hobo directory
       NOTE: files in the hobo directory are raw files produced by
            the HobowarePro software.
       '''
       list = os.listdir('hobo')
       return list

    def archive_hobo_data(self):
       '''
       given: nothing
       return: nothing, but transfers hobo files from the hobo directory
               into the archive\hobo directory
       '''
       files = os.listdir('hobo')
       if len(files) !=0:
	       for file in files:
                   src = os.path.join('hobo',file)
                   dst = os.path.join('archive','hobo',file)
	           shutil.move(src,dst)
 
