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
    This script reshapes Hoboware saved .csv files into a specific format for 
    data viewing.  The script allows for renaming and deleting serial-names 
    from the saved .csv files.    
"""

# Import Libraries
import os
import datetime
import csv 
import logging
import shutil

logging.basicConfig(filename='ADASEED_log',level=logging.DEBUG,format='%(asctime)s %(message)s')

class HobowareUtility:
    def __init__(self):
        # Initialize the class variables
        # Serial names dictionary for editing names
        # Output file name 
        self._serial_names = {}     
        self._output_filename = "output1.csv"
        
    def __add_output_file_location(self, filename):
        """
            This function returns the filename in the output directory.
        """
        # Return the joined path of the output directory and the filename
        return os.path.join(self._output_file_location, filename)

    def __in_delete_serial_name(self, name):
        """
            This checks whether the given serial name is in the delete .csv file.
            The delete .csv file holds the serial names that we want to remove
            from the final output.
        """
        # Open the delete .csv file
        # Initialize the reader
        # For a row in the reader,
        #     If the checking name is in the row, 
        #     return True
        # Return False if the name is not found
        input_filename = "HobowareDeleteSerial.csv"
        input_file = open(os.path.join('Hobowarebin',input_filename), "r")
        reader = csv.reader(input_file, delimiter = ",")
        
        for input_row in reader:
            if name in input_row:
                return True
            
        input_file.close()
        
        return False
        
    def __skip_null_values(self, row, index):
        """
            This function checks if there no value for the row index.
        """
        # If there is no value at the row index,
        # Return True
        # Return False if there is a value
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
        # Open the input file with hobo in the filename
        # Create the reader for the input file 
        input_file = open(os.path.join('hobo',filename), "r")
        reader = csv.reader(input_file, delimiter = ",")
        
        # Open an output file
        # Create the writer for the output file
        output_filename = "output1.csv"
        output_file = open(output_filename, "wb")
        writer = csv.writer(output_file) 
        
        # Skip the header
        input_row = reader.next()
        while len(input_row) < 3:
            input_row = reader.next()
        
        # Intialize a list of sensors to be the input row of sensor ids
        sensors_list = []
        sensors_list = input_row[2:]
        # Clean up the sensors list to just be the serial names
        # Check if the serial name in this sensors list is in the dictionary
        for sensors_index in range(len(sensors_list)):
            try: 
                sensors_list[sensors_index] = self.\
                    _serial_names[str(sensors_list[sensors_index].\
                    split(",")[2].split(" ")[-1].replace(")", ""))]
            except:
                sensors_list[sensors_index] = str(sensors_list[sensors_index].\
                    split(",")[2].split(" ")[-1].replace(")", ""))
        
        # Write the reformatted inputs to the output file
        # For each line of data in the input file,
        #     Write the output with [timestamp, sensor id, value]
        #     the sensor id is from the list of formated sensor ids
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
        """
            This function updates the class dictionary for serial names.
        """
        # Open the .csv with edit serial names
        # Create the reader        
        input_filename = "HobowareEditSerialNames.csv"
        input_file = open(os.path.join('Hobowarebin',input_filename), "r")
        reader = csv.reader(input_file, delimiter = ",")
        
        # If the serial name in the reader is not in the dictionary,
        #     Add the new dictionary item.
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
	returns: Deletes serial names in the reshaped output
	'''
        # Update the dictionary
        # Open the reshaped output file
        # Create the reader
        self._update_edit_serial_names()
        input_filename = "output1.csv"
        input_file = open(input_filename, "r")
        reader =  csv.reader(input_file, delimiter = ",")
        
        # Open an output file for the edits
        # Create the writer
        output_filename = "output2.csv"
        output_file = open(output_filename, "wb")
        writer = csv.writer(output_file, delimiter = ",")
        
        # For each row in the reader,
        #     Check if the row should be deleted
        #     Check if the row should be skipped
        #     Write the row       
        for input_row in reader:
            if self.__in_delete_serial_name(input_row[1]) == False:
                if self.__skip_null_values(input_row, 2) == False:
                    writer.writerow(input_row)  
        self._output_filename = output_filename  
        input_file.close()
        output_file.close()
	return output_filename
    
    def add_delete_serial_names(self, serial_name):
        """
            This adds serial names to the delete .csv file.
        """
        # Open the delete .csv file
        # Create the reader
        # Check if the serial name exists
        # Remove it the serial name exists
        input_filename = "HobowareDeleteSerial.csv"
        delete_serial_name = False    
        name = str(serial_name)
        with open(input_filename, "r") as input_file:
            for row in csv.reader(input_file):
                if name in row:
                    delete_serial_name = True
        
        if delete_serial_name == True:
            self.remove_delete_serial_names(name)

        # Open the delete .csv file
        # Append the new delete name to the file
        with open(input_filename, "a") as input_file:
            row = (str(name) + "\n")
            input_file.write(row)

    def remove_delete_serial_names(self, serial_name):
        """
            This removes serial names in the delete .csv file.
        """
        # Open the delete .csv file
        # Open a temporary file
        # In the input file, 
        #     If the serial name exists, 
        #     Delete the row with the serial name
        input_filename = "DeleteSerial.csv"
        temp_filename = "temp.csv"
        name = str(serial_name)
        with open(temp_filename, "wb") as temp_file:   
            with open(input_filename, "r") as input_file:
                for input_row in csv.reader(input_file):
                    if name not in input_row:
                        csv.writer(temp_file).writerow(input_row)
                        
        # Remove and replace the delete .csv file with the temporary file
        os.remove(input_filename)
        os.rename(temp_filename, input_filename)
   
    def add_edit_serial_names(self, serial_name, new_name):
        """
            This adds serial names into the edit .csv file.
        """        
        # Open the edit .csv file
        # Check if the serial name exists
        #     If it exists,
        #         delete the current serial name row
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
        
        # Open the edit .csv file
        # Append the new serial name row
        with open(input_filename, "a") as input_file:
            row = (str(name) + "," + str(new) + "\n")
            input_file.write(row)

    def remove_edit_serial_names(self, serial_name):
        """
            This removes serial names from the edit .csv file.
        """
        # Open the edit .csv file
        # Open a temporary .csv file
        # Copy the edit .csv file to the temporary file excluding the row 
        # with the serial name
        input_filename = "HobowareEditSerialNames.csv"
        temp_filename = "temp.csv"
        name = str(serial_name)
        with open(temp_filename, "wb") as temp_file:   
            with open(input_filename, "r") as input_file:
                for input_row in csv.reader(input_file):
                    if name not in input_row:
                        csv.writer(temp_file).writerow(input_row)
          
        # Remove and replace the edit .csv file with the temporary .csv file              
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
       # Return a list of files in the hobo directory
       list = os.listdir('hobo')
       return list

    def archive_hobo_data(self):
       '''
       given: nothing
       return: nothing, but transfers hobo files from the hobo directory
               into the archive\hobo directory
       '''
       # Create a list of files in the hobo directory
       # If the directory is not empty,
       #     For each file in the list,
       #         move the file to the archive directory
       files = self.get_hobo_files()
       if len(files) !=0:
	       for file_name in files:
                   src = os.path.join('hobo', file_name)
                   dst = os.path.join('archive','hobo', file_name)
	           shutil.move(src,dst)
 
