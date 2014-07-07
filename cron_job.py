# Author: Reed Shinsato
# 2014-06-17
# File: cron_job.py
# Desc:
'''
	Simple task schedular program to run jobs at a specified time.
	The script needs to be running for the job to occur.
'''

# Import Libraries
from apscheduler.scheduler import Scheduler
import subprocess

# Setup Scheduler
scheduler = Scheduler()
scheduler.start()

# Create Jobs
def job1():
	subprocess.call(["python","main.py"])

scheduler.add_cron_job(job1, hour = 19)

# Make sure the program is running
try:
	while True:
		pass
except KeyboardInterrupt:
	quit()

