# weather-data-pipeline
This is my first data pipeline I have worked on whilst uploading to GitHub. 

weathercallergithub.py takes weather information from openweathermap.org through their free api then selects a few of the returned rows.
The program parses the file as a python dictionary, making it easier to work with. 
It then checks to see if there is a file created, making one if necessary, and then appends the dictionary to the csv file. 

The secondary program named s3upload.py uses the boto3 library to upload the csv file at the end of the day to a s3 bucket (in progress)

The s3 bucket contains a directory of the files ordered by date. (in progress)

Databricks receives the raw csv files from the s3 bucket and then runs the data through a cleaning process, bronze to silver.
From there, it is ported into a dashboard showing weather trends across the day, week, month and beyond. (in progress)
