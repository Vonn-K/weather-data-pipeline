# using python, call api to get data. move data to s3 bucket
#move data to databricks to clean and transform it
#load data to a dashboard or something.

import requests
import pandas as pd
from datetime import datetime
import csv
import os
api_key = 'api_key' # Replace with actual key
units = 'imperial'
url = f"https://api.openweathermap.org/data/2.5/weather?lat={40.2969}&lon={-111.6946}&units={units}&appid={api_key}"

response = requests.get(url)
today = datetime.now().strftime("%Y-%m-%d")
filename = f"weather_{today}.csv"
# 3. Check if the request was successful (Status Code 200)
file_exists = os.path.exists(filename)
desired_columns = ['Date', 'Temperature', 'Humidity', 'Feels Like', 'Weather Description', 'Wind Speed', 'Visibility']

if response.status_code == 200:
    # 4. Parse the response data as a Python dictionary
    data = response.json()
    print("Success!")
    print(data)
    with open (filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            # Write the header row if the file doesn't exist
            writer.writerow(desired_columns)
        # Write the data row
        writer.writerow([datetime.now().isoformat(),
            data["main"]["temp"],
            data["main"]["humidity"],
            data["main"]["feels_like"],
            data["weather"][0]["description"],
            data["wind"]["speed"],
            data["visibility"]]) 
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

