import boto3
from datetime import datetime
today = datetime.now().strftime("%Y-%m-%d")
filename = f"weather_{today}.csv"
s3 = boto3.client("s3")

s3.upload_file(
    filename,
    "weather-bucket-databricks", 
    f"weather/{filename}"
)

print("Upload successful!")