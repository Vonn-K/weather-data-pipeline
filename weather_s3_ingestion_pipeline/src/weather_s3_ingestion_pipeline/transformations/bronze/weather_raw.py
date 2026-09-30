from pyspark import pipelines as dp


@dp.table(
    comment="Raw weather data ingested from S3 using Auto Loader",
    table_properties={"delta.columnMapping.mode": "name"}
)
def weather_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("s3://weather-bucket-databricks/weather/")
    )