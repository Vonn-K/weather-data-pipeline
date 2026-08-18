from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, regexp_extract, 
    lower, trim, when, to_date
)
from datetime import datetime
import re

# Configuration
BRONZE_PATH = "/Volumes/dataengineering/default/files/"
CATALOG = "dataengineering"
SCHEMA = "default"
SILVER_TABLE_PREFIX = "weather_silver"

def get_weather_files():
    """
    List all weather CSV files from the bronze volume.
    Returns a list of tuples: (file_path, date_string)
    """
    files_df = spark.sql(f"LIST '{BRONZE_PATH}'")
    weather_files = []
    
    for row in files_df.collect():
        file_name = row['name']
        if file_name.startswith('weather_') and file_name.endswith('.csv'):
            # Extract date from filename (format: weather_YYYY-MM-DD.csv)
            date_match = re.search(r'weather_(\d{4}-\d{2}-\d{2})\.csv', file_name)
            if date_match:
                date_str = date_match.group(1)
                weather_files.append((row['path'], date_str))
    
    # Sort by date to process in chronological order
    weather_files.sort(key=lambda x: x[1])
    return weather_files

def check_silver_table_exists(date_str):
    """
    Check if a silver table already exists for the given date.
    Returns True if exists, False otherwise.
    """
    table_name = f"{SILVER_TABLE_PREFIX}_{date_str.replace('-', '_')}"
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False

def bronze_to_silver_transform(file_path, date_str):
    """
    Perform bronze-to-silver transformation:
    - Read raw CSV data
    - Clean and standardize column names
    - Handle null values and data quality issues
    - Add metadata columns
    - Write to silver table
    """
    table_name = f"{SILVER_TABLE_PREFIX}_{date_str.replace('-', '_')}"
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    
    print(f"Processing: {file_path} -> {full_table_name}")
    
    # Read bronze data
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    
    # Clean and standardize
    # Check if _rescued_data column exists
    has_rescued_data = "_rescued_data" in df.columns
    
    silver_df = df.select(
        col("Date").alias("observation_timestamp"),
        col("Temperature").cast("double").alias("temperature_f"),
        col("Humidity").cast("int").alias("humidity_pct"),
        col("Feels Like").cast("double").alias("feels_like_f"),
        trim(lower(col("Weather Description"))).alias("weather_description"),
        col("Wind Speed").cast("int").alias("wind_speed_mph"),
        col("Visibility").cast("int").alias("visibility_m")
    ) \
    .withColumn("file_date", lit(date_str)) \
    .withColumn("processed_timestamp", current_timestamp())
    
    # Add rescued_data column if it exists in source
    if has_rescued_data:
        silver_df = silver_df.withColumn("rescued_data", col("_rescued_data"))
    else:
        silver_df = silver_df.withColumn("rescued_data", lit(None).cast("string"))
    
    # Add data quality flag
    silver_df = silver_df.withColumn("data_quality_flag", 
        when(col("rescued_data").isNotNull(), "rescued_data_present")
        .when(col("temperature_f").isNull() | 
              col("humidity_pct").isNull(), "missing_core_fields")
        .otherwise("valid")
    )
    
    # Filter out completely invalid records (optional)
    silver_df = silver_df.filter(
        col("observation_timestamp").isNotNull() &
        (col("temperature_f").isNotNull() | col("humidity_pct").isNotNull())
    )
    
    # Write to silver table
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_name)
    
    record_count = silver_df.count()
    print(f"✓ Created {full_table_name} with {record_count} records")
    return record_count

def process_weather_data():
    """
    Main function to process all weather files.
    Checks for new files and processes only those without existing silver tables.
    """
    print("=" * 60)
    print("Starting Bronze-to-Silver Weather Data Transformation")
    print("=" * 60)
    
    # Get all weather files
    weather_files = get_weather_files()
    
    if not weather_files:
        print("No weather files found in bronze layer.")
        return
    
    print(f"\nFound {len(weather_files)} weather file(s):")
    for file_path, date_str in weather_files:
        print(f"  - {date_str}")
    
    # Process each file
    processed_count = 0
    skipped_count = 0
    
    print("\n" + "=" * 60)
    print("Processing files...")
    print("=" * 60)
    
    for file_path, date_str in weather_files:
        if check_silver_table_exists(date_str):
            print(f"\n⊘ Skipping {date_str}: Silver table already exists")
            skipped_count += 1
        else:
            print(f"\n→ Processing {date_str}...")
            try:
                bronze_to_silver_transform(file_path, date_str)
                processed_count += 1
            except Exception as e:
                print(f"✗ Error processing {date_str}: {str(e)}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Processing Complete")
    print("=" * 60)
    print(f"Processed: {processed_count} file(s)")
    print(f"Skipped:   {skipped_count} file(s) (already exist)")
    print(f"Total:     {len(weather_files)} file(s)")

# Run the transformation
if __name__ == "__main__":
    process_weather_data()