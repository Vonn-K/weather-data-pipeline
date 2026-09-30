from pyspark.sql.functions import col, trim, lower, current_timestamp, when
from pyspark import pipelines as dp


@dp.materialized_view(
    comment="Cleaned and standardized weather data from the bronze layer",
    table_properties={"delta.columnMapping.mode": "name"},
)
@dp.expect_or_drop("valid_temperature", "temperature_f IS NOT NULL")
@dp.expect_or_drop("valid_humidity", "humidity_pct IS NOT NULL AND humidity_pct >= 0 AND humidity_pct <= 100")
@dp.expect_or_drop("valid_observation_timestamp", "observation_timestamp IS NOT NULL")
@dp.expect("reasonable_temperature", "temperature_f > -100 AND temperature_f < 200")
def weather_clean():
    return (
        spark.read.table("weather_raw")
        .select(
            col("Date").alias("observation_timestamp"),
            col("Temperature").cast("double").alias("temperature_f"),
            col("Humidity").cast("int").alias("humidity_pct"),
            col("Feels Like").cast("double").alias("feels_like_f"),
            trim(lower(col("Weather Description"))).alias("weather_description"),
            col("Wind Speed").cast("int").alias("wind_speed_mph"),
            col("Visibility").cast("int").alias("visibility_m"),
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn(
            "data_quality_flag",
            when(
                col("temperature_f").isNull() | col("humidity_pct").isNull(),
                "missing_core_fields",
            ).otherwise("valid"),
        )
    )