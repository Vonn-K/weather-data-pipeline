from pyspark.sql.functions import col, to_date, avg, min, max, count, round as round_col
from pyspark import pipelines as dp


@dp.materialized_view(
    comment="Daily weather summary aggregations from the silver layer",
    table_properties={"delta.columnMapping.mode": "name"},
    partition_cols=["observation_date"],
)
def weather_daily_summary():
    return (
        spark.read.table("weather_clean")
        .withColumn("observation_date", to_date(col("observation_timestamp")))
        .groupBy("observation_date")
        .agg(
            round_col(avg("temperature_f"), 2).alias("avg_temp_f"),
            min("temperature_f").alias("min_temp_f"),
            max("temperature_f").alias("max_temp_f"),
            round_col(avg("humidity_pct"), 2).alias("avg_humidity_pct"),
            round_col(avg("wind_speed_mph"), 2).alias("avg_wind_speed_mph"),
            round_col(avg("visibility_m"), 2).alias("avg_visibility_m"),
            count("*").alias("observation_count"),
        )
    )