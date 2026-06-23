from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    count,
    sum as spark_sum,
    avg,
    when,
    expr,
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("transform_chicago_crimes")
    .getOrCreate()
)

raw_path = "data/raw/chicago_crimes"
clean_detail_path = "data/clean/chicago_crimes_detail"
daily_summary_path = "data/clean/chicago_crimes_daily_summary"

df = spark.read.parquet(raw_path)

crime_category_lookup = spark.createDataFrame(
    [
        ("THEFT", "Property Crime"),
        ("BURGLARY", "Property Crime"),
        ("ROBBERY", "Property Crime"),
        ("MOTOR VEHICLE THEFT", "Property Crime"),
        ("BATTERY", "Violent Crime"),
        ("ASSAULT", "Violent Crime"),
        ("CRIMINAL SEXUAL ASSAULT", "Violent Crime"),
        ("HOMICIDE", "Violent Crime"),
        ("NARCOTICS", "Drug Crime"),
        ("CRIMINAL DAMAGE", "Property Damage"),
        ("DECEPTIVE PRACTICE", "Fraud"),
    ],
    ["primary_type", "crime_category"],
)

clean_df = (
    df
    .withColumn("id", expr("try_cast(id as BIGINT)"))
    .withColumn("year", expr("try_cast(year as INT)"))
    .withColumn("district", expr("try_cast(district as INT)"))
    .withColumn("ward", expr("try_cast(ward as INT)"))
    .withColumn("community_area", expr("try_cast(community_area as INT)"))
    .withColumn("latitude", expr("try_cast(latitude as DOUBLE)"))
    .withColumn("longitude", expr("try_cast(longitude as DOUBLE)"))
    .filter(col("id").isNotNull())
    .dropDuplicates(["id"])
    .withColumn("crime_timestamp", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("crime_date", to_date(col("crime_timestamp")))
    .withColumn("updated_timestamp", to_timestamp(col("updated_on"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .join(crime_category_lookup, on="primary_type", how="left")
    .withColumn(
        "crime_category",
        when(col("crime_category").isNull(), "Other").otherwise(col("crime_category")),
    )
)

daily_summary = (
    clean_df
    .groupBy("crime_date", "primary_type", "crime_category")
    .agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
        spark_sum(when(col("domestic") == True, 1).otherwise(0)).alias("domestic_crimes"),
    )
)

window_spec = (
    Window
    .partitionBy("primary_type")
    .orderBy("crime_date")
    .rowsBetween(-6, 0)
)

daily_summary = daily_summary.withColumn(
    "seven_day_rolling_avg",
    avg("total_crimes").over(window_spec)
)

clean_df.write.mode("overwrite").parquet(clean_detail_path)
daily_summary.write.mode("overwrite").parquet(daily_summary_path)

print("Transformation completed")
print(f"Clean detail rows: {clean_df.count()}")
print(f"Daily summary rows: {daily_summary.count()}")

spark.stop()
