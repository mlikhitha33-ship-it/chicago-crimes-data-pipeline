from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = (
    SparkSession.builder
    .appName("raw_ingest_chicago_crimes")
    .getOrCreate()
)

input_path = "data/landing/chicago_crimes/chicago_crimes_2024.csv"
output_path = "data/raw/chicago_crimes"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

df_raw = (
    df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

df_raw.write.mode("overwrite").parquet(output_path)

print("Raw ingestion completed")
print(f"Rows loaded: {df_raw.count()}")

spark.stop()
