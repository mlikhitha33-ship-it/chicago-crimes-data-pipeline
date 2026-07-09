from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

PROJECT_ROOT = Path(__file__).resolve().parents[1]

input_path = str(PROJECT_ROOT / "data/landing/chicago_crimes/chicago_crimes_2024.csv")
output_path = str(PROJECT_ROOT / "data/raw/chicago_crimes")

spark = (
    SparkSession.builder
    .appName("raw_ingest_chicago_crimes")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(input_path)
)

required_columns = [
    "id",
    "case_number",
    "date",
    "primary_type",
    "arrest",
    "domestic",
    "year",
]

missing_columns = [column for column in required_columns if column not in df.columns]

if missing_columns:
    raise ValueError(f"Missing required source columns: {missing_columns}")

df_raw = (
    df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

df_raw.write.mode("overwrite").parquet(output_path)

print("Raw ingestion completed")
print(f"Rows loaded: {df_raw.count()}")
print(f"Output path: {output_path}")

spark.stop()
