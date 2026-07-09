import argparse
import os
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    expr,
    lit,
    sum as spark_sum,
   try_to_timestamp,
    trim,
    coalesce,
)
from pyspark.sql.types import StructType, StructField, StringType


def safe_path_value(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]", "_", value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual_run"))
    parser.add_argument("--logical-date", default=datetime.now(timezone.utc).date().isoformat())
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("raw_data_quality_chicago_crimes")
        .getOrCreate()
    )

    dataset_name = "chicago_crimes"
    layer_name = "raw"
    raw_path = "data/raw/chicago_crimes"

    dq_history_path = "data/quality/raw_chicago_crimes/dq_results_history"
    dq_current_path = f"data/quality/raw_chicago_crimes/dq_results/run_id={safe_path_value(args.run_id)}"
    dq_report_dir = "data/quality/raw_chicago_crimes/reports"
    dq_report_file = f"{dq_report_dir}/raw_dq_latest.md"

    min_expected_rows = int(os.getenv("RAW_MIN_ROWS", "200000"))
    max_expected_rows = int(os.getenv("RAW_MAX_ROWS", "400000"))
    fail_on_error = os.getenv("FAIL_ON_DQ_ERROR", "true").lower() == "true"

    run_timestamp = datetime.now(timezone.utc).isoformat()

    df = spark.read.parquet(raw_path)
    total_rows = df.count()

    required_columns = [
        "id",
        "case_number",
        "date",
        "primary_type",
        "arrest",
        "domestic",
        "year",
    ]

    missing_columns = [c for c in required_columns if c not in df.columns]

    checks = []

    def add_check(check_name, status, severity, observed_value, rule, details):
        checks.append(
            {
                "run_id": args.run_id,
                "logical_date": args.logical_date,
                "run_timestamp": run_timestamp,
                "dataset_name": dataset_name,
                "layer_name": layer_name,
                "check_name": check_name,
                "status": status,
                "severity": severity,
                "observed_value": str(observed_value),
                "rule": rule,
                "details": details,
            }
        )

    add_check(
        "required_columns_exist",
        "FAIL" if missing_columns else "PASS",
        "critical",
        ",".join(missing_columns) if missing_columns else "none",
        "All required columns must exist",
        f"Missing columns: {missing_columns}" if missing_columns else "All required columns exist",
    )

    add_check(
        "row_count_between_expected_range",
        "PASS" if min_expected_rows <= total_rows <= max_expected_rows else "FAIL",
        "critical",
        total_rows,
        f"Row count must be between {min_expected_rows} and {max_expected_rows}",
        "Validates expected 2024 Chicago crimes data volume",
    )

    if missing_columns:
        schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("logical_date", StringType(), True),
            StructField("run_timestamp", StringType(), True),
            StructField("dataset_name", StringType(), True),
            StructField("layer_name", StringType(), True),
            StructField("check_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("observed_value", StringType(), True),
            StructField("rule", StringType(), True),
            StructField("details", StringType(), True),
        ])

        results_df = spark.createDataFrame(checks, schema=schema)
        results_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dq_current_path)
        results_df.write.mode("append").partitionBy("logical_date").parquet(dq_history_path)

        os.makedirs(dq_report_dir, exist_ok=True)
        with open(dq_report_file, "w") as f:
            f.write("# Raw Data Quality Report\n\n")
            f.write(f"Run ID: {args.run_id}\n\n")
            f.write(f"Logical Date: {args.logical_date}\n\n")
            f.write(f"Run Timestamp: {run_timestamp}\n\n")
            f.write("Critical failure: missing required columns.\n")

        raise ValueError(f"Missing required columns: {missing_columns}")

    quality_df = (
        df
        .withColumn("id_str", col("id").cast("string"))
        .withColumn("date_str", col("date").cast("string"))
        .withColumn("primary_type_str", col("primary_type").cast("string"))
        .withColumn("latitude_str", col("latitude").cast("string") if "latitude" in df.columns else lit(None))
        .withColumn("longitude_str", col("longitude").cast("string") if "longitude" in df.columns else lit(None))
        .withColumn("id_cast", expr("try_cast(id_str as BIGINT)"))
        .withColumn(
            "date_cast",
            coalesce(
                try_to_timestamp(col("date_str"), lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
        try_to_timestamp(col("date_str"), lit("yyyy-MM-dd HH:mm:ss")),
        try_to_timestamp(col("date_str")),            ),
        )
        .withColumn("latitude_cast", expr("try_cast(latitude_str as DOUBLE)"))
        .withColumn("longitude_cast", expr("try_cast(longitude_str as DOUBLE)"))
    )

    null_id_count = quality_df.filter(
        col("id_str").isNull() | (trim(col("id_str")) == "")
    ).count()

    invalid_id_count = quality_df.filter(
        col("id_str").isNotNull()
        & (trim(col("id_str")) != "")
        & col("id_cast").isNull()
    ).count()

    null_date_count = quality_df.filter(
        col("date_str").isNull() | (trim(col("date_str")) == "")
    ).count()

    invalid_date_count = quality_df.filter(
        col("date_str").isNotNull()
        & (trim(col("date_str")) != "")
        & col("date_cast").isNull()
    ).count()

    future_date_count = quality_df.filter(
        col("date_cast") > current_timestamp()
    ).count()

    null_primary_type_count = quality_df.filter(
        col("primary_type_str").isNull() | (trim(col("primary_type_str")) == "")
    ).count()

    duplicate_df = (
        quality_df
        .filter(col("id_cast").isNotNull())
        .groupBy("id_cast")
        .agg(count("*").alias("id_count"))
        .filter(col("id_count") > 1)
    )

    duplicate_id_group_count = duplicate_df.count()

    duplicate_record_count_row = duplicate_df.select(
        spark_sum(col("id_count") - lit(1)).alias("duplicate_record_count")
    ).collect()[0]["duplicate_record_count"]

    duplicate_record_count = int(duplicate_record_count_row or 0)

    invalid_latitude_count = quality_df.filter(
        col("latitude_str").isNotNull()
        & (trim(col("latitude_str")) != "")
        & (
            col("latitude_cast").isNull()
            | (col("latitude_cast") < 41.0)
            | (col("latitude_cast") > 42.1)
        )
    ).count()

    invalid_longitude_count = quality_df.filter(
        col("longitude_str").isNotNull()
        & (trim(col("longitude_str")) != "")
        & (
            col("longitude_cast").isNull()
            | (col("longitude_cast") < -88.0)
            | (col("longitude_cast") > -87.0)
        )
    ).count()

    add_check(
        "null_id_count",
        "PASS" if null_id_count == 0 else "FAIL",
        "critical",
        null_id_count,
        "ID must not be null",
        "Primary identifier is required for deduplication and downstream joins",
    )

    add_check(
        "invalid_id_count",
        "PASS" if invalid_id_count == 0 else "FAIL",
        "critical",
        invalid_id_count,
        "ID must be castable to BIGINT",
        "Invalid IDs indicate malformed rows or CSV parsing issues",
    )

    add_check(
        "duplicate_id_group_count",
        "PASS" if duplicate_id_group_count == 0 else "FAIL",
        "critical",
        duplicate_id_group_count,
        "ID must be unique",
        "Counts how many ID values appear more than once",
    )

    add_check(
        "duplicate_record_count",
        "PASS" if duplicate_record_count == 0 else "FAIL",
        "critical",
        duplicate_record_count,
        "No duplicate ID records allowed",
        "Counts extra duplicate records beyond the first occurrence",
    )

    add_check(
        "null_date_count",
        "PASS" if null_date_count == 0 else "FAIL",
        "critical",
        null_date_count,
        "Date must not be null",
        "Crime date is required for time-series analytics",
    )

    add_check(
        "invalid_date_count",
        "PASS" if invalid_date_count == 0 else "FAIL",
        "critical",
        invalid_date_count,
        "Date must be parseable as timestamp",
        "Invalid dates break daily aggregations and rolling averages",
    )

    add_check(
        "future_date_count",
        "PASS" if future_date_count == 0 else "FAIL",
        "critical",
        future_date_count,
        "Crime date must not be in the future",
        "Future dates indicate source or parsing errors",
    )

    add_check(
        "null_primary_type_count",
        "PASS" if null_primary_type_count == 0 else "FAIL",
        "critical",
        null_primary_type_count,
        "primary_type must not be null",
        "Crime type is required for category analysis",
    )

    add_check(
        "invalid_latitude_count",
        "PASS" if invalid_latitude_count == 0 else "WARN",
        "warning",
        invalid_latitude_count,
        "Latitude should be within expected Chicago range",
        "Coordinate issues are warnings because some crimes may not have valid geocoding",
    )

    add_check(
        "invalid_longitude_count",
        "PASS" if invalid_longitude_count == 0 else "WARN",
        "warning",
        invalid_longitude_count,
        "Longitude should be within expected Chicago range",
        "Coordinate issues are warnings because some crimes may not have valid geocoding",
    )

    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("logical_date", StringType(), True),
        StructField("run_timestamp", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("layer_name", StringType(), True),
        StructField("check_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("observed_value", StringType(), True),
        StructField("rule", StringType(), True),
        StructField("details", StringType(), True),
    ])

    results_df = spark.createDataFrame(checks, schema=schema)

    results_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dq_current_path)
    results_df.write.mode("append").partitionBy("logical_date").parquet(dq_history_path)

    os.makedirs(dq_report_dir, exist_ok=True)

    with open(dq_report_file, "w") as f:
        f.write("# Raw Data Quality Report\n\n")
        f.write(f"Run ID: {args.run_id}\n\n")
        f.write(f"Logical Date: {args.logical_date}\n\n")
        f.write(f"Run Timestamp: {run_timestamp}\n\n")
        f.write(f"Dataset: {dataset_name}\n\n")
        f.write(f"Layer: {layer_name}\n\n")
        f.write(f"Total Rows: {total_rows}\n\n")
        f.write("| Check | Status | Severity | Observed Value | Rule |\n")
        f.write("|---|---|---|---:|---|\n")

        for check in checks:
            f.write(
                f"| {check['check_name']} "
                f"| {check['status']} "
                f"| {check['severity']} "
                f"| {check['observed_value']} "
                f"| {check['rule']} |\n"
            )

    print("Raw Data Quality Report")
    print(f"Run ID: {args.run_id}")
    print(f"Logical Date: {args.logical_date}")
    print(f"Total rows: {total_rows}")
    print(f"Report written to: {dq_report_file}")

    failures = [
        check for check in checks
        if check["status"] == "FAIL" and check["severity"] == "critical"
    ]

    if failures and fail_on_error:
        failure_names = [check["check_name"] for check in failures]
        raise ValueError(f"Critical raw data quality checks failed: {failure_names}")

    print("Raw data quality checks completed")

    spark.stop()


if __name__ == "__main__":
    main()
