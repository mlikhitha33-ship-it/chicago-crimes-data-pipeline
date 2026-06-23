# Chicago Crimes Data Engineering Pipeline

This project builds a local data engineering pipeline using PySpark, Apache Airflow, Docker, and Parquet.

## Project Goal

Ingest raw Chicago crimes CSV data, transform it into clean analytics-ready Parquet tables, and orchestrate the workflow using Apache Airflow.

## Current Pipeline

```text
raw_ingest → transform_crimes


Tech Stack
AWS EC2 Ubuntu VM
Docker
Apache Airflow
PySpark
Parquet
Python
