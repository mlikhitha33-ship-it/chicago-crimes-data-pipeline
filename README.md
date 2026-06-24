# Chicago Crimes Data Engineering Pipeline

This project builds a local data engineering pipeline using PySpark, Apache Airflow, Docker, and Parquet.

## Project Goal
```
- Ingest raw Chicago crimes CSV data, transform it into clean analytics-ready Parquet tables, and orchestrate the workflow using Apache Airflow.
- This project follows a common data engineering pattern: Landing Zone → Raw Zone → Clean Zone → Analytics-Ready Outputs
```

## Current Pipeline

raw_ingest → transform_crimes

## Tech Stack

- AWS EC2 Ubuntu VM
- Docker
- Apache Airflow
- PySpark
- Parquet
- Python


## Architecture
```
Chicago Crimes CSV
        ↓
PySpark Raw Ingestion Job
        ↓
Raw Parquet Zone
        ↓
PySpark Transformation Job
        ↓
Clean Parquet Zone
        ↓
Airflow DAG Orchestration

```

# Current Airflow DAG:
raw_ingest → transform_crimes

# DataSet
Chicago Crimes dataset
Source : https://data.cityofchicago.org/resource/ijzp-q8t2.csv

# Project Folder Structure
```
data-engineering-project/
├── dags/
│   └── chicago_crimes_pipeline.py
├── jobs/
│   ├── raw_ingest.py
│   └── transform_crimes.py
├── data/
│   ├── landing/
│   ├── raw/
│   └── clean/
├── Dockerfile
├── docker-compose.yaml
├── README.md
└── .gitignore
```
# Project Execution

1. Launch EC2 Instance [t3.large minumum and 30-50 GB storage
2. Install System Dependencies

```
sudo apt update
sudo apt install -y docker.io docker-compose-v2 git curl unzip openjdk-17-jdk python3-venv

# Start Docker
```
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```
# verify Installation:
```
docker --version
docker compose version
git --version
java -version
```

# Test Docker


# 3. Create Project Folder

```
mkdir -p ~/data-engineering-project
cd ~/data-engineering-project
```

# 4. Download Airflow Docker Compose File

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml'
```

# Create required folders:
```
mkdir -p dags logs plugins config jobs data
```
# Create .env:
- Docker uses this value so Airflow containers write files with the correct Linux permissions. Without it, files in logs/, dags/, or other mounted folders may be owned by the wrong user.
```
echo "AIRFLOW_UID=$(id -u)" > .env
```

Airflow runs inside Docker containers. Because the pipeline uses PySpark, the Airflow image needs Java and PySpark installed inside the container.

# Create a Dockerfile:

# Build and start Airflow:

```
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d
```

# Check containers:
```
docker compose ps
```

# Open Airflow:
```
http://EC2_PUBLIC_IPV4:8080
```

# Create landing folders:
```
mkdir -p data/landing/chicago_crimes data/raw data/clean data/lookup
```
# Download Dataset - 2024 Chicago crimes data:
```
curl -G 'https://data.cityofchicago.org/resource/ijzp-q8t2.csv' \
  --data-urlencode '$limit=500000' \
  --data-urlencode "\$where=date between '2024-01-01T00:00:00' and '2024-12-31T23:59:59'" \
  -o data/landing/chicago_crimes/chicago_crimes_2024.csv
```


# Verify
```
ls -lh data/landing/chicago_crimes/
head -n 3 data/landing/chicago_crimes/chicago_crimes_2024.csv
```

# Run Jobs Manually - Create and activate a local Python virtual environment if you want to test jobs directly on the EC2 host:

```
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
```
# Run raw ingestion:

- Read CSV using an explicit Spark schema
- Add ingestion_timestamp and source_file
- Write partitioned Parquet to the raw bucket
- Avoid relying on automatic schema inference in the final version
- Transformation

python jobs/raw_ingest.py

```
Expected output:
Raw ingestion completed
Rows loaded: 774222
```

# Run transformation:

- Remove duplicates
- Convert strings into dates and numeric types
- Handle missing values
- Join the district lookup
- Calculate daily crime totals and rolling averages
- Write results to the clean bucket
  
python jobs/transform_crimes.py
```
Transformation completed
Clean detail rows: 259170
Daily summary rows: 7627
```

# Pipeline Jobs
- Raw Ingestion Job
- File:
```
jobs/raw_ingest.py
```
# The raw ingestion job:

- Reads raw CSV data from the landing zone
- Infers schema
- Adds metadata columns
- Writes Parquet output to the raw zone

# Metadata columns added:

- ingestion_timestamp
- source_file

# Output path:
```
data/raw/chicago_crimes

```
# Transformation Job
```
jobs/transform_crimes.py
```
#The transformation job:
```
Reads raw Parquet data
Safely casts data types using try_cast
Removes invalid records
Deduplicates rows by id
Parses crime dates
Joins with an in-memory crime category lookup table
Creates daily aggregations
Calculates 7-day rolling averages
Writes clean Parquet outputs
```
Output paths:
```
data/clean/chicago_crimes_detail
data/clean/chicago_crimes_daily_summary
```
# Airflow DAG

dags/chicago_crimes_pipeline.py

The DAG currently contains two tasks: raw_ingest → transform_crimes. Each task uses BashOperator.
The DAG is scheduled daily. Each task has: retries=2

#To run the pipeline:
```
Open Airflow UI
Search for chicago_crimes_pipeline
Toggle the DAG on
Click Trigger DAG
Confirm both tasks turn green
Output Tables
Clean Detail Table
```
Path:

data/clean/chicago_crimes_detail

Description:

Cleaned record-level crime data.

Key fields include:

id
case_number
crime_timestamp
crime_date
primary_type
description
location_description
arrest
domestic
district
ward
community_area
latitude
longitude
crime_category
ingestion_timestamp
source_file
Daily Summary Table

Path:

data/clean/chicago_crimes_daily_summary

Description:

Aggregated daily crime metrics by crime type and category.

Key fields include:

crime_date
primary_type
crime_category
total_crimes
total_arrests
domestic_crimes
seven_day_rolling_avg
