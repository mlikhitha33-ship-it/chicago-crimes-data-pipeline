# Chicago Crimes Data Engineering Pipeline

## Project Overview

This project builds a local data engineering pipeline using PySpark, Apache Airflow, Docker, and Parquet.

The pipeline ingests raw Chicago crimes CSV data, writes it to a raw Parquet zone, validates the raw data with a data quality gate, transforms it into clean analytics-ready Parquet tables, and orchestrates the workflow using Apache Airflow.

This project follows a common data engineering pattern:

```text
Landing Zone → Raw Zone → Data Quality Gate → Clean Zone → Analytics-Ready Outputs
```

## Project Goal

The goal of this project is to demonstrate an end-to-end data engineering pipeline lifecycle:

```text
Raw CSV ingestion
    ↓
Raw Parquet zone
    ↓
Raw data quality validation
    ↓
Clean transformed zone
    ↓
Airflow orchestration
```

## Current Pipeline

Current Airflow DAG:

```text
raw_ingest → transform_crimes
```

The raw data quality job has been implemented and manually tested. The next step is to add it into the Airflow DAG so the pipeline becomes:

```text
raw_ingest → raw_data_quality → transform_crimes
```

## Tech Stack

- AWS EC2 Ubuntu VM
- Docker
- Docker Compose
- Apache Airflow
- PySpark
- Python
- Parquet
- Git
- GitHub

## Architecture

```text
Chicago Crimes CSV
        ↓
PySpark Raw Ingestion Job
        ↓
Raw Parquet Zone
        ↓
Raw Data Quality Job
        ↓
PySpark Transformation Job
        ↓
Clean Parquet Zone
        ↓
Airflow DAG Orchestration
```

## Dataset

Dataset used:

```text
Chicago Crimes dataset
```

Source:

```text
https://data.cityofchicago.org/resource/ijzp-q8t2.csv
```

The pipeline currently uses 2024 Chicago crime records.

## Data Dictonary


| Column                 | Type      | Description                                                                                                                                                    |
| ---------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                   | Number    | Unique identifier for the crime record.                                                                                                                        |
| `case_number`          | Text      | Chicago Police Department Records Division Number. This is unique to the incident.                                                                             |
| `date`                 | Date/Time | Date and time when the incident occurred. This may sometimes be a best estimate.                                                                               |
| `block`                | Text      | Partially redacted address where the incident occurred. The address is shown at the block level only.                                                          |
| `iucr`                 | Text      | Illinois Uniform Crime Reporting code. This code is directly linked to the primary crime type and description.                                                 |
| `primary_type`         | Text      | Primary description of the IUCR code. This represents the broad crime category.                                                                                |
| `description`          | Text      | Secondary description of the IUCR code. This provides a more specific crime subcategory.                                                                       |
| `location_description` | Text      | Description of the type of location where the incident occurred.                                                                                               |
| `arrest`               | Boolean   | Indicates whether an arrest was made.                                                                                                                          |
| `domestic`             | Boolean   | Indicates whether the incident was domestic-related as defined by the Illinois Domestic Violence Act.                                                          |
| `beat`                 | Text      | Police beat where the incident occurred. A beat is the smallest police geographic area.                                                                        |
| `district`             | Text      | Police district where the incident occurred.                                                                                                                   |
| `ward`                 | Number    | City Council ward where the incident occurred.                                                                                                                 |
| `community_area`       | Text      | Community area where the incident occurred. Chicago has 77 community areas.                                                                                    |
| `fbi_code`             | Text      | Crime classification based on the FBI National Incident Based Reporting System, also known as NIBRS.                                                           |
| `x_coordinate`         | Number    | X coordinate of the incident location in the State Plane Illinois East NAD 1983 projection. The location is shifted for privacy but remains on the same block. |
| `y_coordinate`         | Number    | Y coordinate of the incident location in the State Plane Illinois East NAD 1983 projection. The location is shifted for privacy but remains on the same block. |
| `year`                 | Number    | Year when the incident occurred.                                                                                                                               |
| `updated_on`           | Date/Time | Date and time when the record was last updated.                                                                                                                |
| `latitude`             | Number    | Latitude of the incident location. The location is shifted for privacy but remains on the same block.                                                          |
| `longitude`            | Number    | Longitude of the incident location. The location is shifted for privacy but remains on the same block.                                                         |
| `location`             | Location  | Geographic point field used by the data portal for maps and geographic operations. The location is shifted for privacy but remains on the same block.          |

### Dataset Grain and Key Fields


> One row represents one reported crime incident record from the Chicago Police Department source data. Before using the data for transformation or analytics, key fields are reviewed to understand how records can be identified and validated.

| Field       | Role in Dataset                | Notes                                                                                                                                                        |
| ------------| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| id          | Record-level identifier        | Used as the primary identifier for each crime record in the dataset. This field should be unique in the raw data.                                            |
| case_number | Police incident/case reference | Represents the Chicago Police Department case number associated with the incident. This field is useful for traceability back to the source incident record. |
| date        | Incident timestamp             | Used to understand when the incident occurred and to support time-based analysis.                                                                            |
| updated_on  | Record maintenance timestamp   | Used to understand when the source record was last updated. This is important for incremental loading and future pipeline refresh logic.                     |

The id column is used as the main record identifier for data quality checks and duplicate detection. The `case_number` column is also reviewed because it provides an incident reference number, but uniqueness should be validated before relying on it as a key.

The grain and key review helps confirm that downstream transformations are built at the correct level of detail.


## Setup From Scratch

### 1. Launch EC2 Instance

Recommended EC2 setup:

```text
AMI: Ubuntu
Instance type: t3.large
Storage: 30 GB gp3
```

Security group inbound rules:

```text
SSH   port 22    source: My IP
TCP   port 8080  source: My IP
```

Port `8080` is needed for the Airflow web UI.

### 2. Install System Dependencies

SSH into the EC2 instance and run:

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-v2 git curl unzip openjdk-17-jdk python3-venv
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```

Verify:

```bash
docker --version
docker compose version
git --version
java -version
```

### 3. Create Project Folder

```bash
mkdir -p ~/data-engineering-project
cd ~/data-engineering-project
```
## Project Folder Structure

```text
data-engineering-project/
├── dags/
│   └── chicago_crimes_pipeline.py
├── jobs/
│   ├── raw_ingest.py
│   ├── raw_data_quality.py
│   └── transform_crimes.py
├── data/
│   ├── landing/
│   ├── raw/
│   ├── clean/
│   └── quality/
├── Dockerfile
├── docker-compose.yaml
├── README.md
└── .gitignore
```
The `data/` folders are generated locally and are not committed to GitHub.

### 4. Download Airflow Docker Compose File

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml'
```

Create required folders:

```bash
mkdir -p dags logs plugins config jobs data
```

Create `.env`:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

This allows Docker containers to write files with the correct Linux user permissions.

## Custom Airflow Docker Image

Airflow runs inside Docker containers. Because the pipeline uses PySpark, the Airflow image needs Java and PySpark installed inside the container.

Create a `Dockerfile`:

```dockerfile
FROM apache/airflow:3.1.8

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir pyspark
```

Update the `x-airflow-common` section in `docker-compose.yaml`:

```yaml
x-airflow-common:
  &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  image: custom-airflow-spark:latest
```

Also make sure the `jobs` and `data` folders are mounted under the Airflow volumes section:

```yaml
    - ${AIRFLOW_PROJ_DIR:-.}/jobs:/opt/airflow/jobs
    - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
```

Build and start Airflow:

```bash
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d
```

Check containers:

```bash
docker compose ps
```

Open Airflow:

```text
http://EC2_PUBLIC_IPV4:8080
```

Default login:

```text
Username: airflow
Password: airflow
```

## Download Dataset

Create landing folders:

```bash
mkdir -p data/landing/chicago_crimes data/raw data/clean data/lookup
```

Download 2024 Chicago crimes data:

```bash
curl -G 'https://data.cityofchicago.org/resource/ijzp-q8t2.csv' \
  --data-urlencode '$limit=500000' \
  --data-urlencode "\$where=date between '2024-01-01T00:00:00' and '2024-12-31T23:59:59'" \
  -o data/landing/chicago_crimes/chicago_crimes_2024.csv
```

Verify:

```bash
ls -lh data/landing/chicago_crimes/
head -n 3 data/landing/chicago_crimes/chicago_crimes_2024.csv
```

## Run Jobs Manually

Create and activate a local Python virtual environment if you want to test jobs directly on the EC2 host:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
```

Run raw ingestion:

```bash
python3 jobs/raw_ingest.py
```

Expected output:

```text
Raw ingestion completed
Rows loaded: 259170
```

Run raw data quality checks:

```bash
python3 jobs/raw_data_quality.py
```

Expected output:

```text
Raw Data Quality Report
Total rows: 259170
Raw data quality checks completed
```
```
required_columns_exist              PASS
row_count_between_expected_range    PASS
null_id_count                       PASS
invalid_id_count                    PASS
duplicate_id_group_count            PASS
duplicate_record_count              PASS
null_date_count                     PASS
invalid_date_count                  PASS
future_date_count                   PASS
null_primary_type_count             PASS
invalid_latitude_count              PASS
invalid_longitude_count             PASS
```
View the fully generated latest quality report:

```bash
cat data/quality/raw_chicago_crimes/reports/raw_dq_latest.md
```

Run transformation:

```bash
python3 jobs/transform_crimes.py
```

Expected output:

```text
Transformation completed
Clean detail rows: 259170
Daily summary rows: 7627
```

## Pipeline Jobs

### Raw Ingestion Job

File:

```text
jobs/raw_ingest.py
```

The raw ingestion job:

- Reads raw CSV data from the landing zone
- Reads source fields as strings instead of relying on schema inference
- Handles quoted and multiline CSV records
- Adds metadata columns
- Writes Parquet output to the raw zone
- Overwrites the raw zone on each run

Metadata columns added:

```text
ingestion_timestamp
source_file
```

Output path:

```text
data/raw/chicago_crimes
```

Important design decision:

The raw layer does not perform business transformations. It safely lands the source data and preserves the raw values for validation and downstream processing.

### Raw Data Quality Job

File:

```text
jobs/raw_data_quality.py
```

The raw data quality job validates the raw Parquet data before transformation.

Checks performed:

- Required columns exist
- Row count is within expected range
- `id` is not null
- `id` is castable to BIGINT
- Duplicate ID count is zero
- `date` is not null
- `date` is parseable as timestamp
- Future date count is zero
- `primary_type` is not null
- Latitude is within expected Chicago range
- Longitude is within expected Chicago range

The job writes a latest Markdown report to:

```text
data/quality/raw_chicago_crimes/reports/raw_dq_latest.md
```

It also writes historical results to:

```text
data/quality/raw_chicago_crimes/dq_results_history/
```

The latest successful manual run validated:

```text
Total rows: 259170
All critical checks: PASS
```

Critical failures stop the job. For inspection/debugging, the job can be run without stopping on failures:

```bash
FAIL_ON_DQ_ERROR=false python3 jobs/raw_data_quality.py
```

### Transformation Job

File:

```text
jobs/transform_crimes.py
```

The transformation job:

- Reads raw Parquet data
- Safely casts data types using `try_cast`
- Removes invalid records
- Deduplicates rows by `id`
- Parses crime dates
- Joins with an in-memory crime category lookup table
- Creates daily aggregations
- Calculates 7-day rolling averages
- Writes clean Parquet outputs

Output paths:

```text
data/clean/chicago_crimes_detail
data/clean/chicago_crimes_daily_summary
```

## Airflow DAG

File:

```text
dags/chicago_crimes_pipeline.py
```

The DAG currently contains two tasks:

```text
raw_ingest → transform_crimes
```

Each task uses `BashOperator`.

The DAG is scheduled daily:

```text
@daily
```

Each task has:

```text
retries=2
```

To run the pipeline:

1. Open Airflow UI
2. Search for `chicago_crimes_pipeline`
3. Toggle the DAG on
4. Click Trigger DAG
5. Confirm both tasks turn green

Next planned DAG update:

```text
raw_ingest → raw_data_quality → transform_crimes
```

## Output Tables

### Clean Detail Table

Path:

```text
data/clean/chicago_crimes_detail
```

Description:

Cleaned record-level crime data.

Key fields include:

```text
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
```

### Daily Summary Table

Path:

```text
data/clean/chicago_crimes_daily_summary
```

Description:

Aggregated daily crime metrics by crime type and category.

Key fields include:

```text
crime_date
primary_type
crime_category
total_crimes
total_arrests
domestic_crimes
seven_day_rolling_avg
```

### Raw Data Quality Report

Path:

```text
data/quality/raw_chicago_crimes/reports/raw_dq_latest.md
```

Description:

Markdown report generated by the raw data quality job. It summarizes row count, required column checks, null checks, duplicate ID checks, date checks, future date checks, and coordinate validation.

Example checks:

```text
required_columns_exist              PASS
row_count_between_expected_range    PASS
null_id_count                       PASS
invalid_id_count                    PASS
duplicate_id_group_count            PASS
duplicate_record_count              PASS
null_date_count                     PASS
invalid_date_count                  PASS
future_date_count                   PASS
null_primary_type_count             PASS
invalid_latitude_count              PASS
invalid_longitude_count             PASS
```

## Data Quality and Observability

The pipeline includes a pre-transformation data quality job. This job validates the raw Parquet data before transformation begins.

This is important because transformation logic can hide source data problems by dropping invalid records or deduplicating rows. A raw data quality gate makes source issues visible before downstream processing.

The data quality job provides:

```text
Current run quality report
Historical quality result files
Airflow-ready failure behavior
```

Generated reports are stored locally and excluded from GitHub.

## Troubleshooting Notes

### Airflow UI did not open on port 8080

Issue:

The EC2 security group allowed port `8080`, but the source IP was set incorrectly.

Cause:

The rule allowed the EC2 public IP instead of the local machine public IP.

Fix:

Updated inbound rule for port `8080`:

```text
Type: Custom TCP
Port: 8080
Source: My IP
```

### DAG failed because `.venv` was missing

Issue:

The Airflow task failed with:

```text
.venv/bin/activate: No such file or directory
```

Cause:

The virtual environment was created on the EC2 host, but Airflow tasks run inside Docker containers.

Fix:

Removed `.venv/bin/activate` from the DAG command and created a custom Airflow Docker image with Java and PySpark installed.

### Docker tried to pull `custom-airflow-spark`

Issue:

Docker returned:

```text
pull access denied for custom-airflow-spark
```

Cause:

The Docker Compose file had a custom image name but no valid build configuration.

Fix:

Added this build configuration under `x-airflow-common`:

```yaml
build:
  context: .
  dockerfile: Dockerfile
image: custom-airflow-spark:latest
```

### Docker Compose error: additional property `docker file` not allowed

Issue:

Docker Compose rejected the build configuration.

Cause:

The key was written incorrectly as:

```yaml
docker file
```

Fix:

Changed it to:

```yaml
dockerfile
```

### Docker build failed during PySpark install

Issue:

Docker build failed because pip could not find a package.

Cause:

There was a typo:

```text
pysparkwq
```

Fix:

Corrected it to:

```text
pyspark
```

### Python command not found on Ubuntu

Issue:

Running this command failed:

```bash
python jobs/raw_data_quality.py
```

Cause:

On Ubuntu, Python is commonly available as `python3`, not `python`.

Fix:

Use:

```bash
python3 jobs/raw_data_quality.py
```

or:

```bash
.venv/bin/python jobs/raw_data_quality.py
```

### Raw data quality job could not find raw Parquet path

Issue:

The job failed with a path error like:

```text
Path does not exist: data/raw/chicago_crimes
```

Cause:

The command was run from inside the `jobs/` folder, so the relative path resolved incorrectly.

Fix:

Run jobs from the project root:

```bash
cd ~/data-engineering-project
python3 jobs/raw_data_quality.py
```

The job was also improved to use project-root based paths.

### Timestamp parsing failed in data quality job

Issue:

The data quality job failed while parsing dates.

Cause:

The raw dataset had timestamps formatted like:

```text
2024-12-31 23:50:00
```

The job initially expected a different timestamp format.

Fix:

Updated the job to use safe timestamp parsing with multiple supported formats so invalid dates are counted instead of crashing the job.

### Raw ingestion produced incorrect row counts and failed quality checks

Issue:

The initial raw ingestion produced unexpectedly high row counts and many null or malformed fields.

Symptoms included:

```text
Unexpected row count
Null IDs
Invalid IDs
Duplicate IDs
Null dates
Null primary_type values
```

Cause:

The raw CSV was not being parsed safely. Schema inference and CSV parsing behavior caused malformed records in the raw Parquet zone.

Fix:

Updated the raw ingestion job to:

- Disable schema inference in the raw layer
- Read all source fields as strings
- Enable multiline CSV parsing
- Handle quoted and escaped values
- Overwrite the raw Parquet zone on each run

The corrected raw ingestion produced:

```text
Rows loaded: 259170
```

After the fix, all raw data quality checks passed.

## GitHub Notes

The following files and folders should not be committed:

```text
.venv/
__pycache__/
*.pyc
logs/
plugins/
config/
.env
data/landing/
data/raw/
data/clean/
data/quality/
*.csv
*.parquet
```

These are excluded using `.gitignore`.

## Current Project Status

Completed:

```text
Step 3: Raw ingestion layer
Step 4: Transformation layer
Step 5: Airflow DAG with raw_ingest → transform_crimes
Raw data quality job implemented and manually tested
Raw ingestion parsing issue identified and fixed
GitHub documentation checkpoint
```

Pending:

```text
Add raw_data_quality task to Airflow DAG
Step 7: Final analytics table partitioned by year/month
Optional: Add MinIO to simulate S3
```

## Next Steps

Planned improvements:

- Add `raw_data_quality` into the Airflow DAG
- Fail the Airflow pipeline when raw quality checks fail
- Add final analytics table partitioned by year and month
- Document final analytics schema
- Add MinIO to simulate S3 object storage
