# Data Engineering Task Specification

## Task Description
Build a data pipeline that:
1. Waits for the new CSV file named `spotify_history.csv` written daily at around 06:30 ET to `s3://datasets-agentic-ai/`.
2. Copies it into Amazon Redshift cluster 'redshift-cluster-1-airflow', database 'dev', schema 'public', and staging table 'stg_spotify_history_raw'.
3. Creates / replaces an analytics table 'spotify_history_clean' applying specific transformation rules.  
4. Makes the clean table available by 07:30 ET daily.

## Source Datasets & Interfaces
- Daily CSV file: `spotify_history.csv`
- S3 Path: `s3://datasets-agentic-ai/`

## Required Transformations / Business Rules
- Trim whitespace on all string columns:
  * `track_name`, `artist_name`, `album_name`, and `platform`
- CAST:
  * `ms_played` to INT and derive `play_seconds = ms_played / 1000`
  * `ts` to `TIMESTAMP WITH TIME ZONE`
- Filter out rows where `spotify_track_uri` OR `ts` is NULL.
- Remove duplicates based on `(spotify_track_uri, ts)` keeping the latest entry.

## Target Tables / Files & Data Model
- Staging Table: `stg_spotify_history_raw` in Amazon Redshift.
- Cleaning Table: `spotify_history_clean` in Amazon Redshift.

## Schedule / SLA
- The new CSV file should be processed daily, with the clean table available by 07:30 ET.

## Dependencies & Orchestration Hints
- Ensure the Airflow DAG waits for the CSV file before executing any subsequent steps.
- Use a sensor in Airflow to monitor the presence of the file in S3.
- Ensure that proper error handling is implemented for each step to manage any failures or discrepancies in the data.

## Acceptance Criteria
- The data pipeline processes the `spotify_history.csv` correctly, loading it into Redshift.
- The analytics table `spotify_history_clean` is populated with transformed data as per the specified business rules.
- The pipeline runs daily with no manual intervention, and the data is available by 07:30 ET.

## Assumptions & Open Questions
- It is assumed that the file format remains consistent without any changes in the schema.
- Are there any other external dependencies that may affect the data pipeline, such as other data sources or systems?
- What is the expected volume of data in `spotify_history.csv`, and how will that affect processing time?