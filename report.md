# Data Engineering Task Specification

## Task Description  
Build a data pipeline that processes the daily CSV file `spotify_history.csv` from S3, transforms the data as per defined rules, and loads it into a Redshift analytics table.

## Source Datasets & Interfaces  
- **Source CSV file:** `spotify_history.csv`  
- **Location:** `s3://datasets-agentic-ai/`  
- **Columns:**  
  - spotify_track_uri (character varying(256))  
  - ts (timestamp without time zone)  
  - platform (character varying(256))  
  - ms_played (integer)  
  - track_name (character varying(2000))  
  - artist_name (character varying(256))  
  - album_name (character varying(256))  
  - reason_start (character varying(256))  
  - reason_end (character varying(256))  
  - shuffle (boolean)  
  - skipped (boolean)  

## Required Transformations / Business Rules  
1. Concat `ms_played` and `platform` values as `test_column` in the `spotify_history_clean` table.

## Target Tables / Files & Data Model  
- **Staging Table:**  
  - Name: `spotify_upload_new`  
  - Database: `dev`  
  - Schema: `spotify_database`

- **Analytics Table:**  
  - Name: `spotify_history_clean`  
  - Structure: Same columns as the staging table + `test_column`

## Schedule / SLA  
- Daily execution of the pipeline at approximately 06:30 ET to load data from S3, with completion of the clean table available by 07:30 ET.

## Dependencies & Orchestration Hints  
- Ensure that Airflow is scheduled to run at 06:00 ET to check for the availability of the file in S3.
- Confirm the Redshift cluster `redshift-cluster-1-airflow` is up and accessible prior to execution.

## Acceptance Criteria  
1. The pipeline successfully identifies and processes the new `spotify_history.csv` file every day.  
2. Data is correctly copied into the staging table `spotify_upload_new`.  
3. The `spotify_history_clean` table is created/replaced and contains the expected transformations.  
4. The clean table is made available by 07:30 ET.

## Assumptions & Open Questions  
- Assumption: The source CSV file will always be present in the specified S3 location by 06:30 ET.  
- Open Question: What should be the handling mechanism for the cases when the file is not found or is corrupted?