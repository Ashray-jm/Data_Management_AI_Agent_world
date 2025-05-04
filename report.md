# Data Engineering Task Specification

## Task Description  
Build a data pipeline that processes daily Spotify history data from a CSV file, performs necessary transformations, and stores it in a Redshift cluster for analytics purposes.

## Source Datasets & Interfaces  
- **Source File**: s3://datasets-agentic-ai/spotify_history.csv  
- **Columns in the file**:  
  - spotify_track_uri (STRING)  
  - ts (TIMESTAMP in ISO-8601, UTC)  
  - platform (STRING)  
  - ms_played (INTEGER, milliseconds)  
  - track_name (STRING)  
  - artist_name (STRING)  
  - album_name (STRING)  
  - reason_start (STRING)  
  - reason_end (STRING)  
  - shuffle (BOOLEAN encoded as 'true'/'false')  
  - skipped (BOOLEAN encoded as 'true'/'false')  

## Required Transformations / Business Rules  
1. Trim whitespace on all string columns.  
2. CAST ms_played -> INT and derive play_seconds = ms_played / 1000.  
3. CAST ts -> TIMESTAMP WITH TIME ZONE.  
4. Filter out rows where spotify_track_uri OR ts is NULL.  
5. Remove duplicates on (spotify_track_uri, ts), keeping the latest record.

## Target Tables / Files & Data Model  
- **Staging Table**:  
  - Name: stg_spotify_history_raw  
  - Database: dev  
  - Schema: public  

- **Analytics Table**:  
  - Name: spotify_history_clean  
  - Structure:
    - spotify_track_uri (STRING)  
    - ts (TIMESTAMP WITH TIME ZONE)  
    - platform (STRING)  
    - play_seconds (INT)  
    - track_name (STRING)  
    - artist_name (STRING)  
    - album_name (STRING)  
    - reason_start (STRING)  
    - reason_end (STRING)  
    - shuffle (BOOLEAN)  
    - skipped (BOOLEAN)  

## Schedule / SLA  
- The pipeline should run daily, starting at 06:30 ET to pick up the new CSV file and complete the processing by 07:30 ET.

## Dependencies & Orchestration Hints  
- The task depends on the successful arrival of the spotify_history.csv file in the specified S3 bucket.  
- The data pipeline should leverage Apache Airflow for orchestration to ensure proper scheduling and dependency management.

## Acceptance Criteria  
1. The pipeline successfully copies the CSV file to the Redshift staging table.  
2. The analytics table 'spotify_history_clean' is created/replaced daily and meets defined business rules.  
3. The table is available by 07:30 ET daily without errors.

## Assumptions & Open Questions  
- Assumption: The CSV file structure will remain consistent.  
- Assumption: Amazon Redshift cluster and appropriate permissions are already set up.  
- Open Question: Are there any specific data retention policies or old data cleanup requirements for the analytics table?