"""
\
# Airflow DAG for processing Spotify history data
\
from airflow import DAG
\
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
\
from airflow.providers.amazon.redshift.transfers.s3_to_redshift import RedshiftCopyOperator
\
from airflow.operators.sql import SQLExecuteQueryOperator
\
from airflow.utils.dates import days_ago
\
from airflow.utils.email import send_email
\
from datetime import timedelta
\

\
# Default arguments
\
default_args = {
\
    'owner': 'airflow',
\
    'retries': 3,
\
    'retry_delay': timedelta(minutes=5),
\
    'email_on_failure': True,
\
    'email_on_retry': False,
\
    'email': 'your_email@example.com',
\
}
\

\
# Define the DAG
\
dag = DAG(
\
    'spotify_history_processing',
\
    default_args=default_args,
\
    description='A DAG for loading and transforming Spotify history data',
\
    schedule_interval='30 6 * * *',  # Every day at 6:30 AM ET
\
    start_date=days_ago(1),
\
    catchup=False,
\
    sla={'spotify_history_clean': timedelta(hours=1)},  # SLA: by 7:30 AM ET
\
)
\

\
# Task 1: S3 Key Sensor to watch for the CSV file
\
sensor_task = S3KeySensor(
\
    task_id='check_for_spotify_history',
\
    bucket_name='datasets-agentic-ai',
\
    bucket_key='spotify_history.csv',
\
    aws_conn_id='aws_default',
\
    poke_interval=60,
\
    timeout=600,
\
    dag=dag,
\
)
\

\
# Task 2: Load CSV to Redshift staging table
\
load_task = RedshiftCopyOperator(
\
    task_id='load_spotify_history',
\
    schema='public',
\
    table='stg_spotify_history_raw',
\
    s3_bucket='datasets-agentic-ai',
\
    s3_key='spotify_history.csv',
\
    copy_options='FORMAT AS CSV',
\
    aws_conn_id='aws_default',
\
    dag=dag,
\
)
\

\
# Task 3: SQL transformation to create the cleaned table
\
transform_task = SQLExecuteQueryOperator(
\
    task_id='transform_clean_spotify_history',
\
    sql="""
\
    CREATE TABLE IF NOT EXISTS spotify_history_clean AS
\
    SELECT DISTINCT \
\
        TRIM(spotify_track_uri) AS spotify_track_uri,
\
        TRIM(track_name) AS track_name,
\
        TRIM(artist_name) AS artist_name,
\
        TRIM(album_name) AS album_name,
\
        TRIM(platform) AS platform,
\
        TRIM(reason_start) AS reason_start,
\
        TRIM(reason_end) AS reason_end,
\
        CAST(ms_played AS INT) AS ms_played,
\
        (CAST(ms_played AS INT) / 1000) AS play_seconds,
\
        CAST(ts AS TIMESTAMPTZ) AS ts
\
    FROM stg_spotify_history_raw
\
    WHERE spotify_track_uri IS NOT NULL AND ts IS NOT NULL
\
    AND (spotify_track_uri, ts) IN (
\
        SELECT spotify_track_uri, ts
\
        FROM stg_spotify_history_raw
\
        GROUP BY spotify_track_uri, ts
\
        HAVING MAX(ts)
\
    );
\
    """,
\
    redshift_conn_id='aws_default',
\
    dag=dag,
\
)
\

\
# Task dependencies
\
sensor_task >> load_task >> transform_task
\
"""
\
}