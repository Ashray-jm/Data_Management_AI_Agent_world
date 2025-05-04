from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.redshift.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Define default arguments for the tasks
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': False,
    'wait_for_downstream': False,
}

# Define the DAG
dag = DAG(
    dag_id='spotify_history_processing',
    default_args=default_args,
    description='A DAG for processing daily Spotify history data from CSV to Redshift',
    schedule_interval='30 6 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task to wait for new CSV file in S3
check_csv_file = S3KeySensor(
    task_id='check_csv_file',
    bucket_name='datasets-agentic-ai',
    object_key='spotify_history.csv',
    poke_interval=300,
    timeout=600,
    aws_conn_id='aws_default',
    dag=dag,
)

# Task to load CSV data to Redshift staging table
load_data_to_redshift = RedshiftToS3Operator(
    task_id='load_data_to_redshift',
    schema='public',
    table='stg_spotify_history_raw',
    copy_options='CSV',
    redshift_conn_id='redshift_conn_id',
    dag=dag,
)

# Task to transform data and create the cleaned analytics table
transform_and_create_table = SQLExecuteQueryOperator(
    task_id='transform_and_create_table',
    sql="""
    CREATE TABLE IF NOT EXISTS public.spotify_history_clean AS
    SELECT DISTINCT
        TRIM(sp.spotify_track_uri) AS spotify_track_uri,
        CAST(ts AS TIMESTAMP WITH TIME ZONE) AS ts,
        TRIM(sp.platform) AS platform,
        ms_played / 1000 AS play_seconds,
        TRIM(sp.track_name) AS track_name,
        TRIM(sp.artist_name) AS artist_name,
        TRIM(sp.album_name) AS album_name,
        TRIM(sp.reason_start) AS reason_start,
        TRIM(sp.reason_end) AS reason_end,
        CAST(sp.shuffle AS BOOLEAN) AS shuffle,
        CAST(sp.skipped AS BOOLEAN) AS skipped
    FROM public.stg_spotify_history_raw sp
    WHERE sp.spotify_track_uri IS NOT NULL AND sp.ts IS NOT NULL;
    """,
    redshift_conn_id='redshift_conn_id',
    dag=dag,
)

# Set task dependencies
check_csv_file >> load_data_to_redshift >> transform_and_create_table