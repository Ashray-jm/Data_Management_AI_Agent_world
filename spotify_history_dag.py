
"""
Airflow DAG for processing Spotify history data from S3 to Amazon Redshift.

This DAG waits for a CSV file in S3, loads it into a staging table in Redshift,
transforms the data, and creates an analytics table. It is scheduled to run daily.
"""

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook
from airflow.operators.sql import PostgresOperator
from datetime import datetime, timedelta
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['alert@example.com'],  # Change this to your alert email
}

# Initialize the DAG
dag_id = 'spotify_history_dag'
dag = DAG(dag_id=dag_id,
          default_args=default_args,
          schedule_interval='30 6 * * *',
          catchup=False)

# Step 1: Wait for the new CSV file in S3
wait_for_file = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='datasets-agentic-ai',
    bucket_key='spotify_history.csv',
    aws_conn_id='aws_default',
    poke_interval=30,
    timeout=600,
    dag=dag
)

# Step 2: Load the CSV data into Redshift staging table
load_raw_data = S3ToRedshiftOperator(
    task_id='load_raw_data',
    schema='public',
    table='stg_spotify_history_raw',
    s3_bucket='datasets-agentic-ai',
    s3_key='spotify_history.csv',
    copy_options='FORMAT AS CSV',
    redshift_conn_id='redshift-cluster-1-airflow',
    dag=dag
)

# Step 3: Transform the loaded data into the clean analytics table
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='redshift-cluster-1-airflow',
    sql="""
        CREATE TABLE IF NOT EXISTS public.spotify_history_clean AS (
            SELECT 
                TRIM(track_name) AS track_name,
                TRIM(artist_name) AS artist_name,
                TRIM(album_name) AS album_name,
                TRIM(platform) AS platform,
                CAST(ms_played AS INT) AS ms_played,
                (ms_played / 1000) AS play_seconds,
                CAST(ts AS TIMESTAMP WITH TIME ZONE) AS ts
            FROM 
                public.stg_spotify_history_raw
            WHERE 
                spotify_track_uri IS NOT NULL AND ts IS NOT NULL
            GROUP BY 
                spotify_track_uri, ts
            ORDER BY 
                ts DESC
        );
    """,
    dag=dag
)

# Set task dependencies
wait_for_file >> load_raw_data >> transform_data
