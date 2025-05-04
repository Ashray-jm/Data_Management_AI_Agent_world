#!/usr/bin/env python

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.cncf.kubernetes.sensors.kubernetes_pod_sensor import KubernetesPodSensor
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.operators.python import PythonOperator
import logging


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['alert-smtp'],
}

# Define the DAG
with DAG(
    dag_id='spotify_history_processing_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # Every day at 07:00 AM
    start_date=days_ago(1),
    catchup=False,
    sla={'07:30': '07:30 ET'},
) as dag:

    # Step 1: Wait for the S3 file using S3KeySensor
    wait_for_file = S3KeySensor(
        task_id='wait_for_file',
        bucket_name='datasets-agentic-ai',
        bucket_key='spotify_history.csv',
        poke_interval=60,
        timeout=600,
    )

    # Step 2: Load raw data into the Redshift staging table
    load_spotify_history = RedshiftCopyOperator(
        task_id='load_spotify_history_raw',
        schema='public',
        table='stg_spotify_history_raw',
        copy_options='CSV, IGNOREHEADER 1',
        aws_conn_id='redshift-cluster-1-airflow',
        source_format='CSV',
        s3_bucket='datasets-agentic-ai',
        s3_key='spotify_history.csv',
        redshift_conn_id='redshift-cluster-1-airflow'
    )

    # Step 3: Transform raw data into the cleaned table with SQL
    transform_data = RedshiftSQLOperator(
        task_id='transform_data',
        sql="""
            CREATE TABLE IF NOT EXISTS spotify_history_clean AS
            SELECT DISTINCT
                TRIM(spotify_track_uri) AS spotify_track_uri,
                CAST(ts AS TIMESTAMP WITH TIME ZONE) AS ts,
                TRIM(platform) AS platform,
                ms_played / 1000 AS play_seconds,
                TRIM(track_name) AS track_name,
                TRIM(artist_name) AS artist_name,
                TRIM(album_name) AS album_name,
                TRIM(reason_start) AS reason_start,
                TRIM(reason_end) AS reason_end,
                CASE WHEN TRIM(shuffle) = 'true' THEN TRUE ELSE FALSE END AS shuffle,
                CASE WHEN TRIM(skipped) = 'true' THEN TRUE ELSE FALSE END AS skipped
            FROM stg_spotify_history_raw
            WHERE spotify_track_uri IS NOT NULL AND ts IS NOT NULL;
        """,
        redshift_conn_id='redshift-cluster-1-airflow',
    )

    # Defining task dependencies
    wait_for_file >> load_spotify_history >> transform_data

