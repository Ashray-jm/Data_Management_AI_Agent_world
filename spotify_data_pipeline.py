
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.redshift.hooks.redshift import RedshiftHook
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='spotify_data_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # every day at 6:00 AM ET
    catchup=False,
    tags=['example', 'spotify'],
) as dag:

    # Step 1: Load the CSV file from S3 to Redshift Staging Table
    load_to_staging = S3ToRedshiftOperator(
        task_id='load_spotify_history_to_staging',
        schema='public',
        table='stg_spotify_history_raw',
        s3_bucket='datasets-agentic-ai',
        s3_key='spotify_history.csv',
        copy_options=['CSV', 'IGNOREHEADER 1'],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift_upload',
        dag=dag,
    )

    # Step 2: Transform data from staging to cleaned table
    transform_data = SQLExecuteQueryOperator(
        task_id='transform_data',
        sql="""
        CREATE TABLE IF NOT EXISTS public.spotify_history_clean AS
        SELECT DISTINCT 
            TRIM(spotify_track_uri) AS spotify_track_uri,
            TRIM(ts)::TIMESTAMPTZ AS ts,
            TRIM(platform) AS platform,
            CAST(ms_played AS INT) AS ms_played,
            TRIM(track_name) AS track_name,
            TRIM(artist_name) AS artist_name,
            TRIM(album_name) AS album_name,
            TRIM(reason_start) AS reason_start,
            TRIM(reason_end) AS reason_end,
            TRIM(shuffle) AS shuffle,
            TRIM(skipped) AS skipped
        FROM public.stg_spotify_history_raw
        WHERE spotify_track_uri IS NOT NULL AND ts IS NOT NULL
        ORDER BY ts DESC;
        """,
        redshift_conn_id='redshift_analytics',
        dag=dag,
    )

    # Define task dependencies
    load_to_staging >> transform_data

