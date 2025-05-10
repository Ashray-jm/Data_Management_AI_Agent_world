from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.redshift.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

# Define the DAG
with DAG(
    dag_id='spotify_history_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 1),
    end_date=datetime(2023, 10, 31),
    default_args=default_args,
    catchup=False,
) as dag:

    # Task 1: Check if the file exists in S3
    check_file_existence = S3KeySensor(
        task_id='check_file_existence',
        bucket_name='datasets-agentic-ai',
        bucket_key='spotify_history.csv',
        timeout=600,
        poke_interval=60,
    )

    # Task 2: Copy data from S3 to Redshift staging table
    copy_to_redshift = S3ToRedshiftOperator(
        task_id='copy_to_redshift',
        table='stg_spotify_history_raw',
        schema='public',
        s3_bucket='datasets-agentic-ai',
        s3_key='spotify_history.csv',
        aws_conn_id='aws_default',
        redshift_conn_id='redshift-default',
        copy_options=['CSV'],
        database='dev',
        cluster_identifier='redshift-cluster-1-airflow',
    )

    # Task 3: Transform data in Redshift and create analytics table
    transform_data = PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='redshift-default',
        sql="""
        CREATE TABLE IF NOT EXISTS spotify_history_clean AS
        SELECT 
            TRIM(spotify_track_uri) AS spotify_track_uri,
            ts::TIMESTAMP WITH TIME ZONE AS ts,
            TRIM(platform) AS platform,
            (ms_played::INT / 1000) AS play_seconds,
            TRIM(track_name) AS track_name,
            TRIM(artist_name) AS artist_name,
            TRIM(album_name) AS album_name,
            TRIM(reason_start) AS reason_start,
            TRIM(reason_end) AS reason_end,
            TRIM(shuffle) AS shuffle,
            TRIM(skipped) AS skipped
        FROM stg_spotify_history_raw
        WHERE spotify_track_uri IS NOT NULL AND ts IS NOT NULL
        GROUP BY spotify_track_uri, ts
        ORDER BY ts desc;
        """,
    )

    # Task 4: Clean up staging table after transformation
    clean_up = PostgresOperator(
        task_id='clean_up',
        postgres_conn_id='redshift-default',
        sql='DROP TABLE IF EXISTS stg_spotify_history_raw;'
    )

    # Define task dependencies
    check_file_existence >> copy_to_redshift >> transform_data >> clean_up
