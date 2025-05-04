from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'transactions_pipeline',
    default_args=default_args,
    description='A pipeline to load and clean transaction data',
    schedule_interval='0 7 * * *',  # Runs daily at 7 AM ET
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    def load_data_from_s3():
        # Logic to load data from S3
        s3_hook = S3Hook()
        # Assume the file is being pulled for a specific date
        s3_key = f'transactions_{datetime.now().strftime("%Y%m%d")}.csv'
        # Here you would add logic to load data to Snowflake
        print(f'Loading data from S3: {s3_key}')

    load_to_raw = PythonOperator(
        task_id='load_to_raw',
        python_callable=load_data_from_s3,
    )

    def transform_data_to_clean():
        # Logic for data transformation
        snowflake_hook = SnowflakeHook()
        print('Transforming data: trimming, casting, and filtering')
        # Here you would add logic for the transformations and insert into clean transactions

    transform_and_clean = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_data_to_clean,
    )

    start >> load_to_raw >> transform_and_clean
