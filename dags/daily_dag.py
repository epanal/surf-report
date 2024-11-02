from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from datetime import date
import boto3
import logging
import pendulum
import os

# Set up default arguments for the daily DAG
default_args_daily = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('America/Los_Angeles').subtract(days=1),  # Start yesterday
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the S3 upload function
def load_s3_data():
    today = pendulum.today('America/Los_Angeles')
    surfdate = today.strftime("%Y%m%d")
    dag_path = '/opt/airflow'  # Adjust this path as necessary
    session = boto3.Session() # No need to pass aws_access_key_id or aws_secret_access_key
    s3 = session.resource('s3')
    
    try:
        # Upload the file to S3
        s3.meta.client.upload_file(
            os.path.join(dag_path, 'processed_data/buoy', f'aptos_data_{surfdate}.csv'), 
            'surflinehmbbucket', 
            f'aptos_data_{surfdate}.csv'
        )
        logging.info(f'Successfully uploaded aptos_data_{surfdate}.csv to S3 bucket "surflinehmbbucket".')
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")

# Create the new DAG for daily execution
with DAG(
    's3_data',
    default_args=default_args_daily,
    description='Load Daily Data to S3',
    schedule='@daily',  # This ensures it runs once a day
    catchup=False
) as dag_daily:
    
    load_s3_task = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=load_s3_data
    )
