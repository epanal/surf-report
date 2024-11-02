from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
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
    # Add debug logging to verify credentials are being retrieved
    aws_access_key = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')
    
    logging.info("AWS Access Key length: %d", len(aws_access_key) if aws_access_key else 0)
    logging.info("AWS Secret Key length: %d", len(aws_secret_key) if aws_secret_key else 0)
    
    today = pendulum.today('America/Los_Angeles')
    surfdate = today.strftime("%Y%m%d")
    dag_path = '/opt/airflow'
    
    try:
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name='us-west-1'  # Add your AWS region here
        )
        s3 = session.resource('s3')
        
        # Add debug logging for file path
        file_path = os.path.join(dag_path, 'raw_data/buoy', f'aptos_data_{surfdate}.csv')
        logging.info(f"Attempting to upload file from: {file_path}")
        
        s3.meta.client.upload_file(
            file_path, 
            'surflinehmbbucket', 
            f'aptos_data_{surfdate}.csv'
        )
        logging.info(f'Successfully uploaded aptos_data_{surfdate}.csv to S3 bucket "surflinehmbbucket".')
    except Exception as e:
        logging.error(f"Error uploading to S3: {str(e)}")
        raise  # This will help show the full error in Airflow logs

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
