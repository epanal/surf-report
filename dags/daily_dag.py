from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from datetime import date
import boto3
import logging
import pendulum
import os
import psycopg2


# Set the timezone to GMT
gmt_timezone = pendulum.timezone("GMT")

# Set up default arguments for the daily DAG
default_args_daily = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, tzinfo=gmt_timezone),
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

# download and load the data into the SQL database
def download_and_load_s3_data():

    aws_access_key = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')
    # Data directory matches your docker-compose volume mapping
    DATA_DIR = './processed_data/buoy'


    #     # Establishing the connection
    conn = psycopg2.connect(
        database="airflow",
        user="airflow",
        password="airflow",
        host="host.docker.internal",
        port="5432"
    )
    cursor = conn.cursor()

    # Executing a function using the execute() method
    cursor.execute("SELECT version()")
    data = cursor.fetchone()
    print("Connection established to: ", data)

    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key,)
    
    # Get latest file from S3
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    objs = s3.list_objects_v2(Bucket='surflinehmbbucket')['Contents']
    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]
    print(last_added)
    # Download file from S3 # Use basename for the local file path
    local_file_path = os.path.join(DATA_DIR, os.path.basename(last_added))
    print(local_file_path)
    s3.download_file('surflinehmbbucket', last_added, local_file_path)

    command = """
            CREATE TABLE IF NOT EXISTS surf_report_aptos (
                id SERIAL PRIMARY KEY,
                report_timestamp TIMESTAMP,
                swell_height_ft NUMERIC,
                swell_period_sec NUMERIC,
                air_temp_f NUMERIC,
                water_temp_f NUMERIC
            );
            
            CREATE TEMPORARY TABLE staging_surf_report (
                report_date VARCHAR(255),
                report_time VARCHAR(255),
                swell_height_ft NUMERIC,
                swell_period_sec NUMERIC,
                air_temp_f NUMERIC,
                water_temp_f NUMERIC
            );
        """
    cursor.execute(command)
    conn.commit()

    # Load data from file
    with open(local_file_path, 'r') as f:
        # Skip header row
        next(f)
        try:
            cursor.copy_from(f, 'staging_surf_report', sep=',', null='NA')
            print("Data inserted into staging table successfully")
        except (Exception, psycopg2.DatabaseError) as err:
            print("Error loading data:", err)
            raise

    command = """
        INSERT INTO surf_report_aptos 
        (report_timestamp, swell_height_ft, swell_period_sec, air_temp_f, water_temp_f)
        SELECT 
            TO_TIMESTAMP(report_date || ' ' || report_time, 'MM/DD/YY HH24:MI GMT'),
            swell_height_ft,
            swell_period_sec,
            air_temp_f,
            water_temp_f
        FROM staging_surf_report
        WHERE NOT EXISTS (
            SELECT 1
            FROM surf_report_aptos
            WHERE surf_report_aptos.report_timestamp = 
                TO_TIMESTAMP(staging_surf_report.report_date || ' ' || 
                            staging_surf_report.report_time, 'MM/DD/YY HH24:MI GMT')
            );
    """
    cursor.execute(command)
    conn.commit()
    cursor.close()
    conn.close()

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
    download_s3_load_sql_task= PythonOperator(
    task_id='download_and_load_s3_data',
    python_callable=download_and_load_s3_data
)

# Setting task dependencies
load_s3_task >> download_s3_load_sql_task