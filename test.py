import time
from airflow import DAG
from datetime import date, datetime
from datetime import timedelta
import os
import psycopg2
import pysurfline
from pysurfline.reports import SurfReport
from pysurfline.reports import SpotForecasts
import pandas as pd
import requests
pd.set_option('display.max_columns', None)
import boto3
from _scproxy import _get_proxy_settings
import logging


# _get_proxy_settings()
# os.environ['NO_PROXY'] = '*'

# params = {
#             "spotId": "5842041f4e65fad6a7708970",
#             "days": 1,
#             "intervalHours": 1,
#         }

# today = date.today()
# reportdate = today.strftime("%d-%m-%y")

# #report = pysurfline.get_spot_forecasts(params)

# princeton_jetty_hmb = '5842041f4e65fad6a7708970'
# spot_forecasts = pysurfline.get_spot_forecasts(
#     spotId='5842041f4e65fad6a7708970',
#     days=1,
#     intervalHours=1,
# )

# print(spot_forecasts.get_dataframe().head())


# def download_data(**context):
#     # Set up logging
#     logger = logging.getLogger('airflow.task')
    
#     princeton_jetty_hmb = '5842041f4e65fad6a7708970'
#     max_retries = 5
#     base_backoff_time = 1  # Start with 1 second
    
#     logger.info(f"Attempting to download surf forecast for spot {princeton_jetty_hmb}")

#     for attempt in range(max_retries):
#         try:
#             logger.info(f"Attempt {attempt + 1} of {max_retries}")
            
#             # Calculate exponential backoff time
#             backoff_time = base_backoff_time * (2 ** attempt)
            
#             try:
#                 # Attempt to get the forecast
#                 report = pysurfline.get_spot_forecasts(
#                     spotId=princeton_jetty_hmb, 
#                     days=1, 
#                     intervalHours=1
#                 )
                
#                 # Log successful retrieval
#                 logger.info("Surf forecast successfully retrieved")
                
#                 # Optional: Save the report to a file or do further processing
#                 if report:
#                     # Example: save to a file in the raw_data directory
#                     import json
#                     from datetime import date
                    
#                     today = date.today()
#                     filename = f"/opt/airflow/raw_data/{today.strftime('%m-%d-%y')}-surf-report.json"
                    
#                     with open(filename, 'w') as f:
#                         json.dump(report, f, indent=2)
                    
#                     logger.info(f"Saved surf report to {filename}")
                
#                 return report
            
#             except requests.exceptions.RequestException as req_error:
#                 logger.error(f"Request error on attempt {attempt + 1}: {req_error}")
                
#                 # Check for specific HTTP error codes
#                 if hasattr(req_error, 'response'):
#                     status_code = req_error.response.status_code
                    
#                     if status_code == 429:  # Too Many Requests
#                         logger.warning(f"Rate limit hit. Waiting {backoff_time} seconds...")
#                         time.sleep(backoff_time)
#                         continue
#                     elif 500 <= status_code < 600:  # Server errors
#                         logger.error(f"Server error (HTTP {status_code}). Retrying in {backoff_time} seconds...")
#                         time.sleep(backoff_time)
#                         continue
#                     else:
#                         logger.error(f"Unhandled HTTP error: {status_code}")
#                         raise
                
#                 # For other request exceptions
#                 logger.error(f"Unexpected request error: {req_error}")
#                 time.sleep(backoff_time)
        
#         except Exception as e:
#             logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
            
#             # If it's the last attempt, raise an Airflow exception
#             if attempt == max_retries - 1:
#                 raise AirflowException(f"Failed to download surf forecast after {max_retries} attempts") # type: ignore
            
#             # Wait before retrying
#             time.sleep(base_backoff_time * (2 ** attempt))
    
#     # This should never be reached due to the exception above, 
#     # but included for completeness
#     raise AirflowException("Maximum retries exceeded for surf forecast download") # type: ignore

# download_data()


# import boto3
# import os

# # Create a session using environment variables
# session = boto3.Session(
#     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#     region_name=os.getenv('AWS_DEFAULT_REGION')
# )

# # Try listing S3 buckets
# s3 = session.resource('s3')
# for bucket in s3.buckets.all():
#     print(bucket.name)

############################
def load_data():
    # Data directory matches your docker-compose volume mapping
    DATA_DIR = './processed_data/buoy'


    #     # Establishing the connection
    conn = psycopg2.connect(
        database="airflow", user='airflow', password='airflow', host='localhost', port='5432'
    )
    cursor = conn.cursor()

    # Executing a function using the execute() method
    cursor.execute("SELECT version()")
    data = cursor.fetchone()
    print("Connection established to: ", data)

    s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
    
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

load_data()