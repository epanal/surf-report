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

#     # Establishing the connection
conn = psycopg2.connect(
    database="storm", user='postgres', password='wavestorm', host='172.17.0.1', port='5432'
)
cursor = conn.cursor()

#     # Executing a function using the execute() method
#     cursor.execute("SELECT version()")
#     data = cursor.fetchone()
#     print("Connection established to: ", data)

#     s3 = boto3.client('s3', aws_access_key_id="", aws_secret_access_key="")
#     get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
#     objs = s3.list_objects_v2(Bucket='wavestorm')['Contents']
#     last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]

#     command = """
#         CREATE TEMPORARY TABLE IF NOT EXISTS staging_surf_report (
#             timestamp TIMESTAMP PRIMARY KEY,
#             surf_min INTEGER,
#             surf_max INTEGER,
#             surf_optimalScore INTEGER,
#             surf_plus BOOL,
#             surf_humanRelation VARCHAR(255),
#             surf_raw_min NUMERIC,
#             surf_raw_max NUMERIC,
#             speed NUMERIC,
#             direction NUMERIC,
#             directionType VARCHAR(255),
#             gust NUMERIC,
#             optimalScore INTEGER,
#             temperature NUMERIC,
#             condition VARCHAR(255)
#         );
#     """
#     cursor.execute(command)
#     conn.commit()

#     print(last_added)
#     with open(dag_path + '/processed_data/' + last_added, 'r') as f:
#         try:
#             cursor.copy_from(f, 'staging_surf_report', sep=",")
#             print("Data inserted using copy_from_datafile() successfully....")
#         except (Exception, psycopg2.DatabaseError) as err:
#             print("Database Error: ", err)
#             cursor.close()
#             conn.close()
#             return

#     command = """
#         INSERT INTO surf_report_hmb
#         (timestamp, surf_min, surf_max, surf_optimalScore, surf_plus, surf_humanRelation, surf_raw_min, surf_raw_max, speed, direction, directionType, gust, optimalScore, temperature, condition)
#         SELECT *
#         FROM staging_surf_report
#         WHERE NOT EXISTS (
#             SELECT timestamp
#             FROM surf_report_hmb
#             WHERE staging_surf_report.timestamp = surf_report_hmb.timestamp
#         );
#     """
#     cursor.execute(command)
#     conn.commit()
#     cursor.close()
#     conn.close()