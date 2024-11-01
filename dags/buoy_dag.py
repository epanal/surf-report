from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import logging
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_buoy_data(**context):
    # Specify the raw_data folder path
    raw_data_folder = '/opt/airflow/raw_data/buoy'
    
    # Create the raw_data folder if it doesn't exist
    os.makedirs(raw_data_folder, exist_ok=True)
    
    # List of buoy stations
    buoy_stations = [
        '46012',   # buoy 24 NM SSW of San Francisco
        '46026',   # buoy 18 NM SSW of San Francisco
        '46282',    # Aptos Creek
        '1801593'   #saildrone data
    ]
    
    all_buoy_data = {}
    
    for station in buoy_stations:
        try:
            url = f"https://www.ndbc.noaa.gov/data/latest_obs/{station}.txt"
            
            # Fetch the data
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse the data
            parsed_data = parse_buoy_text(response.text, station)
            
            if parsed_data:
                all_buoy_data[station] = parsed_data
                logging.info(f"Successfully downloaded data for station {station}")
                
                # Save raw text to a file
                raw_file_path = os.path.join(raw_data_folder, f"{station}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
                with open(raw_file_path, 'w') as f:
                    f.write(response.text)
        
        except requests.RequestException as e:
            logging.error(f"Error downloading data for station {station}: {e}")
    
    # Log the data
    logging.info(f"Total stations data collected: {len(all_buoy_data)}")
    
    # Save parsed data to a JSON file
    json_file_path = os.path.join(raw_data_folder, f"buoy_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(json_file_path, 'w') as f:
        json.dump(all_buoy_data, f, indent=2)
    
    # Push the data to XCom for potential further processing
    context['ti'].xcom_push(key='buoy_data', value=all_buoy_data)
    
    return all_buoy_data

def parse_buoy_text(text_data, station):
    """
    Parse the NDBC buoy text data into a structured format
    """
    try:
        # Split the text into lines
        lines = text_data.strip().split('\n')
        
        # Create a dictionary to store parsed data
        parsed_data = {
            'station': station,
            'raw_text': text_data,
            'timestamp': lines[0].strip(),
            'data': {}
        }
        
        # Parse the header (second line)
        headers = lines[1].split()
        
        # Parse the data line (third line)
        data_values = lines[2].split()
        
        # Combine headers and values
        for header, value in zip(headers, data_values):
            parsed_data['data'][header] = value
        
        return parsed_data
    
    except Exception as e:
        logging.error(f"Error parsing data for station {station}: {e}")
        return None

# Create the DAG
with DAG(
    'buoy_data_collection',
    default_args=default_args,
    description='Collect NDBC Buoy Data for Half Moon Bay and Aptos',
    schedule_interval=timedelta(hours=1),
    catchup=False
) as dag:
    
    # Task to download buoy data
    download_task = PythonOperator(
        task_id='download_buoy_data',
        python_callable=download_buoy_data,
        provide_context=True
    )