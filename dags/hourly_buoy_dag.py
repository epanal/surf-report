from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta
import requests
import logging
import csv
import os
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('America/Los_Angeles').subtract(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_daily_file_paths(data_folder):
    """Generate file paths for daily data files"""
    current_date = pendulum.today('America/Los_Angeles').strftime("%Y%m%d")
    return {
        'csv': os.path.join(data_folder, f"aptos_data_{current_date}.csv")
    }

def extract_value(text, pattern):
    """Helper function to extract numeric values from text"""
    match = re.search(pattern, text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def parse_buoy_text(text_data, csv_path):
    """Parse NDBC buoy text data with timestamp checking against existing CSV data."""
    try:
        lines = text_data.split('\n')
        data = {}
        in_wave_summary = False
        found_temps = False
        
        # Extract timestamp
        time_pattern = r"(\d+:\d+ (?:am|pm) PDT) (\d+/\d+/\d+)"
        match_count = 0
        
        # Check if this timestamp already exists in the CSV
        def timestamp_exists(date, time):
            if os.path.exists(csv_path):
                with open(csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row['report_date'] == date and row['report_time'] == time:
                            return True
            return False

        for line in lines:
            match = re.search(time_pattern, line)
            if match:
                match_count += 1
                # Check if this is the second instance
                if match_count == 2:
                    data['report_time'] = match.group(1)
                    data['report_date'] = match.group(2)
                    # Skip if this timestamp already exists
                    if timestamp_exists(data['report_date'], data['report_time']):
                        logging.info("Timestamp already processed; skipping.")
                        return None
                    break
        
        # Continue with additional parsing for other data points
        for line in lines:
            if 'Wave Summary' in line:
                in_wave_summary = True
                continue
            if not in_wave_summary and not found_temps:
                if 'Air Temp:' in line:
                    data['air_temp_f'] = extract_value(line, r'Air Temp: ([\d.]+) °F')
                elif 'Peak Period:' in line:
                    data['swell_period_sec'] = extract_value(line, r'Peak Period: ([\d.]+) sec')    
                elif 'Water Temp:' in line:
                    data['water_temp_f'] = extract_value(line, r'Water Temp: ([\d.]+) °F')
            if in_wave_summary:
                if 'Swell:' in line:
                    data['swell_height_ft'] = extract_value(line, r'Swell: ([\d.]+) ft')
        
        logging.info(f"Parsed data: {data}")
        
        return data
    
    except Exception as e:
        logging.error(f"Error parsing buoy data: {e}")
        logging.error(f"Raw text:\n{text_data}")
        return None


def write_to_csv(csv_path, data):
    """Write or append data to CSV file with wave and temperature data"""
    file_exists = os.path.exists(csv_path)
    
    with open(csv_path, 'a', newline='') as f:
        writer = csv.writer(f)
        
        if not file_exists:
            headers = [
                'report_date',
                'report_time',
                'swell_height_ft',
                'swell_period_sec',
                'air_temp_f',
                'water_temp_f'
            ]
            writer.writerow(headers)
        
        row = [
            data.get('report_date', 'NA'),
            data.get('report_time', 'NA'),
            data.get('swell_height_ft', 'NA'),
            data.get('swell_period_sec', 'NA'),
            data.get('air_temp_f', 'NA'),
            data.get('water_temp_f', 'NA')
        ]
        writer.writerow(row)

def download_buoy_data(**context):
    raw_data_folder = '/opt/airflow/raw_data/buoy'
    processed_data_folder = '/opt/airflow/processed_data/buoy'
    os.makedirs(raw_data_folder, exist_ok=True)
    os.makedirs(processed_data_folder, exist_ok=True)
    
    try:
        url = "https://www.ndbc.noaa.gov/data/latest_obs/46282.txt"
        
        response = requests.get(url)
        response.raise_for_status()
        
        logging.info(f"Raw data received:\n{response.text}")
        
        # Get daily file paths
        daily_files = get_daily_file_paths(raw_data_folder)
        
        # Parse the data
        parsed_data = parse_buoy_text(response.text, daily_files['csv'])
        
        if parsed_data:
            write_to_csv(daily_files['csv'], parsed_data)
            logging.info("New data appended successfully")
        else:
            logging.info("No new data to append (duplicate timestamp).")
    
    except requests.RequestException as e:
        logging.error(f"Error downloading data: {e}")
    
    return "Data collection completed"


def cleanup_old_files(**context):
    """Remove files older than 2 days"""
    raw_data_folder = '/opt/airflow/raw_data/buoy'
    processed_data_folder = '/opt/airflow/processed_data/buoy'

    cutoff_date = pendulum.now('America/Los_Angeles') - timedelta(days=2)
    
    for filename in os.listdir(raw_data_folder):
        try:
            date_str = filename.split('_')[-1].split('.')[0]
            file_date = datetime.strptime(date_str, '%Y%m%d')
            
            # Convert file_date to Pacific Time
            file_date = pendulum.instance(file_date).in_tz('America/Los_Angeles')
            
            if file_date < cutoff_date:
                file_path = os.path.join(raw_data_folder, filename)
                os.remove(file_path)
                logging.info(f"Removed old file: {filename}")
        except (ValueError, IndexError) as e:
            continue

    for filename in os.listdir(processed_data_folder):
        try:
            date_str = filename.split('_')[-1].split('.')[0]
            file_date = datetime.strptime(date_str, '%Y%m%d')
            
            # Convert file_date to Pacific Time
            file_date = pendulum.instance(file_date).in_tz('America/Los_Angeles')
            
            if file_date < cutoff_date:
                file_path = os.path.join(processed_data_folder, filename)
                os.remove(file_path)
                logging.info(f"Removed old file: {filename}")
        except (ValueError, IndexError) as e:
            continue
        

with DAG(
    'aptos_buoy_data',
    default_args=default_args,
    description='Collect NDBC Buoy Data for Aptos (Station 46282)',
    schedule=timedelta(hours=1),
    catchup=False
) as dag:
    
    download_task = PythonOperator(
        task_id='download_buoy_data',
        python_callable=download_buoy_data
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_old_files',
        python_callable=cleanup_old_files
    )
    
    download_task >> cleanup_task