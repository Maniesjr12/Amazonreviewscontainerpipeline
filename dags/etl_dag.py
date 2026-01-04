# data_etl_pipeline.py (CORRECTED)
import os
import subprocess
import sys
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the absolute base path that matches the Docker volume mount
# Volume: ./spark_scripts:/opt/spark/spark_app
SPARK_APP_BASE_PATH = "/opt/spark/spark_app"

# --- Configuration & Utility Functions ---

def _get_download_links_and_download():
    """
    Combines the web scraping and downloading logic.
    """
    
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd

    # 1. Configuration (Path Correction)
    # MUST use the correct container path: /opt/spark/spark_app/data/raw
    RAW_DIR = os.path.join(SPARK_APP_BASE_PATH, "data/raw") 
    
    # ... (Web Scraping Logic remains the same)
    url = 'https://mcauleylab.ucsd.edu/public_datasets/gdrive/googlelocal'
    response = requests.get(url, timeout=15)
    response.raise_for_status() 
    
    html_doc = response.content
    soup = BeautifulSoup(html_doc, 'html.parser')
    table_element_list = soup.find_all('table')
    full_data_table = table_element_list[1]
    table_row_element = full_data_table.find_all('tr')
    
    outer_list = []
    for row in table_row_element:
        inner_list = []
        for cell in row.find_all('td'):
            a_tag = cell.find('a', href=True)
            if a_tag:
                inner_list.append(a_tag['href'])
            else:
                inner_list.append(cell.get_text().strip())
        if inner_list: 
            outer_list.append(inner_list)

    Google_Rev_df = pd.DataFrame(outer_list, 
        columns=['State', 'Review_data', 'Business_Metadata'])

    TARGET_STATES = ['Alabama', 'Alaska', 'Arizona'] 
    
    Google_Rev_df['State'] = Google_Rev_df['State'].str.strip()
    filtered_df = Google_Rev_df[Google_Rev_df['State'].isin(TARGET_STATES)]
    result_lst = filtered_df[[ 'Review_data', 'Business_Metadata']].copy().values.tolist()
    link_list = [item for sublist in result_lst for item in sublist]
    
    if not link_list:
        print("No download links found for the target states. Exiting.")
        sys.exit(1)
    
    # Downloading Logic 
    print(f"Ensuring data storage directory exists at: '{RAW_DIR}'")
    
    # os.makedirs will now use the correct path: /opt/spark/spark_app/data/raw
    os.makedirs(RAW_DIR, exist_ok=True)
    
    for file_url in link_list:
        file_name = file_url.split('/')[-1]
        local_path = os.path.join(RAW_DIR, file_name)
        
        command = ["curl", "-o", local_path, file_url]
        
        print(f"\nStarting download of: {file_name}. Saving to: {local_path}")
        
        subprocess.run(command, check=True, capture_output=False) 
        print(f"Download completed successfully for {file_name}!")
    
    print("All downloads complete.")


# --- Spark Transformation Logic (As Bash Operator) ---
# 1. Path Correction for the application file
SPARK_APP_PATH = os.path.join(SPARK_APP_BASE_PATH, "transform_job.py") 

# 2. Add the JDBC JAR (Needed since transform_job.py writes to Postgres)
SPARK_SUBMIT_COMMAND = f"""
spark-submit \\
     --master spark://spark-master:7077 \\
     --packages org.postgresql:postgresql:42.7.4 \\
    --jars /opt/spark/jars/postgresql-42.7.4.jar \\
     --conf spark.driver.memory=2g \\
     --conf spark.executor.memory=4g \\
     --num-executors 2 \\
     --executor-cores 2 \\
     {SPARK_APP_PATH}
"""

# --- DAG Definition ---
with DAG(
     dag_id='data_ingestion_and_etl_pipeline',
     start_date=datetime(2025, 11, 12),
     schedule=timedelta(days=1),
     catchup=False,
     tags=['etl', 'scrape', 'spark'],
     default_args={
         'owner': 'airflow',
         'retries': 1,
         'retry_delay': timedelta(minutes=5),
     }
) as dag:
    
     # Setup Task: (Path Correction applied here)
     setup_directories = BashOperator(
         task_id='setup_directories',
         bash_command=f'''
            # Create the Spark Event Log Dir 
            mkdir -p /opt/spark/events && chmod -R 777 /opt/spark/events && \
            
            # Create the data subdirectories inside the CORRECT mounted path
            mkdir -p {SPARK_APP_BASE_PATH}/data/raw \
                     {SPARK_APP_BASE_PATH}/data/processed && \
            
            #  Ensure Airflow has full permissions on the data volume contents
            chmod -R 777 {SPARK_APP_BASE_PATH}/data
         ''',
     )
    
     # Ingestion Task: Scrape links and download files (uses corrected path internally)
     download_raw_data = PythonOperator(
         task_id='download_raw_data',
         python_callable=_get_download_links_and_download,
         execution_timeout=timedelta(minutes=30),
     )
    
     # Transformation Task: Run the PySpark ETL job (uses corrected path externally)
     transform_data_spark = BashOperator(
         task_id='transform_data_spark',
         bash_command=SPARK_SUBMIT_COMMAND,
         execution_timeout=timedelta(minutes=60),
     )
    
     # --- Task Dependencies ---
     setup_directories >> download_raw_data >> transform_data_spark