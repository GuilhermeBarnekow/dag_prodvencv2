# mongodb_to_snowflakeV5_dag.py

from datetime import datetime, timedelta
from airflow import DAG

from tasks2 import (
    extract_and_ingest,
    update_processed_flags,
    stop_warehouse
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5, 
    'retry_delay': timedelta(minutes=2),  
}

with DAG(
    dag_id='mongodb_to_snowflakeV7',
    default_args=default_args,
    description='Extracts data from MongoDB and loads it into Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    
) as dag:

    extracted = extract_and_ingest()
    updated = update_processed_flags(extracted)
    stopped = stop_warehouse()

    extracted >>  updated >> stopped
