from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from tasks import (
    extract_process_data,
    ingest_to_snowflake,
    update_processed_flags,
    stop_warehouse,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='mongo_ingest_etlV2',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    processed_data = extract_process_data()
    ingest_result = ingest_to_snowflake(processed_data)
    update = update_processed_flags(ingest_result)
    stop_wh = stop_warehouse()

    processed_data >> ingest_result >> update >> stop_wh
