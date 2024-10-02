from airflow import DAG
from airflow.utils.dates import days_ago
from tasks import extract_process_data, ingest_to_snowflake, update_processed_flags, stop_warehouse
import logging
from datetime import datetime, timedelta
import pytz

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
)

with DAG(
    dag_id='mongo_to_snowflake_etlV2',
    default_args=default_args,
    description='DAG para extrair dados do MongoDB, converter para Parquet e ingerir no Snowflake',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_process_task = extract_process_data()
    ingest_task = ingest_to_snowflake(extract_process_task)
    update_flags_task = update_processed_flags(ingest_task)
    stop_warehouse_task = stop_warehouse()

    extract_process_task >> ingest_task >> update_flags_task >> stop_warehouse_task
