import logging
import os
import tempfile
from datetime import datetime

import pandas as pd
import pytz
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pymongo import MongoClient
import snowflake.connector

from utils import getMongoDbCredentials

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
)

# Definir a timezone de São Paulo
SAO_PAULO_TZ = pytz.timezone('America/Sao_Paulo')

def get_snowflake_hook(conn_id='snowflake_default', timezone='America/Sao_Paulo'):
    """Helper function to initialize a SnowflakeHook with the specified timezone."""
    return SnowflakeHook(
        snowflake_conn_id=conn_id,
        session_parameters={'TIMEZONE': timezone}
    )

def execute_snowflake_query(hook, query, params=None, fetch_one=False):
    """Helper function to execute a query using SnowflakeHook."""
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if fetch_one:
                return cursor.fetchone()
            else:
                return cursor.fetchall()

@task
def extract_process_data():
    """
    Extracts data from MongoDB based on the last `DATA_UPLOAD` recorded in Snowflake,
    processes the data, and saves it to a Parquet file.
    """
    try:
        # Initialize SnowflakeHook
        snowflake_hook = get_snowflake_hook()
        logging.info("Fetching the last DATA_UPLOAD from Snowflake.")

        # Get the last DATA_UPLOAD
        last_data_upload = execute_snowflake_query(
            snowflake_hook,
            "SELECT MAX(DATA_UPLOAD) AS last_data_upload FROM BRONZE.CONTROLE_PRODVENC",
            fetch_one=True
        )[0]
        logging.info(f"Last DATA_UPLOAD retrieved: {last_data_upload}")

        # Connect to MongoDB
        mongo_uri = getMongoDbCredentials()
        client = MongoClient(
            f"mongodb://{mongo_uri['mongo_user']}:{mongo_uri['mongo_password']}@"
            f"{mongo_uri['mongo_host']}:{mongo_uri['mongo_port']}/"
            f"{mongo_uri['mongo_db']}?authSource={mongo_uri['mongo_authSource']}&"
            f"readPreference={mongo_uri['mongo_readPreference']}&"
            f"appname={mongo_uri['mongo_appname']}&"
            f"replicaSet={mongo_uri['mongo_replicaSet']}&"
            f"directConnection={mongo_uri['mongo_directConnection']}"
        )
        collection = client[mongo_uri["mongo_db"]]['prodvenc']
        logging.info("Connected to MongoDB.")

        # Prepare query for MongoDB
        if last_data_upload:
            if isinstance(last_data_upload, str):
                last_data_upload = datetime.strptime(
                    last_data_upload, '%Y-%m-%d %H:%M:%S'
                ).replace(tzinfo=SAO_PAULO_TZ)
            elif last_data_upload.tzinfo is None:
                last_data_upload = last_data_upload.replace(tzinfo=SAO_PAULO_TZ)
            query_mongo = {'data_registro': {'$gt': last_data_upload}}
        else:
            query_mongo = {}
        logging.info(f"MongoDB query: {query_mongo}")

        # Fetch documents from MongoDB
        documentos = list(collection.find(query_mongo))
        client.close()
        logging.info(f"Documents found: {len(documentos)}")

        if not documentos:
            logging.info("No new documents to process. Skipping DAG.")
            raise AirflowSkipException("No new documents to process.")

        # Process data
        df = pd.DataFrame(documentos)
        df['_id'] = df['_id'].astype(str)
        for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            df[col] = df[col].dt.tz_localize(SAO_PAULO_TZ, ambiguous='NaT', nonexistent='NaT')

        # Save to Parquet
        data_extracao = datetime.now(SAO_PAULO_TZ)
        nome_arquivo = f"mongodb_prodvenc_{data_extracao.strftime('%Y%m%d%H%M%S')}.parquet"
        parquet_file_path = os.path.join(tempfile.gettempdir(), nome_arquivo)
        df.to_parquet(parquet_file_path, index=False)
        logging.info(f"Data exported to Parquet: {parquet_file_path}")

        return {
            'parquet_path': parquet_file_path,
            'nome_arquivo': nome_arquivo,
            'total_registros': len(df),
            'data_extracao': data_extracao
        }

    except Exception as e:
        logging.error(f"Error in extract_process_data: {e}")
        raise

@task
def ingest_to_snowflake(processed_data: dict):
    """
    Ingests data from the Parquet file to Snowflake and updates the CONTROLE_PRODVENC table.
    """
    try:
        parquet_file_path = processed_data['parquet_path']
        nome_arquivo = processed_data['nome_arquivo']
        total_registros = processed_data.get('total_registros', 0)
        data_extracao = processed_data.get('data_extracao')

        if not parquet_file_path:
            logging.info("No Parquet file to ingest.")
            return {'nome_arquivo': None}

        snowflake_hook = get_snowflake_hook()
        logging.info("Starting data ingestion to Snowflake.")

        # Upload Parquet file to Snowflake stage
        put_command = (
            f"PUT file://{parquet_file_path} @BRONZE.PRODVENC/{nome_arquivo} AUTO_COMPRESS=TRUE"
        )
        execute_snowflake_query(snowflake_hook, put_command)
        logging.info(f"File {parquet_file_path} uploaded to stage as {nome_arquivo}.")

        # Insert record into CONTROLE_PRODVENC
        insert_query = """
            INSERT INTO BRONZE.CONTROLE_PRODVENC (
                NOME_ARQUIVO, DATA_UPLOAD, DATA_PROCESSAMENTO, FLAG_PROCESSADO, TOTAL_REGISTROS
            )
            VALUES (%s, %s, NULL, FALSE, %s)
        """
        execute_snowflake_query(
            snowflake_hook,
            insert_query,
            params=(nome_arquivo, data_extracao, total_registros)
        )
        logging.info("Record inserted into CONTROLE_PRODVENC.")

        # Remove temporary Parquet file
        os.remove(parquet_file_path)
        logging.info(f"Temporary Parquet file removed: {parquet_file_path}")

        return {'nome_arquivo': nome_arquivo}

    except Exception as e:
        logging.error(f"Error in ingest_to_snowflake: {e}")
        raise

@task
def update_processed_flags(data: dict):
    """
    Updates the CONTROLE_PRODVENC table in Snowflake after processing the data.
    """
    try:
        nome_arquivo = data.get('nome_arquivo')
        if not nome_arquivo:
            logging.info("No file to update in CONTROLE_PRODVENC.")
            return

        snowflake_hook = get_snowflake_hook()
        logging.info("Updating CONTROLE_PRODVENC in Snowflake.")

        update_query = """
            UPDATE BRONZE.CONTROLE_PRODVENC
            SET DATA_PROCESSAMENTO = %s,
                FLAG_PROCESSADO = TRUE
            WHERE NOME_ARQUIVO = %s
        """
        data_processamento = datetime.now(SAO_PAULO_TZ)
        execute_snowflake_query(
            snowflake_hook,
            update_query,
            params=(data_processamento, nome_arquivo)
        )
        logging.info(f"CONTROLE_PRODVENC updated for file: {nome_arquivo}")

    except Exception as e:
        logging.error(f"Error in update_processed_flags: {e}")
        raise

@task(trigger_rule='all_done')
def stop_warehouse():
    """
    Stops the Snowflake warehouse after processing is complete.
    """
    try:
        snowflake_hook = get_snowflake_hook()
        conn = BaseHook.get_connection('snowflake_default')
        warehouse_name = conn.extra_dejson.get('warehouse')

        if not warehouse_name:
            raise ValueError("Warehouse name not found in connection extras.")

        suspend_query = f"ALTER WAREHOUSE {warehouse_name} SUSPEND"
        execute_snowflake_query(snowflake_hook, suspend_query)
        logging.info(f"Warehouse suspended successfully.")

    except snowflake.connector.errors.ProgrammingError as e:
        error_message = str(e)
        if "cannot be suspended" in error_message.lower() or "invalid state" in error_message.lower():
            logging.warning(f"Warehouse cannot be suspended: {e}. It may already be suspended.")
            # Optionally, you can check the warehouse status
            try:
                status_query = f"SHOW WAREHOUSES LIKE '{warehouse_name}'"
                result = execute_snowflake_query(snowflake_hook, status_query, fetch_one=True)
                if result:
                    state = result[1]  # The state is usually in the second column
                    logging.info(f"Warehouse current state: {state}")
                else:
                    logging.warning(f"Warehouse not found.")
            except Exception as status_error:
                logging.error(f"Error checking warehouse status: {status_error}")
        else:
            logging.error(f"Error in stop_warehouse: ")
            raise AirflowFailException(f"Error in stop_warehouse: ")

    except Exception as e:
        logging.error(f"Error in stop_warehouse: {e}")
        raise AirflowFailException(f"Error in stop_warehouse: {e}")
