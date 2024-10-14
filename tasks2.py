# tasks.py

import logging
import os
import tempfile
from datetime import datetime

import pandas as pd
import pytz
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pymongo import MongoClient

from utils import getMongoCredentials, parse_iso_date

# Configurações globais
SAO_PAULO_TZ = pytz.timezone('America/Sao_Paulo')
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_TABLE_CONTROLE = 'BRONZE.CONTROLE_PRODVENC'
SNOWFLAKE_STAGE = '@STAGE.PRODVENC'
MONGO_COLLECTION_NAME = 'prodvenc'


def execute_snowflake_query(hook, query, params=None, fetch_one=False):
    """Executa uma consulta no Snowflake usando o SnowflakeHook."""
    try:
        result = hook.run(
            query,
            parameters=params,
            handler=lambda cur: cur.fetchone() if fetch_one else cur.fetchall()
        )
        return result
    except Exception as e:
        logging.error(f"Erro na consulta Snowflake: {e}")
        raise AirflowFailException(f"Erro na consulta Snowflake: {e}")


def get_last_data_upload(hook):
    """Obtém a última DATA_UPLOAD da tabela de controle no Snowflake."""
    query = f"SELECT MAX(DATA_UPLOAD) AS last_upload FROM {SNOWFLAKE_TABLE_CONTROLE}"
    result = execute_snowflake_query(hook, query, fetch_one=True)
    last_upload = result[0] if result and result[0] else None
    if last_upload:
        last_upload = last_upload.astimezone(SAO_PAULO_TZ) if last_upload.tzinfo else SAO_PAULO_TZ.localize(last_upload)
        logging.info(f"Último DATA_UPLOAD: {last_upload}")
    else:
        logging.info("Nenhuma DATA_UPLOAD encontrada na tabela de controle.")
    return last_upload


def get_mongo_documents(last_upload):
    """Conecta ao MongoDB e retorna os documentos com base na última DATA_UPLOAD."""
    creds = getMongoCredentials()
    
    if isinstance(last_upload, str):
        try:
            last_upload_dt = parse_iso_date(last_upload)
        except AirflowFailException:
            raise AirflowFailException("Erro ao parsear a data de upload.")
    elif isinstance(last_upload, datetime):
        last_upload_dt = last_upload.astimezone(SAO_PAULO_TZ) if last_upload.tzinfo else SAO_PAULO_TZ.localize(last_upload)
    else:
        last_upload_dt = None

    query = {'metadata.dataalteracao': {'$gt': last_upload_dt}} if last_upload_dt else {}

    with MongoClient(creds['mongo_uri']) as client:
        collection = client[creds['mongo_db']][creds.get('mongo_collection', MONGO_COLLECTION_NAME)]
        documents = list(collection.find(query)) if last_upload_dt else list(collection.find())

    if documents:
        invalid_ids = [doc['_id'] for doc in documents if not isinstance(doc.get('data_registro'), datetime)]
        if invalid_ids:
            logging.warning(f"Documentos com _id {invalid_ids} têm 'data_registro' inválido.")
        logging.info(f"{len(documents)} documentos encontrados no MongoDB.")
    else:
        logging.info("Nenhum documento encontrado no MongoDB para o critério especificado.")

    return documents


def process_documents(documents):
    """Processa os documentos do MongoDB em um DataFrame."""
    df = pd.DataFrame(documents)
    df['_id'] = df['_id'].astype(str)
    if 'metadata' in df.columns:
        df['metadata'] = df['metadata'].astype(str)
    datetime_cols = df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns
    for col in datetime_cols:
        df[col] = df[col].dt.tz_convert(SAO_PAULO_TZ) if df[col].dt.tz else df[col].dt.tz_localize(SAO_PAULO_TZ)
    return df


def save_dataframe_to_parquet(df, extraction_date):
    """Salva o DataFrame em um arquivo Parquet temporário."""
    filename = f"mongodb_prodvenc_{extraction_date.strftime('%Y%m%d%H%M%S')}.parquet"
    parquet_path = os.path.join(tempfile.gettempdir(), filename)
    df.to_parquet(parquet_path, index=False)
    logging.info(f"Dados salvos em Parquet: {filename}")
    return parquet_path, filename


@task
def extract_and_ingest():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, session_parameters={'TIMEZONE': 'America/Sao_Paulo'})
    last_upload = get_last_data_upload(hook)
    extraction_date = datetime.now(SAO_PAULO_TZ)
    documents = get_mongo_documents(last_upload)
    
    if not documents:
        raise AirflowSkipException("Nenhum novo documento para processar.")
    
    df = process_documents(documents)
    parquet_path, filename = save_dataframe_to_parquet(df, extraction_date)
    
    # Ingerir no Snowflake
    execute_snowflake_query(hook, f"PUT file://{parquet_path} {SNOWFLAKE_STAGE} AUTO_COMPRESS=TRUE")
    
    insert_query = f"""
        INSERT INTO {SNOWFLAKE_TABLE_CONTROLE} (
            NOME_ARQUIVO, DATA_UPLOAD, FLAG_PROCESSADO, TOTAL_REGISTROS
        )
        VALUES (%(filename)s, %(extraction_date)s, FALSE, %(total_records)s)
    """
    params = {
        'filename': filename,
        'extraction_date': extraction_date,
        'total_records': len(df)
    }
    execute_snowflake_query(hook, insert_query, params=params)
    logging.info(f"Registro inserido na tabela de controle para o arquivo: {filename}")
    
    # Remover arquivo Parquet temporário
    try:
        os.remove(parquet_path)
        logging.info(f"Arquivo Parquet removido: {parquet_path}")
    except OSError as e:
        logging.warning(f"Não foi possível remover o arquivo Parquet: {parquet_path}. Erro: {e}")
    
    return filename


@task
def update_processed_flags(filename):
    if not filename:
        return
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, session_parameters={'TIMEZONE': 'America/Sao_Paulo'})
    update_query = f"""
        UPDATE {SNOWFLAKE_TABLE_CONTROLE}
        SET DATA_PROCESSAMENTO = CURRENT_TIMESTAMP(),
            FLAG_PROCESSADO = TRUE
        WHERE NOME_ARQUIVO = %(filename)s
    """
    execute_snowflake_query(hook, update_query, params={'filename': filename})
    logging.info(f"Flag de processamento atualizada para o arquivo: {filename}")


@task(trigger_rule='all_done')
def stop_warehouse():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, session_parameters={'TIMEZONE': 'America/Sao_Paulo'})
    connection = hook.get_connection(SNOWFLAKE_CONN_ID)
    warehouse = connection.extra_dejson.get('warehouse')
    if not warehouse:
        logging.error("Nome do warehouse não encontrado nas configurações da conexão.")
        return
    try:
        execute_snowflake_query(hook, f"ALTER WAREHOUSE {warehouse} SUSPEND")
        logging.info(f"Warehouse {warehouse} suspenso com sucesso.")
    except Exception as e:
        logging.warning(f"Erro ao suspender o warehouse: {e}")
