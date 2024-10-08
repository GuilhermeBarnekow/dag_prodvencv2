import logging
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import pytz
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pymongo import MongoClient

from utils import getMongoCredentials

# Configurações globais
SAO_PAULO_TZ = pytz.timezone('America/Sao_Paulo')
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_TABLE_CONTROLE = 'BRONZE.CONTROLE_PRODVENC'
SNOWFLAKE_STAGE = '@STAGE.PRODVENC'
MONGO_COLLECTION_NAME = 'prodvenc'

def get_snowflake_hook(conn_id: str = SNOWFLAKE_CONN_ID) -> SnowflakeHook:
    """
    Inicializa um SnowflakeHook com o fuso horário especificado.

    :param conn_id: ID da conexão do Snowflake no Airflow.
    :return: Instância de SnowflakeHook.
    """
    return SnowflakeHook(
        snowflake_conn_id=conn_id,
        session_parameters={'TIMEZONE': 'America/Sao_Paulo'}
    )

def execute_snowflake_query(
    hook: SnowflakeHook,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    fetch_one: bool = False
) -> Any:
    """
    Executa uma consulta no Snowflake usando o SnowflakeHook.
    """
    try:
        logging.debug(f"Executando consulta SQL: {query} com parâmetros: {params}")
        return hook.run(
            query,
            parameters=params,
            handler=lambda cur: cur.fetchone() if fetch_one else cur.fetchall()
        )
    except Exception as e:
        logging.error(f"Erro ao executar a consulta no Snowflake: {e}")
        raise AirflowFailException(f"Erro na consulta Snowflake: {e}")


def get_last_data_upload(snowflake_hook: SnowflakeHook) -> Optional[datetime]:
    """
    Obtém a última DATA_UPLOAD registrada na tabela de controle no Snowflake.

    :param snowflake_hook: Instância de SnowflakeHook.
    :return: Última data de upload ou None se não houver registros.
    """
    logging.info("Buscando o último DATA_UPLOAD no Snowflake.")
    result = execute_snowflake_query(
        snowflake_hook,
        f"SELECT MAX(DATA_UPLOAD) FROM {SNOWFLAKE_TABLE_CONTROLE}",
        fetch_one=True
    )
    last_data_upload = result[0] if result and result[0] else None
    if last_data_upload:
        last_data_upload = last_data_upload.replace(tzinfo=SAO_PAULO_TZ)
    logging.info(f"Último DATA_UPLOAD obtido: {last_data_upload}")
    return last_data_upload

def prepare_mongo_query(last_data_upload: Optional[datetime]) -> Dict[str, Any]:
    """
    Prepara a consulta para o MongoDB baseada na última DATA_UPLOAD.

    :param last_data_upload: Última data de upload ou None.
    :return: Dicionário representando a consulta MongoDB.
    """
    if last_data_upload:
        query = {'data_registro': {'$gt': last_data_upload}}
    else:
        query = {}
    logging.debug(f"Consulta MongoDB: {query}")
    return query

def process_dataframe(documents: list) -> pd.DataFrame:
    """
    Processa a lista de documentos MongoDB em um DataFrame do Pandas.

    :param documents: Lista de documentos do MongoDB.
    :return: DataFrame processado.
    """
    df = pd.DataFrame(documents)
    df['_id'] = df['_id'].astype(str)
    datetime_cols = df.select_dtypes(include='datetime').columns
    for col in datetime_cols:
        df[col] = df[col].dt.tz_convert(SAO_PAULO_TZ) if df[col].dt.tz else df[col].dt.tz_localize(SAO_PAULO_TZ)
    return df

def save_to_parquet(df: pd.DataFrame, extraction_date: datetime) -> Tuple[str, str]:
    """
    Salva o DataFrame em um arquivo Parquet temporário.

    :param df: DataFrame a ser salvo.
    :param extraction_date: Data e hora da extração.
    :return: Tupla contendo o caminho completo do arquivo Parquet e o nome do arquivo.
    """
    filename = f"mongodb_prodvenc_{extraction_date.strftime('%Y%m%d%H%M%S')}.parquet"
    parquet_file_path = os.path.join(tempfile.gettempdir(), filename)
    df.to_parquet(parquet_file_path, index=False)
    logging.info(f"Dados exportados para Parquet: {parquet_file_path}")
    return parquet_file_path, filename

@task
def extract_process_data() -> Dict[str, Any]:
    """
    Extrai dados do MongoDB com base na última DATA_UPLOAD registrada no Snowflake,
    processa os dados e os salva em um arquivo Parquet.

    :return: Dicionário com informações sobre o processamento.
    """
    try:
        snowflake_hook = get_snowflake_hook()
        last_data_upload = get_last_data_upload(snowflake_hook)

        mongo_credentials = getMongoCredentials()
        mongo_uri = mongo_credentials['mongo_uri']
        mongo_db = mongo_credentials['mongo_db']
        mongo_collection = mongo_credentials['mongo_collection']

        extraction_date = datetime.now(SAO_PAULO_TZ)
        mongo_query = prepare_mongo_query(last_data_upload)

        logging.info("Conectando ao MongoDB.")
        with MongoClient(mongo_uri) as client:
            collection = client[mongo_db][mongo_collection]
            documents = list(collection.find(mongo_query))
            logging.info(f"{len(documents)} documentos encontrados no MongoDB.")

        if not documents:
            logging.info("Nenhum novo documento para processar. DAG será pulado.")
            raise AirflowSkipException("Nenhum novo documento para processar.")

        data_frame = process_dataframe(documents)
        parquet_file_path, filename = save_to_parquet(data_frame, extraction_date)

        return {
            'parquet_path': parquet_file_path,
            'filename': filename,
            'total_records': len(data_frame),
            'extraction_date': extraction_date.strftime('%Y-%m-%d %H:%M:%S')
        }

    except AirflowSkipException:
        # Repassa a exceção para que o Airflow possa lidar com o skip corretamente
        raise
    except Exception as e:
        logging.exception(f"Erro na tarefa extract_process_data: {e}")
        raise AirflowFailException(f"Erro na extração e processamento de dados: {e}")

@task
def ingest_to_snowflake(processed_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Ingere dados do arquivo Parquet para o Snowflake e atualiza a tabela de controle.

    :param processed_data: Dicionário com informações sobre o processamento.
    :return: Dicionário com o nome do arquivo processado.
    """
    try:
        parquet_file_path = processed_data.get('parquet_path')
        filename = processed_data.get('filename')
        total_records = processed_data.get('total_records', 0)
        extraction_date = processed_data.get('extraction_date')

        if not parquet_file_path:
            logging.warning("Nenhum arquivo Parquet para ingerir.")
            return {'filename': None}

        snowflake_hook = get_snowflake_hook()
        logging.info("Iniciando a ingestão de dados para o Snowflake.")

        put_command = f"PUT file://{parquet_file_path} {SNOWFLAKE_STAGE} AUTO_COMPRESS=TRUE"
        execute_snowflake_query(snowflake_hook, put_command)
        logging.info(f"Arquivo {filename} carregado para o stage {SNOWFLAKE_STAGE}.")

        insert_query = f"""
            INSERT INTO {SNOWFLAKE_TABLE_CONTROLE} (
                NOME_ARQUIVO, DATA_UPLOAD, FLAG_PROCESSADO, TOTAL_REGISTROS
            )
            VALUES (%(filename)s, %(extraction_date)s, FALSE, %(total_records)s)
        """
        params = {
            'filename': filename,
            'extraction_date': extraction_date,
            'total_records': total_records
        }
        execute_snowflake_query(snowflake_hook, insert_query, params=params)
        logging.info("Registro inserido na tabela de controle.")

        if os.path.exists(parquet_file_path):
            os.remove(parquet_file_path)
            logging.debug(f"Arquivo Parquet temporário removido: {parquet_file_path}")
        else:
            logging.warning(f"Arquivo Parquet não encontrado: {parquet_file_path}")

        return {'filename': filename}

    except Exception as e:
        logging.exception(f"Erro na tarefa ingest_to_snowflake: {e}")
        raise AirflowFailException(f"Erro na ingestão para o Snowflake: {e}")

@task
def update_processed_flags(data: Dict[str, Optional[str]]) -> None:
    """
    Atualiza a tabela de controle no Snowflake após o processamento dos dados.

    :param data: Dicionário com o nome do arquivo processado.
    """
    try:
        filename = data.get('filename')
        if not filename:
            logging.info("Nenhum arquivo para atualizar na tabela de controle.")
            return

        snowflake_hook = get_snowflake_hook()
        logging.info("Atualizando a tabela de controle no Snowflake.")

        update_query = f"""
            UPDATE {SNOWFLAKE_TABLE_CONTROLE}
            SET DATA_PROCESSAMENTO = CURRENT_TIMESTAMP(),
                FLAG_PROCESSADO = TRUE
            WHERE NOME_ARQUIVO = %(filename)s
        """
        params = {'filename': filename}
        execute_snowflake_query(snowflake_hook, update_query, params=params)
        logging.info(f"Tabela de controle atualizada para o arquivo: {filename}")

    except Exception as e:
        logging.exception(f"Erro na tarefa update_processed_flags: {e}")
        raise AirflowFailException(f"Erro na atualização da tabela de controle: {e}")

@task(trigger_rule='all_done')
def stop_warehouse() -> None:
    """
    Suspende o warehouse do Snowflake após a conclusão do processamento.

    """
    try:
        snowflake_hook = get_snowflake_hook()
        warehouse_name = snowflake_hook.get_connection(SNOWFLAKE_CONN_ID).extra_dejson.get('warehouse')
        if not warehouse_name:
            raise ValueError("Nome do warehouse não encontrado nas configurações da conexão.")

        suspend_query = f"ALTER WAREHOUSE {warehouse_name} SUSPEND"
        execute_snowflake_query(snowflake_hook, suspend_query)
        logging.info(f"Warehouse {warehouse_name} suspenso com sucesso.")

    except Exception as e:
        logging.warning(f"Erro ao suspender o warehouse: {e}")
