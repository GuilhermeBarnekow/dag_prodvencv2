from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException, AirflowFailException
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import os
import tempfile
import logging
import pytz
from utils import get_mongo_uri, get_snowflake_stage
import snowflake.connector

# Definir a timezone de São Paulo
sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

@task
def extract_process_data():
    try:
        # Inicializa o SnowflakeHook com TIMEZONE America/Sao_Paulo
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id='snowflake_default',
            session_parameters={'TIMEZONE': 'America/Sao_Paulo'}
        )
        logging.info("Conectando ao Snowflake para obter o último DATA_UPLOAD.")

        # Consulta para obter o último DATA_UPLOAD
        query = "SELECT MAX(DATA_UPLOAD) AS last_data_upload FROM CONTROLE_PRODVENC"


        with snowflake_hook.get_conn() as snowflake_conn:
            with snowflake_conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                last_data_upload = result[0] if result and result[0] else None

        logging.info(f"Último DATA_UPLOAD recuperado: {last_data_upload}")
        logging.info(f"Tipo de last_data_upload: {type(last_data_upload)}")

        # Conectar-se ao MongoDB
        mongo_uri = get_mongo_uri('mongo_default')
        logging.info(f"Conectando ao MongoDB usando a URI: {mongo_uri}")

        client = MongoClient(mongo_uri)
        db = client['prodvenc']  # Substitua pelo nome do seu banco de dados
        collection = db['prodvenc']  # Substitua pelo nome da sua coleção

        # Capturar o timestamp de extração
        data_extracao = datetime.now(sao_paulo_tz)
        logging.info(f"Data de extração: {data_extracao}")

        # Definir critério para identificar documentos não processados
        if last_data_upload:
            if isinstance(last_data_upload, str):
                last_data_upload = datetime.strptime(last_data_upload, '%Y-%m-%d %H:%M:%S').replace(tzinfo=sao_paulo_tz)
            elif last_data_upload.tzinfo is None:
                # Se não tiver informação de fuso horário, definir como America/Sao_Paulo
                last_data_upload = last_data_upload.replace(tzinfo=sao_paulo_tz)
            query_mongo = {'data_registro': {'$gt': last_data_upload}}
        else:
            query_mongo = {}
        logging.info(f"Consulta ao MongoDB: {query_mongo}")

        # Realizar a consulta e carregar em um DataFrame do Pandas
        documentos = list(collection.find(query_mongo))
        logging.info(f"Documentos encontrados: {len(documentos)}")

        if not documentos:
            logging.info("Nenhum novo documento para exportar. Interrompendo a DAG.")
            client.close()
            logging.info("Conexão com o MongoDB fechada.")
            raise AirflowSkipException("Nenhum novo documento para processar.")

        # Converter documentos para DataFrame do Pandas
        df = pd.DataFrame(documentos)
        logging.info("Dados carregados em DataFrame do Pandas.")

        # Converter '_id' para string
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)

        # Tratar colunas de data/hora
        datetime_cols = df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns
        for col in datetime_cols:
            if df[col].dt.tz is None:
                # Timestamps são tz-naive, vamos localizá-los
                df[col] = df[col].dt.tz_localize('America/Sao_Paulo')
                logging.info(f"Coluna '{col}' localizada para 'America/Sao_Paulo'.")
            else:
                # Timestamps já são tz-aware, converter para 'America/Sao_Paulo'
                df[col] = df[col].dt.tz_convert('America/Sao_Paulo')
                logging.info(f"Coluna '{col}' convertida para 'America/Sao_Paulo'.")

        # Obter o nome do arquivo Parquet
        nome_arquivo = f"mongodb_prodvenc_{data_extracao.strftime('%Y%m%d%H%M%S')}.parquet"
        parquet_file_path = os.path.join(tempfile.gettempdir(), nome_arquivo)

        # Salvar o DataFrame diretamente em Parquet
        df.to_parquet(parquet_file_path, index=False)
        logging.info(f"Dados exportados para arquivo Parquet: {parquet_file_path}")

        # Fechar a conexão com o MongoDB
        client.close()
        logging.info("Conexão com o MongoDB fechada.")

        # Retornar o caminho do Parquet, total de registros e data de extração
        return {
            'parquet_path': parquet_file_path,
            'nome_arquivo': nome_arquivo,
            'total_registros': len(df),
            'data_extracao': data_extracao
        }

    except Exception as e:
        logging.error(f"Erro na tarefa extract_process_data: {e}")
        raise

@task
def ingest_to_snowflake(processed_data: dict):
    """
    Ingera os dados do arquivo Parquet para o Snowflake e atualiza a tabela CONTROLE_PRODVENC.
    """
    try:
        parquet_file_path = processed_data['parquet_path']
        nome_arquivo = processed_data['nome_arquivo']
        total_registros = processed_data.get('total_registros', 0)
        data_extracao = processed_data.get('data_extracao')

        if parquet_file_path:
            # Conexão para ingestão com TIMEZONE America/Sao_Paulo
            snowflake_ingest_conn_id = 'snowflake_default1'
            snowflake_ingest_hook = SnowflakeHook(
                snowflake_conn_id=snowflake_ingest_conn_id,
                session_parameters={'TIMEZONE': 'America/Sao_Paulo'}
            )
            logging.info("Inicializando conexão com o Snowflake para ingestão.")

            # Obter o nome da stage a partir dos parâmetros extras da conexão
            stage_name = get_snowflake_stage(snowflake_ingest_conn_id)
            logging.info(f"Nome da stage obtido da conexão: {stage_name}")

            try:
                with snowflake_ingest_hook.get_conn() as snowflake_conn:
                    with snowflake_conn.cursor() as cursor:
                        # Executa o comando PUT para carregar o arquivo na stage
                        put_command = f"PUT file://{parquet_file_path} @{stage_name}/{nome_arquivo} AUTO_COMPRESS=TRUE"
                        logging.info(f"Executando comando PUT: {put_command}")
                        cursor.execute(put_command)
                        logging.info(f"Arquivo {parquet_file_path} carregado na stage {stage_name} como {nome_arquivo}.")

            except Exception as e:
                logging.error(f"Erro ao carregar o arquivo na stage do Snowflake: {e}")
                raise

            # Conexão para atualizar a tabela CONTROLE_PRODVENC
            snowflake_control_conn_id = 'snowflake_default'
            snowflake_control_hook = SnowflakeHook(
                snowflake_conn_id=snowflake_control_conn_id,
                session_parameters={'TIMEZONE': 'America/Sao_Paulo'}
            )
            logging.info("Inicializando conexão com o Snowflake para atualizar a tabela CONTROLE_PRODVENC.")

            # Utilizar data_extracao como data_upload
            data_upload = data_extracao
            logging.info(f"Data upload (data_extracao): {data_upload}")

            try:
                with snowflake_control_hook.get_conn() as snowflake_conn:
                    with snowflake_conn.cursor() as cursor:
                        # Inserir registro na tabela CONTROLE_PRODVENC
                        insert_query = """
                        INSERT INTO CONTROLE_PRODVENC (NOME_ARQUIVO, DATA_UPLOAD, DATA_PROCESSAMENTO, FLAG_PROCESSADO, TOTAL_REGISTROS)
                        VALUES (%s, %s, NULL, FALSE, %s)
                        """
                        cursor.execute(insert_query, (nome_arquivo, data_upload, total_registros))
                        logging.info("Registro inserido na tabela CONTROLE_PRODVENC com data de upload da extração.")

            except Exception as e:
                logging.error(f"Erro ao inserir registro na tabela CONTROLE_PRODVENC: {e}")
                raise

            finally:
                # Remove o arquivo Parquet temporário
                try:
                    os.remove(parquet_file_path)
                    logging.info(f"Arquivo temporário Parquet removido: {parquet_file_path}")
                except Exception as e:
                    logging.error(f"Erro ao remover o arquivo temporário Parquet: {e}")
                    raise

            # Retornar o nome do arquivo para a próxima tarefa
            return {'nome_arquivo': nome_arquivo}

        else:
            logging.info("Nenhum arquivo Parquet para ingerir no Snowflake.")
            return {'nome_arquivo': None}

    except Exception as e:
        logging.error(f"Erro na tarefa ingest_to_snowflake: {e}")
        raise

@task
def update_processed_flags(data: dict):
    """
    Atualiza a tabela CONTROLE_PRODVENC no Snowflake após o processamento.
    """
    try:
        nome_arquivo = data.get('nome_arquivo')

        if nome_arquivo:
            # Inicializa o SnowflakeHook com TIMEZONE America/Sao_Paulo
            snowflake_hook = SnowflakeHook(
                snowflake_conn_id='snowflake_default',
                session_parameters={'TIMEZONE': 'America/Sao_Paulo'}
            )
            logging.info("Conectando ao Snowflake para atualizar a tabela CONTROLE_PRODVENC.")

            data_processamento = datetime.now(sao_paulo_tz)
            logging.info(f"Data processamento type: {type(data_processamento)}, value: {data_processamento}")

            try:
                with snowflake_hook.get_conn() as snowflake_conn:
                    with snowflake_conn.cursor() as cursor:
                        update_query = """
                        UPDATE CONTROLE_PRODVENC
                        SET DATA_PROCESSAMENTO = %s,
                            FLAG_PROCESSADO = TRUE
                        WHERE NOME_ARQUIVO = %s
                        """
                        cursor.execute(update_query, (data_processamento, nome_arquivo))
                        logging.info(f"Tabela CONTROLE_PRODVENC atualizada para o arquivo: {nome_arquivo}")
            except Exception as e:
                logging.error(f"Erro ao atualizar a tabela CONTROLE_PRODVENC: {e}")
                raise

        else:
            logging.info("Nenhum arquivo para atualizar na tabela CONTROLE_PRODVENC.")

    except Exception as e:
        logging.error(f"Erro na tarefa update_processed_flags: {e}")
        raise

@task(trigger_rule='all_done')
def stop_warehouse():
    try:
        # Nome da conexão Snowflake
        snowflake_conn_id = 'snowflake_default'  # Substitua se necessário

        # Inicializa o SnowflakeHook
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=snowflake_conn_id
        )
        logging.info("Conectando ao Snowflake para parar o warehouse.")

        # Obter o nome do warehouse da conexão
        conn = BaseHook.get_connection(snowflake_conn_id)
        warehouse_name = conn.extra_dejson.get('warehouse')

        if not warehouse_name:
            raise ValueError(f"Nome do warehouse não encontrado nos parâmetros extras da conexão '{snowflake_conn_id}'. Por favor, adicione 'warehouse' aos parâmetros extras.")

        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Comando para suspender o warehouse
                suspend_query = f"ALTER WAREHOUSE {warehouse_name} SUSPEND;"
                cursor.execute(suspend_query)
                logging.info(f"Warehouse {warehouse_name} suspenso com sucesso.")

    except snowflake.connector.errors.ProgrammingError as e:
        if "cannot be suspended" in str(e).lower():
            logging.warning(f"Não foi possível suspender o warehouse {warehouse_name}: {e}. Verificando se o warehouse já está suspenso.")
            # Opcional: Verificar o estado atual do warehouse
            try:
                query = f"SHOW WAREHOUSES LIKE '{warehouse_name}';"
                with snowflake_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        result = cursor.fetchone()
                        if result:
                            state = result[1].upper()  # Estado do warehouse
                            logging.info(f"Estado atual do warehouse {warehouse_name}: {state}")
                        else:
                            logging.warning(f"Warehouse {warehouse_name} não encontrado.")
            except Exception as inner_e:
                logging.error(f"Erro ao verificar o estado do warehouse {warehouse_name}: {inner_e}")
        else:
            logging.error(f"Erro ao suspender o warehouse {warehouse_name}: {e}")
            raise AirflowFailException(f"Erro ao suspender o warehouse {warehouse_name}: {e}")

    except Exception as e:
        logging.error(f"Tipo da exceção: {type(e)}")
        logging.error(f"Erro ao suspender o warehouse {warehouse_name}: {e}")
        raise AirflowFailException(f"Erro ao suspender o warehouse {warehouse_name}: {e}")
