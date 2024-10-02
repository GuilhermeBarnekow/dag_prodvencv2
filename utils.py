from airflow.hooks.base import BaseHook
from pymongo import MongoClient
import pytz

# Definir a timezone de São Paulo
sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

def get_mongo_uri(conn_id):
    conn = BaseHook.get_connection(conn_id)
    login = conn.login
    password = conn.password
    hosts = [f"{h}:{conn.port}" if conn.port else h for h in conn.host.split(',')]
    host_str = ','.join(hosts)
    credentials = f"{login}:{password}@" if login and password else ""
    database = f"/{conn.schema}" if conn.schema else "/"
    params = f"?{'&'.join(f'{k}={v}' for k, v in conn.extra_dejson.items())}" if conn.extra_dejson else ""
    uri = f"mongodb://{credentials}{host_str}{database}{params}"
    return uri

def get_snowflake_stage(conn_id):
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    stage = extra.get('stage')
    if not stage:
        raise ValueError(f"Stage não encontrada nos parâmetros extras da conexão '{conn_id}'. Por favor, adicione 'stage' aos parâmetros extras.")
    return stage
