import logging
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

def getMongoCredentials():
    try:
        variavel_uri = Variable.get("mongo_credentials", deserialize_json=True)
        return variavel_uri
    except Exception as e:
        logging.error(f"Erro ao obter credenciais do MongoDB: {e}")
        raise AirflowFailException(f"Erro ao obter credenciais do MongoDB: {e}")


def parse_iso_date(date_str):
    try:
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        return datetime.fromisoformat(date_str)
    except ValueError as e:
        logging.error(f"Formato de data inválido: {e}")
        raise AirflowFailException(f"Formato de data inválido: {e}")
