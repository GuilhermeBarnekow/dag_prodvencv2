import json
import logging
from airflow.models import Variable

def getMongoDbCredentials():
    try:
        variables_str = Variable.get("mongo_credentials")
        variables = json.loads(variables_str)
        MongoDB = variables['MongoDB_credentials_prodvenc']
        logging.info(f"MongoDB credentials retrieved:")
        return MongoDB
    except json.JSONDecodeError as e:
        raise
    except KeyError as e:
        raise
