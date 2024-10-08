
from airflow.models import Variable
import json
import logging

def getMongoCredentials():
    variavel_uri = Variable.get("mongo_credentials", deserialize_json=True)
    return variavel_uri
