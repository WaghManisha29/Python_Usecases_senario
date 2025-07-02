import yaml
from pymongo import MongoClient

def load_config(path='config/db_config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def connect_to_mongodb():
    config = load_config()
    uri = config['mongodb']['uri']
    db_name = config['mongodb']['database']
    collection_name = config['mongodb']['collection']

    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]

def fetch_documents(collection, query={}):
    return list(collection.find(query))
