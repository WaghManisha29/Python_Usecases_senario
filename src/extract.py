from pymongo import MongoClient
import pandas as pd

def extract_projects(config):
    client = MongoClient(config["uri"])
    collection = client[config["database"]][config["collection"]]
    documents = list(collection.find())
    return pd.DataFrame(documents)
