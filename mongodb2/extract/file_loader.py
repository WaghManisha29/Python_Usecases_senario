import json
from config import get_mongo_collection

def insert_projects_from_file(file_path="data/project.txt"):
    collection = get_mongo_collection()
    with open(file_path, "r") as f:
        data = json.load(f)
    collection.delete_many({})
    collection.insert_many(data if isinstance(data, list) else [data])
    print(f" Inserted {len(data)} document(s) into MongoDB.")
