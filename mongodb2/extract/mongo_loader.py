from config import get_mongo_collection

def load_projects_from_mongo():
    collection = get_mongo_collection()
    return list(collection.find({}))
