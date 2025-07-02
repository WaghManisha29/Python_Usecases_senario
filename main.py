import json
from src.extract import extract_projects
from src.transform import transform
from src.load import load_to_dynamodb

with open("config/config.json") as f:
    config = json.load(f)

df = extract_projects(config["mongodb"])
df = transform(df)
load_to_dynamodb(df, config["aws"])

print(" Project data loaded from MongoDB to DynamoDB.")
