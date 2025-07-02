import yaml
from pymongo import MongoClient
import mysql.connector
import pyodbc

# Load config once
with open("config/db_config.yaml") as f:
    config = yaml.safe_load(f)

# MongoDB connection
def get_mongo_collection():
    mongo = config["mongodb"]
    client = MongoClient(mongo["uri"])
    return client[mongo["database"]][mongo["collection"]]

# MySQL connection
def get_mysql_connection():
    mysql_conf = config["mysql"]  
    return mysql.connector.connect(
        host=mysql_conf["host"],
        user=mysql_conf["user"],
        password=mysql_conf["password"],
        database=mysql_conf["database"]
    )

def get_sqlserver_connection():
    sql = config["sqlserver"]
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={sql['server']};"
        f"DATABASE={sql['database']};"
        f"UID={sql['user']};"
        f"PWD={sql['password']}"
    )


