import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import configparser

# Load config
config = configparser.ConfigParser()
config.read("config/config.ini")

def get_sqlserver_engine():
    driver = config.get("SQL_SERVER", "driver")
    server = config.get("SQL_SERVER", "server")
    database = config.get("SQL_SERVER", "database")
    username = config.get("SQL_SERVER", "username")
    password = config.get("SQL_SERVER", "password")

    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={quote_plus(driver)}"
    )
    return create_engine(conn_str)

def get_mysql_engine():
    username = config.get("MYSQL", "username")
    password = config.get("MYSQL", "password")
    host = config.get("MYSQL", "host")
    port = config.get("MYSQL", "port")
    database = config.get("MYSQL", "database")

    conn_str = (
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    )
    return create_engine(conn_str)
