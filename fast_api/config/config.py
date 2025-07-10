import os
from dotenv import load_dotenv
import urllib

load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER = os.getenv("DB_DRIVER")

# Encode the driver for use in a connection string
params = urllib.parse.quote_plus(
    f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}"
)

DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={params}"
