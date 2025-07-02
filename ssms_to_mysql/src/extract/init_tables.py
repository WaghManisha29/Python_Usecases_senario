import pyodbc
from src.utils.config_reader import get_config

def run():
    config = get_config()
    sql_conf = config["SQL_SERVER"]

    # Construct connection string
    conn_str = (
        f"Driver={{{sql_conf['driver']}}};"
        f"Server={sql_conf['server']};"
        f"Database={sql_conf['database']};"
        f"UID={sql_conf['username']};"
        f"PWD={sql_conf['password']};"
    )

    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Read full SQL script
    sql_file_path = "data_tables/ssms_tables.sql"  # ✅ Ensure this file exists
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_script = file.read()

    # Execute full SQL script as one batch
    cursor.execute(sql_script)

    conn.commit()
    conn.close()

    print(f"✅ SQL Server initialized with script from {sql_file_path}")

if __name__ == "__main__":
    run()
