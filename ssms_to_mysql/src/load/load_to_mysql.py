import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    return config

def get_sql_server_connection(sql_conf):
    conn_str = (
        f"Driver={{{sql_conf['driver']}}};"
        f"Server={sql_conf['server']};"
        f"Database={sql_conf['database']};"
        f"UID={sql_conf['username']};"
        f"PWD={sql_conf['password']};"
    )
    return pyodbc.connect(conn_str)

def get_mysql_engine(mysql_conf):
    conn_str = (
        f"mysql+pymysql://{mysql_conf['username']}:{mysql_conf['password']}"
        f"@{mysql_conf['host']}:{mysql_conf['port']}/{mysql_conf['database']}"
    )
    return create_engine(conn_str)

def main():
    print("🔧 Loading configuration...")
    config = load_config()
    sql_conf = config["SQL_SERVER"]
    mysql_conf = config["MYSQL"]

    # Step 1: Extract data from SQL Server
    print("📥 Connecting to SQL Server...")
    sql_conn = get_sql_server_connection(sql_conf)
    query = "SELECT * FROM passenger_master"
    df = pd.read_sql(query, sql_conn)
    sql_conn.close()
    print(f"✅ Retrieved {len(df)} rows from SQL Server.")

    # Add any date columns / flags for SCD tables
    now = datetime.now()

    # Prepare dataframes for each SCD table:

    # SCD1: passenger_master (just direct copy)
    df_scd1 = df.copy()

    # SCD2: passenger_history
    # For initial load, every record is current with start_date=now, end_date=NULL, is_current=1
    df_scd2 = df.copy()
    df_scd2["start_date"] = now
    df_scd2["end_date"] = None
    df_scd2["is_current"] = True

    # SCD3: passenger_type3
    # previous_country is NULL, updated_at = now
    df_scd3 = df.copy()
    df_scd3["previous_country"] = None
    df_scd3["updated_at"] = now

    # SCD4: passenger_history_type4
    # create new ID for history, and changed_at as now
    # For initial load, treat each row as a change event
    df_scd4 = df.copy()
    df_scd4["changed_at"] = now
    # history_id is auto_increment, so no need to add in df

    # SCD6: passenger_scd6
    # hybrid approach: previous_country NULL, start_date now, end_date NULL, is_current True
    df_scd6 = df.copy()
    df_scd6["previous_country"] = None
    df_scd6["start_date"] = now
    df_scd6["end_date"] = None
    df_scd6["is_current"] = True

    # Step 2: Connect to MySQL
    print("📤 Connecting to MySQL...")
    mysql_engine = get_mysql_engine(mysql_conf)

    try:
        print("🚀 Loading data into MySQL tables...")

        # Load SCD1 table (overwrite)
        df_scd1.to_sql(name="passenger_master", con=mysql_engine, if_exists="replace", index=False)
        print("✅ Loaded data into passenger_master (SCD1)")

        # Load SCD2 table (overwrite)
        df_scd2.to_sql(name="passenger_history", con=mysql_engine, if_exists="replace", index=False)
        print("✅ Loaded data into passenger_history (SCD2)")

        # Load SCD3 table (overwrite)
        df_scd3.to_sql(name="passenger_type3", con=mysql_engine, if_exists="replace", index=False)
        print("✅ Loaded data into passenger_type3 (SCD3)")

        # Load SCD4 table (overwrite)
        df_scd4.to_sql(name="passenger_history_type4", con=mysql_engine, if_exists="replace", index=False)
        print("✅ Loaded data into passenger_history_type4 (SCD4)")

        # Load SCD6 table (overwrite)
        df_scd6.to_sql(name="passenger_scd6", con=mysql_engine, if_exists="replace", index=False)
        print("✅ Loaded data into passenger_scd6 (SCD6)")

        print("🎉 All tables loaded successfully.")

    except Exception as e:
        print(f"❌ Failed to load data into MySQL: {e}")

if __name__ == "__main__":
    main()
