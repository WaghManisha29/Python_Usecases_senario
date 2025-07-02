import sys
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import text

# Ensure 'src' folder is on path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.db_connections import get_sqlserver_engine, get_mysql_engine

def scd_type4_sync(table_name="passenger_master", history_table="passenger_history_type4", primary_key="passenger_id"):
    print(f"\n🔄 Starting Interactive SCD Type 4 Sync for table: {table_name}")

    # Engines
    sql_engine = get_sqlserver_engine()
    mysql_engine = get_mysql_engine()

    try:
        # Extract source data from SQL Server
        source_df = pd.read_sql(f"SELECT * FROM {table_name}", sql_engine)

        # Extract historical data from MySQL
        history_df = pd.read_sql(f"SELECT * FROM {history_table}", mysql_engine)

        print(f"📦 Retrieved {len(source_df)} records from source.")
        print(f"📚 Retrieved {len(history_df)} records from history table.")

        changed_rows = []
        for _, row in source_df.iterrows():
            existing = history_df[history_df[primary_key] == row[primary_key]]
            if not existing.empty:
                latest = existing.sort_values(by="changed_at", ascending=False).iloc[0]
                if (
                    row["name"] != latest["name"]
                    or row["gender"] != latest["gender"]
                    or row["country"] != latest["country"]
                ):
                    changed_rows.append(row)
            else:
                # New insert
                changed_rows.append(row)

        if not changed_rows:
            print("✅ No changes found. History table is already up-to-date.")
            return

        # Insert into history
        with mysql_engine.begin() as conn:
            insert_sql = text(f"""
                INSERT INTO {history_table} (
                    {primary_key}, name, gender, country, changed_at
                ) VALUES (
                    :passenger_id, :name, :gender, :country, :changed_at
                )
            """)
            for row in changed_rows:
                conn.execute(insert_sql, {
                    "passenger_id": row[primary_key],
                    "name": row["name"],
                    "gender": row["gender"],
                    "country": row["country"],
                    "changed_at": datetime.now()
                })

        print(f"\n✅ {len(changed_rows)} change(s) inserted into `{history_table}`.")

    except Exception as e:
        print(f"❌ Error during SCD Type 4 sync: {e}")

def run():
    scd_type4_sync()

if __name__ == "__main__":
    run()