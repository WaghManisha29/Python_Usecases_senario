import sys, os
from datetime import datetime
import pandas as pd
from sqlalchemy import text

# Add the root path to allow importing modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.db_connections import get_sqlserver_engine, get_mysql_engine

def interactive_scd6_etl():
    print("\n🔄 Running Interactive SCD Type 6 Sync...")

    sql_engine = get_sqlserver_engine()
    mysql_engine = get_mysql_engine()

    table_name = "passenger_master"
    warehouse_table = "passenger_scd6"
    primary_key = "passenger_id"

    pk = input(f"\n🔍 Enter {primary_key} to update: ").strip()

    try:
        # Fetch source record from SQL Server
        source_df = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE {primary_key} = ?", 
            sql_engine, params=(pk,)
        )
        if source_df.empty:
            print("❌ No such record in source.")
            return
        row = source_df.iloc[0]

        print("\n📋 Current Source Record:")
        print(source_df)

        updates = {}
        for col in source_df.columns:
            if col == primary_key:
                continue
            new_val = input(f"✏️ New value for '{col}' (blank = keep current): ").strip()
            if new_val:
                updates[col] = new_val

        if not updates:
            print("ℹ️ No updates provided.")
            return

        now = datetime.now()

        # Fetch current version from MySQL warehouse table
        current_df = pd.read_sql(
            f"SELECT * FROM {warehouse_table} WHERE {primary_key} = %s AND is_current = 1", 
            mysql_engine, params=(pk,)
        )

        if not current_df.empty:
            current = current_df.iloc[0]

            # Mark current record as inactive
            with mysql_engine.begin() as conn:
                conn.execute(
                    text(f"""
                        UPDATE {warehouse_table} 
                        SET is_current = 0, end_date = :end 
                        WHERE {primary_key} = :pid AND is_current = 1
                    """),
                    {"end": now, "pid": pk}
                )

            # Insert new version with changes and track previous value
            insert_values = {
                "passenger_id": pk,
                "name": updates.get("name", current["name"]),
                "gender": updates.get("gender", current["gender"]),
                "country": updates.get("country", current["country"]),
                "previous_country": current["country"] if "country" in updates else current["previous_country"],
                "start_date": now,
                "end_date": None,
                "is_current": True
            }

            with mysql_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {warehouse_table} 
                    (passenger_id, name, gender, country, previous_country, start_date, end_date, is_current)
                    VALUES 
                    (:passenger_id, :name, :gender, :country, :previous_country, :start_date, :end_date, :is_current)
                """), insert_values)

            print("✅ SCD6 row inserted into warehouse.")

        else:
            # If no existing record, insert as new
            insert_values = {
                "passenger_id": row["passenger_id"],
                "name": row["name"],
                "gender": row["gender"],
                "country": row["country"],
                "previous_country": None,
                "start_date": now,
                "end_date": None,
                "is_current": True
            }
            with mysql_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {warehouse_table} 
                    (passenger_id, name, gender, country, previous_country, start_date, end_date, is_current)
                    VALUES 
                    (:passenger_id, :name, :gender, :country, :previous_country, :start_date, :end_date, :is_current)
                """), insert_values)

            print("✅ New record added to warehouse.")

    except Exception as e:
        print(f"❌ Error during SCD6 ETL: {e}")

# ✅ Make this callable from menu or command line
def run():
    interactive_scd6_etl()

if __name__ == "__main__":
    run()
