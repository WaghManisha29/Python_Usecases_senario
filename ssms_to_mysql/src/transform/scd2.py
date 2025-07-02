import pandas as pd
from datetime import datetime
from sqlalchemy import text
from src.utils.db_connections import get_sqlserver_engine, get_mysql_engine

def run():
    table_name = "passenger_master"
    history_table = "passenger_history"
    primary_key = "passenger_id"

    sql_engine = get_sqlserver_engine()
    mysql_engine = get_mysql_engine()

    print(f"\n🔄 Starting Interactive SCD Type 2 Sync for table: {table_name}\n")

    pk_value = input(f"🔍 Enter {primary_key} to update: ").strip()

    # Fetch current source record (SQL Server)
    query = f"SELECT * FROM {table_name} WHERE {primary_key} = :pk"
    try:
        source_df = pd.read_sql(text(query), sql_engine, params={"pk": pk_value})
    except Exception as e:
        print(f"❌ Error fetching data from source: {e}")
        return

    if source_df.empty:
        print("❌ No such record found in source system.")
        return

    current = source_df.iloc[0]
    print("\n📋 Current Source Record:")
    print(current.to_frame().T)

    # Collect updated values interactively
    updates = {}
    for col in source_df.columns:
        if col == primary_key:
            continue
        val = input(f"✏️ Update value for '{col}' (leave blank to keep current): ").strip()
        updates[col] = val if val else current[col]

    now = datetime.now()

    try:
        # Update source system (SQL Server)
        update_cols = ", ".join([f"{col} = :{col}" for col in updates])
        update_sql = text(f"""
            UPDATE {table_name}
            SET {update_cols}
            WHERE {primary_key} = :pk
        """)
        params = {**updates, "pk": pk_value}
        with sql_engine.connect() as conn:
            result = conn.execute(update_sql, params)
            conn.commit()
            print(f"✅ Source table '{table_name}' updated in SQL Server.")
            print(f"Rows affected: {result.rowcount}")

        # Update SCD Type 2 history table in MySQL
        with mysql_engine.begin() as conn:
            # Expire current version
            expire_sql = text(f"""
                UPDATE {history_table}
                SET end_date = :end_date, is_current = 0
                WHERE {primary_key} = :pk AND is_current = 1
            """)
            conn.execute(expire_sql, {
                "end_date": now,
                "pk": pk_value
            })

            # Insert new version with new start_date and NULL end_date
            insert_sql = text(f"""
                INSERT INTO {history_table} (
                    {primary_key}, name, gender, country,
                    start_date, end_date, is_current
                )
                VALUES (
                    :passenger_id, :name, :gender, :country,
                    :start_date, NULL, :is_current
                )
            """)
            conn.execute(insert_sql, {
                "passenger_id": int(pk_value),
                "name": updates["name"],
                "gender": updates["gender"],
                "country": updates["country"],
                "start_date": now,
                "is_current": 1
            })

        print("\n✅ SCD Type 2 update complete: source updated and history recorded in MySQL.")

    except Exception as e:
        print(f"❌ Error during SCD Type 2 update: {e}")

