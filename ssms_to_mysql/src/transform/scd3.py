import pandas as pd
from datetime import datetime
from sqlalchemy import text
from src.utils.db_connections import get_sqlserver_engine, get_mysql_engine

def run():
    table_name = "passenger_master"
    scd3_table = "passenger_type3"
    primary_key = "passenger_id"

    sql_engine = get_sqlserver_engine()
    mysql_engine = get_mysql_engine()

    print(f"\n🔄 Running Interactive SCD Type 3 Sync for table: {table_name}\n")

    pk_value = input(f"🔍 Enter {primary_key} to update: ").strip()

    # Fetch current data from SQL Server (source)
    try:
        query_sql = f"SELECT * FROM {table_name} WHERE {primary_key} = :pk"
        source_df = pd.read_sql(text(query_sql), sql_engine, params={"pk": pk_value})
    except Exception as e:
        print(f"❌ Error fetching data from SQL Server: {e}")
        return

    if source_df.empty:
        print("❌ No such record found in source system.")
        return

    current = source_df.iloc[0]
    print("\n📋 Current Source Record:")
    print(current.to_frame().T)

    # Get new values interactively
    updates = {}
    for col in source_df.columns:
        if col == primary_key:
            continue
        new_val = input(f"✏️ Update value for '{col}' (leave blank to keep current): ").strip()
        updates[col] = new_val if new_val else current[col]

    now = datetime.now()

    try:
        # 1. Update the source SQL Server table with new values
        update_source_sql = text(f"""
            UPDATE {table_name}
            SET name = :name,
                gender = :gender,
                country = :country
            WHERE {primary_key} = :pk
        """)
        with sql_engine.begin() as sql_conn:
            sql_conn.execute(update_source_sql, {
                "name": updates["name"],
                "gender": updates["gender"],
                "country": updates["country"],
                "pk": pk_value
            })

        # 2. Update or insert in MySQL SCD3 table
        with mysql_engine.begin() as mysql_conn:
            # Check if record exists in SCD3 table
            result = mysql_conn.execute(
                text(f"SELECT * FROM {scd3_table} WHERE {primary_key} = :pk"),
                {"pk": pk_value}
            ).fetchone()

            if result:
                # Update existing SCD3 record - set previous_country = old country value, update others
                update_sql = text(f"""
                    UPDATE {scd3_table}
                    SET name = :name,
                        gender = :gender,
                        previous_country = country,
                        country = :country,
                        updated_at = :updated_at
                    WHERE {primary_key} = :pk
                """)
                mysql_conn.execute(update_sql, {
                    "name": updates["name"],
                    "gender": updates["gender"],
                    "country": updates["country"],
                    "updated_at": now,
                    "pk": pk_value
                })
                print("✅ SCD Type 3 record updated (current + previous values tracked).")
            else:
                # Insert new record into SCD3 table, previous_country = NULL
                insert_sql = text(f"""
                    INSERT INTO {scd3_table} (
                        {primary_key}, name, gender, country,
                        previous_country, updated_at
                    ) VALUES (
                        :pk, :name, :gender, :country, NULL, :updated_at
                    )
                """)
                mysql_conn.execute(insert_sql, {
                    "pk": pk_value,
                    "name": updates["name"],
                    "gender": updates["gender"],
                    "country": updates["country"],
                    "updated_at": now
                })
                print("✅ New record inserted into Type 3 table.")
    except Exception as e:
        print(f"❌ Error during SCD Type 3 ETL: {e}")
