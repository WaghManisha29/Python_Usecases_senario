import pandas as pd
from sqlalchemy import text
from src.utils.db_connections import get_sqlserver_engine, get_mysql_engine

def run():
    table_name = "passenger_master"
    primary_key = "passenger_id"

    sql_engine = get_sqlserver_engine()
    mysql_engine = get_mysql_engine()

    print(f"\n🚀 Running Interactive SCD Type 1 Sync for table: {table_name}\n")

    pk_value = input(f"🔍 Enter {primary_key} to update: ").strip()

    query = f"SELECT * FROM {table_name} WHERE {primary_key} = :pk"

    try:
        source_df = pd.read_sql(text(query), sql_engine, params={"pk": pk_value})
        target_df = pd.read_sql(text(query), mysql_engine, params={"pk": pk_value})
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return

    if source_df.empty and target_df.empty:
        print("❌ No such record found in either database.")
        return

    print("\n📋 Current Record:")
    df = source_df if not source_df.empty else target_df
    print(df)

    updates = {}
    for col in df.columns:
        if col == primary_key:
            continue
        new_value = input(f"✏️ Update value for '{col}' (leave blank to skip): ").strip()
        if new_value != "":
            updates[col] = new_value

    if not updates:
        print("ℹ️ No changes made.")
        return

    set_clause = ", ".join([f"{col} = :{col}" for col in updates])
    update_query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = :{primary_key}"

    updates[primary_key] = pk_value  # add primary key to parameters

    try:
        with sql_engine.begin() as conn:
            conn.execute(text(update_query), updates)
        with mysql_engine.begin() as conn:
            conn.execute(text(update_query), updates)
        print("\n✅ Record successfully updated in both SQL Server and MySQL.")
    except Exception as e:
        print(f"❌ Error updating records: {e}")
