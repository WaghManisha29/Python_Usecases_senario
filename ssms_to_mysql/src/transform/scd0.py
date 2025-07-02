import pandas as pd
from sqlalchemy import inspect, text
import sys
import os

# Setup import path for utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.db_connections import get_sqlserver_engine, get_mysql_engine

def run():
    table_name = "passenger_master"
    primary_key = "passenger_id"

    print(f"\n🚀 Running Interactive SCD Type 0 Sync for table: {table_name}\n")

    sql_engine = get_sqlserver_engine()
    mysql_engine = get_mysql_engine()

    pk_value = input(f"🔍 Enter {primary_key} to update: ").strip()

    # Fetch current source record (SQL Server)
    try:
        query = f"SELECT * FROM {table_name} WHERE {primary_key} = :pk"
        with sql_engine.connect() as conn:
            source_df = pd.read_sql(text(query), conn, params={"pk": pk_value})
    except Exception as e:
        print(f"❌ Error fetching data from source: {e}")
        return

    if source_df.empty:
        print("❌ No such record found in source system.")
        return

    current = source_df.iloc[0]

    # Show current record in tabular format (one row)
    print("\n📋 Current Source Record:")
    print(pd.DataFrame([current]))

    # Prompt for updates
    updates = {}
    for col in source_df.columns:
        if col == primary_key:
            continue
        val = input(f"✏️ Update value for '{col}' (leave blank to keep current): ").strip()
        updates[col] = val if val else current[col]

    # Now check MySQL for this PK
    try:
        with mysql_engine.connect() as conn:
            target_df = pd.read_sql(
                text(f"SELECT * FROM {table_name} WHERE {primary_key} = :pk"), conn, params={"pk": pk_value}
            )
    except Exception as e:
        print(f"❌ Error fetching data from target: {e}")
        return

    if target_df.empty:
        print("\nℹ️ Record not found in target MySQL table. It will be inserted.")
        # Insert new record after confirmation or just print message
        return

    target = target_df.iloc[0]

    # Check for violations (any column values different from source)
    violations = []
    for col in source_df.columns:
        if col == primary_key:
            continue
        src_val = updates[col]
        tgt_val = target[col]
        if str(src_val) != str(tgt_val):
            violations.append({
                "Column": col,
                "SourceValue": src_val,
                "TargetValue": tgt_val
            })

    # Print violation report
    if violations:
        print("\n❌ SCD 0 Violations Detected:")
        for v in violations:
            print(f" - Column: {v['Column']} | Updated: {v['SourceValue']} | Existing: {v['TargetValue']}")
    else:
        print("\n✅ No SCD 0 violations found. Data matches target.")

if __name__ == "__main__":
    run()
