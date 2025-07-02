import importlib
import os
import sys

# Add the project root (parent of 'src') to sys.path so 'src' can be imported properly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.extract import init_tables

scd_modules = {
    "0": "transform.scd0",
    "1": "transform.scd1",
    "2": "transform.scd2",
    "3": "transform.scd3",
    "4": "transform.scd4",
    "6": "transform.scd6"
}

def main():
    print("\n🚀 Initializing SQL Server with sample data...")
    init_tables.run()

    while True:
        print("\n🔧 Choose SCD Type to Transform:")
        print("0 - Type 0 (Static Snapshot)")
        print("1 - Type 1 (Overwrite)")
        print("2 - Type 2 (Track history)")
        print("3 - Type 3 (Previous value)")
        print("4 - Type 4 (History table)")
        print("6 - Type 6 (Hybrid)")
        print("X - Exit")

        choice = input("\n👉 Enter your choice: ").strip().upper()

        if choice == "X":
            print("👋 ETL Process completed.")
            break

        module_name = scd_modules.get(choice)
        if not module_name:
            print("❌ Invalid choice.")
            continue

        try:
            module = importlib.import_module(f"src.{module_name}")
            module.run()
        except Exception as e:
            print(f"❌ Failed to run SCD module: {e}")

if __name__ == "__main__":
    main()
