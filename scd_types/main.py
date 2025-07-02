

from src import (
    scd_0,
    scd_1,
    scd_2,
    scd_3,
    scd_4,
    scd_6
)


def run_scd0():
    print("\n Running SCD Type 0 (Fixed Attributes)...")
    scd_0.scd_type_0()


def run_scd1():
    print("\n Running SCD Type 1 (Overwrite)...")
    scd_1.scd_type_1()


def run_scd2():
    print("\n Running SCD Type 2 (Full History)...")
    scd_2.setup_full_scd2_schema()
    scd_2.incremental_load_scd2(last_load_date='2025-06-24')


def run_scd3():
    print("\n Running SCD Type 3 (Previous Column)...")
    scd_3.create_employee_scd3()
    scd_3.apply_scd3_update(emp_id=1, new_city="Bangalore")


def run_scd4():
    print("\n Running SCD Type 4 (Separate History Table)...")
    scd_4.create_employee_type4_tables()
    scd_4.apply_scd_type4_update()


def run_scd6():
    print("\n Running SCD Type 6 (Hybrid)...")
    scd_6.create_employee_type6_table()
    # If simulate_type6_source_change() is not defined, comment the next line:
    # scd_6.simulate_type6_source_change()
    scd_6.apply_scd_type6_update()


def main():
    print(" Starting SCD Demonstration\n" + "-" * 40)
    run_scd0()
    run_scd1()
    run_scd2()
    run_scd3()
    run_scd4()
    run_scd6()
    print("\n All SCD processes completed.")


if __name__ == '__main__':
    main()
