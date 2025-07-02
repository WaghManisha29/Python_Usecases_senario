from config.db_connection import get_connection, db_cursor
import pandas as pd
from IPython.display import display

def create_employee_scd3():
    try:
        with db_cursor() as cursor:
            cursor.execute("IF OBJECT_ID('EMPLOYEE_SCD3', 'U') IS NOT NULL DROP TABLE EMPLOYEE_SCD3")

            cursor.execute("""
                CREATE TABLE EMPLOYEE_SCD3 (
                    EMP_ID INT PRIMARY KEY,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),
                    CITY VARCHAR(100),
                    PREVIOUS_CITY VARCHAR(100),
                    StartDate DATE DEFAULT GETDATE()
                )
            """)

            cursor.execute("""
                INSERT INTO EMPLOYEE_SCD3 (EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, PREVIOUS_CITY)
                SELECT EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, NULL
                FROM Source_EMPLOYEE
            """)

        with get_connection() as conn:
            print(" EMPLOYEE_SCD3 created with initial data (without IsActive).\n")
            df = pd.read_sql("SELECT * FROM EMPLOYEE_SCD3", conn)
            display(df)

    except Exception as e:
        print(" Error in create_employee_scd3:", e)

def apply_scd3_update(emp_id, new_city):
    try:
        with db_cursor() as cursor:
            cursor.execute("SELECT CITY FROM EMPLOYEE_SCD3 WHERE EMP_ID = ?", emp_id)
            current = cursor.fetchone()

            if not current:
                print(f" No record found for EMP_ID = {emp_id}")
                return

            current_city = current[0]

            if current_city.lower() == new_city.lower():
                print("ℹ No change in CITY. Nothing to update.")
                return

            cursor.execute("""
                UPDATE EMPLOYEE_SCD3
                SET PREVIOUS_CITY = CITY,
                    CITY = ?,
                    StartDate = GETDATE()
                WHERE EMP_ID = ?
            """, (new_city, emp_id))

        with get_connection() as conn:
            print(f" SCD Type 3 applied: CITY updated to '{new_city}', old CITY moved to PREVIOUS_CITY.\n")
            df = pd.read_sql("SELECT * FROM EMPLOYEE_SCD3 WHERE EMP_ID = ?", conn, params=(emp_id,))
            display(df)

    except Exception as e:
        print(" Error in apply_scd3_update:", e)