
from config.db_connection import get_connection, db_cursor
from datetime import date
import pandas as pd
from IPython.display import display

def create_employee_type4_tables():
    try:
        with db_cursor() as cursor:
            cursor.execute("IF OBJECT_ID('EMPLOYEE_TYPE4_HISTORY', 'U') IS NOT NULL DROP TABLE EMPLOYEE_TYPE4_HISTORY")
            cursor.execute("IF OBJECT_ID('EMPLOYEE_TYPE4', 'U') IS NOT NULL DROP TABLE EMPLOYEE_TYPE4")

            cursor.execute("""
                CREATE TABLE EMPLOYEE_TYPE4 (
                    EMP_ID INT PRIMARY KEY,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),
                    CITY VARCHAR(100)
                )
            """)

            cursor.execute("""
                CREATE TABLE EMPLOYEE_TYPE4_HISTORY (
                    History_ID INT IDENTITY(1,1) PRIMARY KEY,
                    EMP_ID INT,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),
                    CITY VARCHAR(100),
                    ChangeDate DATE
                )
            """)

            cursor.execute("""
                INSERT INTO EMPLOYEE_TYPE4
                SELECT EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY
                FROM Source_EMPLOYEE
            """)

        with get_connection() as conn:
            print(" EMPLOYEE_TYPE4 and EMPLOYEE_TYPE4_HISTORY created and loaded.\n")
            df = pd.read_sql("SELECT * FROM EMPLOYEE_TYPE4", conn)
            display(df)

    except Exception as e:
        print(" Error creating SCD Type 4 tables:", e)

def apply_scd_type4_update():
    try:
        today = date.today()
        updates = 0

        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM Source_EMPLOYEE")
            source_data = cursor.fetchall()

            for row in source_data:
                emp_id, name, dob, gender, email, city, _ = row

                cursor.execute("SELECT * FROM EMPLOYEE_TYPE4 WHERE EMP_ID = ?", emp_id)
                current = cursor.fetchone()

                if not current:
                    continue

                _, _, _, _, curr_email, curr_city = current

                if curr_email.lower() != email.lower() or curr_city.lower() != city.lower():
                    cursor.execute("""
                        INSERT INTO EMPLOYEE_TYPE4_HISTORY (EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, ChangeDate)
                        SELECT EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, ?
                        FROM EMPLOYEE_TYPE4 WHERE EMP_ID = ?
                    """, (today, emp_id))

                    cursor.execute("""
                        UPDATE EMPLOYEE_TYPE4
                        SET EMP_NAME = ?, DOB = ?, GENDER = ?, EMAIL = ?, CITY = ?
                        WHERE EMP_ID = ?
                    """, (name, dob, gender, email, city, emp_id))

                    updates += 1

        with get_connection() as conn:
            print(f" SCD Type 4 applied. {updates} record(s) updated.\n")
            print(" EMPLOYEE_TYPE4 (Current Data):")
            display(pd.read_sql("SELECT * FROM EMPLOYEE_TYPE4", conn))

            print(" EMPLOYEE_TYPE4_HISTORY (Archived Changes):")
            display(pd.read_sql("SELECT * FROM EMPLOYEE_TYPE4_HISTORY", conn))

    except Exception as e:
        print(" Error in SCD Type 4 update:", e)
