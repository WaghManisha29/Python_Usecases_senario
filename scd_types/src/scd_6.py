
from config.db_connection import get_connection, db_cursor
from datetime import date
import pandas as pd
from IPython.display import display

# To run:
# from scd.scd6_hybrid import create_employee_type6_table, apply_scd_type6_update
# create_employee_type6_table()
# apply_scd_type6_update()

def create_employee_type6_table():
    try:
        with db_cursor() as cursor:
            cursor.execute("IF OBJECT_ID('EMPLOYEE_TYPE6', 'U') IS NOT NULL DROP TABLE EMPLOYEE_TYPE6")

            cursor.execute("""
                CREATE TABLE EMPLOYEE_TYPE6 (
                    Record_ID INT IDENTITY(1,1) PRIMARY KEY,
                    EMP_ID INT,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),         -- SCD 1 (Overwrites)
                    CITY VARCHAR(100),           -- SCD 2 (tracks changes)
                    PREVIOUS_CITY VARCHAR(100),  -- SCD 3
                    IsActive BIT,
                    StartDate DATE,
                    EndDate DATE
                )
            """)

            cursor.execute("""
                INSERT INTO EMPLOYEE_TYPE6 (
                    EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, PREVIOUS_CITY,
                    IsActive, StartDate, EndDate
                )
                SELECT
                    EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, NULL,
                    1, GETDATE(), '2999-12-31'
                FROM Source_EMPLOYEE
            """)

        with get_connection() as conn:
            print(" EMPLOYEE_TYPE6 created and loaded with initial data.")
            df = pd.read_sql("SELECT * FROM EMPLOYEE_TYPE6", conn)
            display(df)

    except Exception as e:
        print(" Error creating EMPLOYEE_TYPE6:", e)


def apply_scd_type6_update():
    try:
        today = date.today()

        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM Source_EMPLOYEE")
            source_data = cursor.fetchall()

            for row in source_data:
                emp_id, name, dob, gender, email, new_city, _ = row

                cursor.execute("""
                    SELECT Record_ID, CITY, EMAIL, StartDate
                    FROM EMPLOYEE_TYPE6
                    WHERE EMP_ID = ? AND IsActive = 1
                """, emp_id)
                current = cursor.fetchone()

                if not current:
                    continue

                record_id, current_city, current_email, start_date = current

                city_changed = current_city.lower() != new_city.lower()
                email_changed = current_email.lower() != email.lower()

                if city_changed:
                    cursor.execute("""
                        UPDATE EMPLOYEE_TYPE6
                        SET IsActive = 0, EndDate = ?
                        WHERE Record_ID = ?
                    """, (today, record_id))

                    cursor.execute("""
                        INSERT INTO EMPLOYEE_TYPE6 (
                            EMP_ID, EMP_NAME, DOB, GENDER,
                            EMAIL, CITY, PREVIOUS_CITY,
                            IsActive, StartDate, EndDate
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, '2999-12-31')
                    """, (emp_id, name, dob, gender, email, new_city, current_city, today))

                elif email_changed:
                    cursor.execute("""
                        UPDATE EMPLOYEE_TYPE6
                        SET EMAIL = ?
                        WHERE Record_ID = ?
                    """, (email, record_id))

        with get_connection() as conn:
            print(" SCD Type 6 update completed.\n")
            df = pd.read_sql("SELECT * FROM EMPLOYEE_TYPE6", conn)
            display(df)

    except Exception as e:
        print(" Error in SCD Type 6 update:", e)
