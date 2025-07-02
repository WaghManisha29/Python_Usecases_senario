
from config.db_connection import get_connection
from datetime import date
import pandas as pd
from IPython.display import display
from contextlib import contextmanager

@contextmanager
def db_cursor():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def setup_full_scd2_schema():
    try:
        with db_cursor() as cursor:
            print(" Connected to SQL Server")

            cursor.execute("IF OBJECT_ID('EMPLOYEE_HISTORY', 'U') IS NOT NULL DROP TABLE EMPLOYEE_HISTORY")
            cursor.execute("IF OBJECT_ID('EMPLOYEE', 'U') IS NOT NULL DROP TABLE EMPLOYEE")
            cursor.execute("IF OBJECT_ID('Source_EMPLOYEE', 'U') IS NOT NULL DROP TABLE Source_EMPLOYEE")

            cursor.execute("""
                CREATE TABLE Source_EMPLOYEE (
                    EMP_ID INT,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),
                    CITY VARCHAR(100),
                    LastModifiedDate DATE
                )
            """)

            cursor.execute("""
                INSERT INTO Source_EMPLOYEE (EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, LastModifiedDate)
                VALUES
                (1, 'SHIVA', '2002-09-29', 'MALE', 'shiva143@gmail.com', 'Hyderabad', GETDATE()),
                (2, 'PARVATHI', '2003-10-30', 'FEMALE', 'parvathi123@gmail.com', 'Bangalore', GETDATE()),
                (3, 'NARAYAN', '2003-08-25', 'MALE', 'narayan321@gmail.com', 'Chennai', GETDATE()),
                (4, 'LAXMI', '2004-12-18', 'FEMALE', 'laxmi193@gmail.com', 'Mumbai', GETDATE()),
                (5, 'BHARMA', '2008-05-14', 'MALE', 'bharma943@gmail.com', 'Pune', GETDATE()),
                (6, 'SARASWATHI', '2007-09-09', 'FEMALE', 'sarawathi671@gmail.com', 'Bangalore', GETDATE())
            """)

            cursor.execute("""
                CREATE TABLE EMPLOYEE (
                    EMP_ID INT,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),
                    CITY VARCHAR(100),
                    IsActive BIT,
                    StartDate DATE,
                    EndDate DATE
                )
            """)

            cursor.execute("""
                CREATE TABLE EMPLOYEE_HISTORY (
                    History_ID INT IDENTITY(1,1) PRIMARY KEY,
                    EMP_ID INT,
                    EMP_NAME VARCHAR(255),
                    DOB DATE,
                    GENDER VARCHAR(10),
                    EMAIL NVARCHAR(100),
                    CITY VARCHAR(100),
                    IsActive BIT,
                    StartDate DATE,
                    EndDate DATE
                )
            """)

        with get_connection() as conn:
            print(" Source_EMPLOYEE DataFrame:")
            display(pd.read_sql("SELECT * FROM Source_EMPLOYEE", conn))

            print(" EMPLOYEE DataFrame (should be empty):")
            display(pd.read_sql("SELECT * FROM EMPLOYEE", conn))

            print(" EMPLOYEE_HISTORY DataFrame (should be empty):")
            display(pd.read_sql("SELECT * FROM EMPLOYEE_HISTORY", conn))

    except Exception as e:
        print(" Error during setup:", e)

def incremental_load_scd2(last_load_date):
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM Source_EMPLOYEE
                WHERE LastModifiedDate > ?
            """, last_load_date)
            changed_records = cursor.fetchall()

            today = date.today()

            for row in changed_records:
                emp_id, name, dob, gender, email, city, _ = row

                cursor.execute("""
                    SELECT EMP_NAME, DOB, GENDER, EMAIL, CITY, StartDate
                    FROM EMPLOYEE
                    WHERE EMP_ID = ? AND IsActive = 1
                """, emp_id)
                current = cursor.fetchone()

                if current is None:
                    continue

                old_name, old_dob, old_gender, old_email, old_city, old_start = current

                if old_city.lower() != city.lower():
                    cursor.execute("""
                        INSERT INTO EMPLOYEE_HISTORY (
                            EMP_ID, EMP_NAME, DOB, GENDER, EMAIL, CITY, IsActive, StartDate, EndDate
                        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
                    """, (emp_id, old_name, old_dob, old_gender, old_email, old_city, old_start, today))

                    cursor.execute("""
                        UPDATE EMPLOYEE
                        SET EMP_NAME = ?, DOB = ?, GENDER = ?, EMAIL = ?, CITY = ?,
                            StartDate = ?, EndDate = '2999-12-31', IsActive = 1
                        WHERE EMP_ID = ?
                    """, (name, dob, gender, email, city, today, emp_id))

        with get_connection() as conn:
            print(" EMPLOYEE (updated in-place):")
            display(pd.read_sql("SELECT * FROM EMPLOYEE", conn))

            print(" EMPLOYEE_HISTORY (archived changes):")
            display(pd.read_sql("SELECT * FROM EMPLOYEE_HISTORY", conn))

    except Exception as e:
        print(" Error in incremental update:", e)
