from config.db_connection import get_connection
from datetime import date

def scd_type_0():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        print(" Connected successfully")

        # Drop table and trigger if they exist
        cursor.execute("IF OBJECT_ID('EMPLOYEE', 'U') IS NOT NULL DROP TABLE EMPLOYEE")
        cursor.execute("IF OBJECT_ID('TR_EMPLOYEE_SCD0_PROTECT_DOB', 'TR') IS NOT NULL DROP TRIGGER TR_EMPLOYEE_SCD0_PROTECT_DOB")

        # Create EMPLOYEE table
        cursor.execute("""
            CREATE TABLE EMPLOYEE (
                EMP_ID INT PRIMARY KEY,
                EMP_NAME VARCHAR(255),
                DOB DATE,
                EMAIL NVARCHAR(100),
                CITY VARCHAR(100)
            )
        """)

        # Insert data
        cursor.execute("""
            INSERT INTO EMPLOYEE (EMP_ID, EMP_NAME, DOB, EMAIL, CITY) VALUES
            (1, 'SHIVA', '2002-09-29', 'shiva143@gmail.com', 'hyderabad'),
            (2, 'PARVATHI', '2003-10-30', 'parvathi123@gmail.com', 'banglore'),
            (3, 'NARAYAN', '2003-08-25', 'narayan321@gmail.com', 'chennai'),
            (4, 'LAXMI', '2004-12-18', 'laxmi193@gmail.com', 'mumbai'),
            (5, 'BHARMA', '2008-05-14', 'bharma943@gmail.com', 'pune'),
            (6, 'SARASWATHI', '2007-09-09', 'sarawathi671@gmail.com', 'banglore')
        """)
        conn.commit()
        print(" Inserted initial records")

        #  Add SCD Type 0 TRIGGER to block DOB updates
        cursor.execute("""
            CREATE TRIGGER TR_EMPLOYEE_SCD0_PROTECT_DOB
            ON EMPLOYEE
            AFTER UPDATE
            AS
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM inserted i
                    JOIN deleted d ON i.EMP_ID = d.EMP_ID
                    WHERE i.DOB <> d.DOB
                )
                BEGIN
                    RAISERROR ('DOB changes are not allowed (SCD Type 0)', 16, 1)
                    ROLLBACK TRANSACTION
                END
            END
        """)
        conn.commit()
        print(" Trigger created to block DOB updates (SCD Type 0)")

        #  Attempt to update DOB (should fail)
        print(" Attempting to update DOB — should be blocked")
        try:
            cursor.execute("""
                UPDATE EMPLOYEE
                SET DOB = '1992-02-02', EMAIL = 'shiva@gmail.com'
                WHERE EMP_ID = 1
            """)
            conn.commit()
        except Exception as e:
            print(" Trigger blocked update:", e)

        #  View current data
        cursor.execute("SELECT * FROM EMPLOYEE")
        rows = cursor.fetchall()
        print("\n Current EMPLOYEE Table:")
        for row in rows:
            print(row)

    except Exception as e:
        print(" Error:", e)

    finally:
        cursor.close()
        conn.close()

#  Run the function
#scd_type_0()
