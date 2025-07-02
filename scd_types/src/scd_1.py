from config.db_connection import get_connection
from datetime import date


def scd_type_1():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        print(" Connected successfully")

        #  Apply SCD Type 1 update (overwrite existing values)
        print(" UPDATING AN EMAIL AND CITY FOR EMP_ID=1")
        cursor.execute("""
            UPDATE EMPLOYEE
            SET EMAIL = 'shivaparvathi4@gmail.com',
                CITY='banglore'
            WHERE EMP_ID = 1
        """)
        conn.commit()

        print("Update applied")


        # Fetch & Show
        cursor.execute("SELECT * FROM EMPLOYEE")
        rows = cursor.fetchall()
        print("\n employee scd type 1:")
        for row in rows:
            print(row)

    except Exception as e:
        print(" Error:", e)

    finally:
        cursor.close()
        conn.close()

# Run the function
#scd_type_1()
