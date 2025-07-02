from config.db_connection import get_connection

def simulate_source_change():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Change CITY for EMP_ID = 1 to trigger SCD Type 2
        cursor.execute("""
            UPDATE Source_EMPLOYEE
            SET CITY = 'banglore', LastModifiedDate = '2025-06-24'
            WHERE EMP_ID = 1
        """)

        conn.commit()
        print("🔁 Simulated source change: EMP_ID = 1, CITY changed to 'Pune'.")

    except Exception as e:
        print("❌ Error during simulation:", e)
    finally:
        cursor.close()
        conn.close()
