'''
in sql server first
CREATE TABLE Resumes (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    experience NVARCHAR(100),
    summary NVARCHAR(MAX),
    filename NVARCHAR(255),
    created_at DATETIME DEFAULT GETDATE()
);

'''


import configparser
import pyodbc

# Load DB config
config = configparser.ConfigParser()
config.read('config/config.ini')

driver = config['SQLSERVER']['driver']
server = config['SQLSERVER']['server']
database = config['SQLSERVER']['database']
username = config['SQLSERVER']['username']
password = config['SQLSERVER']['password']

# SQL connection string
conn_str = f"""
    DRIVER={driver};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""

def insert_resume_to_db(data, filename):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        sql = """
        INSERT INTO Resumes (name, email, phone, experience, summary, filename)
        VALUES (?, ?, ?, ?, ?, ?)
        """

        values = (
            data.get("name"),
            data.get("email"),
            data.get("phone"),
            data.get("experience"),
            data.get("summary"),
            filename
        )

        cursor.execute(sql, values)
        conn.commit()
        print(f"✅ Inserted resume: {filename}")

    except Exception as e:
        print(f"❌ Error inserting {filename}: {e}")

    finally:
        if 'conn' in locals():
            conn.close()

# For testing
if __name__ == "__main__":
    test_data = {
        "name": "John Doe",
        "email": "john@example.com",
        "phone": "9876543210",
        "experience": "5 years",
        "summary": "Experienced developer in Python and SQL."
    }
    insert_resume_to_db(test_data, "john_resume.pdf")
