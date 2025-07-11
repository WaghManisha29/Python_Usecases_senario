import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};"
        f"PWD={os.getenv('SQL_PASSWORD')}"
    )
    return pyodbc.connect(conn_str)

def create_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Email_Communications'
    )
    CREATE TABLE Email_Communications (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        sender_name NVARCHAR(500),
        receiver_name NVARCHAR(500),
        cc NVARCHAR(500),
        subject NVARCHAR(MAX),
        body NVARCHAR(MAX),
        attachment_1_url NVARCHAR(MAX),
        attachment_2_url NVARCHAR(MAX),
        received_date DATETIME
    )
    """)
    conn.commit()
    conn.close()

def insert_email_data(email):
    create_table()
    conn = get_connection()
    cursor = conn.cursor()

    sender = email.get("Sender", "")
    receiver = email.get("Receiver", "")
    cc = email.get("CC", "")
    subject = email.get("Subject", "")
    body = email.get("Body", "")
    received = email.get("ReceivedDate")

    attachment_1_url = email.get("attachment_1_url", None)
    attachment_2_url = email.get("attachment_2_url", None)

    cursor.execute("""
        INSERT INTO Email_Communications (
            sender_name, receiver_name, cc, subject, body,
            attachment_1_url, attachment_2_url, received_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        sender,
        receiver,
        cc,
        subject,
        body,
        attachment_1_url,
        attachment_2_url,
        received
    ))

    conn.commit()
    conn.close()
