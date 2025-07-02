from config import get_mysql_connection, get_sqlserver_connection

def load_to_mysql(projects):
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS projects (
        project_id VARCHAR(20),
        project_name VARCHAR(255),
        client VARCHAR(255),
        domain VARCHAR(100),
        location VARCHAR(100),
        technologies TEXT,
        project_manager VARCHAR(100),
        start_date DATE,
        end_date DATE,
        status VARCHAR(50)
    )""")

    for p in projects:
        cursor.execute("""INSERT INTO projects VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
            p.get("project_id"),
            p.get("project_name"),
            p.get("client"),
            p.get("domain"),
            p.get("location"),
            ", ".join(p.get("technologies", [])),
            p.get("project_manager"),
            p.get("start_date"),
            p.get("end_date"),
            p.get("status")
        ))

    conn.commit()
    conn.close()
    print(" MySQL load complete.")

def load_to_sqlserver(projects):
    conn = get_sqlserver_connection()
    cursor = conn.cursor()

    cursor.execute("""IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='projects' AND xtype='U')
    CREATE TABLE projects (
        project_id VARCHAR(20),
        project_name VARCHAR(255),
        client VARCHAR(255),
        domain VARCHAR(100),
        location VARCHAR(100),
        technologies TEXT,
        project_manager VARCHAR(100),
        start_date DATE,
        end_date DATE,
        status VARCHAR(50)
    )""")

    for p in projects:
        cursor.execute("""INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
            p.get("project_id"),
            p.get("project_name"),
            p.get("client"),
            p.get("domain"),
            p.get("location"),
            ", ".join(p.get("technologies", [])),
            p.get("project_manager"),
            p.get("start_date"),
            p.get("end_date"),
            p.get("status")
        ))

    conn.commit()
    conn.close()
    print(" SQL Server load complete.")
