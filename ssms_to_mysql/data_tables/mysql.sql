use mysqlpydb;

CREATE TABLE IF NOT EXISTS passenger_master (
        passenger_id INT PRIMARY KEY,
        name VARCHAR(255),
        gender VARCHAR(10),
        country VARCHAR(100)
);
    


CREATE TABLE IF NOT EXISTS passenger_history (
        passenger_id INT,
        name VARCHAR(255),
        gender VARCHAR(10),
        country VARCHAR(100),
        start_date DATETIME,
        end_date DATETIME,
        is_current BOOLEAN,
        PRIMARY KEY (passenger_id, start_date)
);    

CREATE TABLE IF NOT EXISTS passenger_type3 (
        passenger_id INT PRIMARY KEY,
        name VARCHAR(100),
        gender CHAR(10),
        country VARCHAR(50),
        previous_country VARCHAR(50),
        updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS passenger_history_type4 (
        history_id INT AUTO_INCREMENT PRIMARY KEY,
        passenger_id INT,
        name VARCHAR(100),
        gender CHAR(10),
        country VARCHAR(50),
        changed_at DATETIME
);

CREATE TABLE IF NOT EXISTS passenger_scd6 (
        surrogate_key INT AUTO_INCREMENT PRIMARY KEY,
        passenger_id INT,
        name VARCHAR(100),
        gender CHAR(10),
        country VARCHAR(50),
        previous_country VARCHAR(50),
        start_date DATETIME,
        end_date DATETIME,
        is_current BOOLEAN
);       