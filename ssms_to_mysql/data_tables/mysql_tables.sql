-- SCD Type 1: Overwrite
CREATE TABLE IF NOT EXISTS passenger_scd1 (
    passenger_id INT PRIMARY KEY,
    name VARCHAR(100),
    gender VARCHAR(10),
    country VARCHAR(100)
);

-- SCD Type 2: Full history with active flag
CREATE TABLE IF NOT EXISTS passenger_scd2 (
    scd_id INT AUTO_INCREMENT PRIMARY KEY,
    passenger_id INT,
    name VARCHAR(100),
    gender VARCHAR(10),
    country VARCHAR(100),
    start_date DATETIME,
    end_date DATETIME,
    is_current BOOLEAN
);

-- SCD Type 3: Previous value tracking
CREATE TABLE IF NOT EXISTS passenger_scd3 (
    passenger_id INT PRIMARY KEY,
    name VARCHAR(100),
    gender VARCHAR(10),
    previous_gender VARCHAR(10),
    country VARCHAR(100),
    previous_country VARCHAR(100)
);

-- SCD Type 4: Separate history table
CREATE TABLE IF NOT EXISTS passenger_scd4 (
    history_id INT AUTO_INCREMENT PRIMARY KEY,
    passenger_id INT,
    name VARCHAR(100),
    gender VARCHAR(10),
    country VARCHAR(100),
    changed_on DATETIME
);

-- SCD Type 6: Hybrid
CREATE TABLE IF NOT EXISTS passenger_scd6 (
    scd_id INT AUTO_INCREMENT PRIMARY KEY,
    passenger_id INT,
    name VARCHAR(100),
    gender VARCHAR(10),
    previous_gender VARCHAR(10),
    country VARCHAR(100),
    previous_country VARCHAR(100),
    start_date DATETIME,
    end_date DATETIME,
    is_current BOOLEAN
);
