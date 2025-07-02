USE sqlpythondb;


-- Safe drop
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[passenger_master]') AND type = N'U')
BEGIN
    DROP TABLE dbo.passenger_master;
END;

-- Create table
CREATE TABLE dbo.passenger_master (
    passenger_id INT PRIMARY KEY,
    name VARCHAR(50),
    gender VARCHAR(10),
    country VARCHAR(50)
);

-- Insert sample data
INSERT INTO dbo.passenger_master (passenger_id, name, gender, country) VALUES
(1, 'Alice', 'Female', 'USA'),
(2, 'Bob', 'Male', 'Canada'),
(3, 'Charlie', 'Male', 'UK'),
(4, 'Diana', 'Female', 'India'),
(5, 'Ethan', 'Male', 'Australia');

