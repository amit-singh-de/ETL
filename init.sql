
-- Create table for production
CREATE TABLE IF NOT EXISTS customers (
    cust_id VARCHAR(20) PRIMARY KEY,
    favourite_product VARCHAR(20),
    longest_streak INT
);


-- Create table for test
CREATE TABLE IF NOT EXISTS test_customers (
    cust_id VARCHAR(20) PRIMARY KEY,
    favourite_product VARCHAR(20),
    longest_streak INT
);
