CREATE DATABASE customer360;
USE customer360;

CREATE TABLE website_staging(
customer_id INT,
name VARCHAR(50),
email VARCHAR(100),
city VARCHAR(50)
);

CREATE TABLE sales_staging(
order_id INT,
customer_id INT,
product VARCHAR(50),
amount INT
);

CREATE TABLE crm_staging(
customer_id INT,
membership VARCHAR(50)
);

LOAD DATA LOCAL INFILE 'A:Testing/customer_360_project/website_customers.csv'
INTO TABLE website_staging
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET GLOBAL local_infile = 1;

SHOW VARIABLES LIKE 'local_infile';



LOAD DATA LOCAL INFILE 'A:Testing/customer_360_project/sales_data.csv'
INTO TABLE sales_staging
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE customer_360(
customer_id INT PRIMARY KEY,
name VARCHAR(50),
email VARCHAR(100),
city VARCHAR(50),
membership VARCHAR(50),
total_spent INT
);


select * from customer_360;