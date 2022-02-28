/***************************************** Create and Manage Tables **********************************************************/
/*************************************************************************************************************************************/
USE IT;

--DROP TABLE employee ;

------------- Create Managed Tables -----------------------------------
CREATE TABLE IF NOT EXISTS it.employees(
emp_id INT COMMENT 'Unique employee ID',
first_name STRING COMMENT 'Employee First Name',
last_name STRING COMMENT 'Employee Last Name',
department_id INT COMMENT 'Department ID',
manager_id INT COMMENT 'Manager ID',
hire_date DATE COMMENT 'Hiring date'
)
COMMENT 'Employees informations'
--LOCATION '/user/hive/warehouse/it/employees/'
TBLPROPERTIES ('created_by' = 'ahmed.ibrahem', 'created_on' = '19-9-2021');

INSERT INTO employees  values(100,'Ahmed','Ibrahem',30,NULL,'2020-01-01');
SELECT * FROM employees;

/* Note: if schema is different Hive will not notify you about it **/


CREATE TABLE departments (
`departement ID` INT COMMENT 'Departement ID',
`departement name` STRING COMMENT 'Departement Name'
)
COMMENT 'Departements Data'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
lines terminated by '\n' 
LOCATION'/inputdata/'
TBLPROPERTIES ("skip.header.line.count"="1");


SHOW CREATE TABLE departments ;

INSERT INTO departments VALUES 
('d001','Marketing'),
('d002','Finance'),
('d003','Human Resources'),
('d004','Production'),
('d005','Development'),
('d006','Quality Management'),
('d007','Sales'),
('d008','Research'),
('d009','Customer Service');

LOAD DATA INPATH '/data/department/department_data.csv'
OVERWRITE INTO TABLE departments; 

SELECT * FROM departments;

CREATE TABLE IF NOT EXISTS managers 
LIKE employees;

/* Note: you can created managed  FROM external, external from managed, even if there is no LOCATION statement */

----------------- Print Table/Column details -----------------------------------
DESCRIBE  employees;
DESCRIBE EXTENDED employees;
DESCRIBE FORMATTED employees;


------------------- Search for table in a database -----------------------------
SHOW TABLES 'empl.*';

--------------------------------------------------------------------- Create External Tables -----------------------------------------------------------------------------------
DROP TABLE tripdata;
CREATE EXTERNAL TABLE IF NOT EXISTS tripdata (
VendorID	int,
tpep_pickup_datetime	string,
tpep_dropoff_datetime	string,
passenger_count	int,
trip_distance	double,
RatecodeID	int,
store_and_fwd_flag	STRING,
PULocationID	int,
DOLocationID	int,
payment_type	int,
fare_amount	 double,
extra	double,
mta_tax double,
tip_amount	double,
tolls_amount	double,
improvement_surcharge double,
total_amount double
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
lines terminated by '\n' 
LOCATION'/inputdata/'
TBLPROPERTIES ("skip.header.line.count"="1");


SELECT * FROM tripdata LIMIT 500;
--Note: You can name the files as you want it doesn't matter here
--Note: how to deal with timestamp

--DROP TABLE hr_employees;
CREATE EXTERNAL TABLE IF NOT EXISTS hr_employees(
Name STRING COMMENT 'Employee Full name',
`Job Titles` STRING COMMENT 'Job Title',
Department	STRING COMMENT 'Departement Name',
`Full or Part-Time` STRING COMMENT 'Contract Type',
`Salary or Hourly` STRING COMMENT 	'Payroll',
`Typical Hours` INT COMMENT 'Working hours',
`Annual Salary` INT COMMENT 'Annual Salary of the employee over 12 months',
`Hourly Rate` DOUBLE COMMENT 'Hourly Rate'
)
--ROW FORMAT FIELDS TERMINATED BY ','
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     ='\"'
)
LOCATION '/data/employee/'
TBLPROPERTIES ("skip.header.line.count"="1");

SHOW CREATE TABLE hr_employees;

--** Note: default here is considering the files in hdfs:// to specify local file you have to be more specific
--** not specificying the qouted strings will lead to reading the columns in wrong way

SELECT * FROM hr_employees LIMIT 100 ;


------------- Create Similar tables -------------------------------------------------------------------------------------------------------
DROP TABLE finance_employees;

/* Like create the table without its data */
CREATE TABLE IF NOT EXISTS finance_employees
LIKE hr_employees
LOCATION '/data/finance_employees';

SHOW CREATE TABLE finance_employees

ALTER TABLE finance_employees
SET TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM finance_employees;

----------------------------------------------------------------- Change data location -------------------------------------------------
ALTER TABLE hr_employees 
SET LOCATION '/data/employee_2/';

SELECT * FROM hr_employees ;


--------------------------------------------------- Another Examples -----------------------------------------------------------------
DROP TABLE products_transactions;
CREATE EXTERNAL TABLE IF NOT EXISTS products_transactions (
OrderDate	varchar(10), 
Region	varchar(15),
Product String,
Quantity	SMALLINT, 
UnitPrice	float,
TotalPrice	float,
City	varchar(30),
Category varchar(30)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
lines terminated by '\n' 
LOCATION'/products/transactions/'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM products_transactions;

------------- Create Temporary file
CREATE TEMPORARY TABLE test_employees
LIKE hr_employees;

#################################################################################
--------------------------------------- Partitioned Table ---------------------------------------------------
#################################################################################
DROP TABLE products_transactions;
CREATE EXTERNAL TABLE IF NOT EXISTS products_transactions (
OrderDate	varchar(10), 
Region	varchar(15),
Product String,
Quantity	SMALLINT, 
UnitPrice	float,
TotalPrice	float,
City	varchar(30),
Category varchar(30)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
lines terminated by '\n' 
LOCATION'/products/transactions/'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM products_transactions;

CREATE TABLE IF NOT EXISTS STG_SALES( 
ORDERNUMBER	INTEGER,
QUANTITYORDERED INTEGER,
PRICEEACH	FLOAT,
ORDERLINENUMBER	INTEGER,
SALES	FLOAT,
ORDERDATE VARCHAR(20),
STATUS	STRING,
QTR_ID	INT,
MONTH_ID	INTEGER,
YEAR_ID	INTEGER,
PRODUCTLINE	STRING,
MSRP	INTEGER,
PRODUCTCODE	STRING,
CUSTOMERNAME	STRING,
PHONE	STRING,
ADDRESSLINE1 STRING,	
ADDRESSLINE2	STRING,
CITY	STRING,
STATE	STRING,
POSTALCODE	STRING,
COUNTRY	STRING,
TERRITORY	STRING,
CONTACTLASTNAME	STRING,
CONTACTFIRSTNAME	STRING
)
PARTITIONED BY (DEALSIZE  STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
lines terminated by '\n' 
TBLPROPERTIES ("skip.header.line.count"="1");

DESCRIBE formatted STG_SALES;

LOAD DATA INPATH '/inputdata'
--OVERWRITE
INTO TABLE STG_SALES;

SELECT * FROM STG_SALES
WHERE ORDERNUMBER=10180;

SHOW PARTITIONS STG_SALES;


SELECT COUNTRY,CITY, SUM(SALES)
FROM STG_SALES
GROUP BY COUNTRY, CITY;

SELECT DEALSIZE,SUM(SALES)
FROM STG_SALES
GROUP BY DEALSIZE;

SELECT * FROM STG_SALES WHERE DEALSIZE='Large';
SELECT * FROM STG_SALES WHERE COUNTRY='USA';

CREATE TABLE IF NOT EXISTS FINAL_SALES( 
ORDERNUMBER	INTEGER,
QUANTITYORDERED INTEGER,
PRICEEACH	FLOAT,
ORDERLINENUMBER	INTEGER,
SALES	FLOAT,
ORDERDATE VARCHAR(20),
STATUS	STRING)
PARTITIONED BY (COUNTRY  STRING, STATE STRING, CITY STRING);

SELECT DISTINCT COUNTRY, STATE, CITY FROM STG_SALES;

INSERT INTO final_sales 
SELECT ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER,SALES,ORDERDATE,STATUS,COUNTRY, STATE, CITY FROM STG_SALES;

INSERT OVERWRITE TABLE  final_sales 
SELECT ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER,SALES,ORDERDATE,STATUS,'EGYPT' AS COUNTRY, 'CAPITAL'  AS STATE, 'CAIRO' AS CITY 
FROM STG_SALES 
limit 5;

SHOW PARTITIONS FINAL_SALES;

SELECT * FROM final_sales;

--SET hive.exec.dynamic.partition=TRUE;
--SET hive.exec.dynamic.partition.mode=strict;

####################################### Custom Datatypes ########################################################

--DROP TABLE BROWSING_HISTORY;
CREATE TABLE IF NOT EXISTS BROWSING_HISTORY (
USERID INTEGER, 
NAME VARCHAR(50),
BROWSED_PRODUCTS ARRAY<String>,
DURATION ARRAY<integer>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED  BY ','
COLLECTION ITEMS TERMINATED BY '$';

SHOW CREATE TABLE BROWSING_HISTORY;

--INSERT INTO BROWSING_HISTORY VALUES(100, 'AHMED', ARRAY('IPhone13','Lenovo T380','Samsung Screen'), ARRAY(3, 2, 6));

LOAD DATA LOCAL INPATH '/home/hadoop/shopping_history.csv'
INTO TABLE BROWSING_HISTORY;

SELECT USERID, BROWSED_PRODUCTS[0] FROM BROWSING_HISTORY;

SELECT  EXPLODE(BROWSED_PRODUCTS) 
FROM BROWSING_HISTORY;

SELECT userid, products
FROM BROWSING_HISTORY
LATERAL view explode(BROWSED_PRODUCTS) P AS products;

SELECT  userid, products, screenTime
FROM BROWSING_HISTORY 
LATERAL VIEW explode(BROWSED_PRODUCTS) PTable AS products
LATERAL VIEW explode(duration) DTable AS screenTime;

--DROP TABLE products_specs;
CREATE TABLE IF NOT EXISTS products_specs (
product_ID INTEGER, 
product_name VARCHAR(100),
category ARRAY<String>,
specs MAP<string,string>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED  BY '\t'
COLLECTION ITEMS TERMINATED BY '$'
MAP keys terminated BY ':';

LOAD DATA LOCAL INPATH '/home/hadoop/products_specs.txt'
INTO TABLE products_specs;

SELECT product_id, product_name, category[0], specs["screen"], specs["size"] FROM products_specs;

SELECT explode(specs) AS (spec, value)
FROM products_specs;

SELECT product_ID, product_name, specName, specValue
FROM products_specs
LATERAL view explode(specs) specsTable AS specName, specValue;




