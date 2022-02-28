/***************************************** Create and Manage Databases **********************************************************/
/*************************************************************************************************************************************/

-- Show list of available databases 
SHOW DATABASES;

-- Create Databases

CREATE DATABASE HR;
CREATE DATABASE Finance;

CREATE DATABASE APPS 
WITH DBPROPERTIES ('environment'='SIT', 'team'='Applications_Development');

CREATE DATABASE IF NOT EXISTS HR;

-- Search for databases
SHOW databases LIKE 'h.*';

-- Create database with location
CREATE DATABASE IF NOT EXISTS IT 
LOCATION '/user/hive/warehouse/it_database';

-- Print database details
DESCRIBE DATABASE IT;

DESCRIBE DATABASE EXTENDED APPS;

-- Switch to Database
USE IT;

-- Drop Database
DROP DATABASE IF EXISTS it;

--- Drop Database with tables
USE IT;

CREATE TABLE employee (ID int, name STRING, department_id int);

DROP DATABASE IT;

DROP DATABASE IF EXISTS IT CASCADE;

-- ALter databases
ALTER DATABASE financials SET DBPROPERTIES ('edited-by' = 'Joe Dba');