***********************************************************************************************************************************************************************************
--------------------------------------------------------------- Altering Tables/Columns  -------------------------------------------------------------------------
***********************************************************************************************************************************************************************************
--- Renaming Tables
ALTER TABLE orders rename TO order_transactions;

--- Changing Partition location
ALTER TABLE log_messages PARTITION(year = 2011, month = 12, day = 2)
SET LOCATION 's3n://ourbucket/logs/2011/01/02';

ALTER TABLE order_transactions 
CHANGE COLUMN order_delivered_customer_date transaction_delivered_date STRING
COMMENT 'The date where the transaction happens';
-- AFTER/FIRST <COLUMN name>

set hive.metastore.disallow.incompatible.col.type.changes=false;

ALTER TABLE order_transactions 
CHANGE COLUMN transaction_delivered_date transaction_delivered_date DATE
COMMENT 'The date where the transaction happens';

DESCRIBE FORMATTED order_transactions;


-- Adding new columns
ALTER TABLE order_transactions 
ADD COLUMNS  (DUMMY_COLUMN INT COMMENT 'Dummy Test');

SELECT * FROM order_transactions ;


---- Replacing with new columns
ALTER TABLE log_messages REPLACE COLUMNS (
hours_mins_secs INT COMMENT 'hour, minute, seconds from timestamp',
severity STRING COMMENT 'The message severity'
message STRING COMMENT 'The rest of the message');

/* Note: The REPLACE statement can only be used with tables that use one of the native SerDe
modules: DynamicSerDe or MetadataTypedColumnsetSerDe */


--- Alter table properties
ALTER TABLE order_transactions SET TBLPROPERTIES (
'project' = 'the new DWH project');

DESCRIBE formatted  order_transactions;

------------------------------------------------------------------------------------------ TO TEST ------------------------------------------------------------------------------------------------
ALTER TABLE log_messages
PARTITION(year = 2012, month = 1, day = 1)
SET FILEFORMAT SEQUENCEFILE;

ALTER TABLE table_using_JSON_storage
SET SERDE 'com.example.JSONSerDe'
WITH SERDEPROPERTIES (
'prop1' = 'value1',
'prop2' = 'value2');

ALTER TABLE table_using_JSON_storage
SET SERDEPROPERTIES (
'prop3' = 'value3',
'prop4' = 'value4');

ALTER TABLE stocks
CLUSTERED BY (exchange, symbol)
SORTED BY (symbol)
INTO 48 BUCKETS;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------








