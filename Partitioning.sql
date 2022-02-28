***********************************************************************************************************************************************************************************
--------------------------------------------------------------- Partitioning in Managed Tables -------------------------------------------------------------------------
***********************************************************************************************************************************************************************************
DROP TABLE IF EXISTS products;
CREATE TABLE IF NOT EXISTS products (
OrderDate STRING,
Region STRING,
Product STRING,
Quantity INT,
UnitPrice DOUBLE,
TotalPrice DOUBLE
)
PARTITIONED BY (City STRING, Category STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/inputdata/products' INTO TABLE products;

SELECT * FROM products;

------------ Restrict the access without partitioning columns ------------------
set hive.mapred.mode=nonstrict;

SELECT * FROM products
WHERE city='Boston'  -- AND category='Crackers'

--- Show partitioning informations
DESCRIBE FORMATTED products;

SHOW partitions products;
SHOW PARTITIONS products PARTITION(City='New York'); 

----------- Load external data -----------------------------------------------------

LOAD DATA LOCAL INPATH '/home/hadoop/products_transaction_data.txt'
INTO TABLE products
PARTITION(City='New York',Category='Cookies');


***********************************************************************************************************************************************************************************
--------------------------------------------------------------- Partitioning in External Tables -------------------------------------------------------------------------
***********************************************************************************************************************************************************************************

--- data not ordered --> approved, cancelled,unavailable,created
--- data ordered --> processing, delivered, invoiced


CREATE EXTERNAL TABLE IF NOT EXISTS orders(
customer_id	STRING,
order_purchase_timestamp	STRING,
order_approved_at	STRING,
order_delivered_carrier_date	STRING,
order_delivered_customer_date	STRING,
order_estimated_delivery_date STRING
) 
PARTITIONED BY (order_status STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");


SELECT * FROM orders;

SHOW PARTITIONS orders;

ALTER TABLE orders ADD PARTITION (order_status='processing')
LOCATION 'file:///home/hadoop/orders_data/processing';

SHOW PARTITIONS orders;
DESCRIBE  EXTENDED orders PARTITION(order_status='processing');

CREATE EXTERNAL TABLE IF NOT EXISTS staging_orders(
customer_id	STRING,
order_status STRING,
order_purchase_timestamp	STRING,
order_approved_at	STRING,
order_delivered_carrier_date	STRING,
order_delivered_customer_date	STRING,
order_estimated_delivery_date STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD  DATA LOCAL INPATH '/home/hadoop/orders_data/created'
INTO TABLE staging_orders;

SELECT * FROM staging_orders;


INSERT INTO TABLE orders PARTITION(order_status='created')
SELECT customer_id, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date FROM staging_orders;

ALTER TABLE orders
DROP  IF EXISTS PARTITION (order_status='created');

/* In case of loading partitions manually, use the repair command to make Hive metastore aware of the newly added partitions */
MSCK REPAIR TABLE orders ;



