--- Explore the data ---------------------
SELECT *
FROM departments;

SELECT *
FROM hr_employees LIMIT
10;

SELECT *
FROM tripdata LIMIT
100;

SELECT *
FROM products_transactions
;

---------- Analyze the data --------------------------------------------------------
SELECT e.name, e.`full or part-time` AS contract_type , e.`annual salary` AS annual_salary
FROM hr_employees e 
WHERE e.`annual salary`  > 90000;

-----
SELECT name, lower(department)
FROM hr_employees
WHERE NAME LIKE '__A%';
----
SELECT DISTINCT product
FROM products_transactions
;
----
SELECT name, CASE when upper(`full or part-time`)='F' THEN 'Full_Time' 
                                WHEN upper(`full or part-time`)='P' THEN 'part_time'
                                ELSE 'na' END AS contract_type
FROM hr_employees;
----
SELECT name, `job titles`, CASE WHEN lower
(`salary or hourly`)='salary' THEN `annual salary`/30 
                                                WHEN  lower
(`salary or hourly`)='hourly' THEN `typical hours` * 4 * `hourly rate`  
                                                ELSE 0
END AS monthly_salary
FROM hr_employees
WHERE department='TRANSPORTN';
----
SELECT *
FROM tripdata
WHERE trip_distance > 5 AND passenger_count > 4 AND total_amount > 40;
---
SELECT x.vendorid, MAX(x.trip_distance), sum(x.passenger_count)
FROM (SELECT *
  FROM tripdata
  WHERE trip_distance > 5 AND passenger_count > 4 AND total_amount > 40) AS x
GROUP BY x.vendorid;

FROM
(SELECT *
FROM tripdata
WHERE trip_distance > 5 AND passenger_count > 4 AND total_amount > 40) x
SELECT x.vendorid, MAX(x.trip_distance), sum(x.passenger_count)
GROUP BY x.vendorid
----
SELECT AVG(`annual salary`) AS avgerage_salary, department
FROM hr_employees
GROUP BY department;

SELECT city, QUANTITYORDERED, ordernumber, rank() over(PARTITION BY City ORDER BY sales desc) AS rnk
FROM final_sales;

SHOW partitions products;

SELECT *
FROM products
sort BY totalprice desc ;



