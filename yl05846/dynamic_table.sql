create database dynamic;

create or replace table table_a (id INT, name VARCHAR, city VARCHAR);

create or replace table table_b (order_id INT, customer_id INT, item VARCHAR, qnty INT);


select * from customers;
select * from orders;


SELECT customers.name, customers.city, SUM(orders.qnty) AS total_quantity
FROM orders
LEFT JOIN customers ON customers.id = orders.customer_id
GROUP BY customers.name, customers.city
ORDER BY total_quantity DESC;


CREATE OR REPLACE DYNAMIC TABLE customer_orders
TARGET_LAG = '10 seconds'
WAREHOUSE = 'DYNAMIC'
AS
SELECT customers.name, customers.city, SUM(orders.qnty) AS total_quantity
FROM orders
LEFT JOIN customers ON customers.id = orders.customer_id
GROUP BY customers.name, customers.city
ORDER BY total_quantity DESC;

ALTER DYNAMIC TABLE customer_orders REFRESH;


insert into customers (id, name, city) values (300, 'Sebastian', 'Zürich');
insert into orders values (222, 222, 'TEST', 500);

SELECT * FROM customer_orders;