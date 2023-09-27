CREATE OR REPLACE TABLE dim_users AS (
  SELECT
    user_id as dim_user_id
  FROM
    orders
);

CREATE OR REPLACE TABLE dim_products AS (
  SELECT
    product_id as dim_product_id,
    product_name as dim_product_name
  FROM
    products
);


CREATE OR REPLACE TABLE dim_aisles AS (
  SELECT
    aisle_id as dim_aisle_id,
    aisle as dim_aisle
  FROM
    aisles
);

CREATE OR REPLACE TABLE dim_departments AS (
  SELECT
    department_id as dim_department_id,
    department as dim_department
  FROM
    departments
);

CREATE OR REPLACE TABLE dim_orders AS (
  SELECT
    order_id as dim_order_id,
    order_number as dim_order_number,
    order_dow as dim_order_dow,
    order_hour_of_day as dim_order_hour_of_day,
    days_since_prior_order as dim_days_since_prior_order
  FROM
    orders
);

CREATE TABLE fact_order_products AS (
  SELECT
    op.order_id as dim_order_id,
    op.product_id as dim_product_id,
    o.user_id as dim_user_id,
    p.department_id as dim_department_id,
    p.aisle_id as dim_aisle_id,
    op.add_to_cart_order,
    op.reordered
  FROM
    order_products op
  JOIN
    orders o ON op.order_id = o.order_id
  JOIN
    products p ON op.product_id = p.product_id
);

-- Check tables 

select * from dim_aisles;
select * from dim_departments;
select * from dim_orders;
select * from dim_products;
select * from dim_users;