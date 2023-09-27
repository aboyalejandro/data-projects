-- Table creation 

-- Source: https://www.kaggle.com/competitions/instacart-market-basket-analysis/data?select=orders.csv.zip

-- Aisles

create table aisles (
        aisle_id INTEGER PRIMARY KEY,
        aisle VARCHAR
);

COPY INTO aisles (aisle_id, aisle)
FROM @my_stage/aisles.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');

-- Departments 

CREATE TABLE departments (
        department_id INTEGER PRIMARY KEY,
        department VARCHAR
    );

COPY INTO departments (department_id, department)
FROM @my_stage/departments.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');

-- Products 

CREATE OR REPLACE TABLE products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR,
        aisle_id INTEGER,
        department_id INTEGER
    );

COPY INTO products (product_id, product_name, aisle_id, department_id)
FROM @my_stage/products.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');

-- Order products 

CREATE OR REPLACE TABLE order_products (
        order_id INTEGER,
        product_id INTEGER,
        add_to_cart_order INTEGER,
        reordered INTEGER,
        PRIMARY KEY (order_id, product_id)
    );
    
COPY INTO order_products (order_id, product_id, add_to_cart_order, reordered)
FROM @my_stage/order_products.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');

-- Orders 

CREATE OR REPLACE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        eval_set STRING,
        order_number INTEGER,
        order_dow INTEGER,
        order_hour_of_day INTEGER,
        days_since_prior_order INTEGER
    );

COPY INTO orders (order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order)
FROM @my_stage/orders.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');