CREATE DATABASE demo;
USE demo;

CREATE TABLE customers(
customer_id INT AUTO_INCREMENT PRIMARY KEY,
name VARCHAR(50) Not Null,
email VARCHAR(50),
city VARCHAR(50),
country VARCHAR(50),
created_on DATE
);

CREATE TABLE products(
    pid INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    brand VARCHAR(50),
    base_price FLOAT,
    discount_pct FLOAT,
    current_stock INT,
    supplier VARCHAR(100),
    created_at DATE
);

CREATE TABLE orders(
 	oid INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_status VARCHAR(15),
    payment_method VARCHAR(15),
    shipping_address TEXT,
    total_amount float,
    discount_applied float,
   final_amount float
);

CREATE TABLE order_items (
	order_item_id INT auto_increment PRIMARY KEY,
	order_id INT,
	product_id INT,
	quantity INT,
	unit_price_at_order DECIMAL(10,2),
	discount_at_order DECIMAL(5,2) DEFAULT 0.00,
	line_total DECIMAL(10,2)
);

CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT,
    warehouse_location VARCHAR(50),
    quantity_on_hand INT DEFAULT 0,
    reorder_threshold INT DEFAULT 10,
    last_restocked_at DATE
);

CREATE TABLE order_status_log (
    log_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_at DATETIME,
    changed_by VARCHAR(20),
    remarks VARCHAR(255)
);