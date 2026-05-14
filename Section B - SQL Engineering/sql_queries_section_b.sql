-- Q1. Intermediate SQL Queries

-- 1.Find top 10 customers by revenue.

SELECT customer_id,
    SUM(total_usd) AS total_revenue
FROM orders
GROUP BY customer_id
ORDER BY SUM(total_usd) DESC
LIMIT 10;

-- 2. Find month-over-month sales growth.

WITH monthly_sales AS (
    SELECT
        YEAR(order_time) AS sales_year,
        MONTH(order_time) AS sales_month,
        SUM(total_usd) AS total_sales
    FROM orders
    GROUP BY YEAR(order_time), MONTH(order_time)
)

SELECT
    sales_year,
    sales_month,
    total_sales,
    LAG(total_sales) OVER (
        ORDER BY sales_year, sales_month
    ) AS previous_month_sales,
    ROUND(((total_sales - LAG(total_sales) OVER (
            ORDER BY sales_year, sales_month))* 100.0/
            LAG(total_sales) OVER (ORDER BY sales_year, sales_month)),2
    ) AS sales_growth_percentage

FROM monthly_sales
ORDER BY sales_year, sales_month;

-- 3. Find customers who ordered in consecutive months.

WITH customer_months AS (
    SELECT DISTINCT
        customer_id,
        YEAR(order_time) AS order_year,
        MONTH(order_time) AS order_month,
        DATE_FORMAT(order_time, '%Y-%m-01') AS month_start
    FROM orders
),

ranked_months AS (
    SELECT
        customer_id,
        month_start,
        LAG(month_start) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
        ) AS previous_month
    FROM customer_months
)

SELECT DISTINCT
    customer_id
FROM ranked_months
WHERE TIMESTAMPDIFF(MONTH, previous_month,month_start) = 1;

-- 4. Find products never ordered.

SELECT p.product_id,
    p.name
FROM products as p
LEFT JOIN order_items as oi
ON p.product_id = oi.product_id
WHERE oi.product_id is NULL;

-- 5. Find revenue contribution percentage by category.

WITH revenue_category AS (
    SELECT p.category,
        SUM(oi.quantity * oi.unit_price_usd) AS total_sales  
    FROM products as p
    LEFT JOIN order_items as oi
    ON p.product_id = oi.product_id
    GROUP BY p.category
)

SELECT category,
    total_sales,
    ROUND (total_sales*100 / SUM(total_sales) OVER (), 2) AS revenue_contribution
FROM revenue_category
ORDER BY total_sales DESC;

-- Q2. Advanced SQL

--1. Rank customers based on total revenue.

WITH customer_revenue AS (
    SELECT customer_id,
        SUM(total_usd) AS total_revenue
    FROM orders
    GROUP BY customer_id
)

SELECT customer_id,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS customer_rank
FROM customer_revenue;

--2. Find running total sales by month.

WITH monthly_sales AS (
    SELECT
        YEAR(order_time) AS sales_year,
        MONTH(order_time) AS sales_month,
        SUM(total_usd) AS total_sales
    FROM orders
    GROUP BY YEAR(order_time), MONTH(order_time)
)

SELECT
    sales_year,
    sales_month,
    total_sales,
    SUM(total_sales) OVER (ORDER BY sales_year, sales_month) AS running_total_sales_month

FROM monthly_sales
ORDER BY sales_year, sales_month;

--3. Find highest selling product per category.

WITH revenue_category AS (
    SELECT p.category,
        p.name,
        SUM(oi.quantity * oi.unit_price_usd) AS total_sales  
    FROM products as p
    LEFT JOIN order_items as oi
    ON p.product_id = oi.product_id
    GROUP BY p.category, p.name
),

ranked_products AS (
    SELECT category,
        name,
        total_sales,
        RANK() OVER (PARTITION BY category 
            ORDER BY total_sales DESC) AS rank_product
    FROM revenue_category
)

SELECT category,
    name
FROM ranked_products
WHERE rank_product = 1;


--4. Find 7-day rolling average sales.

WITH daily_sales AS (
    SELECT
        DATE(order_time) AS sales_date,
        SUM(total_usd) AS daily_revenue
    FROM orders
    GROUP BY DATE(order_time)
)

SELECT
    sales_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM daily_sales
ORDER BY sales_date;

/* Q3. SQL Optimization Scenario

The following query takes 18 minutes:

SELECT c.name, 
SUM(o.total_usd) 
FROM orders o 
JOIN customers c 
ON o.customer_id = c.customer_id
WHERE order_time >= '2025-01-01' 
GROUP BY c.name
Tasks

1. Identify possible bottlenecks.

    If there is no index in order_time, the database scans every single row to find the records for 2025-01-01.
    Without an index in the join key, each row in orders searches customers.
    Grouping on customer_name (string) is slower than grouping by customer_id (integer). 
    If one customer has thousands of orders, the Group By operation becomes a bottleneck.

2. Suggest indexes.

    orders (order_time): Allows the database to quickly jump to the 2025 records.
    orders (customer_id): Speeds up the join to the customers table.

3. Explain execution plan analysis.


4. Suggest partitioning improvements.

    Since the query filters by order_time we could implement range partitioning, 
    splitting the orders table into monthly or yearly partitions.

5. Explain materialized views usefulness.

    A Materialized View is like a saved query result that is physically stored on disk. 
    Improves resources performance, queries are faster, no need to recalculate, ideal for reporting and dashboards


Q4. Database Design

Design schema for:

1. Shipment tracking

    shipment_id (PK): iinteger, unique, not null
    order_id (FK): Links to orders.order_id.
    tracking_number: Unique, varchar(20) from the carrier. Index
    status: varchar(50) (deliverd, in transit, out for delivery). Index
    location: varchar(100)
    delivery_date: Timestamp.

2. Product inventory

    inventory_id (PK): integer, unique, not null
    product_id (FK): Links to products.product_id. Index
    stock: integer, current available stock. Index
    quantity_reserved: integer (stock currently in pending orders).
    warehouse: varchar(100)

3. Customer support tickets

    ticket_id (PK): integer, unique, not null
    customer_id (FK): Links to customers.customer_id.
    order_id (FK): Optional link to a specific order issue.
    agent_id: ID of the support staff member.
    ticket_category: varchar(100). 
    status: varchar(50) (Open, Pending, Resolved, Closed). Index
    priority: varchar(50). Index
    created_at: Timestamp.

Requirements:

• normalized design
• primary keys
• foreign keys
• indexing recommendations */
