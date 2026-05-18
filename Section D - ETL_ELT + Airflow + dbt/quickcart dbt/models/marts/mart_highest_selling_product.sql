{{ config(materialized='table') }}

WITH order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

revenue_category AS (
    SELECT p.category,
        p.product_name,
        SUM(oi.quantity * oi.unit_price_usd) AS total_sales  
    FROM products as p
    LEFT JOIN order_items as oi
    ON p.product_id = oi.product_id
    GROUP BY p.category, p.product_name
),

ranked_products AS (
    SELECT category,
        product_name,
        total_sales,
        RANK() OVER (PARTITION BY category 
            ORDER BY total_sales DESC) AS rank_product
    FROM revenue_category
)

SELECT category,
    product_name
FROM ranked_products
WHERE rank_product = 1