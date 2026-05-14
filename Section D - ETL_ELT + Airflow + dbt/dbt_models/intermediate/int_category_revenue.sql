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


