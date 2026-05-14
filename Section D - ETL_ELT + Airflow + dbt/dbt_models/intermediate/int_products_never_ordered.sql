WITH order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

SELECT p.product_id,
    p.name
FROM products as p
LEFT JOIN order_items as oi
ON p.product_id = oi.product_id
WHERE oi.product_id is NULL;