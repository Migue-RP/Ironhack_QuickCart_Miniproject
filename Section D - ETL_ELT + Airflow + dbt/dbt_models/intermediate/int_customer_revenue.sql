WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
)

SELECT customer_id,
    SUM(total_usd) AS total_revenue
FROM orders
GROUP BY customer_id
ORDER BY SUM(total_usd) DESC;