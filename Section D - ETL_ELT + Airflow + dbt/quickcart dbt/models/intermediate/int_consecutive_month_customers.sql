WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

customer_months AS (
    SELECT DISTINCT
        customer_id,
        DATE_TRUNC('month', order_time) AS month_start
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
WHERE previous_month IS NOT NULL
    AND DATE_DIFF('month', previous_month, month_start) = 1
