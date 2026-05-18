{{ config(
    materialized='table'
) }}


WITH customer_revenue AS (
    SELECT *
    FROM {{ ref('int_customer_revenue') }}
)
SELECT customer_id,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS customer_rank
FROM customer_revenue

