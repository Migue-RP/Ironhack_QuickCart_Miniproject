{{ config(
    materialized='incremental',
    unique_key='sales_date'
) }}

WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

daily_sales AS (
    SELECT DATE(order_time) AS sales_date,
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


{% if is_incremental() %}

-- recompute last 7 days to keep rolling window correct
WHERE sales_date >= (
    SELECT DATEADD(day, -7, MAX(sales_date))
    FROM {{ this }}
)

{% endif %}