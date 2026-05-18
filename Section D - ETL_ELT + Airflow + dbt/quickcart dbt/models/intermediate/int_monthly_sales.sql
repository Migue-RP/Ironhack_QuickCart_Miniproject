WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

monthly_sales AS (
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
            LAG(total_sales) OVER (ORDER BY sales_year, sales_month)), 2
    ) AS sales_growth_percentage

FROM monthly_sales
ORDER BY sales_year, sales_month